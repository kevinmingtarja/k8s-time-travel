// Package e2e runs the built ktt binary against a real kube-apiserver + etcd
// from envtest. Run from hack/:
//
//	KUBEBUILDER_ASSETS=$(setup-envtest use -i -p path) go test ./e2e/ -v
package e2e

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type env struct {
	t       *testing.T
	bin     string
	envVars []string
	kube    *kubernetes.Clientset
}

func setup(t *testing.T) *env {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with KUBEBUILDER_ASSETS=$(setup-envtest use -i -p path)")
	}
	te := &envtest.Environment{}
	cfg, err := te.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { te.Stop() })

	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, te.KubeConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "ktt")
	build := exec.Command("go", "build", "-o", bin, "./cmd/ktt")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build ktt: %v\n%s", err, out)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, bin: bin, kube: kube, envVars: append(os.Environ(), "KTT_HOME="+filepath.Join(dir, "home"), "KUBECONFIG="+kubeconfig)}
	t.Cleanup(func() { e.ktt("shutdown") })
	return e
}

// ktt runs the binary and returns combined output and exit code.
func (e *env) ktt(args ...string) (string, int) {
	e.t.Helper()
	cmd := exec.Command(e.bin, args...)
	cmd.Env = e.envVars
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		e.t.Fatalf("ktt %v: %v", args, err)
	}
	e.t.Logf("$ ktt %s  (exit %d)\n%s", strings.Join(args, " "), code, out.String())
	return out.String(), code
}

func (e *env) mustKtt(args ...string) string {
	e.t.Helper()
	out, code := e.ktt(args...)
	if code != 0 {
		e.t.Fatalf("ktt %v exited %d", args, code)
	}
	return out
}

func contains(t *testing.T, out string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Fatalf("output lacks %q", w)
		}
	}
}

func (e *env) patchStatus(name, patch string) {
	e.t.Helper()
	if _, err := e.kube.CoreV1().Pods("default").Patch(context.Background(), name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}, "status"); err != nil {
		e.t.Fatal(err)
	}
}

const (
	crashLoop = `{"status":{"phase":"Running","containerStatuses":[{"name":"main","image":"nginx","imageID":"","ready":false,"restartCount":1,"started":false,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}`
	ready     = `{"status":{"phase":"Running","containerStatuses":[{"name":"main","image":"nginx","imageID":"","ready":true,"restartCount":1,"started":true,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}}`

	isCrashLoop = `object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") == "CrashLoopBackOff")`
	isReady     = `object.?status.?containerStatuses.orValue([]).exists(c, c.ready == true)`
)

func TestTransientStateAndRestartGap(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "default", Labels: map[string]string{"app": "web"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "nginx"}}},
	}
	if _, err := e.kube.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	out := e.mustKtt("record", "pods", "--context", "envtest", "-n", "default", "-l", "app=web")
	contains(t, out, "v1/pods in default -l app=web on envtest", "v1/events in default --field-selector involvedObject.kind=Pod")

	// A wait that must fail on a CrashLoopBackOff lasting one write, even
	// though the pod is ready right after.
	wait := exec.Command(e.bin, "wait", "pod/web-0", "--for", isReady, "--fail-on", isCrashLoop, "--timeout", "30s")
	wait.Env = e.envVars
	var waitOut bytes.Buffer
	wait.Stdout, wait.Stderr = &waitOut, &waitOut
	if err := wait.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	e.patchStatus("web-0", crashLoop)
	e.patchStatus("web-0", ready)
	err := wait.Wait()
	t.Logf("wait output:\n%s", waitOut.String())
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
		t.Fatalf("wait err = %v, want exit 1 (--fail-on matched)", err)
	}
	contains(t, waitOut.String(), "--fail-on matched", "CrashLoopBackOff")

	out = e.mustKtt("when", "pod/web-0", isCrashLoop)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "true") || !strings.Contains(lines[1], "false") {
		t.Fatalf("when = %q, want a true then a false transition", out)
	}
	blipStart := strings.Fields(lines[0])[0]
	out = e.mustKtt("at", blipStart, "pod/web-0")
	contains(t, out, "CrashLoopBackOff")
	out = e.mustKtt("history", "pod/web-0")
	contains(t, out, "+ status.containerStatuses: ", "CrashLoopBackOff", "- status.containerStatuses[name=main].state.waiting")

	// Restart gap: changes made while the daemon is down show up as list
	// revisions after a GAP.
	e.mustKtt("shutdown")
	if err := e.kube.CoreV1().Pods("default").Delete(ctx, "web-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	out = e.mustKtt("history", "pod/web-0", "--since", "-5s")
	contains(t, out, "GAP", "ktt daemon was not running", "DELETED", "[list]")
	out = e.mustKtt("at", "now", "pod/web-0")
	contains(t, out, "did not exist", "relist", "unknown earlier time")

	_, code := e.ktt("when", "pod/web-0", `object.?status.?phase.orValue("") == "Failed"`)
	if code != 1 {
		t.Fatalf("when never-true exit = %d, want 1", code)
	}
}
