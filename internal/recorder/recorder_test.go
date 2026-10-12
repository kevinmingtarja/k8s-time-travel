package recorder

import (
	"context"
	"io"
	"log"
	"math"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
	"github.com/kevinmingtarja/k8s-time-travel/internal/store"
)

var podsGVR = schema.GroupVersionResource{Version: "v1", Resource: "pods"}

func pod(name, rv string, labels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind("Pod")
	u.SetNamespace("default")
	u.SetName(name)
	u.SetResourceVersion(rv)
	u.SetLabels(labels)
	return u
}

type harness struct {
	t        *testing.T
	store    *store.Store
	client   *dynfake.FakeDynamicClient
	watchers chan *watch.FakeWatcher
	watchRVs chan string
	rec      *Recorder
}

func newHarness(t *testing.T, selector string, objs ...runtime.Object) *harness {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{podsGVR: "PodList"}, objs...)
	h := &harness{t: t, store: st, client: client, watchers: make(chan *watch.FakeWatcher, 10), watchRVs: make(chan string, 10)}
	client.PrependWatchReactor("pods", func(action clienttesting.Action) (bool, watch.Interface, error) {
		h.watchRVs <- action.(clienttesting.WatchActionImpl).GetWatchRestrictions().ResourceVersion
		w := watch.NewFakeWithChanSize(10, false)
		h.watchers <- w
		return true, w, nil
	})
	rec := api.Recording{
		ID:   "r1",
		Spec: api.RecordingSpec{Context: "c", Resource: "pods", Namespace: "default", LabelSelector: selector},
		Info: api.ResourceInfo{Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true},
	}
	r, err := New(st, client.Resource(podsGVR).Namespace("default"), rec, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	h.rec = r
	t.Cleanup(r.Close)
	return h
}

func (h *harness) nextWatcher() (*watch.FakeWatcher, string) {
	h.t.Helper()
	select {
	case w := <-h.watchers:
		return w, <-h.watchRVs
	case <-time.After(5 * time.Second):
		h.t.Fatal("recorder did not open a watch")
		return nil, ""
	}
}

// waitRevisions polls until the store holds n revisions.
func (h *harness) waitRevisions(n int) []api.Revision {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		revs, _, err := h.store.History(store.Filter{}, 0, math.MaxInt64, -1)
		if err != nil {
			h.t.Fatal(err)
		}
		if len(revs) >= n {
			if len(revs) > n {
				h.t.Fatalf("got %d revisions, want %d: %+v", len(revs), n, revs)
			}
			return revs
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("got %d revisions, want %d", len(revs), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func check(t *testing.T, r api.Revision, name, rv, typ, source string) {
	t.Helper()
	if r.Name != name || r.RV != rv || r.Type != typ || r.Source != source {
		t.Fatalf("revision = %s@%s %s %s, want %s@%s %s %s", r.Name, r.RV, r.Type, r.Source, name, rv, typ, source)
	}
}

func TestWatchErrorRelistsAndRecordsGap(t *testing.T) {
	h := newHarness(t, "", pod("a", "1", nil), pod("b", "2", nil))
	if err := h.rec.Start(0, ""); err != nil {
		t.Fatal(err)
	}
	revs := h.waitRevisions(2)
	check(t, revs[0], "a", "1", api.Added, api.SourceList)
	check(t, revs[1], "b", "2", api.Added, api.SourceList)

	w1, _ := h.nextWatcher()
	w1.Modify(pod("a", "3", nil))
	revs = h.waitRevisions(3)
	check(t, revs[2], "a", "3", api.Modified, api.SourceWatch)
	syncBefore := h.rec.Recording().LastSync

	// While the watch is broken, a changes and b is deleted.
	ctx := context.Background()
	pods := h.client.Resource(podsGVR).Namespace("default")
	if _, err := pods.Update(ctx, pod("a", "5", nil), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := pods.Delete(ctx, "b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	w1.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old resource version"})

	revs = h.waitRevisions(5)
	check(t, revs[3], "a", "5", api.Modified, api.SourceList)
	check(t, revs[4], "b", "2", api.Deleted, api.SourceList)
	gaps, err := h.store.Gaps("c", "v1/pods", 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 1 || gaps[0].Start != syncBefore || gaps[0].End != revs[3].TS {
		t.Fatalf("gaps = %+v, want one from %d to %d", gaps, syncBefore, revs[3].TS)
	}
	if rec := h.rec.Recording(); rec.State != api.StateRunning || rec.Revisions != 5 {
		t.Fatalf("recording = %+v", rec)
	}
	h.nextWatcher() // watching again after the relist
}

func TestCleanCloseResumesWithoutGap(t *testing.T) {
	h := newHarness(t, "", pod("a", "1", nil))
	if err := h.rec.Start(0, ""); err != nil {
		t.Fatal(err)
	}
	h.waitRevisions(1)
	w1, _ := h.nextWatcher()
	w1.Modify(pod("a", "7", nil))
	h.waitRevisions(2)
	w1.Stop()

	w2, rv := h.nextWatcher()
	if rv != "7" {
		t.Fatalf("re-watch from rv %q, want 7", rv)
	}
	w2.Add(pod("c", "8", nil))
	h.waitRevisions(3)
	if gaps, _ := h.store.Gaps("", "", 0, math.MaxInt64); len(gaps) != 0 {
		t.Fatalf("unexpected gaps %+v", gaps)
	}
}

func TestDeleteOutOfSelectorIsAnnotated(t *testing.T) {
	h := newHarness(t, "app=web", pod("a", "1", map[string]string{"app": "web"}))
	if err := h.rec.Start(0, ""); err != nil {
		t.Fatal(err)
	}
	h.waitRevisions(1)
	w1, _ := h.nextWatcher()
	w1.Delete(pod("a", "2", map[string]string{"app": "other"}))
	revs := h.waitRevisions(2)
	check(t, revs[1], "a", "2", api.Deleted, api.SourceWatch)
	if revs[1].Note == "" {
		t.Fatal("expected a note on a delete caused by leaving the selector")
	}
}

func TestResumeRelistsWithGapAndRetries(t *testing.T) {
	h := newHarness(t, "", pod("a", "1", nil))
	failures := 2
	h.client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, io.ErrUnexpectedEOF
		}
		return false, nil, nil
	})
	h.rec.Resume(42, "ktt daemon was not running")
	h.waitRevisions(1)
	gaps, _ := h.store.Gaps("c", "v1/pods", 0, math.MaxInt64)
	if len(gaps) != 1 || gaps[0].Start != 42 || gaps[0].Reason != "ktt daemon was not running" {
		t.Fatalf("gaps = %+v", gaps)
	}
}
