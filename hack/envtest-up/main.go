// Command envtest-up starts a real etcd + kube-apiserver (no kubelet or
// controllers) via controller-runtime's envtest, writes a kubeconfig with
// context "envtest", and runs until interrupted.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func main() {
	kubeconfig := flag.String("out", "envtest.kubeconfig", "where to write the kubeconfig")
	flag.Parse()
	env := &envtest.Environment{}
	if _, err := env.Start(); err != nil {
		log.Fatalf("start envtest (set KUBEBUILDER_ASSETS, e.g. $(setup-envtest use -i -p path)): %v", err)
	}
	defer env.Stop()
	if err := os.WriteFile(*kubeconfig, env.KubeConfig, 0o600); err != nil {
		log.Fatal(err)
	}
	log.Printf("apiserver %s; kubeconfig %s (context envtest)", env.Config.Host, *kubeconfig)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
