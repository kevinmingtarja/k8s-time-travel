// Package server is the kubectl-timeline daemon: the single process that owns the Pebble
// store, runs recorders, and answers queries over a unix socket.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kevinmingtarja/kubectl-timeline/internal/api"
	"github.com/kevinmingtarja/kubectl-timeline/internal/recorder"
	"github.com/kevinmingtarja/kubectl-timeline/internal/store"
)

// maxSocketPath is the usable length of a unix socket path (sun_path is 104
// bytes on macOS, 108 on Linux, including the NUL).
const maxSocketPath = 103

// SocketPath is where the daemon listens: inside home, or under /tmp when
// that path is too long for a unix socket.
func SocketPath(home string) string {
	p := filepath.Join(home, "timeline.sock")
	if len(p) <= maxSocketPath {
		return p
	}
	abs, err := filepath.Abs(home)
	if err != nil {
		abs = home
	}
	h := sha256.Sum256([]byte(abs))
	return filepath.Join("/tmp", fmt.Sprintf("kube-timeline-%d-%s.sock", os.Getuid(), hex.EncodeToString(h[:6])))
}

type cluster struct {
	dyn       dynamic.Interface
	disc      discovery.DiscoveryInterface
	cache     discovery.CachedDiscoveryInterface
	deferred  *restmapper.DeferredDiscoveryRESTMapper
	mapper    meta.RESTMapper
	namespace string // the context's default namespace
}

type Daemon struct {
	home  string
	store *store.Store
	log   *log.Logger

	mu        sync.Mutex
	recorders map[string]*recorder.Recorder
	clusters  map[string]*cluster

	shutdown chan struct{}
	once     sync.Once
}

// Run opens the store, resumes recordings, and serves until SIGINT/SIGTERM or
// a shutdown request.
func Run(home string) error {
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	st, err := store.Open(filepath.Join(home, "db"))
	if err != nil {
		return fmt.Errorf("open store (is another kubectl-timeline daemon running?): %w", err)
	}
	defer st.Close()

	// Holding the Pebble lock means no other daemon owns this home, so a
	// leftover socket is stale.
	sock := SocketPath(home)
	if err := os.Remove(sock); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The socket may live in /tmp; create it owner-only.
	umask := syscall.Umask(0o077)
	ln, err := net.Listen("unix", sock)
	syscall.Umask(umask)
	if err != nil {
		return err
	}
	defer os.Remove(sock)

	d := &Daemon{
		home:      home,
		store:     st,
		log:       logger,
		recorders: map[string]*recorder.Recorder{},
		clusters:  map[string]*cluster{},
		shutdown:  make(chan struct{}),
	}
	d.resume()

	srv := &http.Server{Handler: d.routes()}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sig:
			logger.Printf("received %s, shutting down", s)
		case <-d.shutdown:
			logger.Printf("shutdown requested")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	logger.Printf("kubectl-timeline daemon pid=%d listening on %s", os.Getpid(), sock)
	err = srv.Serve(ln)

	d.mu.Lock()
	for _, r := range d.recorders {
		r.Close()
	}
	d.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// resume restarts every recording that was not explicitly stopped. Whatever
// happened while the daemon was down is unknown, so each gets a gap from its
// last sync.
func (d *Daemon) resume() {
	recs, err := d.store.Recordings()
	if err != nil {
		d.log.Printf("load recordings: %v", err)
		return
	}
	for _, rec := range recs {
		if rec.State == api.StateStopped {
			continue
		}
		r, err := d.newRecorder(rec)
		if err != nil {
			rec.State = api.StateError
			rec.LastError = err.Error()
			d.log.Printf("%s: cannot resume: %v", rec.ID, err)
			if perr := d.store.PutRecording(&rec); perr != nil {
				d.log.Printf("%s: persist: %v", rec.ID, perr)
			}
			continue
		}
		d.recorders[rec.ID] = r
		r.Resume(rec.LastSync, "daemon was not running")
		d.log.Printf("%s: resuming %s", rec.ID, describe(rec))
	}
}

func describe(rec api.Recording) string {
	s := rec.Info.GVR()
	if rec.Spec.Namespace != "" {
		s += " in " + rec.Spec.Namespace
	}
	if rec.Spec.LabelSelector != "" {
		s += " -l " + rec.Spec.LabelSelector
	}
	if rec.Spec.FieldSelector != "" {
		s += " --field-selector " + rec.Spec.FieldSelector
	}
	return s + " on " + rec.Spec.Context
}

func (d *Daemon) cluster(kubeconfig []string, context string) (*cluster, error) {
	key := strings.Join(kubeconfig, string(os.PathListSeparator)) + "\x00" + context
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.clusters[key]; ok {
		return c, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if len(kubeconfig) > 0 {
		rules.Precedence = kubeconfig
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: context})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig context %q: %w", context, err)
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, err
	}
	cfg.UserAgent = "kubectl-timeline"
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	cache := memory.NewMemCacheClient(disc)
	deferred := restmapper.NewDeferredDiscoveryRESTMapper(cache)
	mapper := restmapper.NewShortcutExpander(deferred, disc, nil)
	c := &cluster{dyn: dyn, disc: disc, cache: cache, deferred: deferred, mapper: mapper, namespace: ns}
	d.clusters[key] = c
	return c, nil
}

// resolve maps user input ("po", "deploy", "deployments.apps",
// "apps/v1/deployments") to a resource via discovery, refreshing the cached
// discovery once on failure so CRDs installed since the last lookup resolve.
func resolve(c *cluster, input string) (api.ResourceInfo, error) {
	info, err := resolveOnce(c, input)
	if err == nil {
		return info, nil
	}
	c.cache.Invalidate()
	c.deferred.Reset()
	if info, err := resolveOnce(c, input); err == nil {
		return info, nil
	}
	return api.ResourceInfo{}, fmt.Errorf("resource %q: %w", input, err)
}

func resolveOnce(c *cluster, input string) (api.ResourceInfo, error) {
	var gvr schema.GroupVersionResource
	if parts := strings.Split(input, "/"); len(parts) == 2 {
		gvr = schema.GroupVersionResource{Version: parts[0], Resource: parts[1]}
	} else if len(parts) == 3 {
		gvr = schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: parts[2]}
	} else {
		full, gr := schema.ParseResourceArg(strings.ToLower(input))
		if full != nil {
			if g, err := c.mapper.ResourceFor(*full); err == nil {
				gvr = g
			}
		}
		if gvr.Empty() {
			g, err := c.mapper.ResourceFor(gr.WithVersion(""))
			if err != nil {
				return api.ResourceInfo{}, err
			}
			gvr = g
		}
	}
	list, err := c.disc.ServerResourcesForGroupVersion(gvr.GroupVersion().String())
	if err != nil {
		return api.ResourceInfo{}, err
	}
	for _, r := range list.APIResources {
		if r.Name != gvr.Resource {
			continue
		}
		if !slices.Contains(r.Verbs, "list") || !slices.Contains(r.Verbs, "watch") {
			return api.ResourceInfo{}, fmt.Errorf("%s does not support list and watch", gvr)
		}
		return api.ResourceInfo{
			Group: gvr.Group, Version: gvr.Version, Resource: r.Name, Kind: r.Kind,
			Singular: r.SingularName, ShortNames: r.ShortNames, Namespaced: r.Namespaced,
		}, nil
	}
	return api.ResourceInfo{}, fmt.Errorf("resource %s not found in discovery", gvr)
}

func recordingID(spec api.RecordingSpec, gvr string) string {
	h := sha256.Sum256([]byte(strings.Join([]string{spec.Context, gvr, spec.Namespace, spec.LabelSelector, spec.FieldSelector}, "\x00")))
	return hex.EncodeToString(h[:4])
}

func (d *Daemon) newRecorder(rec api.Recording) (*recorder.Recorder, error) {
	c, err := d.cluster(rec.Spec.Kubeconfig, rec.Spec.Context)
	if err != nil {
		return nil, err
	}
	gvr := schema.GroupVersionResource{Group: rec.Info.Group, Version: rec.Info.Version, Resource: rec.Info.Resource}
	var client dynamic.ResourceInterface = c.dyn.Resource(gvr)
	if rec.Spec.Namespace != "" {
		client = c.dyn.Resource(gvr).Namespace(rec.Spec.Namespace)
	}
	return recorder.New(d.store, client, rec, d.log)
}

// RecordRequest is the body of POST /v1/recordings.
type RecordRequest struct {
	Spec          api.RecordingSpec `json:"spec"`
	AllNamespaces bool              `json:"allNamespaces"`
}

// record starts (or returns the already running) recording for spec, plus
// its Events recording when spec.Events is set.
func (d *Daemon) record(req RecordRequest) ([]api.Recording, error) {
	spec := req.Spec
	if spec.Context == "" {
		return nil, errors.New("--context is required")
	}
	c, err := d.cluster(spec.Kubeconfig, spec.Context)
	if err != nil {
		return nil, err
	}
	info, err := resolve(c, spec.Resource)
	if err != nil {
		return nil, err
	}
	switch {
	case !info.Namespaced && spec.Namespace != "":
		return nil, fmt.Errorf("%s is cluster-scoped; drop -n", info.GVR())
	case req.AllNamespaces:
		spec.Namespace = ""
	case info.Namespaced && spec.Namespace == "":
		spec.Namespace = c.namespace
	}
	parent, err := d.start(spec, info, "")
	if err != nil {
		return nil, err
	}
	out := []api.Recording{parent}
	if spec.Events && !(info.Group == "" && info.Resource == "events") {
		evSpec := api.RecordingSpec{
			Context:       spec.Context,
			Kubeconfig:    spec.Kubeconfig,
			Resource:      "events",
			Namespace:     spec.Namespace,
			FieldSelector: "involvedObject.kind=" + info.Kind,
		}
		evInfo, err := resolve(c, "v1/events")
		if err != nil {
			return out, fmt.Errorf("events: %w", err)
		}
		child, err := d.start(evSpec, evInfo, parent.ID)
		if err != nil {
			return out, fmt.Errorf("events: %w", err)
		}
		out = append(out, child)
	}
	return out, nil
}

func (d *Daemon) start(spec api.RecordingSpec, info api.ResourceInfo, parentID string) (api.Recording, error) {
	id := recordingID(spec, info.GVR())
	d.mu.Lock()
	if r, ok := d.recorders[id]; ok {
		d.mu.Unlock()
		return r.Recording(), nil
	}
	d.mu.Unlock()

	rec := api.Recording{ID: id, ParentID: parentID, Spec: spec, Info: info, State: api.StateRunning, StartedAt: time.Now().UnixNano()}
	var gapStart int64
	var gapReason string
	recs, err := d.store.Recordings()
	if err != nil {
		return api.Recording{}, err
	}
	for _, old := range recs {
		if old.ID == id {
			// Restarting a stopped recording: the stopped window is a gap.
			rec = old
			rec.State = api.StateRunning
			rec.StoppedAt = 0
			gapStart, gapReason = old.StoppedAt, "recording was stopped"
			if gapStart == 0 {
				gapStart, gapReason = old.LastSync, "recording was not running"
			}
		}
	}
	r, err := d.newRecorder(rec)
	if err != nil {
		return api.Recording{}, err
	}
	if err := r.Start(gapStart, gapReason); err != nil {
		return api.Recording{}, err
	}
	d.mu.Lock()
	d.recorders[id] = r
	d.mu.Unlock()
	d.log.Printf("%s: recording %s", id, describe(rec))
	return r.Recording(), nil
}

// stop stops a recording and its Events child.
func (d *Daemon) stop(id string) ([]api.Recording, error) {
	recs, err := d.recordings()
	if err != nil {
		return nil, err
	}
	var out []api.Recording
	for _, rec := range recs {
		if rec.ID != id && rec.ParentID != id {
			continue
		}
		d.mu.Lock()
		r, ok := d.recorders[rec.ID]
		delete(d.recorders, rec.ID)
		d.mu.Unlock()
		if ok {
			if err := r.Stop(); err != nil {
				return out, err
			}
			rec = r.Recording()
		} else if rec.State != api.StateStopped {
			rec.State = api.StateStopped
			rec.StoppedAt = time.Now().UnixNano()
			if err := d.store.PutRecording(&rec); err != nil {
				return out, err
			}
		}
		out = append(out, rec)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no recording %q", id)
	}
	return out, nil
}

// recordings returns every recording, with live state for running ones.
func (d *Daemon) recordings() ([]api.Recording, error) {
	recs, err := d.store.Recordings()
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, rec := range recs {
		if r, ok := d.recorders[rec.ID]; ok {
			recs[i] = r.Recording()
		}
	}
	return recs, nil
}

func (d *Daemon) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/ping", func(w http.ResponseWriter, r *http.Request) {
		reply(w, api.PingResponse{PID: os.Getpid(), Home: d.home}, nil)
	})
	mux.HandleFunc("POST /v1/shutdown", func(w http.ResponseWriter, r *http.Request) {
		reply(w, struct{}{}, nil)
		d.once.Do(func() { close(d.shutdown) })
	})
	mux.HandleFunc("GET /v1/recordings", func(w http.ResponseWriter, r *http.Request) {
		recs, err := d.recordings()
		reply(w, recs, err)
	})
	mux.HandleFunc("POST /v1/recordings", func(w http.ResponseWriter, r *http.Request) {
		var req RecordRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			reply(w, nil, err)
			return
		}
		recs, err := d.record(req)
		reply(w, recs, err)
	})
	mux.HandleFunc("DELETE /v1/recordings/{id}", func(w http.ResponseWriter, r *http.Request) {
		recs, err := d.stop(r.PathValue("id"))
		reply(w, recs, err)
	})
	mux.HandleFunc("GET /v1/revisions", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, until, after, err := int64Params(q.Get("since"), q.Get("until"), q.Get("after"))
		if err != nil {
			reply(w, nil, err)
			return
		}
		if q.Get("until") == "" {
			until = math.MaxInt64
		}
		revs, head, err := d.store.History(filter(q.Get), since, until, after)
		reply(w, api.RevisionsResponse{Revisions: revs, Head: head}, err)
	})
	mux.HandleFunc("GET /v1/state", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		at, _, _, err := int64Params(q.Get("at"), "", "")
		if err != nil {
			reply(w, nil, err)
			return
		}
		if q.Get("at") == "" {
			at = math.MaxInt64
		}
		revs, head, err := d.store.State(filter(q.Get), at)
		reply(w, api.RevisionsResponse{Revisions: revs, Head: head}, err)
	})
	mux.HandleFunc("GET /v1/gaps", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, until, _, err := int64Params(q.Get("since"), q.Get("until"), "")
		if err != nil {
			reply(w, nil, err)
			return
		}
		if q.Get("until") == "" {
			until = math.MaxInt64
		}
		gaps, err := d.store.Gaps(q.Get("context"), q.Get("gvr"), since, until)
		reply(w, gaps, err)
	})
	return mux
}

func filter(get func(string) string) store.Filter {
	return store.Filter{Context: get("context"), GVR: get("gvr"), Namespace: get("namespace"), Name: get("name")}
}

func int64Params(vals ...string) (int64, int64, int64, error) {
	var out [3]int64
	for i, v := range vals {
		if v == "" {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("bad integer %q", v)
		}
		out[i] = n
	}
	return out[0], out[1], out[2], nil
}

// ErrorResponse is the body of a non-2xx reply.
type ErrorResponse struct {
	Error string `json:"error"`
}

func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(v)
}
