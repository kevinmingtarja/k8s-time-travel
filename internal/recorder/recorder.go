// Package recorder runs one recording: a list, then a watch whose every event
// becomes a stored revision.
//
// The watch is resumed from its last resourceVersion only when the stream
// closes cleanly and the immediate re-watch succeeds; in that case nothing is
// lost and receive times stay accurate. Any watch error (including 410 Gone)
// instead triggers a relist plus a Gap covering [LastSync, relist time], so
// queries over that window report "unknown" rather than "no change".
package recorder

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
	"github.com/kevinmingtarja/k8s-time-travel/internal/store"
)

const maxBackoff = 30 * time.Second

type Recorder struct {
	store  *store.Store
	client dynamic.ResourceInterface
	labels labels.Selector
	fields fields.Selector
	log    *log.Logger
	now    func() time.Time

	mu  sync.Mutex
	rec api.Recording

	cancel context.CancelFunc
	done   chan struct{}
}

// New builds a recorder. client must already be scoped to rec.Spec.Namespace.
func New(st *store.Store, client dynamic.ResourceInterface, rec api.Recording, logger *log.Logger) (*Recorder, error) {
	ls, err := labels.Parse(rec.Spec.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("label selector: %w", err)
	}
	fs, err := fields.ParseSelector(rec.Spec.FieldSelector)
	if err != nil {
		return nil, fmt.Errorf("field selector: %w", err)
	}
	return &Recorder{
		store:  st,
		client: client,
		labels: ls,
		fields: fs,
		log:    logger,
		now:    time.Now,
		rec:    rec,
		done:   make(chan struct{}),
	}, nil
}

// Recording returns a snapshot of the recording's current state.
func (r *Recorder) Recording() api.Recording {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rec
}

// Start lists synchronously, so RBAC and selector errors reach the caller,
// then watches in the background. gapStart > 0 records a gap from gapStart to
// the list time.
func (r *Recorder) Start(gapStart int64, gapReason string) error {
	ctx, cancel := context.WithCancel(context.Background())
	rv, err := r.sync(ctx, gapStart, gapReason)
	if err != nil {
		cancel()
		return err
	}
	r.cancel = cancel
	go r.loop(ctx, rv, 0, "")
	return nil
}

// Resume starts in the background with a relist and a gap from gapStart,
// retrying until the list succeeds. Used when the daemon restarts.
func (r *Recorder) Resume(gapStart int64, gapReason string) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go r.loop(ctx, "", gapStart, gapReason)
}

// Close stops the watch and waits for it to exit, leaving the recording
// running in the store so a restarted daemon resumes it.
func (r *Recorder) Close() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
}

// Stop closes the recorder and marks the recording stopped.
func (r *Recorder) Stop() error {
	r.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rec.State = api.StateStopped
	r.rec.StoppedAt = r.now().UnixNano()
	return r.store.PutRecording(&r.rec)
}

func (r *Recorder) listOptions() metav1.ListOptions {
	return metav1.ListOptions{LabelSelector: r.rec.Spec.LabelSelector, FieldSelector: r.rec.Spec.FieldSelector}
}

// sync lists, reconciles the result against the store (emitting ADDED,
// MODIFIED and DELETED revisions for whatever changed since the store last
// knew), and records a gap if gapStart > 0. It returns the list's
// resourceVersion.
func (r *Recorder) sync(ctx context.Context, gapStart int64, gapReason string) (string, error) {
	list, err := r.client.List(ctx, r.listOptions())
	if err != nil {
		return "", err
	}
	now := r.now().UnixNano()

	rec := r.Recording()
	known, _, err := r.store.State(store.Filter{Context: rec.Spec.Context, GVR: rec.Info.GVR(), Namespace: rec.Spec.Namespace}, math.MaxInt64)
	if err != nil {
		return "", err
	}
	alive := map[string]api.Revision{}
	for _, k := range known {
		if k.Type == api.Deleted {
			continue
		}
		var u unstructured.Unstructured
		if err := u.UnmarshalJSON(k.Object); err != nil {
			return "", fmt.Errorf("decode stored %s/%s: %w", k.Namespace, k.Name, err)
		}
		if r.inScope(&u) {
			alive[k.Namespace+"/"+k.Name] = k
		}
	}

	var revs []api.Revision
	for i := range list.Items {
		item := &list.Items[i]
		key := item.GetNamespace() + "/" + item.GetName()
		typ := api.Added
		if _, ok := alive[key]; ok {
			typ = api.Modified
			delete(alive, key)
		}
		rev, err := r.revision(item, typ, api.SourceList, now)
		if err != nil {
			return "", err
		}
		revs = append(revs, rev)
	}
	for _, k := range alive {
		k.TS = now
		k.Type = api.Deleted
		k.Source = api.SourceList
		k.Note = "absent from list: deleted, or stopped matching the selector, at an unknown time since the last sync"
		revs = append(revs, k)
	}

	var gap *api.Gap
	if gapStart > 0 {
		gap = &api.Gap{Context: rec.Spec.Context, GVR: rec.Info.GVR(), RecordingID: rec.ID, Start: gapStart, End: now, Reason: gapReason}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.rec.State = api.StateRunning
	r.rec.LastError = ""
	r.rec.LastSync = max(r.rec.LastSync, now)
	written, err := r.store.Commit(&r.rec, revs, gap)
	if err != nil {
		return "", err
	}
	r.log.Printf("%s: listed %d objects at rv=%s, wrote %d revisions", r.rec.ID, len(list.Items), list.GetResourceVersion(), len(written))
	return list.GetResourceVersion(), nil
}

// inScope reports whether a stored object falls inside this recording's
// selectors. The namespace is already pinned by the store filter.
func (r *Recorder) inScope(u *unstructured.Unstructured) bool {
	if !r.labels.Matches(labels.Set(u.GetLabels())) {
		return false
	}
	if r.fields.Empty() {
		return true
	}
	set := fields.Set{}
	for _, req := range r.fields.Requirements() {
		v, _, _ := unstructured.NestedFieldNoCopy(u.Object, strings.Split(req.Field, ".")...)
		if v != nil {
			set[req.Field] = fmt.Sprint(v)
		}
	}
	return r.fields.Matches(set)
}

func (r *Recorder) revision(u *unstructured.Unstructured, typ, source string, ts int64) (api.Revision, error) {
	data, err := u.MarshalJSON()
	if err != nil {
		return api.Revision{}, err
	}
	return api.Revision{
		TS:        ts,
		Context:   r.rec.Spec.Context,
		GVR:       r.rec.Info.GVR(),
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
		UID:       string(u.GetUID()),
		RV:        u.GetResourceVersion(),
		Type:      typ,
		Source:    source,
		Object:    data,
	}, nil
}

func (r *Recorder) loop(ctx context.Context, rv string, gapStart int64, gapReason string) {
	defer close(r.done)
	backoff := time.Second
	sleep := func() {
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
	for ctx.Err() == nil {
		if rv == "" {
			var err error
			rv, err = r.sync(ctx, gapStart, gapReason)
			if err != nil {
				if ctx.Err() == nil {
					r.setError(fmt.Errorf("list: %w", err))
					sleep()
				}
				continue
			}
			backoff = time.Second
		}
		started := r.now()
		next, err := r.watch(ctx, rv)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			// Server-side watch timeouts close streams after minutes; one that
			// closes immediately is a misbehaving server, so don't spin on it.
			if r.now().Sub(started) < time.Second {
				sleep()
			} else {
				backoff = time.Second
			}
			rv = next
			continue
		}
		gapStart = r.Recording().LastSync
		gapReason = err.Error()
		rv = ""
		// A watch that ran for a while and then failed (e.g. 410 Gone) is
		// routine: relist right away. One that fails as soon as it starts is
		// a persistent problem: surface it and back off.
		if r.now().Sub(started) < 5*time.Second {
			r.setError(fmt.Errorf("watch: %w", err))
			sleep()
		} else {
			r.log.Printf("%s: watch: %v; relisting", r.rec.ID, err)
			backoff = time.Second
		}
	}
}

// watch consumes one watch stream from rv. It returns the last resourceVersion
// seen and nil when the stream closes cleanly, or an error when the watch
// fails and the caller must relist.
func (r *Recorder) watch(ctx context.Context, rv string) (string, error) {
	timeout := int64(300 + rand.IntN(300))
	opts := r.listOptions()
	opts.ResourceVersion = rv
	opts.AllowWatchBookmarks = true
	opts.TimeoutSeconds = &timeout
	w, err := r.client.Watch(ctx, opts)
	if err != nil {
		return rv, err
	}
	defer w.Stop()
	for {
		var ev watch.Event
		var ok bool
		select {
		case <-ctx.Done():
			// The stream was healthy until the recorder was closed, so it was in
			// sync until now; the gap a restarted daemon records starts here
			// rather than at the last event.
			if err := r.commit(r.now().UnixNano(), nil); err != nil {
				r.log.Printf("%s: persist last sync: %v", r.rec.ID, err)
			}
			return rv, nil
		case ev, ok = <-w.ResultChan():
		}
		if !ok {
			return rv, nil
		}
		now := r.now().UnixNano()
		switch ev.Type {
		case watch.Added, watch.Modified, watch.Deleted:
			u, isU := ev.Object.(*unstructured.Unstructured)
			if !isU {
				return rv, fmt.Errorf("unexpected watch object %T", ev.Object)
			}
			rev, err := r.revision(u, string(ev.Type), api.SourceWatch, now)
			if err != nil {
				return rv, err
			}
			if ev.Type == watch.Deleted && !r.inScope(u) {
				rev.Note = "stopped matching the selector; the object may still exist"
			}
			if err := r.commit(now, []api.Revision{rev}); err != nil {
				return rv, err
			}
			rv = u.GetResourceVersion()
		case watch.Bookmark:
			u, isU := ev.Object.(*unstructured.Unstructured)
			if !isU {
				return rv, fmt.Errorf("unexpected bookmark object %T", ev.Object)
			}
			if err := r.commit(now, nil); err != nil {
				return rv, err
			}
			rv = u.GetResourceVersion()
		case watch.Error:
			err := apierrors.FromObject(ev.Object)
			if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
				return rv, fmt.Errorf("resourceVersion %s expired: %w", rv, err)
			}
			return rv, err
		default:
			return rv, fmt.Errorf("unexpected watch event type %q", ev.Type)
		}
	}
}

// commit writes revisions and marks the recording in sync as of now.
func (r *Recorder) commit(now int64, revs []api.Revision) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec.State != api.StateRunning {
		r.log.Printf("%s: recovered", r.rec.ID)
	}
	r.rec.State = api.StateRunning
	r.rec.LastError = ""
	r.rec.LastSync = max(r.rec.LastSync, now)
	_, err := r.store.Commit(&r.rec, revs, nil)
	return err
}

func (r *Recorder) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Printf("%s: %v", r.rec.ID, err)
	r.rec.State = api.StateError
	r.rec.LastError = err.Error()
	if perr := r.store.PutRecording(&r.rec); perr != nil {
		r.log.Printf("%s: persist error state: %v", r.rec.ID, perr)
	}
}
