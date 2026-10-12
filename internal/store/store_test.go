package store

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func rev(ns, name, rv, typ string, ts int64) api.Revision {
	obj, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": name, "namespace": ns, "resourceVersion": rv}})
	return api.Revision{TS: ts, Context: "c", GVR: "v1/pods", Namespace: ns, Name: name, RV: rv, Type: typ, Source: api.SourceWatch, Object: obj}
}

func commit(t *testing.T, s *Store, revs ...api.Revision) []api.Revision {
	t.Helper()
	w, err := s.Commit(nil, revs, nil)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func rvs(revs []api.Revision) []string {
	var out []string
	for _, r := range revs {
		out = append(out, r.Namespace+"/"+r.Name+"@"+r.RV)
	}
	return out
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestCommitAssignsStrictlyIncreasingTS(t *testing.T) {
	s := open(t)
	w := commit(t, s, rev("a", "p", "1", api.Added, 100), rev("a", "q", "2", api.Added, 100), rev("a", "p", "3", api.Modified, 50))
	if w[0].TS != 100 || w[1].TS != 101 || w[2].TS != 102 {
		t.Fatalf("ts = %d %d %d", w[0].TS, w[1].TS, w[2].TS)
	}
	if s.Head() != 102 {
		t.Fatalf("head = %d", s.Head())
	}
}

func TestCommitDedupsRepeatedRevision(t *testing.T) {
	s := open(t)
	commit(t, s, rev("a", "p", "1", api.Added, 100))
	if w := commit(t, s, rev("a", "p", "1", api.Modified, 200)); len(w) != 0 {
		t.Fatalf("same rv rewritten: %v", rvs(w))
	}
	// A relist DELETED with the same RV is a new fact: the object is gone.
	del := rev("a", "p", "1", api.Deleted, 300)
	del.Source = api.SourceList
	if w := commit(t, s, del); len(w) != 1 {
		t.Fatalf("delete not written")
	}
	del.TS = 400
	if w := commit(t, s, del); len(w) != 0 {
		t.Fatalf("delete rewritten")
	}
}

func TestCommitDedupsOverlappingRecordings(t *testing.T) {
	s := open(t)
	// Two recordings see the same stream; B lags A.
	var got []string
	for _, r := range []api.Revision{
		rev("a", "p", "1", api.Added, 10),    // A
		rev("a", "p", "2", api.Modified, 11), // A
		rev("a", "p", "1", api.Added, 12),    // B, behind
		rev("a", "p", "3", api.Modified, 13), // A
		rev("a", "p", "2", api.Modified, 14), // B, behind and older than the latest
		rev("a", "p", "3", api.Modified, 15), // B
	} {
		got = append(got, rvs(commit(t, s, r))...)
	}
	eq(t, got, []string{"a/p@1", "a/p@2", "a/p@3"})
	h, _, _ := s.History(Filter{}, 0, math.MaxInt64, -1)
	eq(t, rvs(h), []string{"a/p@1", "a/p@2", "a/p@3"})
}

func TestSelectorExitDeleteSkippedWhenAnotherRecordingSeesObject(t *testing.T) {
	s := open(t)
	commit(t, s, rev("a", "p", "5", api.Modified, 10)) // broad recording: label changed at rv 5
	if w := commit(t, s, rev("a", "p", "5", api.Deleted, 11)); len(w) != 0 {
		t.Fatalf("watch DELETED for a live rv was written: %v", rvs(w))
	}
	listDel := rev("a", "p", "5", api.Deleted, 12)
	listDel.Source = api.SourceList
	if w := commit(t, s, listDel); len(w) != 1 {
		t.Fatal("relist DELETED was skipped")
	}
}

func TestStateAndHistory(t *testing.T) {
	s := open(t)
	commit(t, s,
		rev("a", "p", "1", api.Added, 100),
		rev("a", "p-2", "2", api.Added, 110), // shares the "p" prefix
		rev("b", "p", "3", api.Added, 120),
		rev("a", "p", "4", api.Modified, 130),
		rev("a", "p-2", "5", api.Deleted, 140),
		rev("a", "p", "6", api.Modified, 150),
	)

	st, head, err := s.State(Filter{Context: "c", GVR: "v1/pods"}, 135)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, rvs(st), []string{"a/p@4", "a/p-2@2", "b/p@3"})
	if head != 150 {
		t.Fatalf("head = %d", head)
	}
	st, _, _ = s.State(Filter{}, 145)
	eq(t, rvs(st), []string{"a/p@4", "a/p-2@5", "b/p@3"})
	st, _, _ = s.State(Filter{Context: "c", GVR: "v1/pods", Name: "p"}, math.MaxInt64)
	eq(t, rvs(st), []string{"a/p@6", "b/p@3"})
	st, _, _ = s.State(Filter{Context: "c", GVR: "v1/pods", Namespace: "a", Name: "p"}, 99)
	eq(t, rvs(st), nil)

	// Single-object path.
	h, _, _ := s.History(Filter{Context: "c", GVR: "v1/pods", Namespace: "a", Name: "p"}, 100, 140, -1)
	eq(t, rvs(h), []string{"a/p@1", "a/p@4"})
	// Time-index path, filtered by name across namespaces.
	h, _, _ = s.History(Filter{Context: "c", GVR: "v1/pods", Name: "p"}, 0, math.MaxInt64, -1)
	eq(t, rvs(h), []string{"a/p@1", "b/p@3", "a/p@4", "a/p@6"})
	// Cursor.
	h, _, _ = s.History(Filter{}, 0, math.MaxInt64, 130)
	eq(t, rvs(h), []string{"a/p-2@5", "a/p@6"})
	h, _, _ = s.History(Filter{Context: "c", GVR: "v1/pods", Namespace: "a", Name: "p"}, 0, math.MaxInt64, 130)
	eq(t, rvs(h), []string{"a/p@6"})
}

func TestReopenRestoresHead(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commit(t, s, rev("a", "p", "1", api.Added, 100))
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Head() != 100 {
		t.Fatalf("head = %d", s.Head())
	}
	w := commit(t, s, rev("a", "p", "2", api.Modified, 1))
	if w[0].TS != 101 {
		t.Fatalf("ts after reopen = %d", w[0].TS)
	}
}

func TestOpenFailsWhileLocked(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s2, err := Open(dir); err == nil {
		s2.Close()
		t.Fatal("second Open succeeded")
	}
}

func TestGapsAndRecordings(t *testing.T) {
	s := open(t)
	rec := &api.Recording{ID: "r1", StartedAt: 5}
	if _, err := s.Commit(rec, []api.Revision{rev("a", "p", "1", api.Added, 100)}, &api.Gap{Context: "c", GVR: "v1/pods", RecordingID: "r1", Start: 50, End: 90}); err != nil {
		t.Fatal(err)
	}
	if rec.Revisions != 1 || rec.LastSync != 100 {
		t.Fatalf("rec = %+v", rec)
	}
	recs, _ := s.Recordings()
	if len(recs) != 1 || recs[0].Revisions != 1 {
		t.Fatalf("recordings = %+v", recs)
	}
	for _, tc := range []struct {
		ctx, gvr string
		lo, hi   int64
		want     int
	}{
		{"c", "v1/pods", 0, 49, 0},
		{"c", "v1/pods", 0, 50, 1},
		{"c", "", 90, 100, 1},
		{"", "", 91, 100, 0},
		{"c", "apps/v1/deployments", 0, 100, 0},
	} {
		g, err := s.Gaps(tc.ctx, tc.gvr, tc.lo, tc.hi)
		if err != nil {
			t.Fatal(err)
		}
		if len(g) != tc.want {
			t.Errorf("Gaps(%q,%q,%d,%d) = %d, want %d", tc.ctx, tc.gvr, tc.lo, tc.hi, len(g), tc.want)
		}
	}
}
