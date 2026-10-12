// Package store persists recordings, object revisions and gaps in Pebble.
//
// Keyspace (all integers big-endian uint64 unix nanos):
//
//	r/<recording id>                                  -> api.Recording
//	o/<ctx>\0<gvr>\0<ns>\0<name>\0<ts>                -> api.Revision (object-major)
//	l/<ts>                                            -> <ctx>\0<gvr>\0<ns>\0<name>\0 (time index)
//	g/<ctx>\0<gvr>\0<start><recording id>             -> api.Gap
//	v/<ctx>\0<gvr>\0<ns>\0<name>\0<rv>\0<L|D>          -> ts (every stored (rv, deleted) pair)
//
// Object identity is (context, gvr, namespace, name), not UID, so the history
// of pod/foo spans deletes and same-name recreations.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
)

const (
	pRec  = "r/"
	pObj  = "o/"
	pLog  = "l/"
	pGap  = "g/"
	pSeen = "v/"
)

// Filter selects objects by identity. Empty fields match anything.
type Filter struct {
	Context   string
	GVR       string
	Namespace string
	Name      string
}

func (f Filter) match(id identity) bool {
	return (f.Context == "" || f.Context == id.Context) &&
		(f.GVR == "" || f.GVR == id.GVR) &&
		(f.Namespace == "" || f.Namespace == id.Namespace) &&
		(f.Name == "" || f.Name == id.Name)
}

// prefix returns the longest identity prefix fixed by the filter, and whether
// it pins a single object.
func (f Filter) prefix() ([]byte, bool) {
	var b []byte
	for i, part := range []string{f.Context, f.GVR, f.Namespace, f.Name} {
		if part == "" {
			return b, false
		}
		b = append(append(b, part...), 0)
		if i == 3 {
			return b, true
		}
	}
	return b, false
}

type identity struct {
	Context, GVR, Namespace, Name string
}

func identityOf(r *api.Revision) identity {
	return identity{r.Context, r.GVR, r.Namespace, r.Name}
}

func (id identity) bytes() []byte {
	var b []byte
	for _, part := range []string{id.Context, id.GVR, id.Namespace, id.Name} {
		b = append(append(b, part...), 0)
	}
	return b
}

func parseIdentity(b []byte) (identity, error) {
	parts := bytes.Split(b, []byte{0})
	if len(parts) != 5 || len(parts[4]) != 0 {
		return identity{}, fmt.Errorf("malformed object identity %q", b)
	}
	return identity{string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])}, nil
}

func be64(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func objKey(id []byte, ts int64) []byte {
	return append(append([]byte(pObj), id...), be64(ts)...)
}

// objKeyAfter is the smallest key past every revision of id with TS <= at.
func objKeyAfter(id []byte, at int64) []byte {
	if at == math.MaxInt64 {
		return successor(append([]byte(pObj), id...))
	}
	return objKey(id, at+1)
}

func logKey(ts int64) []byte {
	return append([]byte(pLog), be64(ts)...)
}

// successor returns the smallest key greater than every key with prefix p.
func successor(p []byte) []byte {
	s := bytes.Clone(p)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] < 0xff {
			s[i]++
			return s[:i+1]
		}
	}
	return nil
}

// Store is safe for concurrent use.
type Store struct {
	db *pebble.DB
	// mu serializes Commit so the dedup read, TS assignment and write are
	// atomic with respect to other commits.
	mu   sync.Mutex
	head atomic.Int64
}

// quietLogger drops Pebble's routine info logs (WAL replay, compactions).
type quietLogger struct{}

func (quietLogger) Infof(string, ...any) {}
func (quietLogger) Errorf(format string, args ...any) {
	log.Printf("pebble: "+format, args...)
}
func (quietLogger) Fatalf(format string, args ...any) {
	log.Fatalf("pebble: "+format, args...)
}

func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(pLog), UpperBound: successor([]byte(pLog))})
	if err != nil {
		db.Close()
		return nil, err
	}
	if it.Last() {
		s.head.Store(int64(binary.BigEndian.Uint64(it.Key()[len(pLog):])))
	}
	if err := it.Close(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Head is the TS of the latest committed revision.
func (s *Store) Head() int64 { return s.head.Load() }

// seenKey indexes a stored (object, resourceVersion, deleted) triple.
func seenKey(id []byte, rv string, deleted bool) []byte {
	k := append(append(append([]byte(pSeen), id...), rv...), 0)
	if deleted {
		return append(k, 'D')
	}
	return append(k, 'L')
}

// Commit writes revisions, an optional gap and an optional recording in one
// atomic batch.
//
// A revision whose (object, RV, deleted-ness) was ever stored is skipped.
// Overlapping recordings deliver the same events at slightly different times,
// so the copy that arrives first wins and the lagging copies are dropped, even
// when they arrive after newer revisions. Relists that find nothing new are
// dropped the same way. A watch DELETED for an RV already stored as live is
// also skipped: it means the object left one recording's selector while
// another recording still sees it.
//
// Each written revision gets a unique, strictly increasing TS no earlier than
// the TS the caller set. rec.Revisions and rec.LastSync are advanced to account
// for the written revisions. Returns the revisions written.
func (s *Store) Commit(rec *api.Recording, revs []api.Revision, gap *api.Gap) ([]api.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.db.NewBatch()
	defer b.Close()
	head := s.head.Load()
	pending := map[string]bool{}
	seen := func(k []byte) (bool, error) {
		if pending[string(k)] {
			return true, nil
		}
		_, closer, err := s.db.Get(k)
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, closer.Close()
	}
	var written []api.Revision
	for i := range revs {
		r := revs[i]
		idb := identityOf(&r).bytes()
		deleted := r.Type == api.Deleted
		dup, err := seen(seenKey(idb, r.RV, deleted))
		if err != nil {
			return nil, err
		}
		if !dup && deleted && r.Source == api.SourceWatch {
			if dup, err = seen(seenKey(idb, r.RV, false)); err != nil {
				return nil, err
			}
		}
		if dup {
			continue
		}
		r.TS = max(r.TS, head+1)
		head = r.TS
		v, err := json.Marshal(&r)
		if err != nil {
			return nil, err
		}
		sk := seenKey(idb, r.RV, deleted)
		for k, val := range map[string][]byte{string(objKey(idb, r.TS)): v, string(logKey(r.TS)): idb, string(sk): be64(r.TS)} {
			if err := b.Set([]byte(k), val, nil); err != nil {
				return nil, err
			}
		}
		pending[string(sk)] = true
		written = append(written, r)
	}
	if gap != nil {
		v, err := json.Marshal(gap)
		if err != nil {
			return nil, err
		}
		k := []byte(pGap + gap.Context + "\x00" + gap.GVR + "\x00")
		k = append(append(k, be64(gap.Start)...), gap.RecordingID...)
		if err := b.Set(k, v, nil); err != nil {
			return nil, err
		}
	}
	if rec != nil {
		rec.Revisions += int64(len(written))
		if len(written) > 0 {
			rec.LastSync = max(rec.LastSync, written[len(written)-1].TS)
		}
		v, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		if err := b.Set([]byte(pRec+rec.ID), v, nil); err != nil {
			return nil, err
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	s.head.Store(head)
	return written, nil
}

// PutRecording persists a recording.
func (s *Store) PutRecording(rec *api.Recording) error {
	_, err := s.Commit(rec, nil, nil)
	return err
}

// Recordings returns every recording ever made, sorted by start time.
func (s *Store) Recordings() ([]api.Recording, error) {
	var out []api.Recording
	err := s.scan([]byte(pRec), func(_, v []byte) error {
		var r api.Recording
		if err := json.Unmarshal(v, &r); err != nil {
			return err
		}
		out = append(out, r)
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt < out[j].StartedAt })
	return out, err
}

func decodeRevision(it *pebble.Iterator) (*api.Revision, error) {
	v, err := it.ValueAndErr()
	if err != nil {
		return nil, err
	}
	var r api.Revision
	if err := json.Unmarshal(v, &r); err != nil {
		return nil, fmt.Errorf("decode revision at %q: %w", it.Key(), err)
	}
	return &r, nil
}

// History returns the revisions of objects matching f with since <= TS <=
// until and TS > after, in TS order, plus the head read before the scan.
func (s *Store) History(f Filter, since, until, after int64) ([]api.Revision, int64, error) {
	head := s.Head()
	lo := max(since, after+1)
	if lo > until {
		return nil, head, nil
	}
	var out []api.Revision
	if p, single := f.prefix(); single {
		start := append([]byte(pObj), p...)
		it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: successor(start)})
		if err != nil {
			return nil, head, err
		}
		defer it.Close()
		for ok := it.SeekGE(objKey(p, lo)); ok; ok = it.Next() {
			r, err := decodeRevision(it)
			if err != nil {
				return nil, head, err
			}
			if r.TS > until {
				break
			}
			out = append(out, *r)
		}
		return out, head, it.Error()
	}

	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: logKey(lo), UpperBound: logUpper(until)})
	if err != nil {
		return nil, head, err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		idb, err := it.ValueAndErr()
		if err != nil {
			return nil, head, err
		}
		id, err := parseIdentity(idb)
		if err != nil {
			return nil, head, err
		}
		if !f.match(id) {
			continue
		}
		ts := int64(binary.BigEndian.Uint64(it.Key()[len(pLog):]))
		v, closer, err := s.db.Get(objKey(idb, ts))
		if err != nil {
			return nil, head, fmt.Errorf("time index points at missing revision %s@%d: %w", id, ts, err)
		}
		var r api.Revision
		err = json.Unmarshal(v, &r)
		closer.Close()
		if err != nil {
			return nil, head, err
		}
		out = append(out, r)
	}
	return out, head, it.Error()
}

// logUpper is the exclusive upper bound of time-index keys with TS <= until.
func logUpper(until int64) []byte {
	if until == math.MaxInt64 {
		return successor([]byte(pLog))
	}
	return logKey(until + 1)
}

// State returns, for every object matching f, its newest revision with TS <=
// at (including DELETED ones, so callers can tell "deleted" from "never
// seen"), plus the head read before the scan.
func (s *Store) State(f Filter, at int64) ([]api.Revision, int64, error) {
	head := s.Head()
	p, _ := f.prefix()
	lo := append([]byte(pObj), p...)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: successor(lo)})
	if err != nil {
		return nil, head, err
	}
	defer it.Close()
	var out []api.Revision
	for ok := it.First(); ok; {
		k := it.Key()
		idb := bytes.Clone(k[len(pObj) : len(k)-8])
		id, err := parseIdentity(idb)
		if err != nil {
			return nil, head, err
		}
		objPrefix := append([]byte(pObj), idb...)
		if f.match(id) && it.SeekLT(objKeyAfter(idb, at)) && bytes.HasPrefix(it.Key(), objPrefix) {
			r, err := decodeRevision(it)
			if err != nil {
				return nil, head, err
			}
			out = append(out, *r)
		}
		ok = it.SeekGE(successor(objPrefix))
	}
	return out, head, it.Error()
}

// Gaps returns gaps for (context, gvr) overlapping [since, until]. Empty
// context or gvr match anything.
func (s *Store) Gaps(context, gvr string, since, until int64) ([]api.Gap, error) {
	var p []byte
	if context != "" {
		p = append(append(p, context...), 0)
		if gvr != "" {
			p = append(append(p, gvr...), 0)
		}
	}
	var out []api.Gap
	err := s.scan(append([]byte(pGap), p...), func(_, v []byte) error {
		var g api.Gap
		if err := json.Unmarshal(v, &g); err != nil {
			return err
		}
		if (gvr == "" || g.GVR == gvr) && g.End >= since && g.Start <= until {
			out = append(out, g)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out, err
}

func (s *Store) scan(prefix []byte, fn func(k, v []byte) error) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: successor(prefix)})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return errors.Join(err, it.Close())
		}
		if err := fn(it.Key(), v); err != nil {
			return errors.Join(err, it.Close())
		}
	}
	return errors.Join(it.Error(), it.Close())
}
