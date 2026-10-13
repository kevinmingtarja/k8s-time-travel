package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"github.com/spf13/cobra"

	"github.com/kevinmingtarja/kubectl-timeline/internal/api"
	"github.com/kevinmingtarja/kubectl-timeline/internal/render"
)

const predicateHelp = `Predicates are CEL expressions over:
  object   the object as recorded (map; integers are ints)
  deleted  true for the revision that records a deletion

Missing fields are errors in CEL; guard with has() or optional syntax:
  object.status.phase == "Running"
  object.?status.?phase.orValue("") == "Failed"
  object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") == "CrashLoopBackOff")
  has(object.metadata.deletionTimestamp)
  object.metadata.?finalizers.orValue([]).size() == 0
A revision on which the predicate errors counts as false; the command reports how many
errored and the first error.`

type predicate struct {
	expr   string
	prg    cel.Program
	errors int
	first  string
}

func compilePredicate(expr string) (*predicate, error) {
	env, err := cel.NewEnv(
		cel.Variable("object", cel.DynType),
		cel.Variable("deleted", cel.BoolType),
		cel.OptionalTypes(),
	)
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("predicate %q: %w", expr, iss.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("predicate %q returns %s, want bool", expr, t)
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &predicate{expr: expr, prg: prg}, nil
}

// eval returns the predicate's value on a revision; evaluation errors count
// as false and are tallied.
func (p *predicate) eval(r api.Revision) bool {
	obj, err := decodeForCEL(r.Object)
	if err == nil {
		var out any
		val, _, evalErr := p.prg.Eval(map[string]any{"object": obj, "deleted": r.Type == api.Deleted})
		err = evalErr
		if err == nil {
			out = val.Value()
			if b, ok := out.(bool); ok {
				return b
			}
			err = fmt.Errorf("evaluated to %T, want bool", out)
		}
	}
	p.errors++
	if p.first == "" {
		p.first = fmt.Sprintf("%s rv=%s: %v", fmtTS(r.TS), r.RV, err)
	}
	return false
}

func (p *predicate) report() {
	if p.errors > 0 {
		fmt.Fprintf(os.Stderr, "warning: predicate %q errored on %d revisions (counted as false); first: %s\n", p.expr, p.errors, p.first)
	}
}

// decodeForCEL decodes JSON keeping integers as int64, so CEL comparisons
// like restartCount > 3 type-check against int literals.
func decodeForCEL(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return convertNumbers(v), nil
}

func convertNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			t[k] = convertNumbers(e)
		}
	case []any:
		for i, e := range t {
			t[i] = convertNumbers(e)
		}
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	}
	return v
}

func newWhenCmd(o *rootOptions) *cobra.Command {
	var (
		q            queryFlags
		since, until string
		first        bool
	)
	cmd := &cobra.Command{
		Use:   "when <resource[/name]> <predicate>",
		Short: "Find when a predicate became true (and false) on recorded objects",
		Long: `Evaluate a predicate on every recorded revision and print each transition.
Exit status: 0 if it was ever true, 1 if never, 2 on error.

` + predicateHelp,
		Example: `  kubectl timeline when pod/web-0 'object.status.phase == "Failed"'
  kubectl timeline when pods -n default 'object.?status.?containerStatuses.orValue([]).exists(c, c.restartCount > 0)' --first`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			pred, err := compilePredicate(args[1])
			if err != nil {
				return err
			}
			lo, hi, err := timeRange(since, until)
			if err != nil {
				return err
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			t, err := resolveTarget(c, args[0], q.namespace, q.context)
			if err != nil {
				return err
			}
			gaps, err := gapsFor(c, t, lo, hi)
			if err != nil {
				return err
			}
			for _, n := range coverageNotes(t.recs, gaps, lo, hi) {
				if !strings.Contains(n, "nothing before") {
					fmt.Println("# " + n)
				}
			}

			byObject := map[string][]api.Revision{}
			state := map[string]bool{}
			var order []string
			for _, f := range t.filters() {
				if lo > 0 {
					st, err := c.State(f, lo-1)
					if err != nil {
						return err
					}
					for _, r := range st.Revisions {
						state[idKey(r)] = r.Type != api.Deleted && pred.eval(r)
					}
				}
				h, err := c.History(f, lo, hi, -1)
				if err != nil {
					return err
				}
				for _, r := range h.Revisions {
					k := idKey(r)
					if _, ok := byObject[k]; !ok {
						order = append(order, k)
					}
					byObject[k] = append(byObject[k], r)
				}
			}
			type transition struct {
				r    api.Revision
				to   bool
				note string
			}
			var out []transition
			everTrue := false
			for k, v := range state {
				if v {
					everTrue = true
					fmt.Printf("# %s was already true at --since\n", strings.ReplaceAll(k, "\x00", " "))
				}
			}
			for _, k := range order {
				cur, seen := state[k]
				for i, r := range byObject[k] {
					v := pred.eval(r)
					if seen && v == cur {
						continue
					}
					note := ""
					if v && !seen && i == 0 && lo == 0 {
						note = "already true when first observed"
						if r.Source == api.SourceList {
							note += " (initial list: it may have become true earlier)"
						}
					} else if r.Source == api.SourceList {
						note = "seen on a relist after a gap: the change happened at an unknown time within it"
					}
					if v || seen {
						out = append(out, transition{r, v, note})
					}
					cur, seen = v, true
					everTrue = everTrue || v
				}
			}
			sort.SliceStable(out, func(i, j int) bool { return out[i].r.TS < out[j].r.TS })
			multi := len(t.contexts()) > 1
			for _, tr := range out {
				if first && !tr.to {
					continue
				}
				line := fmt.Sprintf("%s  %-5v  %s  (%s rv=%s)  %s", fmtTS(tr.r.TS), tr.to, t.label(tr.r, multi), tr.r.Type, tr.r.RV, render.Summary(decode(tr.r)))
				if tr.note != "" {
					line += "  [" + tr.note + "]"
				}
				fmt.Println(line)
				if first {
					break
				}
			}
			pred.report()
			if !everTrue {
				total := 0
				for _, revs := range byObject {
					total += len(revs)
				}
				return &ExitError{Code: 1, Msg: fmt.Sprintf("never true across %d revisions of %d objects", total, len(byObject))}
			}
			return nil
		},
	}
	q.register(cmd)
	cmd.Flags().StringVar(&since, "since", "", "start time (default: beginning of recording)")
	cmd.Flags().StringVar(&until, "until", "", "end time (default: now)")
	cmd.Flags().BoolVar(&first, "first", false, "print only the first time it became true")
	return cmd
}

func newWaitCmd(o *rootOptions) *cobra.Command {
	var (
		q       queryFlags
		forExpr string
		failOn  string
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "wait <resource/name> --for <predicate> [--fail-on <predicate>]",
		Short: "Block until a recorded object satisfies a predicate, failing fast on another",
		Long: `Wait on the recording rather than polling the cluster: the predicates are
evaluated on every revision in order, so a state that lasts a few milliseconds
still counts. The current state is checked first.

Exit status: 0 when --for matches, 1 when --fail-on matches, 2 on error
(including the recording failing or stopping), 124 on timeout.

` + predicateHelp,
		Example: `  kubectl timeline wait pod/web-0 --for 'object.?status.?phase.orValue("") == "Running"' \
    --fail-on 'deleted || object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") in ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"])'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if forExpr == "" {
				return fmt.Errorf("--for is required")
			}
			forP, err := compilePredicate(forExpr)
			if err != nil {
				return err
			}
			var failP *predicate
			if failOn != "" {
				if failP, err = compilePredicate(failOn); err != nil {
					return err
				}
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			t, err := resolveTarget(c, args[0], q.namespace, q.context)
			if err != nil {
				return err
			}
			if t.name == "" {
				return fmt.Errorf("wait needs a single object: <resource>/<name>")
			}
			if err := recordingsHealthy(t.recs); err != nil {
				return err
			}
			filters := t.filters()
			if len(filters) != 1 {
				return fmt.Errorf("%s is recorded on %d contexts; pass --context", args[0], len(filters))
			}
			f := filters[0]
			multi := false
			matched := func(r api.Revision) error {
				line := fmt.Sprintf("%s  %s  %s  rv=%s  %s", fmtTS(r.TS), r.Type, t.label(r, multi), r.RV, render.Summary(decode(r)))
				if failP != nil && failP.eval(r) {
					failP.report()
					return &ExitError{Code: 1, Msg: line + "\n--fail-on matched"}
				}
				if forP.eval(r) {
					fmt.Println(line)
					return &ExitError{Code: 0}
				}
				return nil
			}

			st, err := c.State(f, math.MaxInt64)
			if err != nil {
				return err
			}
			if namespaces := distinctNamespaces(st.Revisions); len(namespaces) > 1 {
				return fmt.Errorf("%s exists in namespaces %s; pass -n", t.name, strings.Join(namespaces, ", "))
			}
			for _, r := range st.Revisions {
				if err := matched(r); err != nil {
					return exitOK(err)
				}
			}
			cursor := st.Head
			started := time.Now()
			deadline := started.Add(timeout)
			lastHealth := started
			warned := map[string]bool{}
			var last *api.Revision
			if len(st.Revisions) > 0 {
				last = &st.Revisions[0]
			}
			for {
				resp, err := c.History(f, 0, math.MaxInt64, cursor)
				if err != nil {
					return err
				}
				for i := range resp.Revisions {
					r := resp.Revisions[i]
					if f.Namespace == "" && last != nil && r.Namespace != last.Namespace {
						return fmt.Errorf("%s now exists in namespaces %s and %s; pass -n", t.name, last.Namespace, r.Namespace)
					}
					last = &r
					if err := matched(r); err != nil {
						return exitOK(err)
					}
				}
				cursor = resp.Head
				if time.Since(lastHealth) > time.Second {
					lastHealth = time.Now()
					if err := checkHealth(c, t, started.UnixNano(), warned); err != nil {
						return err
					}
				}
				if time.Now().After(deadline) {
					forP.report()
					msg := fmt.Sprintf("timed out after %s", timeout)
					if last != nil {
						msg += fmt.Sprintf("; last revision %s %s rv=%s: %s", fmtTS(last.TS), last.Type, last.RV, render.Summary(decode(*last)))
					} else {
						msg += "; the object was never observed"
					}
					return &ExitError{Code: 124, Msg: msg}
				}
				time.Sleep(100 * time.Millisecond)
			}
		},
	}
	q.register(cmd)
	cmd.Flags().StringVar(&forExpr, "for", "", "predicate to wait for (required)")
	cmd.Flags().StringVar(&failOn, "fail-on", "", "predicate that fails the wait immediately")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "give up after this long")
	return cmd
}

// exitOK turns the success sentinel into nil.
func exitOK(err error) error {
	if ee, ok := err.(*ExitError); ok && ee.Code == 0 {
		return nil
	}
	return err
}

func distinctNamespaces(revs []api.Revision) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range revs {
		if !seen[r.Namespace] {
			seen[r.Namespace] = true
			out = append(out, r.Namespace)
		}
	}
	sort.Strings(out)
	return out
}

// staleAfter is how long a recording may go without syncing while in the
// error state before a wait gives up on it; shorter errors are retried
// relists that usually recover.
const staleAfter = 10 * time.Second

func recordingsHealthy(recs []api.Recording) error {
	running := false
	for _, r := range recs {
		switch r.State {
		case api.StateRunning:
			running = true
		case api.StateError:
			if time.Since(time.Unix(0, r.LastSync)) > staleAfter {
				return fmt.Errorf("recording %s (%s) is failing and last synced %s: %s", r.ID, r.Info.GVR(), fmtTS(r.LastSync), r.LastError)
			}
			running = true
		}
	}
	if !running {
		return fmt.Errorf("no running recording covers this object (see 'kubectl timeline ls')")
	}
	return nil
}

// checkHealth fails if a recording broke during the wait, and warns once per
// gap that opened since the wait started.
func checkHealth(c interface {
	Recordings() ([]api.Recording, error)
	Gaps(string, string, int64, int64) ([]api.Gap, error)
}, t *target, since int64, warned map[string]bool) error {
	recs, err := c.Recordings()
	if err != nil {
		return err
	}
	ids := map[string]bool{}
	var mine []api.Recording
	for _, r := range recs {
		for _, tr := range t.recs {
			if r.ID == tr.ID {
				mine = append(mine, r)
				ids[r.ID] = true
			}
		}
	}
	if err := recordingsHealthy(mine); err != nil {
		return err
	}
	gaps, err := c.Gaps(t.context, "", since, math.MaxInt64)
	if err != nil {
		return err
	}
	for _, g := range gaps {
		k := fmt.Sprint(g.RecordingID, g.Start)
		if ids[g.RecordingID] && !warned[k] {
			warned[k] = true
			fmt.Fprintln(os.Stderr, "warning: GAP "+gapLine(g))
		}
	}
	return nil
}
