package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
	"github.com/kevinmingtarja/k8s-time-travel/internal/client"
	"github.com/kevinmingtarja/k8s-time-travel/internal/render"
	"github.com/kevinmingtarja/k8s-time-travel/internal/store"
)

const valueLimit = 200

func idKey(r api.Revision) string {
	return r.Context + "\x00" + r.GVR + "\x00" + r.Namespace + "\x00" + r.Name
}

func decode(r api.Revision) map[string]any {
	var m map[string]any
	if err := json.Unmarshal(r.Object, &m); err != nil {
		return map[string]any{"_decodeError": err.Error()}
	}
	return m
}

type queryFlags struct {
	namespace string
	context   string
}

func (q *queryFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&q.namespace, "namespace", "n", "", "only this namespace (default: all recorded)")
	cmd.Flags().StringVar(&q.context, "context", "", "only this context (default: all recorded)")
}

func newHistoryCmd(o *rootOptions) *cobra.Command {
	var (
		q            queryFlags
		since, until string
		output       string
		full         bool
		noEvents     bool
	)
	cmd := &cobra.Command{
		Use:   "history [resource[/name]]",
		Short: "Show every recorded revision as a field-level diff, with Events and gaps interleaved",
		Example: `  ktt history pod/web-0
  ktt history pods -n default --since -10m
  ktt history deploy/web --since 15:04:00 --until 15:05:00 -o json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := o.client()
			if err != nil {
				return err
			}
			t, err := resolveTarget(c, strings.Join(args, ""), q.namespace, q.context)
			if err != nil {
				return err
			}
			lo, hi, err := timeRange(since, until)
			if err != nil {
				return err
			}

			var revs []api.Revision
			prev := map[string]api.Revision{}
			for _, f := range t.filters() {
				resp, err := c.History(f, lo, hi, -1)
				if err != nil {
					return err
				}
				revs = append(revs, resp.Revisions...)
				if lo > 0 {
					st, err := c.State(f, lo-1)
					if err != nil {
						return err
					}
					for _, r := range st.Revisions {
						prev[idKey(r)] = r
					}
				}
			}
			var events []api.Revision
			if !noEvents {
				uids := map[string]map[string]bool{}
				for _, rs := range [][]api.Revision{revs, slices.Collect(maps.Values(prev))} {
					for _, r := range rs {
						if uids[idKey(r)] == nil {
							uids[idKey(r)] = map[string]bool{}
						}
						uids[idKey(r)][r.UID] = true
					}
				}
				if events, err = eventsFor(c, t, lo, hi, uids); err != nil {
					return err
				}
			}
			gaps, err := gapsFor(c, t, lo, hi)
			if err != nil {
				return err
			}

			if output == "json" {
				return printJSON(map[string]any{"recordings": t.recs, "revisions": nonNil(revs), "events": nonNil(events), "gaps": nonNil(gaps)})
			}
			if output != "" {
				return fmt.Errorf("unknown output %q (want json)", output)
			}
			limit := valueLimit
			if full {
				limit = 0
			}
			multi := len(t.contexts()) > 1
			for _, r := range t.recs {
				fmt.Printf("# recording %s: %s, since %s (%s)\n", r.ID, describeRec(r), fmtTS(r.StartedAt), r.State)
			}
			for _, n := range coverageNotes(t.recs, nil, lo, hi) {
				if !strings.Contains(n, "nothing before") {
					fmt.Println("# " + n)
				}
			}

			type item struct {
				ts    int64
				rev   *api.Revision
				event *api.Revision
				gap   *api.Gap
			}
			var items []item
			for i := range revs {
				items = append(items, item{ts: revs[i].TS, rev: &revs[i]})
			}
			for i := range events {
				items = append(items, item{ts: events[i].TS, event: &events[i]})
			}
			for i := range gaps {
				items = append(items, item{ts: gaps[i].Start, gap: &gaps[i]})
			}
			sort.SliceStable(items, func(i, j int) bool { return items[i].ts < items[j].ts })
			if len(items) == 0 {
				fmt.Println("no changes recorded in this range")
				return nil
			}
			for _, it := range items {
				switch {
				case it.gap != nil:
					fmt.Printf("%s  %-8s  %s\n", fmtTS(it.gap.Start), "GAP", gapLine(*it.gap))
				case it.event != nil:
					obj := decode(*it.event)
					line := fmt.Sprintf("%s  %-8s  %s  %s", fmtTS(it.event.TS), "EVENT", involvedLabel(t, obj, multi), render.EventLine(obj))
					if it.event.Source == api.SourceList {
						line += "  [list"
						if at, ok := render.EventTime(obj); ok {
							line += ": last seen " + fmtTS(at.UnixNano())
						}
						line += "]"
					}
					fmt.Println(line)
				default:
					r := *it.rev
					var before map[string]any
					p, hasPrev := prev[idKey(r)]
					if hasPrev {
						before = decode(p)
					}
					printRevision(t, r, before, hasPrev && p.UID != r.UID, multi, limit)
					prev[idKey(r)] = r
				}
			}
			return nil
		},
	}
	q.register(cmd)
	cmd.Flags().StringVar(&since, "since", "", "start time (default: beginning of recording)")
	cmd.Flags().StringVar(&until, "until", "", "end time (default: now)")
	cmd.Flags().StringVarP(&output, "output", "o", "", "json for raw revisions, events and gaps")
	cmd.Flags().BoolVar(&full, "full", false, "do not truncate long values")
	cmd.Flags().BoolVar(&noEvents, "no-events", false, "omit Events")
	return cmd
}

func printRevision(t *target, r api.Revision, before map[string]any, newUID, multi bool, limit int) {
	after := decode(r)
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %-8s  %s  rv=%s", fmtTS(r.TS), r.Type, t.label(r, multi), r.RV)
	// An object first seen in a list could have been written by anyone, ever.
	firstListed := before == nil && r.Source == api.SourceList
	if w := render.Writers(before, after); len(w) > 0 && r.Type != api.Deleted && !firstListed {
		fmt.Fprintf(&b, "  by %s", strings.Join(w, ", "))
	}
	if r.Source == api.SourceList {
		b.WriteString("  [list]")
	}
	if newUID && r.Type == api.Added {
		b.WriteString("  (recreated: new uid)")
	}
	if r.Type != api.Modified || before == nil {
		fmt.Fprintf(&b, "  %s", render.Summary(after))
	}
	if r.Note != "" {
		fmt.Fprintf(&b, "  (%s)", r.Note)
	}
	fmt.Println(b.String())
	if r.Type != api.Modified || before == nil {
		return
	}
	changes := render.Diff(before, after)
	if len(changes) == 0 {
		fmt.Println("    (only resourceVersion/managedFields changed)")
	}
	for _, c := range changes {
		fmt.Println("    " + render.FormatChange(c, limit))
	}
}

// eventsFor returns the recorded Events about the objects in uids (identity
// key -> UIDs the object had during the query window). Matching on UID drops
// Events about objects outside the label selector and about earlier objects
// that had the same name.
func eventsFor(c *client.Client, t *target, lo, hi int64, uids map[string]map[string]bool) ([]api.Revision, error) {
	kindGVR := map[string]string{}
	for _, r := range t.recs {
		if r.Info.GVR() == eventsGVR {
			return nil, nil
		}
		kindGVR[r.Info.Kind] = r.Info.GVR()
	}
	var out []api.Revision
	for _, ctx := range t.contexts() {
		recorded := false
		for _, r := range t.allRecs {
			recorded = recorded || (r.Spec.Context == ctx && r.Info.GVR() == eventsGVR)
		}
		if !recorded {
			continue
		}
		resp, err := c.History(store.Filter{Context: ctx, GVR: eventsGVR, Namespace: t.namespace}, lo, hi, -1)
		if err != nil {
			return nil, err
		}
		for _, ev := range resp.Revisions {
			if ev.Type == api.Deleted {
				continue // Event TTL expiry, not something that happened to the object
			}
			obj := decode(ev)
			kind, ns, name := involved(obj)
			gvr, ok := kindGVR[kind]
			if !ok || (t.name != "" && name != t.name) {
				continue
			}
			known := uids[idKey(api.Revision{Context: ctx, GVR: gvr, Namespace: ns, Name: name})]
			if uid := involvedUID(obj); known[uid] || (uid == "" && len(known) > 0) {
				out = append(out, ev)
			}
		}
	}
	return out, nil
}

func involved(ev map[string]any) (kind, namespace, name string) {
	io, _ := ev["involvedObject"].(map[string]any)
	kind, _ = io["kind"].(string)
	namespace, _ = io["namespace"].(string)
	name, _ = io["name"].(string)
	return
}

func involvedUID(ev map[string]any) string {
	io, _ := ev["involvedObject"].(map[string]any)
	uid, _ := io["uid"].(string)
	return uid
}

func involvedLabel(t *target, ev map[string]any, multi bool) string {
	kind, ns, name := involved(ev)
	gvr := ""
	for g, info := range t.infos {
		if info.Kind == kind && g != eventsGVR {
			gvr = g
		}
	}
	return t.label(api.Revision{GVR: gvr, Namespace: ns, Name: name}, multi)
}

func timeRange(since, until string) (int64, int64, error) {
	now := time.Now()
	lo, hi := int64(0), int64(math.MaxInt64)
	var err error
	if since != "" {
		if lo, _, err = parseTime(since, now); err != nil {
			return 0, 0, err
		}
	}
	if until != "" {
		if _, hi, err = parseTime(until, now); err != nil {
			return 0, 0, err
		}
	}
	if lo > hi {
		return 0, 0, fmt.Errorf("--since is after --until")
	}
	return lo, hi, nil
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func newAtCmd(o *rootOptions) *cobra.Command {
	var (
		q                 queryFlags
		output            string
		showManagedFields bool
	)
	cmd := &cobra.Command{
		Use:   "at <time> [resource[/name]]",
		Short: "Show recorded objects as they were at a point in time",
		Example: `  ktt at 15:04:05.123 pods -n default
  ktt at -2m pod/web-0 -o yaml`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, at, err := parseTime(args[0], time.Now())
			if err != nil {
				return err
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			t, err := resolveTarget(c, strings.Join(args[1:], ""), q.namespace, q.context)
			if err != nil {
				return err
			}
			var live []api.Revision
			for _, f := range t.filters() {
				st, err := c.State(f, at)
				if err != nil {
					return err
				}
				for _, r := range st.Revisions {
					if r.Type != api.Deleted {
						live = append(live, r)
					}
				}
			}
			sort.Slice(live, func(i, j int) bool { return idKey(live[i]) < idKey(live[j]) })
			gaps, err := gapsFor(c, t, at, at)
			if err != nil {
				return err
			}
			notes := coverageNotes(t.recs, gaps, at, at)

			switch output {
			case "json":
				var objs []map[string]any
				for _, r := range live {
					objs = append(objs, maybeStrip(decode(r), showManagedFields))
				}
				return printJSON(map[string]any{"at": at, "objects": nonNil(objs), "notes": nonNil(notes)})
			case "yaml":
				for _, n := range notes {
					fmt.Println("# " + n)
				}
				for i, r := range live {
					if i > 0 {
						fmt.Println("---")
					}
					b, err := yaml.Marshal(maybeStrip(decode(r), showManagedFields))
					if err != nil {
						return err
					}
					fmt.Printf("# %s rv=%s, revision of %s\n%s", t.label(r, false), r.RV, fmtTS(r.TS), b)
				}
			case "":
				for _, n := range notes {
					fmt.Println("# " + n)
				}
				if len(live) > 0 {
					multi := len(t.contexts()) > 1
					w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
					fmt.Fprintln(w, "OBJECT\tSTATUS\tLAST CHANGE\tRV")
					for _, r := range live {
						fmt.Fprintf(w, "%s\t%s\t%s before (%s)\t%s\n", t.label(r, multi), render.Summary(decode(r)), fmtDur(time.Duration(at-r.TS)), fmtTS(r.TS), r.RV)
					}
					w.Flush()
				}
			default:
				return fmt.Errorf("unknown output %q (want yaml or json)", output)
			}
			if len(live) == 0 {
				msg, err := absence(c, t, at)
				if err != nil {
					return err
				}
				fmt.Println(msg)
			}
			return nil
		},
	}
	q.register(cmd)
	cmd.Flags().StringVarP(&output, "output", "o", "", "yaml or json (default: table)")
	cmd.Flags().BoolVar(&showManagedFields, "show-managed-fields", false, "keep metadata.managedFields in yaml/json")
	return cmd
}

func maybeStrip(obj map[string]any, keep bool) map[string]any {
	if keep {
		return obj
	}
	return render.StripManagedFields(obj)
}

// absence explains why nothing matched at time at.
func absence(c *client.Client, t *target, at int64) (string, error) {
	if t.name == "" {
		return fmt.Sprintf("no recorded objects existed at %s", fmtTS(at)), nil
	}
	for _, f := range t.filters() {
		st, err := c.State(f, at)
		if err != nil {
			return "", err
		}
		// Only DELETED revisions can remain here, since nothing was live.
		for _, r := range st.Revisions {
			if r.Source == api.SourceList {
				return fmt.Sprintf("%s did not exist at %s: a relist at %s found it gone; it was deleted at an unknown earlier time (see the GAP in 'ktt history')", t.label(r, false), fmtTS(at), fmtTS(r.TS)), nil
			}
			return fmt.Sprintf("%s did not exist at %s: deleted at %s", t.label(r, false), fmtTS(at), fmtTS(r.TS)), nil
		}
		later, err := c.History(f, at+1, math.MaxInt64, -1)
		if err != nil {
			return "", err
		}
		if len(later.Revisions) > 0 {
			r := later.Revisions[0]
			return fmt.Sprintf("%s did not exist at %s: first observed at %s", t.label(r, false), fmtTS(at), fmtTS(r.TS)), nil
		}
	}
	return fmt.Sprintf("%s was never observed by the recordings", t.name), nil
}

func newDiffCmd(o *rootOptions) *cobra.Command {
	var (
		q    queryFlags
		full bool
	)
	cmd := &cobra.Command{
		Use:   "diff <t1> <t2> [resource[/name]]",
		Short: "Show the net change between two times, and how many revisions happened in between",
		Long: `Show the net change of each object between t1 and t2. Objects that changed
and changed back still appear, with their revision count, because the net diff
hides transient states; use 'ktt history' to see every intermediate revision.`,
		Example: `  ktt diff 15:04:00 15:05:00 pods -n default
  ktt diff -10m now deploy/web`,
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			now := time.Now()
			_, t1, err := parseTime(args[0], now)
			if err != nil {
				return err
			}
			_, t2, err := parseTime(args[1], now)
			if err != nil {
				return err
			}
			if t1 > t2 {
				return fmt.Errorf("t1 is after t2")
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			t, err := resolveTarget(c, strings.Join(args[2:], ""), q.namespace, q.context)
			if err != nil {
				return err
			}
			s1, s2 := map[string]api.Revision{}, map[string]api.Revision{}
			between := map[string][]api.Revision{}
			for _, f := range t.filters() {
				for _, pair := range []struct {
					at  int64
					out map[string]api.Revision
				}{{t1, s1}, {t2, s2}} {
					st, err := c.State(f, pair.at)
					if err != nil {
						return err
					}
					for _, r := range st.Revisions {
						if r.Type != api.Deleted {
							pair.out[idKey(r)] = r
						}
					}
				}
				h, err := c.History(f, t1+1, t2, -1)
				if err != nil {
					return err
				}
				for _, r := range h.Revisions {
					between[idKey(r)] = append(between[idKey(r)], r)
				}
			}
			gaps, err := gapsFor(c, t, t1, t2)
			if err != nil {
				return err
			}
			for _, n := range coverageNotes(t.recs, gaps, t1, t2) {
				fmt.Println("# " + n)
			}

			keys := map[string]bool{}
			for _, m := range []map[string]api.Revision{s1, s2} {
				for k := range m {
					keys[k] = true
				}
			}
			for k := range between {
				keys[k] = true
			}
			var sorted []string
			for k := range keys {
				sorted = append(sorted, k)
			}
			sort.Strings(sorted)
			limit := valueLimit
			if full {
				limit = 0
			}
			multi := len(t.contexts()) > 1
			printed, transient := 0, false
			for _, k := range sorted {
				a, inA := s1[k]
				b, inB := s2[k]
				revs := between[k]
				n := len(revs)
				counted := fmt.Sprintf("%d revisions", n)
				if n == 1 {
					counted = "1 revision"
				}
				switch {
				case inA && inB:
					if n == 0 {
						continue
					}
					changes := render.Diff(decode(a), decode(b))
					if a.UID != b.UID {
						counted += ", deleted and recreated: new uid"
					}
					if len(changes) == 0 {
						counted += ", net: no change"
					}
					fmt.Printf("~ %s  (%s)\n", t.label(b, multi), counted)
					for _, ch := range changes {
						fmt.Println("    " + render.FormatChange(ch, limit))
					}
				case inB:
					fmt.Printf("+ %s  first observed %s, now %s  (%s)\n", t.label(b, multi), fmtTS(revs[0].TS), render.Summary(decode(b)), counted)
				case inA:
					fmt.Printf("- %s  deleted %s  (%s)\n", t.label(a, multi), fmtTS(revs[n-1].TS), counted)
				default:
					fmt.Printf("± %s  appeared %s and was deleted %s  (%s)\n", t.label(revs[0], multi), fmtTS(revs[0].TS), fmtTS(revs[n-1].TS), counted)
				}
				printed++
				transient = transient || n > 1
			}
			if printed == 0 {
				fmt.Printf("no changes between %s and %s\n", fmtTS(t1), fmtTS(t2))
			} else if transient {
				fmt.Println("# intermediate states are not shown; run 'ktt history' to see every revision")
			}
			return nil
		},
	}
	q.register(cmd)
	cmd.Flags().BoolVar(&full, "full", false, "do not truncate long values")
	return cmd
}
