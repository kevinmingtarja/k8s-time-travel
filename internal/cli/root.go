// Package cli implements the kubectl-timeline command line.
package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/kevinmingtarja/kubectl-timeline/internal/api"
	"github.com/kevinmingtarja/kubectl-timeline/internal/client"
	"github.com/kevinmingtarja/kubectl-timeline/internal/store"
)

// ExitError carries a specific process exit code.
type ExitError struct {
	Code int
	Msg  string
}

func (e *ExitError) Error() string { return e.Msg }

const eventsGVR = "v1/events"

type rootOptions struct {
	home string
}

func (o *rootOptions) client() (*client.Client, error) {
	c := client.New(o.home)
	return c, c.Ensure()
}

// NewRoot builds the command tree. version is what --version prints.
func NewRoot(version string) *cobra.Command {
	o := &rootOptions{}
	root := &cobra.Command{
		Use:     "kubectl-timeline",
		Version: version,
		Annotations: map[string]string{
			cobra.CommandDisplayNameAnnotation: "kubectl timeline",
		},
		Short: "Record Kubernetes object changes and query them back in time",
		Long: `kubectl timeline records every change to selected Kubernetes objects (via list+watch) into a
local DB on disk, so you can ask what an object looked like at any moment,
what changed between two moments, and when a condition first became true.

A single daemon owns the DB; any kubectl timeline command starts it if needed.

Times accept: RFC3339 ("2026-10-12T15:04:05Z"), local wall time as printed by
kubectl timeline ("2026-10-12T15:04:05.123", "15:04:05"), relative ("-30s", "-5m"), or "now".
A printed time includes everything printed with that time: "at 15:04:05.123"
covers revisions received up to 15:04:05.123999999.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	home := os.Getenv("KUBE_TIMELINE_HOME")
	if home == "" {
		if h, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(h, ".kube-timeline")
		}
	}
	root.PersistentFlags().StringVar(&o.home, "home", home, "state directory (env KUBE_TIMELINE_HOME)")
	root.AddCommand(
		newDaemonCmd(o), newShutdownCmd(o),
		newRecordCmd(o), newLsCmd(o), newStopCmd(o),
		newHistoryCmd(o), newAtCmd(o), newDiffCmd(o),
		newWhenCmd(o), newWaitCmd(o),
	)
	return root
}

// Execute runs the CLI and returns the process exit code.
func Execute(version string) int {
	err := NewRoot(version).Execute()
	if err == nil {
		return 0
	}
	var ee *ExitError
	if errors.As(err, &ee) {
		if ee.Msg != "" {
			fmt.Fprintln(os.Stderr, ee.Msg)
		}
		return ee.Code
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	return 2
}

// parseTime parses a user time into the window [lo, hi] it denotes: a time
// printed with millisecond precision covers that whole millisecond.
func parseTime(s string, now time.Time) (int64, int64, error) {
	s = strings.TrimSpace(s)
	if s == "now" {
		return now.UnixNano(), now.UnixNano(), nil
	}
	if strings.HasPrefix(s, "-") {
		d, err := time.ParseDuration(s[1:])
		if err != nil {
			return 0, 0, fmt.Errorf("bad relative time %q: %w", s, err)
		}
		t := now.Add(-d).UnixNano()
		return t, t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return window(t, s)
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return window(t, s)
		}
	}
	for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			y, m, d := now.Date()
			t = time.Date(y, m, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
			return window(t, s)
		}
	}
	return 0, 0, fmt.Errorf("cannot parse time %q (try 15:04:05, 2026-10-12T15:04:05.123, -5m, now)", s)
}

// window widens t to the end of the precision s was written with.
func window(t time.Time, s string) (int64, int64, error) {
	unit := time.Second
	if strings.Count(s, ":") == 1 && !strings.Contains(s, "T") && !strings.Contains(s, " ") {
		unit = time.Minute
	}
	if i := strings.LastIndex(s, "."); i >= 0 {
		digits := 0
		for _, c := range s[i+1:] {
			if c < '0' || c > '9' {
				break
			}
			digits++
		}
		unit = time.Second
		for range min(digits, 9) {
			unit /= 10
		}
	}
	return t.UnixNano(), t.UnixNano() + int64(unit) - 1, nil
}

func fmtTS(ts int64) string {
	return time.Unix(0, ts).Local().Format("2006-01-02T15:04:05.000")
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// target is what a query is about: one resource (or every recorded one), an
// optional name and namespace, across the matching recordings.
type target struct {
	recs      []api.Recording             // recordings of the target resource(s)
	allRecs   []api.Recording             // every recording, for events lookups
	infos     map[string]api.ResourceInfo // by GVR
	name      string
	namespace string
	context   string
}

// filters returns one store filter per (context, gvr) the target covers.
func (t *target) filters() []store.Filter {
	seen := map[string]bool{}
	var out []store.Filter
	for _, r := range t.recs {
		k := r.Spec.Context + "\x00" + r.Info.GVR()
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, store.Filter{Context: r.Spec.Context, GVR: r.Info.GVR(), Namespace: t.namespace, Name: t.name})
	}
	return out
}

func (t *target) contexts() []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range t.recs {
		if !seen[r.Spec.Context] {
			seen[r.Spec.Context] = true
			out = append(out, r.Spec.Context)
		}
	}
	sort.Strings(out)
	return out
}

// label renders an object identity, e.g. "pod default/foo".
func (t *target) label(r api.Revision, multiContext bool) string {
	kind := r.GVR
	if info, ok := t.infos[r.GVR]; ok {
		kind = strings.ToLower(info.Kind)
	}
	s := kind + " " + r.Name
	if r.Namespace != "" {
		s = kind + " " + r.Namespace + "/" + r.Name
	}
	if multiContext {
		s = "[" + r.Context + "] " + s
	}
	return s
}

// resolveTarget matches "pod", "pods/foo", "deploy/web" etc. against the
// resources that have been recorded. An empty arg means every recorded
// resource except Events (which are shown interleaved instead).
func resolveTarget(c *client.Client, arg, namespace, context string) (*target, error) {
	recs, err := c.Recordings()
	if err != nil {
		return nil, err
	}
	t := &target{allRecs: recs, infos: map[string]api.ResourceInfo{}, namespace: namespace, context: context}
	for _, r := range recs {
		t.infos[r.Info.GVR()] = r.Info
	}
	token := arg
	if i := strings.Index(arg, "/"); i >= 0 && strings.Count(arg, "/") == 1 {
		token, t.name = arg[:i], arg[i+1:]
	}
	gvrs := map[string]bool{}
	for _, r := range recs {
		if context != "" && r.Spec.Context != context {
			continue
		}
		if arg == "" && r.Info.GVR() == eventsGVR {
			continue
		}
		if arg != "" && !r.Info.Matches(token) {
			continue
		}
		t.recs = append(t.recs, r)
		gvrs[r.Info.GVR()] = true
	}
	if len(t.recs) == 0 {
		var have []string
		for _, r := range recs {
			have = append(have, r.Info.GVR()+" on "+r.Spec.Context)
		}
		msg := "no recording covers " + strings.TrimSpace(token+" "+context)
		if len(have) > 0 {
			msg += "; recorded: " + strings.Join(have, ", ")
		} else {
			msg += "; start one with: kubectl timeline record <resource> --context <ctx>"
		}
		return nil, errors.New(msg)
	}
	if arg != "" && len(gvrs) > 1 {
		var names []string
		for g := range gvrs {
			names = append(names, g)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("%q is ambiguous: %s; use group/version/resource", token, strings.Join(names, ", "))
	}
	return t, nil
}

// coverageNotes explains, for each recording, whether time t was observed.
func coverageNotes(recs []api.Recording, gaps []api.Gap, lo, hi int64) []string {
	var notes []string
	for _, r := range recs {
		if hi < r.StartedAt {
			notes = append(notes, fmt.Sprintf("recording %s (%s) started at %s; nothing before that was observed", r.ID, r.Info.GVR(), fmtTS(r.StartedAt)))
		}
		if r.State == api.StateStopped && lo > r.StoppedAt {
			notes = append(notes, fmt.Sprintf("recording %s (%s) stopped at %s; nothing after that was observed", r.ID, r.Info.GVR(), fmtTS(r.StoppedAt)))
		}
		if r.State == api.StateError {
			notes = append(notes, fmt.Sprintf("recording %s (%s) is failing: %s", r.ID, r.Info.GVR(), r.LastError))
		}
	}
	for _, g := range gaps {
		notes = append(notes, "GAP "+gapLine(g))
	}
	return notes
}

func gapLine(g api.Gap) string {
	return fmt.Sprintf("%s → %s in recording %s (%s): %s; changes inside this window may be missing, and revisions marked [list] at its end happened at unknown times within it",
		fmtTS(g.Start), fmtTS(g.End), g.RecordingID, g.GVR, g.Reason)
}

// gapsFor fetches the gaps of the target's recordings overlapping [lo, hi].
func gapsFor(c *client.Client, t *target, lo, hi int64) ([]api.Gap, error) {
	ids := map[string]bool{}
	for _, r := range t.recs {
		ids[r.ID] = true
	}
	all, err := c.Gaps(t.context, "", lo, hi)
	if err != nil {
		return nil, err
	}
	var out []api.Gap
	for _, g := range all {
		if ids[g.RecordingID] {
			out = append(out, g)
		}
	}
	return out, nil
}
