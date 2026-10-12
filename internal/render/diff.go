// Package render turns stored revisions into compact, diff-first text.
package render

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Change is one field-level difference between two object states.
type Change struct {
	Path string
	Op   byte // '+' added, '-' removed, '~' changed
	Old  any
	New  any
}

// ignoredPaths change on every write and carry no state of their own.
// Writers() reports who wrote a revision from managedFields instead.
var ignoredPaths = map[string]bool{
	"metadata.resourceVersion": true,
	"metadata.managedFields":   true,
}

// listKeys are tried in order to match list elements by identity, so
// "status.conditions[type=Ready].status" diffs cleanly when the list reorders
// or grows.
var listKeys = []string{"name", "type"}

// Diff returns the field-level changes from a to b, sorted by path.
func Diff(a, b map[string]any) []Change {
	var out []Change
	walk("", a, b, &out)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func walk(path string, a, b any, out *[]Change) {
	if ignoredPaths[path] {
		return
	}
	am, aIsMap := a.(map[string]any)
	bm, bIsMap := b.(map[string]any)
	if aIsMap && bIsMap {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		for k := range keys {
			p := joinKey(path, k)
			if ignoredPaths[p] {
				continue
			}
			av, inA := am[k]
			bv, inB := bm[k]
			switch {
			case !inA:
				*out = append(*out, Change{Path: p, Op: '+', New: bv})
			case !inB:
				*out = append(*out, Change{Path: p, Op: '-', Old: av})
			default:
				walk(p, av, bv, out)
			}
		}
		return
	}
	al, aIsList := a.([]any)
	bl, bIsList := b.([]any)
	if aIsList && bIsList {
		if key := commonListKey(al, bl); key != "" {
			walkKeyedList(path, key, al, bl, out)
			return
		}
		if len(al) == len(bl) {
			for i := range al {
				walk(fmt.Sprintf("%s[%d]", path, i), al[i], bl[i], out)
			}
			return
		}
	}
	if !reflect.DeepEqual(a, b) {
		*out = append(*out, Change{Path: path, Op: '~', Old: a, New: b})
	}
}

func walkKeyedList(path, key string, a, b []any, out *[]Change) {
	index := func(l []any) (map[string]any, []string) {
		m := map[string]any{}
		var order []string
		for _, e := range l {
			k := e.(map[string]any)[key].(string)
			m[k] = e
			order = append(order, k)
		}
		return m, order
	}
	am, aOrder := index(a)
	bm, bOrder := index(b)
	seen := map[string]bool{}
	for _, k := range append(aOrder, bOrder...) {
		if seen[k] {
			continue
		}
		seen[k] = true
		p := fmt.Sprintf("%s[%s=%s]", path, key, k)
		av, inA := am[k]
		bv, inB := bm[k]
		switch {
		case !inA:
			*out = append(*out, Change{Path: p, Op: '+', New: bv})
		case !inB:
			*out = append(*out, Change{Path: p, Op: '-', Old: av})
		default:
			walk(p, av, bv, out)
		}
	}
}

// commonListKey returns the first listKeys entry that every element of both
// lists carries as a unique string, or "".
func commonListKey(a, b []any) string {
	if len(a) == 0 && len(b) == 0 {
		return ""
	}
outer:
	for _, key := range listKeys {
		for _, l := range [][]any{a, b} {
			seen := map[string]bool{}
			for _, e := range l {
				m, ok := e.(map[string]any)
				if !ok {
					continue outer
				}
				v, ok := m[key].(string)
				if !ok || seen[v] {
					continue outer
				}
				seen[v] = true
			}
		}
		return key
	}
	return ""
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func joinKey(path, k string) string {
	if !identRe.MatchString(k) {
		return fmt.Sprintf("%s[%q]", path, k)
	}
	if path == "" {
		return k
	}
	return path + "." + k
}

// Value renders a JSON value compactly, truncated to limit bytes (0 = no
// limit).
func Value(v any, limit int) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	s := string(b)
	if limit > 0 && len(s) > limit {
		return fmt.Sprintf("%s…(%d bytes)", s[:limit], len(s))
	}
	return s
}

// FormatChange renders one change on one line.
func FormatChange(c Change, limit int) string {
	switch c.Op {
	case '+':
		return fmt.Sprintf("+ %s: %s", c.Path, Value(c.New, limit))
	case '-':
		return fmt.Sprintf("- %s: %s", c.Path, Value(c.Old, limit))
	default:
		return fmt.Sprintf("~ %s: %s → %s", c.Path, Value(c.Old, limit), Value(c.New, limit))
	}
}

// Writers names the managers whose managedFields entry is new, or whose
// time changed, between a and b (a may be nil for a created object), e.g.
// "kubelet/status". An entry that only lost fields had them taken over by
// another manager and is not a writer. managedFields times have one-second
// resolution, so a manager writing twice within a second is not detected the
// second time.
func Writers(a, b map[string]any) []string {
	entries := func(obj map[string]any) map[string]any {
		out := map[string]any{}
		if obj == nil {
			return out
		}
		md, _ := obj["metadata"].(map[string]any)
		mfs, _ := md["managedFields"].([]any)
		for _, e := range mfs {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			out[managerName(m)+"\x00"+fmt.Sprint(m["operation"])] = m
		}
		return out
	}
	old, cur := entries(a), entries(b)
	seen := map[string]bool{}
	var out []string
	for k, e := range cur {
		em := e.(map[string]any)
		if prev, ok := old[k].(map[string]any); ok && reflect.DeepEqual(prev["time"], em["time"]) {
			continue
		}
		name := managerName(em)
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func managerName(m map[string]any) string {
	name := fmt.Sprint(m["manager"])
	if sub, _ := m["subresource"].(string); sub != "" {
		name += "/" + sub
	}
	return name
}

// StripManagedFields returns a shallow copy of obj without
// metadata.managedFields.
func StripManagedFields(obj map[string]any) map[string]any {
	md, ok := obj["metadata"].(map[string]any)
	if !ok {
		return obj
	}
	out := maps.Clone(obj)
	mdc := make(map[string]any, len(md))
	for k, v := range md {
		if k != "managedFields" {
			mdc[k] = v
		}
	}
	out["metadata"] = mdc
	return out
}

// Summary is a one-line status for an object, e.g. "Running", "Pending
// (CrashLoopBackOff, restarts=3)", "Terminating", "Ready=False".
func Summary(obj map[string]any) string {
	var parts []string
	md, _ := obj["metadata"].(map[string]any)
	if md["deletionTimestamp"] != nil {
		parts = append(parts, "Terminating")
	}
	status, _ := obj["status"].(map[string]any)
	if phase, ok := status["phase"].(string); ok {
		parts = append(parts, phase)
	}
	var details []string
	restarts := int64(0)
	for _, field := range []string{"initContainerStatuses", "containerStatuses"} {
		css, _ := status[field].([]any)
		for _, c := range css {
			cm, _ := c.(map[string]any)
			state, _ := cm["state"].(map[string]any)
			for _, st := range []string{"waiting", "terminated"} {
				s, _ := state[st].(map[string]any)
				if r, ok := s["reason"].(string); ok && r != "" && !(st == "terminated" && r == "Completed") {
					details = append(details, r)
				}
			}
			restarts += toInt(cm["restartCount"])
		}
	}
	if restarts > 0 {
		details = append(details, fmt.Sprintf("restarts=%d", restarts))
	}
	if len(details) > 0 {
		parts = append(parts, "("+strings.Join(dedup(details), ", ")+")")
	}
	// Without a phase (Deployments, Kueue Workloads, most CRDs), the True
	// conditions are the state; with none True, the first False one says why.
	if _, hasPhase := status["phase"]; !hasPhase {
		conds, _ := status["conditions"].([]any)
		var trues []string
		firstFalse := ""
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			t, _ := cm["type"].(string)
			switch cm["status"] {
			case "True":
				trues = append(trues, t)
			case "False":
				if firstFalse == "" {
					firstFalse = t + "=False"
					if r, _ := cm["reason"].(string); r != "" {
						firstFalse += "(" + r + ")"
					}
				}
			}
		}
		if len(trues) > 0 {
			parts = append(parts, strings.Join(trues, ","))
		} else if firstFalse != "" {
			parts = append(parts, firstFalse)
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

// EventLine renders a core/v1 Event object, e.g.
// "Warning BackOff (kubelet) x5: Back-off restarting failed container".
func EventLine(obj map[string]any) string {
	typ, _ := obj["type"].(string)
	reason, _ := obj["reason"].(string)
	msg, _ := obj["message"].(string)
	src, _ := obj["reportingComponent"].(string)
	if source, ok := obj["source"].(map[string]any); ok && src == "" {
		src, _ = source["component"].(string)
	}
	count := toInt(obj["count"])
	if series, ok := obj["series"].(map[string]any); ok {
		count = max(count, toInt(series["count"]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", typ, reason)
	if src != "" {
		fmt.Fprintf(&b, " (%s)", src)
	}
	if count > 1 {
		fmt.Fprintf(&b, " x%d", count)
	}
	fmt.Fprintf(&b, ": %s", strings.TrimSpace(msg))
	return b.String()
}

// EventTime is when an Event was last observed, from the most precise field
// the emitter set.
func EventTime(obj map[string]any) (time.Time, bool) {
	var candidates []any
	if series, ok := obj["series"].(map[string]any); ok {
		candidates = append(candidates, series["lastObservedTime"])
	}
	candidates = append(candidates, obj["lastTimestamp"], obj["eventTime"], obj["firstTimestamp"])
	for _, c := range candidates {
		if s, ok := c.(string); ok && s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func toInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func dedup(s []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
