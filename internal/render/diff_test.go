package render

import (
	"encoding/json"
	"strings"
	"testing"
)

func obj(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func lines(cs []Change) string {
	var out []string
	for _, c := range cs {
		out = append(out, FormatChange(c, 0))
	}
	return strings.Join(out, "\n")
}

func TestDiffKeyedListsAndIgnoredFields(t *testing.T) {
	a := obj(t, `{"metadata":{"resourceVersion":"1","labels":{"app.kubernetes.io/name":"x"},"managedFields":[{"manager":"a"}]},
	  "status":{"phase":"Pending","conditions":[{"type":"Ready","status":"False"},{"type":"PodScheduled","status":"True"}],
	  "containerStatuses":[{"name":"main","restartCount":0}]}}`)
	b := obj(t, `{"metadata":{"resourceVersion":"2","labels":{"app.kubernetes.io/name":"y"},"managedFields":[{"manager":"b"}]},
	  "status":{"phase":"Running","conditions":[{"type":"PodScheduled","status":"True"},{"type":"Ready","status":"True"}],
	  "containerStatuses":[{"name":"main","restartCount":1}],"podIP":"10.0.0.1"}}`)
	want := strings.Join([]string{
		`~ metadata.labels["app.kubernetes.io/name"]: "x" → "y"`,
		`~ status.conditions[type=Ready].status: "False" → "True"`,
		`~ status.containerStatuses[name=main].restartCount: 0 → 1`,
		`~ status.phase: "Pending" → "Running"`,
		`+ status.podIP: "10.0.0.1"`,
	}, "\n")
	if got := lines(Diff(a, b)); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestDiffUnkeyedLists(t *testing.T) {
	a := obj(t, `{"metadata":{"finalizers":["a","b"]},"spec":{"args":["x"]}}`)
	b := obj(t, `{"metadata":{"finalizers":["a"]},"spec":{"args":["y"]}}`)
	want := strings.Join([]string{
		`~ metadata.finalizers: ["a","b"] → ["a"]`,
		`~ spec.args[0]: "x" → "y"`,
	}, "\n")
	if got := lines(Diff(a, b)); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestWriters(t *testing.T) {
	a := obj(t, `{"metadata":{"managedFields":[{"manager":"kubectl","operation":"Update","time":"t1"},{"manager":"kubelet","operation":"Update","subresource":"status","time":"t1"}]}}`)
	b := obj(t, `{"metadata":{"managedFields":[{"manager":"kubectl","operation":"Update","time":"t1"},{"manager":"kubelet","operation":"Update","subresource":"status","time":"t2"}]}}`)
	if got := Writers(a, b); len(got) != 1 || got[0] != "kubelet/status" {
		t.Fatalf("Writers = %v", got)
	}
	if got := Writers(nil, a); len(got) != 2 {
		t.Fatalf("Writers(nil) = %v", got)
	}
	// kubectl lost fields to kubelet without writing itself.
	c := obj(t, `{"metadata":{"managedFields":[{"manager":"kubectl","operation":"Update","time":"t1","fieldsV1":{"f:spec":{}}},{"manager":"kubelet","operation":"Update","time":"t1"}]}}`)
	d := obj(t, `{"metadata":{"managedFields":[{"manager":"kubectl","operation":"Update","time":"t1","fieldsV1":{}},{"manager":"kubelet","operation":"Update","time":"t2"}]}}`)
	if got := Writers(c, d); len(got) != 1 || got[0] != "kubelet" {
		t.Fatalf("Writers = %v", got)
	}
}

func TestSummary(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"status":{"phase":"Running"}}`, "Running"},
		{`{"metadata":{"deletionTimestamp":"x"},"status":{"phase":"Running"}}`, "Terminating Running"},
		{`{"status":{"phase":"Running","containerStatuses":[{"restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}`, "Running (CrashLoopBackOff, restarts=3)"},
		{`{"status":{"conditions":[{"type":"Available","status":"True"},{"type":"Progressing","status":"True"}]}}`, "Available,Progressing"},
		{`{"status":{"conditions":[{"type":"QuotaReserved","status":"False","reason":"Pending"}]}}`, "QuotaReserved=False(Pending)"},
		{`{"data":{}}`, "-"},
	} {
		if got := Summary(obj(t, tc.in)); got != tc.want {
			t.Errorf("Summary(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEventLine(t *testing.T) {
	ev := obj(t, `{"type":"Warning","reason":"BackOff","message":"Back-off restarting failed container","source":{"component":"kubelet"},"count":5}`)
	if got, want := EventLine(ev), "Warning BackOff (kubelet) x5: Back-off restarting failed container"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
