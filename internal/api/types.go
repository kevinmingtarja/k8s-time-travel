// Package api holds the types shared by the ktt daemon and CLI.
package api

import (
	"encoding/json"
	"strings"
)

// Revision types. ADDED/MODIFIED/DELETED mirror watch event types.
const (
	Added    = "ADDED"
	Modified = "MODIFIED"
	Deleted  = "DELETED"
)

// Revision sources.
const (
	// SourceWatch revisions were delivered by a watch stream; TS is when the
	// daemon received the change.
	SourceWatch = "watch"
	// SourceList revisions were synthesized from a list (initial list or a
	// relist after a gap); TS is when the list completed, and the change itself
	// happened at an unknown earlier time.
	SourceList = "list"
)

// Recording states.
const (
	StateRunning = "running"
	StateError   = "error"
	StateStopped = "stopped"
)

// RecordingSpec is what a user asks to record.
type RecordingSpec struct {
	Context string `json:"context"`
	// Kubeconfig is the kubeconfig precedence list (KUBECONFIG semantics) the
	// CLI resolved; empty means the daemon's default loading rules.
	Kubeconfig    []string `json:"kubeconfig,omitempty"`
	Resource      string   `json:"resource"`
	Namespace     string   `json:"namespace,omitempty"`
	LabelSelector string   `json:"labelSelector,omitempty"`
	FieldSelector string   `json:"fieldSelector,omitempty"`
	Events        bool     `json:"events,omitempty"`
}

// ResourceInfo is a resource resolved through discovery at record time, so
// queries can resolve user input ("po", "deploy") without a cluster.
type ResourceInfo struct {
	Group      string   `json:"group,omitempty"`
	Version    string   `json:"version"`
	Resource   string   `json:"resource"`
	Kind       string   `json:"kind"`
	Singular   string   `json:"singular,omitempty"`
	ShortNames []string `json:"shortNames,omitempty"`
	Namespaced bool     `json:"namespaced"`
}

// GVR renders the resource as "v1/pods" or "apps/v1/deployments".
func (r ResourceInfo) GVR() string {
	if r.Group == "" {
		return r.Version + "/" + r.Resource
	}
	return r.Group + "/" + r.Version + "/" + r.Resource
}

// Matches reports whether the user-typed token names this resource.
func (r ResourceInfo) Matches(token string) bool {
	t := strings.ToLower(token)
	if t == r.GVR() {
		return true
	}
	names := append([]string{r.Resource, r.Singular, strings.ToLower(r.Kind)}, r.ShortNames...)
	for _, n := range names {
		if n == "" {
			continue
		}
		if t == n || (r.Group != "" && t == n+"."+r.Group) {
			return true
		}
	}
	return false
}

// Recording is a persisted list+watch of one resource selection on one
// cluster context.
type Recording struct {
	ID string `json:"id"`
	// ParentID is set on the Events recording created alongside a recording
	// made with --events.
	ParentID  string        `json:"parentID,omitempty"`
	Spec      RecordingSpec `json:"spec"`
	Info      ResourceInfo  `json:"info"`
	State     string        `json:"state"`
	LastError string        `json:"lastError,omitempty"`
	StartedAt int64         `json:"startedAt"`
	StoppedAt int64         `json:"stoppedAt,omitempty"`
	// LastSync is the last time (unix nanos) the recording was confirmed in
	// sync with the API server: a watch event, a bookmark, or a completed list.
	LastSync  int64 `json:"lastSync"`
	Revisions int64 `json:"revisions"`
}

// Revision is one observed state of one object.
type Revision struct {
	// TS is the unix-nanos receive time. It is unique and strictly increasing
	// across the whole store, so it doubles as a cursor.
	TS        int64           `json:"ts"`
	Context   string          `json:"context"`
	GVR       string          `json:"gvr"`
	Namespace string          `json:"namespace,omitempty"`
	Name      string          `json:"name"`
	UID       string          `json:"uid,omitempty"`
	RV        string          `json:"rv"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	Note      string          `json:"note,omitempty"`
	Object    json.RawMessage `json:"object"`
}

// Gap is a window during which a recording was not in sync with the API
// server, so changes inside it may be missing or mistimed.
type Gap struct {
	Context     string `json:"context"`
	GVR         string `json:"gvr"`
	RecordingID string `json:"recordingID"`
	Start       int64  `json:"start"`
	End         int64  `json:"end"`
	Reason      string `json:"reason"`
}

// RevisionsResponse is returned by the revisions and state endpoints. Head is
// the store's latest TS read before the scan, so a caller polling with
// after=Head never misses a revision.
type RevisionsResponse struct {
	Revisions []Revision `json:"revisions"`
	Head      int64      `json:"head"`
}

// PingResponse identifies the running daemon.
type PingResponse struct {
	PID  int    `json:"pid"`
	Home string `json:"home"`
}
