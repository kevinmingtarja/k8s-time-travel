# kubectl-timeline

> [!WARNING]
> ## ⚠️ This project is FULLY VIBE CODED ⚠️
>
> Every line of code, test and doc here was written by an AI coding agent.
> It has unit tests, an end-to-end test against a real kube-apiserver
> (envtest), and has been run against one live cluster. It has **not** had
> any production use. Treat it as experimental, read the code before you
> trust it, and don't point it at anything you can't afford to debug.

`kubectl-timeline` records every change to selected Kubernetes objects into a local DB on
disk and lets you query them back in time: what an object looked like at a
moment, what changed between two moments, when a condition first became true,
and wait for a condition without missing states that only lasted milliseconds.

It exists because `kubectl get` is a snapshot. Read one second too late and
the CrashLoopBackOff, the re-added finalizer, or the brief `Failed` phase is
gone, and an agent (or a human) concludes the wrong thing.

```console
$ kubectl timeline record pods --context envtest -n default -l app=web
recording 6a62c274: v1/pods in default -l app=web on envtest (1 revisions so far)
recording e20884c0: v1/events in default --field-selector involvedObject.kind=Pod on envtest (0 revisions so far)

$ kubectl timeline history pod/web-0          # abridged; real output from a run against envtest
2026-10-12T15:49:21.734  MODIFIED  pod default/web-0  rv=220  by kubectl-patch/status
    ~ status.containerStatuses[name=main].ready: true → false
    ~ status.containerStatuses[name=main].restartCount: 0 → 1
    - status.containerStatuses[name=main].state.running: {"startedAt":"2026-10-12T22:49:00Z"}
    + status.containerStatuses[name=main].state.waiting: {"message":"back-off 10s restarting failed container","reason":"CrashLoopBackOff"}
2026-10-12T15:49:21.769  MODIFIED  pod default/web-0  rv=221
    ~ status.containerStatuses[name=main].ready: false → true
    + status.containerStatuses[name=main].state.running: {"startedAt":"2026-10-12T22:49:02Z"}
    - status.containerStatuses[name=main].state.waiting: {"message":"back-off 10s restarting failed container","reason":"CrashLoopBackOff"}
2026-10-12T15:49:21.829  EVENT     pod default/web-0  Warning BackOff (kubelet): Back-off restarting failed container main in pod web-0
2026-10-12T15:49:22.893  MODIFIED  pod default/web-0  rv=223  by kubectl-patch
    + metadata.finalizers: ["example.com/hold"]
2026-10-12T15:49:22.929  MODIFIED  pod default/web-0  rv=224
    + metadata.deletionGracePeriodSeconds: 0
    + metadata.deletionTimestamp: "2026-10-12T22:49:22Z"
2026-10-12T15:49:23.978  DELETED   pod default/web-0  rv=225  Terminating Running (restarts=1)
2026-10-12T15:49:25.089  ADDED     pod default/web-0  rv=226  by kubectl-client-side-apply  (recreated: new uid)  Pending

$ kubectl timeline when pod/web-0 'object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") == "CrashLoopBackOff")'
2026-10-12T15:49:21.734  true   pod default/web-0  (MODIFIED rv=220)  Running (CrashLoopBackOff, restarts=1)
2026-10-12T15:49:21.769  false  pod default/web-0  (MODIFIED rv=221)  Running (restarts=1)
```

The CrashLoopBackOff above lasted 35 ms. A poll would almost certainly miss it.

## Install

Download a binary from
[Releases](https://github.com/kevinmingtarja/kubectl-timeline/releases)
(linux and darwin, amd64 and arm64):

```sh
VERSION=v0.0.2 OS=darwin ARCH=arm64
curl -fsSL "https://github.com/kevinmingtarja/kubectl-timeline/releases/download/${VERSION}/kubectl-timeline_${VERSION#v}_${OS}_${ARCH}.tar.gz" | tar xz kubectl-timeline
mv kubectl-timeline /usr/local/bin/   # or any directory on your PATH
kubectl timeline --version
```

Or build from a checkout: `go install ./cmd/kubectl-timeline`.

## Commands

| Command | What it does |
| --- | --- |
| `kubectl timeline record <resource> --context C [-n ns \| -A] [-l sel] [--field-selector fs] [--events=false]` | Start recording. `--context` is required. Recording the same selection again is a no-op. |
| `kubectl timeline ls` | List recordings, their state, last sync, and revision count. |
| `kubectl timeline stop <id>... \| --all` | Stop recordings. Their data stays queryable. |
| `kubectl timeline history [resource[/name]] [--since T] [--until T] [-o json]` | Every revision as a field-level diff, with Events and gaps interleaved. |
| `kubectl timeline at <T> [resource[/name]] [-o yaml\|json]` | Objects as they were at `T`. |
| `kubectl timeline diff <T1> <T2> [resource[/name]]` | Net change per object, with the number of revisions in between, so a change that was reverted still shows up. |
| `kubectl timeline when <resource[/name]> <predicate> [--first]` | Each time a CEL predicate became true or false. Exit 0 if it was ever true, 1 if never. |
| `kubectl timeline wait <resource/name> --for <predicate> [--fail-on <predicate>] [--timeout D]` | Block until a condition holds. Exit 0 on `--for`, 1 on `--fail-on`, 2 on error, 124 on timeout. |
| `kubectl timeline shutdown` | Stop the daemon. Recordings resume (with a gap) when it next starts. |

Resources resolve the way kubectl does, including short names and CRDs: `po`,
`deploy`, `deployments.apps`, `apps/v1/deployments`, `wd` (a CRD short name).
Query commands match against what has been recorded, so they don't contact
the cluster.

Times can be RFC3339, local time as kubectl-timeline prints it
(`2026-10-12T15:04:05.123`, `15:04:05`), relative (`-30s`, `-5m`), or `now`.
A printed time covers its whole precision window, so `kubectl timeline at 15:04:05.123`
includes a revision printed as `15:04:05.123`.

### Predicates

`when` and `wait` take CEL over `object` (the recorded object; JSON integers
become ints) and `deleted` (true on the revision that records a deletion).
A field that is missing is an error in CEL, so guard it with optional syntax:

```sh
kubectl timeline wait pod/web-0 \
  --for 'object.?status.?phase.orValue("") == "Running"' \
  --fail-on 'deleted || object.?status.?containerStatuses.orValue([]).exists(c,
      c.?state.?waiting.?reason.orValue("") in ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"])'

kubectl timeline when pods -n default 'object.metadata.?finalizers.orValue([]).size() > 0' --first
```

A revision on which the predicate errors counts as false. kubectl-timeline reports how many
revisions errored, and the first error.

`wait` evaluates both predicates on **every** recorded revision in order, not
on a periodic snapshot. A `--fail-on` state that lasted 1 ms, with the `--for`
state reached right after, still fails the wait. The end-to-end test checks
exactly this.

## How it works

```
 kubectl timeline ──unix socket──▶ daemon ──list+watch──▶ kube-apiserver
                                   │
                                   └── local DB (~/.kube-timeline/db)
```

- **One daemon.** Recording has to outlive any single CLI call, and only one
  process can open the DB, so a daemon owns both. Every command starts it if
  it isn't running (log: `~/.kube-timeline/daemon.log`, override with `KUBE_TIMELINE_HOME`).
- **Recording.** Each recording lists and then watches through the dynamic
  client, with bookmarks. Every ADDED/MODIFIED/DELETED event becomes a stored
  revision holding the full object, stamped with the time the daemon received
  it. A stream that closes cleanly is resumed from its last resourceVersion,
  so nothing is lost.
- **Gaps.** Any watch error (including 410 Gone), a daemon restart, or
  restarting a stopped recording triggers a relist. kubectl-timeline records a **gap** from
  the last moment it was known to be in sync to the relist, and turns whatever
  changed into revisions marked `[list]`. Objects missing from the relist get a
  synthesized DELETED. Queries print overlapping gaps, so "no change recorded"
  is never shown when the honest answer is "unknown".
- **Identity.** An object is identified by (context, resource, namespace,
  name), so its history spans deletion and same-name recreation.
- **Dedup.** When overlapping recordings (say `pods -n ns` and `pods -A`)
  deliver the same event, the first copy wins and later copies are dropped,
  even if they arrive after a newer revision.
- **Diffs** match list elements by `name` or `type`
  (`status.conditions[type=Ready].status`) and skip `resourceVersion` and
  `managedFields`. kubectl-timeline names who made a change ("by kubelet/status") from the
  managedFields entries that are new or have a new timestamp.

## Limits

- kubectl-timeline only knows about writes the apiserver committed and delivered on the
  watch, not every state the system went through. For example, if a container
  crashes and restarts between two kubelet status updates, all you see is
  `restartCount` going up.
- Nothing from before `kubectl timeline record` is known. Queries say when a time falls
  before a recording started.
- Timestamps are when the daemon received each change, not when the server
  wrote it. They are typically milliseconds late, and seconds late under heavy
  load.
- A gap starts at the last event or bookmark, or at shutdown if the daemon
  stopped cleanly, so it can be wider than the real outage. That errs on the
  side of saying "unknown".
- managedFields timestamps have one-second resolution, so a second write by
  the same manager within one second shows no "by".
- With `--events`, kubectl-timeline records every Event about that kind in the namespace
  (the API can't filter Events by label). `history` shows only the Events
  whose `involvedObject.uid` belongs to a recorded object, so leftover Events
  about an earlier object with the same name don't show up. Events found by
  the initial list are marked `[list: last seen <time>]`.
- The DB only grows. To reset: `kubectl timeline stop --all && kubectl timeline shutdown && rm -rf ~/.kube-timeline`.

## Storage

The local DB is [Pebble](https://github.com/cockroachdb/pebble), the embedded
key-value store from CockroachDB, at `~/.kube-timeline/db`. Its keys:

- `r/<id>`: a recording.
- `o/<ctx>\0<gvr>\0<ns>\0<name>\0<ts>`: a revision (the full object), grouped
  by object. A point-in-time lookup is one `SeekLT` per object.
- `l/<ts>`: a time index, for range scans across objects.
- `v/<object><rv>\0<L or D>`: every stored (resourceVersion, deleted) pair,
  used for dedup.
- `g/<ctx>\0<gvr>\0<start><id>`: a gap.

Timestamps are big-endian unix nanoseconds, strictly increasing across the
whole DB, so they double as a cursor: `wait` tails new revisions with
`after=<last ts>` without missing any. Commits don't wait for fsync, so a
crash can lose the most recent writes.

## Development

```sh
go test ./...                      # unit tests (store, recorder with a fake client, diff, time parsing)

# end-to-end: the built binary against a real kube-apiserver + etcd (envtest)
cd hack && KUBEBUILDER_ASSETS=$(setup-envtest use -i -p path) go test ./e2e/ -v

# a throwaway apiserver to try it by hand (context "envtest")
cd hack && KUBEBUILDER_ASSETS=$(setup-envtest use -i -p path) go run ./envtest-up -out /tmp/envtest.kubeconfig
```

### Releasing

Push a branch named `releases/vX.Y.Z`:

```sh
git push origin main:releases/v0.0.2
```

The [`release`](.github/workflows/release.yml) workflow:
1. Fails if that tag already exists.
2. Runs vet and the tests.
3. Builds linux/darwin × amd64/arm64 with [GoReleaser](https://goreleaser.com)
   ([`.goreleaser.yaml`](.goreleaser.yaml)) into a draft GitHub release on that
   commit.
4. Pushes the `vX.Y.Z` tag, then publishes the release.

`goreleaser release --snapshot --clean` builds the same artifacts locally into
`dist/` without publishing anything.
