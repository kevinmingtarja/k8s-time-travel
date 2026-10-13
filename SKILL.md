---
name: kubectl-timeline
description: >
  Record every change to selected Kubernetes objects with `kubectl timeline` and query it back in time: each revision as a field diff with Events
  interleaved, an object's state at any moment, the net diff between two
  moments, when a CEL predicate became true, and a `wait` that cannot miss a
  state lasting milliseconds. Use whenever a `kubectl get` snapshot is not
  enough because the interesting state is transient or timing-dependent: a pod
  stuck Terminating / won't delete, a finalizer that reappears after you strip
  it, a controller or operator racing your edits, Kueue queueing / admission /
  preemption, a brief CrashLoopBackOff, 409 "object is being deleted", or any
  "works by hand but fails in the flow" Kubernetes timing bug. Also use it
  INSTEAD of writing `until kubectl get ...; sleep` polling loops. Triggers on:
  "why won't this pod delete", "finalizer stuck/reappears", "watch the pod over
  time", "capture state transitions", "is the controller re-adding it", "what
  state was X in at T", "did it ever go Failed", "why was this workload
  pending/preempted", "wait until the pod is Running", "Terminating forever",
  "time travel", "kubectl timeline".
---

# kubectl-timeline: Kubernetes time travel

## Core idea

`kubectl get` shows the state *now*. It can't tell you that a finalizer was
removed at T and re-added at T+50ms, or that a pod was in CrashLoopBackOff for
35ms between two of your reads. `kubectl-timeline` runs a local daemon that lists and then
watches the objects you select, and stores **every committed write** as a
revision (the full object, stamped with when it was received) in a local
Pebble DB. You query that timeline afterwards, or while the repro is still
running, without polling the cluster.

- Install it from a checkout of this repo with `go install ./cmd/kubectl-timeline`, and put it on your
  PATH; `kubectl plugin list` should show `kubectl-timeline`.
- State: `~/.kube-timeline/` holds `db/` (Pebble), `daemon.log`, and `timeline.sock`.
  Override with `KUBE_TIMELINE_HOME` or `--home`.
- The daemon starts automatically on the first `kubectl timeline` command and resumes its
  recordings after a restart.

**Record first, then reproduce.** kubectl-timeline knows nothing from before `kubectl timeline record`.

## Recipe

1. **Record.** The command returns immediately, so no background task is
   needed.
   ```bash
   kubectl timeline record pods --context CTX -n NS -l app=web           # also records core Events about pods
   kubectl timeline record deploy --context CTX -A                       # all namespaces
   kubectl timeline record workloads.kueue.x-k8s.io --context CTX -n NS  # CRDs and short names resolve like kubectl
   kubectl timeline record nodes --context CTX --events=false            # cluster-scoped; skip Events
   kubectl timeline ls                                                   # state, last sync, revision count per recording
   ```
   - `--context` is required: a kubeconfig's current context can change under
     you.
   - Without `-n`, it uses the context's namespace.
   - `--field-selector` works as in kubectl.
   - Recording the same selection twice is a no-op, and overlapping
     recordings (e.g. `-n NS` plus `-A`) are deduplicated.

2. **Run the repro.**

3. **Query.** This reads only the local store, not the cluster.
   ```bash
   kubectl timeline history pod/web-0 -n NS            # every revision as field diffs + Events + GAPs
   kubectl timeline history pods -n NS --since -5m     # every recorded pod in the window
   kubectl timeline at 15:04:05.123 pods -n NS         # table as of that moment; -o yaml for full objects
   kubectl timeline diff 15:04:00 15:05:00 deploy/web  # net change + how many revisions in between
   kubectl timeline when pods -n NS '<CEL>'            # each true/false transition (exit 0 if ever true, 1 if never)
   kubectl timeline wait pod/web-0 -n NS --for '<CEL>' --fail-on '<CEL>' --timeout 5m
   ```
   - Times can be what kubectl-timeline prints (`2026-10-12T15:04:05.123`), wall time
     (`15:04:05`), relative (`-30s`), RFC3339, or `now`. A printed time covers
     its whole millisecond, so pasting a timestamp from `history` into `at`
     includes that revision.
   - Add `-o json` to `history` or `at` to feed jq or a script.
   - `--full` turns off truncation of long values.

4. **Clean up.** Recordings run until you stop them, and `kubectl timeline shutdown`
   alone does not stop them (they resume when the daemon next starts).
   - `kubectl timeline stop <id>` or `kubectl timeline stop --all` stops recording; the data stays
     queryable.
   - `kubectl timeline shutdown` stops the daemon.
   - The store only grows. To reset it:
     `kubectl timeline stop --all && kubectl timeline shutdown && rm -rf ~/.kube-timeline`.

## In an agent loop

- **Use `kubectl timeline wait` instead of hand-written polling loops.** It:
  - checks the current state first, then evaluates every revision in order,
    so a 1 ms CrashLoopBackOff still trips `--fail-on`;
  - exits 0 on `--for`, 1 on `--fail-on`, 2 on error (including a recording
    that is failing or stopped), and 124 on timeout.

  Always pass `--fail-on` for the states that make `--for` unreachable. Record
  the object first, or `wait` errors with "no recording covers …".
- Prefer `when` and `history` over reading `kubectl describe`: they show *when*
  something happened, who wrote it, and what happened in between.
- If the same name exists in several namespaces, pass `-n`; `wait` refuses to
  guess.

## Reading the output

Real output from a live cluster: Kueue preempting a low-priority pod for a
higher-priority one.
```
2026-10-12T16:15:14.381  MODIFIED  pod ktt-test/low-2  rv=313995391  by kueue/status
    + status.conditions[type=TerminationTarget]: {"message":"Preempted to accommodate a workload …
2026-10-12T16:15:14.387  MODIFIED  pod ktt-test/low-2  rv=313995393
    + metadata.deletionGracePeriodSeconds: 5
    + metadata.deletionTimestamp: "2026-10-12T23:15:19Z"
2026-10-12T16:15:14.397  MODIFIED  pod ktt-test/low-2  rv=313995401
    - metadata.finalizers: ["kueue.x-k8s.io/managed"]
2026-10-12T16:15:14.498  EVENT     pod ktt-test/low-2  Normal Stopped (pod-kueue-controller): Preempted to accommodate a workload …
2026-10-12T16:15:19.666  MODIFIED  pod ktt-test/low-2  rv=313995503  by kubelet/status
    + status.containerStatuses[name=main].state.terminated: {…"exitCode":137,…}
    ~ status.phase: "Running" → "Failed"
2026-10-12T16:15:19.965  DELETED   pod ktt-test/low-2  rv=313995509  Terminating Failed (Error)
```
- **`by X`**: the managers whose managedFields entry is new or has a new
  timestamp, e.g. `kubelet/status`. Those timestamps have one-second
  resolution, so there is no `by` when the same manager writes twice within a
  second, or on a delete.
- **List elements** are matched by `name` or `type`, as in
  `status.conditions[type=Ready].status`.
- **`[list]`**: seen by a list (the initial list, or a relist after a gap).
  The change happened at an unknown earlier time.
- **`GAP a → b`**: the recording was not in sync (daemon down, or the watch
  errored). Treat that window as *unknown*, not as "no change".
- **`(recreated: new uid)`**: same name, new object.
  **`(stopped matching the selector…)`**: a DELETED that might only mean the
  labels changed.
- **`EVENT`**: Events whose `involvedObject.uid` belongs to the recorded
  object. Events about an earlier object with the same name are excluded.
- **`# recording … since T`**: coverage. Queries say so when a time falls
  before a recording started or after it stopped.

## CEL predicates

The variables are `object` (the recorded object; JSON integers are ints) and
`deleted` (true on the revision that records a deletion). A missing field is
an error in CEL, so guard fields with `.?field.orValue(default)`. A revision on
which the predicate errors counts as false, and kubectl-timeline prints how many errored.

```
CrashLoopBackOff   object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") == "CrashLoopBackOff")
image pull fail    object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") in ["ImagePullBackOff", "ErrImagePull"])
phase              object.?status.?phase.orValue("") == "Running"
container ready    object.?status.?containerStatuses.orValue([]).exists(c, c.ready == true)
has finalizers     object.metadata.?finalizers.orValue([]).size() > 0
terminating        has(object.metadata.deletionTimestamp)
deleted            deleted
condition true     object.?status.?conditions.orValue([]).exists(c, c.type == "Available" && c.status == "True")
Kueue-gated pod    object.?spec.?schedulingGates.orValue([]).exists(g, g.name == "kueue.x-k8s.io/admission")
Workload queued    object.?status.?conditions.orValue([]).exists(c, c.type == "QuotaReserved" && c.status == "False")
```

A typical guarded wait:
```bash
kubectl timeline wait pod/web-0 -n NS \
  --for 'object.?status.?containerStatuses.orValue([]).exists(c, c.ready == true)' \
  --fail-on 'deleted || object.?status.?containerStatuses.orValue([]).exists(c, c.?state.?waiting.?reason.orValue("") in ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"])'
```

## Kueue

- Record the Pods, the Workloads (`kubectl timeline record workloads.kueue.x-k8s.io`,
  with Events), and the ClusterQueue
  (`clusterqueues --field-selector metadata.name=…`). A plain pod's Workload
  is named `pod-<pod>-<hash>`.
- The Workload history shows the whole admission story in its conditions:
  - `QuotaReserved=False(WaitingForQuota)`, with a message like "300m more
    needed";
  - then `QuotaReserved`/`Admitted`;
  - when preempted: `Evicted` + `Preempted`, `Requeued`, and
    `PendingEvaluation`.

  Kueue's Events (`QuotaReserved`, `Admitted`, `Preempted`,
  `EvictedDueToPreempted`) are interleaved in that history.
- Some of these states last milliseconds. In one live run,
  `WaitingForPreemptedWorkloads` lasted 22 ms, and a pod held its admission
  gate for 15–34 ms after creation. Use `when` to find them, not `at` with a
  guessed time.
- Kueue only manages the job types enabled under `integrations.frameworks`
  (check `kubectl get cm kueue-manager-config -n kueue-system`). Make sure the
  objects you test with are one of them.

## What not to conclude

- kubectl-timeline sees only writes the apiserver committed and delivered on the watch,
  not every state the system went through. For example, if a container
  crashes and restarts between two kubelet status updates, all you see is
  `restartCount` going up.
- Timestamps are when the daemon received a change, not when the server wrote
  it: typically milliseconds late, seconds late under heavy load.
- "Never observed" is not proof the object never existed if the recording's
  selector didn't cover it, or if the time falls in a GAP.

## Kubernetes gotchas this exposes (and the fixes)

- **A pod with `deletionTimestamp` set that won't disappear is held by a
  finalizer.** Controllers and operators keep their finalizers until their own
  condition is met.
  `kubectl timeline when pod/X 'object.metadata.?finalizers.orValue([]).size() > 0'` shows
  exactly when the finalizer was added and removed, and by whom.
- **The default patch type, strategic-merge, is a NO-OP on the `finalizers`
  list.** `kubectl patch ... -p '{"metadata":{"finalizers":[]}}'` does **not**
  clear finalizers. Use a JSON patch,
  `[{"op":"replace","path":"/metadata/finalizers","value":[]}]`, or
  `kubectl patch --type=merge -p '{"metadata":{"finalizers":null}}'`. In
  `kubectl timeline history`, a "remove" that didn't work shows up as no
  `- metadata.finalizers` line after your patch.
- **`--grace-period=0 --force` does NOT remove finalizers.** It only drops the
  grace period, so a finalizer-held pod survives a force-delete. Strip the
  finalizer *first*.
- **Even with no finalizer, a pod lingers for its
  `terminationGracePeriodSeconds`.** For that whole window the object still
  exists and blocks same-name recreation with `409 "object is being
  deleted"`.
- **The `by` column shows re-adds.** If a finalizer you stripped comes back
  `by <controller>`, that controller's webhook or reconcile loop is re-adding
  it. Fighting it directly won't work; address the controller's condition
  instead.

## When NOT to use this

If a single `kubectl get -o yaml` or `kubectl describe` already shows the stuck
state and the cause is static (a known finalizer, an unschedulable
nodeSelector, an image pull error), just read it. Use kubectl-timeline when timing,
ordering, or a controller race is in play, or when you need to wait on state
without risking a missed transition.
