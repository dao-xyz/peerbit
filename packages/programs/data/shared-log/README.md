# Shared log

A log that can be replicated

## Persisted delivery recovery

Transport connections, topic coverage, and persisted-receipt capability are not
entry-leader eligibility. If the first fresh persisted-delivery leader plan for
any committed entry is empty or contains only the writer, delivery still fails
immediately with `NoPeersError` as the cause of `PersistedDeliveryError`; it does
not wait for initial leader discovery or maturity. The error identifies missing
eligible remote entry leaders, not necessarily missing network connections.

Before a new write, applications can use each relevant log's
`waitForReplicator(remoteKey, { timeout, signal })`, retaining its default role
maturity, followed by `waitForPersistedReceiptPeerReadiness(remoteKey, ...)`.
Do not substitute `waitForReplicators({ coverageThreshold: 1 })`: a full local
replica can satisfy that coverage without a remote. The default role maturity
is at least five seconds for replicating writers and can increase with the gap
between known role timestamps; non-replicating writers use zero. Readiness on
one log therefore does not establish readiness on another log with a different
replication role.

These waits are advisory, not a guarantee that a future entry will have the
requested remote leaders. Readiness without `entries` does not check entry
leadership or role maturity. For already committed entries, its `entries` option
also requires a fresh leader plan using `replicas` (the total leader-plan degree,
not `minAcks`). Only the following persisted delivery proves durability. A
`PersistedDeliveryError` retains exact `committedHashes`,
`localCommitSucceeded: true`, and `retrySafe: false`; never replay the original
write merely because its delivery failed.

Persisted delivery maintains bounded advisory recovery for currently selected
entry leaders, including incomplete capability/receive state and unconfirmed or
replacement V2 sessions. It reuses the persisted-readiness watchdog; readiness
never substitutes for the exact-entry, current-session durable receipt.

Each delivery operation reserves at most eight recovery tasks, shared across its
entries. Two-second recovery windows rotate through candidates without occupying
entry-transfer or receipt-request slots, changing the overall delivery deadline,
or replaying the original local write. Already-confirmed peers need no recovery
key lookup or readiness waiter. Removed candidates and completed, cancelled, or
timed-out operations stop their recovery work.

Custom asynchronous public-key resolvers must settle their promises. An
unsettled lookup retains its reservation even after cancellation, and its late
result cannot start new recovery work. Eight permanently unsettled lookups can
therefore exhaust one operation's recovery capacity; the deadline still bounds
the delivery call. The eight-task bound is per operation, not an aggregate limit
across concurrent deliveries. The existing SharedLog-wide readiness-waiter limit
also remains in effect. Missing or mismatched keys and unsupported peers cannot
supply receipt evidence.

## Durable native history checkpoints

Persistent peers created with `createRustPeerbitOptions()` automatically bound
discarded mutation history in the native entry-block mirror on POSIX Node. Once
the mirror's WAL has accumulated at least 64 MiB beyond its last checkpoint, a
mutation acknowledgement periodically waits while the current live block map is
rewritten as a crash-safe checkpoint. The allowance grows with the live map, so
an expanding store is rewritten geometrically instead of every fixed 64 MiB.

This is a physical entry-block WAL checkpoint, not logical log compaction. CUT
heads and other live tombstones remain unchanged for anti-resurrection, and the
coordinate, document-value, and document-signer WALs are outside this slice.
Windows, browsers, and custom persistence backends keep their existing behavior.

## Replication status

`SharedLog.getReplicationStatus()` returns a detached, local snapshot of the
current storage use, minimum range coverage, and active-replicator count. The
local `replication:status` event is emitted for the first snapshot and then only
when its ordered reason set changes, including recovery to an empty reason set.
Explicit calls always perform a one-shot measurement. Automatic refreshes run
only while at least one `replication:status` listener is registered, avoiding
coverage scans for applications that do not consume this telemetry. To start
monitoring reliably, register the listener first and then call
`getReplicationStatus()` once to seed the initial snapshot.

This API is advisory telemetry. It is not persisted, sent over the replication
protocol, forwarded by the canonical SharedLog proxy, or used to make placement,
repair, and pruning decisions.

`replicate.limits.storage` remains a **soft objective** for the adaptive PID
controller, not a hard storage quota. Coverage pressure, discrete entry sizes,
and delayed pruning can make actual use exceed the configured value. The
`storage-objective-exceeded` reason reports that condition; it does not enforce
the objective. `range-coverage-underfilled` reports minimum ring coverage below
`replicas.min`, while `default-replica-target-unattainable` reports fewer active
replicators than that target.

## Opt-in synchronization profiling

The existing `sync.profile(event)` callback receives local diagnostic summaries.
It is disabled by default. Keep the callback cheap and aggregate into fixed-size
buckets; applications can label callbacks with a peer index and a metadata/content
plane without putting application identifiers into these summaries.

- `sharedLog.adaptive.rebalance` reports one adaptive tick: `outcome`,
  `idleRemainingMs`, and, when reached, the existing controller inputs
  `storageUsedBytes`, `storageObjectiveBytes`, `currentFactor`, `totalFactor`,
  `controllerPeerCount`, and `cpuUsage`, plus `proposedFactor`.
  Storage is local block-store usage, not process RSS; the objective is soft.
  `controllerPeerCount` is the controller's existing replication-index size
  input, not a new count of distinct peers. Outcomes distinguish `idle-deferred`,
  `unchanged`, `apply-settled`, `not-permitted`, `stale`, and `error`.
  `apply-settled` only means the awaited update returned: a lower authorization
  check can decline it and still settle. It does not prove that the proposed
  factor was applied, and profiling adds no authorization checks to verify that.
  Resource-limited adaptive logs currently defer a tick after a local append
  until idle for `max(10 seconds, 5 × limits.interval)`; other adaptive logs use
  their rebalance interval. A deferral reports the remaining idle time without
  running the controller. This is a gate observation, not a prediction of when
  content or metadata will converge.
- `sharedLog.placement.pass` reports `phase: "range-change"` or `"repair-sweep"`.
  For range changes, `entries` counts yielded rebalance candidates; `changes`,
  `pruneScan`, and the fast-path flags describe the pass. Its duration starts
  after the initial trim/setup. For repair sweeps, `entries` counts candidate
  inputs (resident-map size for each native pass, fetched batch lengths otherwise),
  with `passes` and `nativePasses`. This is not the number of entries actually
  visited inside a native planner. Both phases report `count` as repair candidates
  handed to dispatch and `repairBatches` as dispatch calls, plus an `outcome`.
- `sharedLog.repair.dispatch` reports `mode`, `transport`, `outcome`, and
  `knownSuppressedEntries`. `entries` is the input size; `count` is the selected
  candidate size after known-peer suppression, captured before dispatch for one
  target even if a custom synchronizer mutates its input map. `dispatched`
  means the local lower-level call settled, not that bytes arrived or a durable
  receipt was obtained. `stale` includes a false result from the last lifecycle
  predicate check made by the simple lower path; profiling adds no such checks.
  It does not detect invalidation the lower path never checked. Suppression,
  observed cancellation/staleness, and errors have separate outcomes.
- `sharedLog.receive.existingHeads` and
  `sharedLog.rawReceive.existingHeads` include `count`: distinct hashes already
  present according to the existing batched lookup. `entries` includes every
  received head, including duplicates. The plain continuation of an already
  classified raw message omits `count`; do not count it as another lookup or hit.

The new summaries and existing-head summaries isolate synchronous callback
exceptions from replication behavior. They add no metadata queries or scans;
with profiling disabled they allocate no event/detail objects or diagnostic
timestamps. Other existing profile hooks retain their existing callback
behavior, so callbacks should still avoid throwing. These diagnostics do not
control replication, install listeners, retain histories, or provide persisted
delivery evidence.

Durations are elapsed time, not CPU time. The adaptive tick's `preStepMs` includes
existing asynchronous metric/prune work; `stepMs` covers the controller call and
`applyMs` covers the awaited range update. Phases can nest or run concurrently:
do not sum their durations into a total latency. Compare each plane's timeline
with independently observed metadata arrival, content arrival and durable
receipt/offline-reopen checks.

Likewise, existing `sharedLog.rawSend.fused.bytes` measures raw entry-block bytes
and `sharedLog.rawReceive.wireStashResolve.bytes` measures stashed payload bytes.
Neither is transport wire-byte accounting (framing, encryption, retransmission,
and other protocol traffic are outside those counts). Do not add byte counts
across these boundaries or infer bandwidth amplification from them alone.
Range health, known-head hits, placement, and repair dispatch are not receipt
proofs. Repeated candidate/known-hit counts can help locate repeated local work,
but cannot establish unique network bytes or end-to-end delivery by themselves.
