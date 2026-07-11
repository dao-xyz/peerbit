# Native Shared-Log Durability Transaction Protocol (Engineering Spec)

Status: **Proposed**

This document defines the decision, recovery contract, and fault-injection gate
for hard-kill-safe native shared-log commits. It is the implementation contract
for the next durability work; it does not describe a guarantee that current
releases already provide.

---

## Decision

Peerbit will use a per-program, redo-only transaction journal to order native
append state across the block store, native graph, heads index, replication
coordinates, document index, and signer facts.

Once a complete `DURABLE_PREPARED` record has reached a strict storage barrier,
recovery always rolls the transaction forward. It does not attempt a
best-effort rollback across those stores. A commit is acknowledged only after a
`COMMITTED` record and its required cleanup intent reach the same barrier.
Destructive trim and prune cleanup runs after commit from a persistent,
reference-aware cleanup plan.

The transaction journal is the recovery authority. Existing block-store data,
heads, coordinate WALs, document WALs, and signer WALs remain projections that
the journal can verify and rebuild; none of them independently defines whether
a transaction committed.

## Goal

For a native program opened on storage that supports the strict durability
contract:

- if an append resolves successfully, a fresh process can recover that append
  without peers after `SIGKILL`, worker termination, or runtime failure;
- recovery never exposes a head without its block and required coordinate,
  document, and signer facts;
- replay is idempotent, including replay after another crash during recovery;
- trim cleanup cannot resurrect deleted bytes or delete a live same-CID block;
  and
- a durable I/O error has an unambiguous outcome and never invites an unsafe
  transparent retry.

## Current Baseline and Gap

Current native persistence guarantees **clean stop/restart**. Entry blocks are
mirrored from the wasm hot store to a durable per-program block sublevel, while
native coordinates, document values, and signer facts have their own journals.
Clean shutdown drains those writes and closes the stores.

That is not a hard-kill transaction boundary:

- native `preparePlainCommitted*` paths can mutate hot blocks, graph,
  coordinates, document rows, and signer facts before the durable block mirror
  has completed;
- some commit-only paths track the mirror promise for a later awaited operation
  or shutdown rather than awaiting it on the append that created the block;
- the default native any-store durability mode may resolve a write before its
  WAL append, and the Node coordinate persistence adapter writes without an
  explicit file sync barrier;
- coordinates, document values, and signer facts are flushed as independent
  WALs, so a crash can split one logical append across them; and
- `appendDurability: "strict"` controls eager head-index writes. It is not a
  cross-store crash-atomicity promise.

The existing clean-restart tests remain valid and must stay green. This design
adds a stronger, explicitly selected contract.

### Current execution seam

The strict native document path currently crosses these layers:

```text
Documents.commitNativeDocumentAppend
  -> SharedLog.appendStrictNativeDocumentPayloadCommitOnly
  -> appendLocallyPreparedPayloadNativeBackboneStorageTransaction
  -> native-backbone preparePlainCommitted*Transaction
  -> Log / EntryIndex.putNativeCommittedAppendFacts
  -> Documents.handlePreparedPlainPutCommit
```

The methods named `preparePlainCommitted*Transaction` already mutate block,
graph, coordinate, document, signer, and trim state. The new phases therefore
cannot be implemented by wrapping those methods in another promise barrier;
their planning and mutation responsibilities must be separated.

The native any-store already has a `durability: "strict"` mode whose Node
journal path calls `FileHandle.sync()` and whose OPFS path calls
`SyncAccessHandle.flush()`. The default rust-client configuration uses normal
durability, where `recordJournal()` may return a resolved promise while its WAL
append remains queued. Crash-safe mode must select and verify the strict
capability explicitly. It must not infer the capability from the store class or
`persisted()`.

| Protocol state | Current seam | Required change |
| --- | --- | --- |
| `PREPARED` | Native-backbone `append_tx` and log-rust append builders | Produce a serializable plan without mutating blocks, graph, shared-log state, document state, signers, or trim state; freeze exact trim hashes |
| `DURABLE_PREPARED` | No equivalent; coordinate persistence is the nearest partial WAL | Add one checksummed transaction journal and strict block receipt covering the entire append |
| `NATIVE_APPLIED` | Current `preparePlainCommitted*` and native block/graph/document commit functions | Replace their combined plan/commit role with validated idempotent `apply(txId, digest, plan)` into a hidden generation and defer physical deletes |
| `PUBLISHED` | Lower-Log committed facts, SharedLog resident maps/indexes, and Documents commit handling | Route host projections through one idempotent, transaction-tagged publisher; split it from user callbacks |
| `COMMITTED` | No durable commit decision; success means the call pipeline returned | Strictly sync the commit plus required cleanup intent, then advance one committed watermark before acknowledgement/events/delivery |
| `CLEANUP_PENDING` | Native trim may delete hot state immediately; durable cleanup has no persistent owner | Persist reference generations and tombstones before conditional physical deletion |
| `CLEAN` | No equivalent | Persist cleanup completion and compact only behind a cross-store checkpoint watermark |

## Scope

The first complete implementation covers:

- native local single-entry and independent-batch appends;
- entry block, native graph, head, coordinate, document-index, and signer
  projection updates produced by those appends;
- native length trim and prune cleanup caused by the transaction;
- non-replicating programs that must recover with no peer; and
- Node hard-kill recovery with a storage adapter that implements the strict
  barrier.

The same coordinator and record format should later cover native receive and
repair commits. Those paths must not invent a second transaction protocol.
While crash-safe mode is enabled, a mutating direct-Log, raw-receive, join, or
columnar path that has not been integrated must reject or use a proven safe
fallback; it cannot bypass the coordinator and weaken the advertised contract.

## Non-goals

- Distributed consensus or atomicity across peers.
- Retracting data already observed by another peer after local commit.
- Exactly-once user callbacks or network delivery. Storage recovery does not
  replay callbacks; network restart uses its existing committed-head sync.
- Hard-kill guarantees for memory-only nodes or adapters without a strict
  barrier.
- Treating `persisted(): true`, a resolved ordinary `put`, or clean `stop()` as
  evidence of strict durability.
- Preserving the current native prepare API shape if it cannot separate pure
  planning from mutation.

Power-loss durability is only claimed for adapters whose strict barrier maps to
the platform's stable-storage primitive, such as `fdatasync`/`fsync` or an OPFS
sync access handle `flush()`. The process-crash tests are required everywhere;
power-loss claims require an adapter-specific conformance test.

## Terms

- **Strict barrier**: resolves only after all earlier writes in that durability
  domain have reached the adapter's declared stable boundary. It is a new
  capability, not the existing `persisted()` flag.
- **Native state**: the wasm hot block/graph/coordinate/document/signer state
  installed idempotently during `NATIVE_APPLIED` in a transaction-tagged or
  otherwise hidden generation.
- **Host projection**: rebuildable lower-Log/indexer state derived from a
  transaction, including heads, length, durable coordinates, document rows,
  signer facts, caches, and the reference ledger.
- **Published**: native state and host projections have been installed in a
  hidden transaction version. User reads, callbacks, network gossip, and a
  successful append result are still gated on the committed watermark.
- **Cleanup**: generation-guarded deletion of blocks and obsolete projection
  versions retired by an already committed transaction.
- **Recovery authority**: the journal whose valid records decide whether to
  discard orphan staging data or roll a transaction forward.

## Safety Invariants

1. **Acknowledgement implies recovery.** After the append promise resolves, a
   peerless reopen returns the same entry/result and all of its required facts.
2. **Intent before mutation.** No native or published mutation occurs before a
   complete `DURABLE_PREPARED` record and all referenced new blocks have crossed
   a strict barrier.
3. **Referential integrity.** Every published head has a verified block and the
   coordinate, document, and signer facts required to interpret it.
4. **One recovery outcome.** A valid `DURABLE_PREPARED` record is rolled forward;
   an operation without one is not recovered. A torn tail never creates a third
   outcome.
5. **Idempotent replay.** Reapplying any state or projection with the same
   retained transaction id and plan digest is a no-op and returns the recorded
   result. Reusing the id with another digest fails closed. An id below the
   permanent checkpoint high-water is never reusable; after result retention
   expires it returns a typed expired-result error.
6. **Ordered visibility.** Transactions for one program publish in journal
   sequence order. Recovery completes older transactions before accepting new
   writes.
7. **Atomic batch result.** A batch has one transaction id and one acknowledgement
   boundary. Recovery does not acknowledge or expose only a subset.
8. **Commit before cleanup.** A transaction never deletes retired durable data
   before its `COMMITTED` record is stable.
9. **No stale resurrection.** Persistent cleanup tombstones are consulted by
   durable read-through, so a block pending deletion cannot repopulate the hot
   native store.
10. **No live deletion.** Cleanup deletes a CID only when its persistent
    reference generation still matches and its committed reference count is
    zero. Re-adding the same CID supersedes stale cleanup.
11. **Fail closed on corruption.** A missing staged block, digest mismatch,
    non-tail journal corruption, or impossible state transition blocks writes
    and reports a typed recovery error.
12. **No generic rollback after native apply.** Once `DURABLE_PREPARED` exists,
    failures retain the transaction for roll-forward recovery instead of
    routing it through ordinary append rollback.
13. **Exact destructive plan.** Planning freezes the exact hashes and reference
    generations to retire. Recovery never reruns a length-based trim selector,
    because the current length may have changed.
14. **Single writer per directory.** An exclusive program/directory lease
    prevents two processes from recovering or appending against the same
    journal concurrently.
15. **Committed visibility.** Native and host mutations before `COMMITTED` are
    transaction-tagged/tentative. Every public read, query, RPC, and native
    lookup resolves through one committed watermark and observes either the old
    version or the complete new version, never a partial projection set.
16. **Committed side effects are isolated.** User callback, event, and network
    failures after commit never turn the storage operation into an ordinary
    rejected append that invites a duplicate retry.

## Liveness Invariants

- Recovery reaches `COMMITTED` and then `CLEAN` when the durable stores resume
  successful operation.
- Cleanup failure may retain bytes, but it does not revoke an acknowledged
  append or permanently poison unrelated reads.
- A pending transaction or cleanup is observable through diagnostics and is
  retried on reopen, explicit recovery, and orderly shutdown.
- Journal compaction never prevents replay of a transaction that has not reached
  `CLEAN`.

## Transaction Record

The journal is per program/log namespace and uses versioned, length-delimited,
checksummed binary records. A record must be distinguishable from a torn tail
without interpreting partially written fields. Each record contains at least:

- format version, record length, checksum, and a strictly increasing
  `recordLsn` for every journal frame;
- transaction id, monotonically increasing `txSequence` (shared by every state
  frame for that transaction), phase ordinal, operation kind, and digest of the
  immutable prepared plan;
- program/log id and any precondition version used during planning;
- ordered new block references: CID, byte length, content digest, and staging
  location;
- graph and head upserts/deletes;
- coordinate upserts/deletes;
- document-index and signer-fact upserts/deletes;
- persistent block-reference deltas and cleanup candidates with their expected
  generations;
- batch ordering and result metadata needed to reproduce the original return
  value; and
- enough native plan data to reapply the transaction without the original JS
  objects or another peer.

Trim and prune records contain exact hashes/reference deltas selected during
planning. They never contain only a policy such as “trim to length N” that
would select a different victim when replayed.

`DURABLE_PREPARED` carries the full immutable redo plan. Later state records may
contain only the transaction id, plan digest, state, sequence, and state-specific
metadata.

The transaction id is allocated before any side effect. It is opaque and stable
for the lifetime of the operation. A future public idempotency key can map to
it, but transparent retry is allowed only when the original transaction id and
plan digest are known.

Transaction ids include the program epoch, transaction sequence, and a random
nonce. A checkpoint permanently retains the high-water sequence, so an expired
old id is rejected as `TransactionResultExpired` rather than accepted as a new
operation. Exact result/digest lookup is guaranteed for the configured result
retention window; durable-id non-reuse is permanent.

### Block staging

Large entry bytes do not need to be duplicated inside the journal. They are
staged in a transaction-private, non-readable, non-notifying blob namespace
owned by the transaction id. Staging in the live CID sublevel is forbidden:
the CID may already have legacy/live references, and normal block hooks could
announce uncommitted data.

Ordering is normative:

1. write and verify every new block;
2. cross the strict block barrier;
3. append `DURABLE_PREPARED`, referencing those blocks; and
4. cross the strict journal barrier.

A crash after step 2 but before a valid record leaves only transaction-private
orphan data. Under the directory lease, recovery may remove that transaction's
staging namespace after proving that no structurally valid prepared frame
exists. It never garbage-collects a live CID merely because no transaction
record names it. A valid record that references a missing or mismatched staged
block fails recovery closed.

Staged bytes remain a recovery source until a strictly synced canonical
checkpoint proves that the live block projection and every dependent projection
cover the transaction. `CLEAN` alone does not authorize staging deletion.

## State Machine

```text
PREPARED
  -> DURABLE_PREPARED
  -> NATIVE_APPLIED
  -> PUBLISHED
  -> COMMITTED
       -> CLEAN                         (no retired references)
       -> CLEANUP_PENDING -> CLEAN      (trim/prune/replacement cleanup)
```

States are monotonic. Missing state markers are recovery hints, not proof that
the preceding action did not occur: recovery repeats the preceding action and
uses its idempotency contract before advancing.

### `PREPARED`

An in-memory, side-effect-free plan. Planning may read current native and
durable state but cannot update blocks, graph, indexes, journals, cleanup state,
or externally visible runtime/projection counters. Failure here is retry-safe
and leaves no journal record.

Planning may reserve a timestamp, transaction sequence, gid, or nonce from a
coordinator-local allocator. A failed plan may leave a gap, but a reservation is
not reused while that process remains open and is not observable as committed
log state. Because no transaction id is exposed before prepared-frame
reconciliation, a sequence with no durable record may be reclaimed after
restart; the random nonce still prevents id collision.

This requires new native plan APIs. Existing APIs whose names contain
`prepare...Committed...` but mutate state cannot serve as this phase.

### `DURABLE_PREPARED`

All new blocks are staged and synced, and the full redo plan is appended and
synced. From this point the transaction is recoverable and recovery must roll it
forward even if the caller never received success.

Failure before the prepared-frame append begins affects only transaction-private
staging and can retry with the same transaction id. Once the first byte of the
prepared frame is issued, any append or sync error is outcome-unknown. While
holding the lease, the coordinator must rescan, repair/truncate only a
structurally incomplete tail, and cross a successful strict barrier before it
can classify the transaction as absent or `DURABLE_PREPARED`. If reconciliation
cannot finish, it returns a typed pending/unknown result containing the
transaction id, blocks newer sequences, and never starts a fresh replacement
append.

### `NATIVE_APPLIED`

The plan is applied to a transaction-tagged native generation by
`(transaction id, plan digest)`. Apply is idempotent and must not perform
irreversible trim/prune block or projection deletion. Native reads remain on the
previous committed watermark. The native runtime records applied transaction
ids for the process lifetime; after restart, replay from the durable journal
reconstructs the same state.

All fallible validation occurs before mutation. Once apply begins, each
sub-operation must be idempotent and either non-fallible or safely repeatable
from the same plan.

If the process dies before this marker is written, recovery calls apply again.

### `PUBLISHED`

All host projections are installed idempotently in a transaction-tagged version:
block references, lower-Log/head facts, coordinate projections, document rows,
and signer facts. Old projection versions and blocks are not destructively
removed here. Publication and cleanup share the per-program mutation lock.
Every read/RPC/query path resolves against the committed watermark and therefore
continues to see the old complete version.

Callbacks, network gossip, and append success are still withheld. If a crash
splits projection writes, recovery replays the complete projection set from the
redo plan.

Transaction recovery does not replay user callbacks. A crash after commit but
before callback dispatch may omit that callback; callers recover the committed
state by reopening/querying. Network synchronization re-announces committed
heads through its normal restart behavior. Exactly-once external effects would
require a separate persistent outbox.

### `COMMITTED`

The commit record and either `CLEANUP_PENDING` (when the plan retires resources)
or `CLEAN` cross one strict journal barrier. Under the mutation lock, Peerbit
then advances the native and host committed watermark to this transaction.
Only after the watermark advances and all transaction locks are released may
Peerbit:

- resolve the append promise;
- enqueue user-facing change events; and
- enqueue network delivery.

If the process dies after the barrier but before the caller observes success,
recovery preserves the commit. Retrying with the same transaction id returns
the recorded result; a caller that lost the id must tolerate the standard
ambiguous-result case.

Protocol projectors and user notifications are separate APIs. Projectors run
before `PUBLISHED`, use `emit: false`, and are replayed by recovery. Notifications
run after commit, outside transaction locks, and are never replayed by storage
recovery. A notification/send failure is reported diagnostically; it cannot
reject or roll back the already committed append result.

### `CLEANUP_PENDING`

The cleanup state and tombstones are durable before deletion starts. The full
cleanup candidates already exist in the prepared plan, so a crash immediately
after `COMMITTED` can reconstruct this state.

For every candidate `(namespace, key, generation)`, cleanup rechecks persistent
ownership under the per-program mutation lock. A block candidate additionally
requires committed reference count zero and no valid prepared owner. An
obsolete projection-version candidate requires that no committed watermark or
active reader snapshot still selects it. Durable read-through treats a matching
block tombstone as absent and cannot repopulate the hot store.

Durable and hot deletion are idempotent and may occur in either order. `CLEAN`
is not written until both sides have completed and the strict delete barrier has
passed. A deletion failure keeps the tombstone and retries later.

### `CLEAN`

No recovery or cleanup work remains. The transaction may be folded into a
checkpoint only after the checkpoint contains canonical native/host projection
state or references a content-digested immutable projection snapshot retained
with the checkpoint generation. A projection watermark alone is insufficient.
The same checkpoint also contains the reference ledger, cleanup state,
program/transaction high-water marks, and retained results. `CLEAN` alone never
authorizes dropping the redo plan or private staged bytes.
The `CLEAN` frame may share a later journal barrier, but until that barrier
succeeds recovery continues to treat the transaction as cleanup-pending and
repeats deletion idempotently.

## Transition Algorithm

The base protocol is serial. Lock order is always:

```text
crash-released directory lease -> sequence/recovery lock -> mutation lock
```

The coordinator executes one program sequence at a time:

1. hold the lifetime directory lease and acquire the sequence/recovery lock;
2. reject new work if an older transaction has unresolved commit recovery
   (`CLEANUP_PENDING` debt alone does not block a newer sequence);
3. under the mutation lock, reserve identifiers and build a pure plan from one
   committed watermark;
4. write transaction-private staged blocks and cross the strict block barrier;
5. under the mutation lock, revalidate every plan precondition/generation,
   append `DURABLE_PREPARED`, and cross the strict journal barrier; this valid
   record becomes a positive prepared ownership claim for its staged keys;
6. idempotently apply the hidden native generation without destructive cleanup;
7. append `NATIVE_APPLIED`;
8. idempotently publish every hidden host projection and reference delta with
   `emit: false`;
9. append `PUBLISHED`;
10. append `COMMITTED`, followed by `CLEANUP_PENDING` when the plan retires
    resources (or `CLEAN` when it does not), and cross the strict commit-journal
    barrier;
11. under the mutation lock, advance the native and host committed watermark;
12. release the mutation and sequence locks;
13. resolve the append result and enqueue callbacks/delivery outside all
    transaction locks; and
14. let the cleanup worker reacquire the mutation lock, execute guarded cleanup,
    cross the strict delete barrier, and append `CLEAN`.

The directory lease is acquired before journal scan and held for the lifetime
of the open program. Node requires an OS advisory lock held by an open file
descriptor (or a storage fencing primitive) that the kernel releases on process
death; a `wx` lockfile, PID file, or timeout is not sufficient. OPFS needs an
equivalent exclusive owner before that adapter can claim support. A second
process fails open deterministically rather than running a second recovery
coordinator.

With separate block and journal domains, acknowledgement requires three strict
barrier operations: staged blocks, `DURABLE_PREPARED`, and the commit journal
tail. An implementation may reduce that count only if staged bytes and the
prepared record share one proven atomic durability domain. Group commit is not
part of the base serial protocol; it requires a later extension with virtual
sequential planning and conflict detection.

Cleanup failure does not hold the transaction sequence lock indefinitely. Its
persistent tombstone remains active, and each cleanup attempt shares the
mutation lock with reference publication so newer same-CID references win.

## Recovery Algorithm

Current lifecycle APIs cannot publish heads before `Log.open()` because that is
where the `EntryIndex` is constructed. Crash-safe mode therefore requires a
two-phase lifecycle:

```text
initialize stores, indexes, projectors, and native runtime in hidden mode
  -> reconcile checkpoint/journal and recover with emit:false
  -> activate reads, queries, RPC, sync, announcements, and events
```

Legacy coordinate/document/signer WALs are not hydrated as trusted visible
state. They must be transaction-tagged and filtered by the committed watermark,
or cleared and rebuilt from the canonical checkpoint plus transaction journal.
Documents supplies a deterministic projector hook that can rebuild index/cache
state with `emit: false` before activation.

Recovery then proceeds as follows:

1. acquire the crash-released directory lease and sequence/recovery lock;
2. initialize the hidden stores/indexes/projectors without registering public
   readers, RPC, synchronizers, announcements, or events;
3. load the newest valid canonical checkpoint and scan journal frames by
   `recordLsn`;
4. truncate only a structurally incomplete final frame (short header/body or
   missing trailer). A fully framed checksum mismatch, including in the final
   frame, fails closed as possible corruption/bit rot. Also fail closed on a
   non-tail fault, record-LSN gap/duplicate, transaction-sequence conflict,
   unsupported version, plan-digest change, or backward phase;
5. rebuild the transaction table, committed watermark, prepared ownership,
   reference ledger, and cleanup tombstones;
6. process transactions in `txSequence` order;
7. for `DURABLE_PREPARED` or `NATIVE_APPLIED`, verify staged blocks, idempotently
   apply hidden native state, and publish hidden host projections;
8. for `PUBLISHED`, verify/replay all projections, append `COMMITTED` plus the
   required `CLEANUP_PENDING`/`CLEAN` frame, and strictly sync that tail;
9. for `COMMITTED`, ensure the cleanup intent is present/durable and advance the
   committed watermark;
10. restore retained results and schedule `CLEANUP_PENDING` work;
11. activate the program only when every transaction is committed, or remain
    inactive with a typed fail-closed error; cleanup debt may continue after
    activation; and
12. in tests, run or crash this recovery again to prove phase idempotency.

Transaction-private staged bytes with no structurally valid prepared record are
collected separately under the lease. Live CID data is never inferred to be an
orphan and is never collected by this rule.

## Reference-Aware Cleanup

A volatile set of deleted CIDs or an in-memory generation counter is
insufficient: both disappear in the crash this protocol is meant to survive.

The journal/checkpoint therefore maintains a per-program resource-ownership
ledger. Block entries carry committed reference counts; versioned head,
coordinate, document, and signer rows carry committed/reader ownership. Every
valid `DURABLE_PREPARED` also pins its staged keys as prepared owners.
Publication applies additions and removals in transaction-sequence order. A
cleanup candidate captures `(namespace, key, generation)` produced by its
removal. Cleanup proceeds only if:

- the candidate generation is still current;
- no valid prepared or newer published transaction owns a replacement;
- a block's committed reference count is zero; and
- an obsolete projection version has no committed watermark or active reader
  snapshot selecting it.

Re-adding identical bytes or the same document/signer key advances its resource
generation before an older cleanup can delete it. Pure-plan state reads,
prepared-ownership registration, native apply, host publication, read-through
repopulation, and cleanup all use the mutation-lock order (or an equivalent
persistent compare-and-swap). The cleanup check and deletion therefore cannot
race a newer owner.

Retention is preferred to unsafe deletion. If reference state is ambiguous,
leave the block and report cleanup debt.

## Failure Matrix

For this table, **old** means the last transaction committed before the tested
operation and **new** means the tested operation. “Roll forward” includes
idempotent native apply, projection publication, and commit.

| Crash or fault point | Durable evidence after restart | Recovery action | Required assertion |
| --- | --- | --- | --- |
| Before or during pure planning | No transaction record | Ignore the attempt | Old state only; no native or durable mutation |
| After staging one or all blocks, before the prepared-frame append | Transaction-private staging only | Keep old state; remove only that transaction's private orphan namespace | No new head/facts/read/announcement; live same-CID data is untouched |
| Structurally incomplete final `DURABLE_PREPARED` frame | Short header/body or missing trailer | Truncate only the incomplete tail and treat the attempt as absent | Old state only; no valid frame follows an incomplete tail |
| Fully framed checksum mismatch, unsupported version, LSN gap/duplicate, or backward phase | Corrupt/invalid journal evidence | Fail closed without applying or deleting | Corruption is never silently converted into an absent transaction |
| Valid `DURABLE_PREPARED` with missing, short, or CID-mismatched staged bytes | Complete intent but invalid recovery payload | Fail closed with transaction/key diagnostics | No projection or cleanup mutation occurs |
| After `DURABLE_PREPARED` sync, before native apply | Complete redo plan and blocks | Roll forward | New state appears exactly once without peers |
| Mid-native apply | Complete redo plan; native state may be partial | Reapply by transaction id and digest | Native graph/block/document/coordinate state equals one complete apply |
| After native apply, before `NATIVE_APPLIED` marker | Complete redo plan; native state may already be complete | Reapply as a no-op, then advance | No duplicate graph edges, coordinates, rows, or counters |
| After `NATIVE_APPLIED`, before publication | Prepared plan plus marker | Publish all projections | Public state remains old until recovery completes; then new is complete |
| Mid block-reference/head publication | Some host projections may be present | Replay the entire projection set | Every recovered head has its verified block and reference |
| Mid coordinate/document/signer publication | Cross-projection state may be split | Replay the entire projection set | Coordinates, document row, signer facts, head, and block agree |
| After publication, before `PUBLISHED` marker | Projections may already be complete | Republish idempotently | One complete new result; no duplicate change notification during recovery |
| After `PUBLISHED`, before `COMMITTED` | Full redo plan and hidden projections | Verify projections, write commit plus cleanup intent, and durably commit | Concurrent readers stay on old until one committed-watermark advance |
| During/torn `COMMITTED` append | Marker absent or structurally incomplete final frame | Repeat commit from `PUBLISHED` | New state is committed once; a fully framed invalid record fails closed and is never accepted as acknowledgement |
| After commit/cleanup-intent sync, before watermark/result | Valid commit, cleanup intent, and recorded result | Advance watermark and return/recover the recorded result | Reopen contains new; retained same transaction id returns the same result |
| After `COMMITTED` append, before cleanup state append/barrier | Commit frame and cleanup plan may exist only in an unsynced tail | Validate the tail and recreate `CLEANUP_PENDING` from the prepared plan before acknowledgement | New state rolls forward; no deletion or acknowledgement precedes durable cleanup intent |
| During/torn `CLEANUP_PENDING` append | Commit plus cleanup plan; pending marker absent or structurally incomplete final frame | Recreate pending state before deletion | No deletion runs without a persistent tombstone; a fully framed invalid record fails closed |
| After cleanup tombstone, before deletion | Valid pending state | Resume cleanup | Reads cannot repopulate the tombstoned CID |
| After hot delete, before durable delete | Pending tombstone; durable bytes may remain | Delete durable bytes, repeat hot delete | Old CID stays logically absent throughout recovery |
| After durable delete, before hot delete | Pending tombstone; hot bytes may remain | Delete hot bytes, repeat durable delete | Old CID stays logically absent throughout recovery |
| After both deletes, before `CLEAN` | Pending marker; both stores may already be clean | Repeat idempotent deletes and mark clean | No error for missing bytes; cleanup debt reaches zero |
| Durable cleanup rejection/disk full | `CLEANUP_PENDING` remains | Retain tombstone and retry later | New commit remains valid; bytes may leak but never resurrect |
| Same CID or document/signer key re-added while old cleanup is pending | Newer resource generation/prepared owner | Supersede/cancel old candidate | Re-added resource remains readable and is not deleted by stale cleanup |
| One of multiple live references to the same CID is retired | Positive committed reference count | Retain physical bytes and remove only the logical reference | Physical deletion occurs only after the final reference retires |
| Read starts before cleanup and returns after the tombstone/generation changes | Pending or superseded cleanup generation | Recheck persistent liveness after I/O; reject/retry stale bytes | The read cannot repopulate hot state with a logically deleted generation |
| Duplicate recovery invocation | Same transaction id and plan digest | Return no-op from every repeated phase | Logical projections/result/ownership are stable and repeated-mutation counters stay zero |
| Transaction id reused with another digest | Conflicting durable/native evidence | Fail closed | No mutation from the conflicting plan |
| Expired transaction id is retried | Sequence is at/below permanent checkpoint high-water but result was evicted | Return `TransactionResultExpired` | The old id is never accepted as a new transaction |
| Mid independent-batch apply or publish | One batch transaction with full ordered plan | Roll the whole batch forward | Final state equals uninterrupted execution, including intentional intra-batch trim; no visible partial prefix |
| Strict private-stage write/barrier rejection before prepared-frame append | No logical transaction record | Keep old state; clean/retry the same private transaction id | No native mutation and no visible new result |
| Prepared-frame append or sync rejects after its first byte is issued | Record may be absent, torn, complete-but-unsynced, or durable | Reconcile under the lease and cross a successful barrier; otherwise retain typed pending/unknown state | Never classify as retry-safe or fork a new sequence while outcome is unknown |
| I/O failure after prepared sync | Valid pending transaction | Block newer sequences and resume roll-forward | Error exposes transaction id; ordinary retry cannot fork state |
| A derived head/coordinate/document/signer projection or legacy WAL is missing/corrupt | Canonical checkpoint/journal remains valid | Rebuild the projection with `emit: false` | Reopen result comes from transaction authority, not accidental survival of a derived WAL |
| Orphan projection facts exist without a valid transaction | Projection contains extra unowned rows | Ignore/remove them while rebuilding the committed version | No orphan fact becomes visible or authoritative |
| Crash during journal checkpoint/compaction | Old checkpoint/journal or new checkpoint generation | Select the newest fully valid generation | Never lose both recovery sources; pending transactions remain replayable |
| Crash during empty-program genesis checkpoint write/switch | No active manifest, possibly with an incomplete private generation; or one fully valid manifest target | Discard an unreferenced incomplete generation or select the valid manifest target | Activation requires a valid manifest; the program never activates against a half-initialized namespace |
| Crash-safe open of a populated legacy directory without a transaction namespace | Legacy data only | Return typed `MigrationRequired` before creating, clearing, or projecting transaction state | Protected data-file digests are identical immediately before and after rejection, excluding scoped lease/diagnostic metadata; a later legacy-mode reopen has the same logical state |
| Crash during orderly close with a transaction in flight | State depends on last valid marker | Use the same recovery rules; never trust close intent | Reopen result matches the corresponding matrix row |
| Callback, event, or network-send failure after commit | Valid `COMMITTED` record | Keep commit; do not replay callbacks from storage recovery; let networking resynchronize committed heads | No storage rollback and no duplicate local append |
| Direct Log or raw/columnar receive attempts to bypass the coordinator | No typed transaction ownership | Reject/fallback before mutation | Crash-safe mode has no unjournaled native mutation path |
| Two processes open the same program directory | Existing exclusive lease | Reject the second opener | Never run dual recovery or concurrent journal writers |
| Replay of a length-trim operation after later journal entries exist | Prepared record contains exact retired hashes | Apply those exact reference deltas only | Recovery never trims an additional entry selected from the new length |
| Adapter exposes only `persisted(): true`, a generic wait hook, or a receipt for the wrong tx/CID/LSN | No valid typed strict capability/receipt | Refuse crash-safe mode or fail before native mutation | No hard-kill guarantee is silently weakened; failures stay owned by the initiating transaction |

## Crash-Injection Test Contract

### Harness shape

Node integration tests first create a non-empty, fully `CLEAN` old fixture in a
separate process and record canonical digests for its blocks, heads, native
state, host projections, ownership ledger, and result. A writer then opens the
same directory with no peers, executes the tested operation, and triggers a
named failpoint. Crash failpoints terminate it with `SIGKILL`; I/O failpoints
use an adapter that can hold, tear, reorder, short-write, or reject a specific
write/barrier.

A recovery process opens the same directory. For every recovery-mutating
failpoint, that process is also killed and a fourth fresh process must finish
recovery. A final clean reopen compares logical canonical digests and
repeated-mutation counters; directory bytes may legitimately differ because of
checkpointing and diagnostics.

The no-peer worker proves storage recovery. A separate latched live-process
harness exercises concurrent reads, callbacks, and a recording transport while
native/host projections are tentative. It verifies that the committed watermark
hides partial state and that no notification/send occurs before commit.

Fixture workers are compiled TypeScript test fixtures or external fixtures that
import package exports. A source `.mjs` worker must not import its own partially
built `dist` tree.

The production coordinator exposes dependency-injected failpoints. Tests must
not infer timing with arbitrary sleeps. Pure-plan conformance is asserted
in-process: the test snapshots native and host mutation fingerprints, reaches
`after-plan`, compares the snapshots before any throw/teardown, and only then
continues or aborts. A killed process cannot prove that a transient Wasm
mutation did not occur.

Native apply additionally exposes a test-only deterministic seam from inside
the Rust/Wasm call. It invokes `afterSuboperation(i)` after every enumerated
mutation boundary and can abort there, so each partial apply is observable and
replayable. A coordinator hook surrounding one synchronous native call is not
sufficient for indexed native-apply coverage.

### Required failpoints

- `after-plan`
- `during-block-stage-write`
- `during-block-stage-sync`
- `after-block-stage-sync`
- `during-durable-prepared-append`
- `after-durable-prepared-sync`
- `during-native-apply:<suboperation-index>`
- `after-native-apply`
- `during-native-applied-record`
- `after-native-applied-record`
- `during-block-reference-publish`
- `during-head-publish`
- `during-coordinate-publish`
- `during-document-publish`
- `during-signer-publish`
- `during-published-record`
- `after-published-record`
- `during-committed-append`
- `after-committed-frame-before-cleanup-intent`
- `during-cleanup-pending-append`
- `after-committed-sync`
- `after-result-release`
- `after-cleanup-pending-sync`
- `after-cleanup-generation-check-before-delete`
- `after-durable-read-before-hot-repopulate`
- `after-hot-delete`
- `after-durable-delete`
- `after-cleanup-delete-sync`
- `during-clean-record`
- `during-checkpoint-write`
- `during-checkpoint-switch`
- `during-directory-lock-acquire`
- `during-stop-at:<transaction-phase>`
- `during-callback-dispatch`
- `during-network-send`

### Required operation matrix

- single append with `meta.next: []`;
- chained append with one or more `next` references;
- independent batch append;
- document put, update, and delete with signer policy enabled;
- non-replicating native document put;
- length trim where the newly committed row retires an old head;
- a batch where a later row trims an earlier row;
- same-CID and same document/signer-key re-add while cleanup is pending;
- multiple committed references to one CID, retiring them one at a time;
- raw receive/join and synchronous `putKnownManyColumns` ownership;
- direct native `Log` append, proving it cannot bypass the coordinator;
- crash-safe open of a populated legacy directory without a transaction
  namespace, followed by an unchanged legacy-mode reopen;
- first crash-safe open of an empty directory, with checkpoint write/switch
  failpoints applied to genesis creation;
- two concurrent openers against one directory;
- two overlapping appends, proving sequence blocking and final order;
- concurrent `stop()` at every transaction phase, plus `SIGKILL` while stop is
  waiting;
- missing/tampered staged blocks; non-tail checksum corruption; record-LSN
  gap/duplicate; backward state; and unsupported format;
- derived projection/WAL removal and injected orphan projection facts;
- cleanup I/O failure followed by restart and retry; and
- torn journal header, tail record, checkpoint, and state marker.

Until rollout phase 6, raw receive/join/columnar cases assert rejection or a
safe non-native fallback before mutation. Once integrated, the same cases must
pass the full transaction/recovery matrix.

### Assertions on every reopen

- expected heads, log length, entries, and document query results;
- every published head resolves locally with `remote: false`;
- block CID and bytes match the prepared digest;
- coordinate, document, and signer projections match the committed plan;
- no uncommitted callbacks or network delivery occurred;
- recovery itself did not replay user callbacks;
- each transaction id has one plan digest and a monotonic state history;
- the deletion audit shows candidate generation equals current generation,
  committed reference count is zero, and no prepared/newer owner exists;
- pending cleanup is visible in diagnostics and cannot resurrect through
  durable read-through;
- pure planning leaves block/graph/coordinate/document/signer/trim mutation
  fingerprints unchanged;
- recovery derives the same canonical native/host/ownership digests even when
  every legacy projection WAL is removed;
- repeated recovery produces no repeated logical mutation/reference delta; and
- native raw send/receive fusion counters remain unchanged where the tested
  path previously avoided JS block materialization.

### Test locations

- Rust/native record parsing, checksums, torn tails, and idempotent apply:
  `packages/utils/native-backbone/test/append-transaction.spec.ts`,
  `packages/utils/native-backbone/test/transaction-persistence.spec.ts`, plus
  Rust host tests beside the native transaction modules.
- Store strict-barrier and torn-write conformance:
  `packages/utils/any-store/rust/test/index.spec.ts` and the corresponding OPFS
  adapter tests.
- Log projection/idempotency tests: `packages/log/test/`.
- Transaction/recovery and trim matrix:
  `packages/programs/data/shared-log/test/durable-native-transaction.spec.ts`;
  its describe is added explicitly to the shared-log CI allowlist.
- Document put/update/delete and worker hard-kill tests:
  `packages/programs/data/document/document/test/durable-native-hard-kill.spec.ts`
  plus its compiled fixture worker.

The first CI gate should run a deterministic representative row from every
phase. A nightly/soak gate should sweep all failpoints, operation variants, and
multiple consecutive recovery crashes.

## Required API Boundaries

Names are illustrative; behavior is normative.

### Strict storage capability

Add an explicit store capability that can stage verified blocks, append framed
transaction records, and cross a strict barrier. It must also provide a strict
delete barrier. Crash-safe mode refuses to open without it.

The existing Rust any-store strict journal implementation can supply part of
this contract, but the coordinator still needs a typed receipt binding the
barrier to one transaction id, exact CIDs, and journal sequence. Coordinate
persistence must gain an equivalent sync path or become a derived projection
whose loss is repaired exclusively from the transaction journal.

For Node power-loss claims, the capability syncs file contents and the parent
directory after file creation, rename, manifest switch, or deletion. Existing
file-handle journal sync is useful but does not by itself make snapshot rename
or directory-entry changes durable.

Do not model this as `persisted(): boolean` or as a generic
`waitForDurableWrites()` hook on `Blocks`: those surfaces neither define which
transaction owns a write nor cover the heads and native journals.

### Pure native plan and idempotent apply

Split native transaction construction from mutation:

- `prepare...` returns an owned, serializable plan and performs no mutation;
- `apply(txId, planDigest, plan)` applies additions idempotently and defers
  destructive cleanup;
- reapplying the same id and digest reports already applied;
- reusing an id with another digest errors; and
- recovery can apply the plan without reconstructing JS entry objects.

The plan freezes trim hashes and preconditions. Native apply must not call the
current `trim_oldest` selection logic during replay.

### Projection publisher

The log/shared-log boundary needs one idempotent publisher for lower-Log/head,
coordinate, document, signer, cache, and reference deltas. Projectors write
transaction-tagged/shadow versions with `emit: false`; destructive replacement
is deferred to cleanup. It replaces generic append rollback once the prepared
record is durable.

SharedLog owns orchestration because it connects the native backbone, lower
Log, durable block sublevel, coordinate state, and delivery. Native-backbone
owns pure planning and idempotent in-memory application. Log and Documents add
hidden initialization/activation seams and deterministic `emit: false`
projectors so recovery runs after internal stores/indexes exist but before any
public query, RPC registration, synchronizer, announcement, or event.

### Transaction result and control

Crash-safe append results and typed errors expose the transaction id. The
coordinator provides a typed `resumeTransaction(txId)`/result lookup used by
recovery and conformance tests:

- a retained committed id returns the original result;
- a pending id resumes/reports the same sequence and never creates a new plan;
- an id at/below the permanent checkpoint high-water whose result expired
  returns `TransactionResultExpired`;
- a conflicting plan digest fails closed; and
- an outcome-unknown write blocks newer sequences until lease-held
  reconciliation establishes absence or a recoverable prepared transaction.

Callback/event/network failures are reported separately from this storage
outcome and cannot change a committed result into an ordinary rejected append.

### Recovery gate

Program open, append, announce, and local read paths share a recovery state.
They cannot bypass or race recovery. A typed status surface reports pending
transaction count, cleanup debt, last recovered sequence, journal size, and the
reason for any fail-closed state.

`stop()` rejects new sequences before mutation, waits for the coordinator to
reach a recoverable/committed boundary, and never closes block, journal, index,
or native resources beneath an active phase. A crash while stop waits is handled
only by the normal journal recovery rules.

## Rejected Approaches

### Await the durable mirror after native commit

This narrows the acknowledgement window but does not close it. Native facts and
trim side effects already exist before durable intent; a crash or mirror failure
still leaves an ambiguous commit that generic rollback cannot safely undo.

### Volatile tombstones and generations

In-memory delete epochs can reduce races during one process lifetime, but they
vanish on the exact crash being handled. They also cannot resolve a same-CID
re-add after restart without persistent reference history.

### Best-effort rollback

Rollback spans independent block, graph, head, coordinate, document, and signer
stores. It can itself crash, miss a projection, or delete a CID that gained a
new reference. Redo from a durable plan has one deterministic outcome.

### Clean-shutdown barriers

Waiting during `stop()` improves lifecycle hygiene but says nothing about
`SIGKILL` or worker termination. Correctness cannot depend on shutdown running.

### Three native WALs as the commit record

Coordinates, document values, and signer facts can be torn across files and do
not cover blocks or heads. They remain useful projections/checkpoints, not the
commit decision.

### A mutex without a journal

Serialization prevents live races but does not preserve ordering or intent
across process death.

### Recomputing trim during recovery

Length-based selection depends on current state. Re-running it can retire a
second entry after a partial first apply. The prepared record must carry exact
reference removals and replay only those removals.

## Rollout Plan

1. **Journal and strict-store primitives.** Implement transaction-private
   staging, framed records, checksums, strict sync/delete barriers,
   crash-released directory leases, checkpoint generations, parser tests, and
   diagnostics without changing append behavior.
2. **Pure native plan/apply.** Add transaction ids and idempotent apply for one
   single-entry no-trim path while preserving native wire-fusion counters.
3. **Single local append.** Coordinate blocks, native apply, projections, commit,
   and peerless hard-kill recovery behind an explicit experimental option.
4. **Batch and document facts.** Add independent batches, document updates,
   deletes, signer policy, and their cross-projection failpoints.
5. **Reference-aware trim cleanup.** Enable native trim/prune only after the
   persistent ledger and tombstone matrix pass.
6. **Receive/repair commits and OPFS.** Reuse the same protocol for native
   receive paths and add browser worker-termination coverage.
7. **Production default decision.** Measure the three base strict barriers,
   evaluate a separately specified group-commit/atomic-domain extension, and
   decide whether crash-safe mode should replace or sit alongside current
   strict append behavior.

Each phase lands with its red-to-green subset of the matrix. The shelved
post-native mirror/tombstone prototype is not a base branch; only its failure
cases should be reused.

## Compatibility and Storage Evolution

- No wire-format change is required.
- The transaction journal and checkpoint use an independent versioned storage
  namespace under the program directory.
- An empty program may create and sync a genesis checkpoint. The first
  crash-safe slice rejects a nonempty program without the transaction namespace
  using a typed `MigrationRequired` error. A later migration may enable it only
  by taking the exclusive lease, quiescing the program, verifying every live
  block/projection/reference, and strictly syncing a complete baseline
  checkpoint before activation.
- A program not opened in crash-safe mode retains current clean-restart
  semantics.
- Crash-safe mode never silently falls back when a store lacks strict sync.
- Recovery of a newer unsupported journal version fails closed without
  modifying existing projections.
- The canonical transaction checkpoint contains committed native/host
  projection state or references a content-digested immutable projection
  snapshot retained with that checkpoint generation. It also contains
  ownership/tombstones, pending cleanup, program epoch/sequence high-water
  marks, and retained results. A watermark alone does not authorize removing
  older redo plans/private staging.
- Checkpoint replacement uses generation files plus a synced manifest switch
  and required parent-directory sync; recovery always retains either the old
  complete generation or the new one.

## Performance and Observability

With separate staging and journal stores, the base protocol requires three
strict barrier operations before acknowledgement: private staged blocks,
`DURABLE_PREPARED`, and the commit/cleanup-intent tail. A single batch amortizes
those barriers across its rows. Cross-transaction group commit is a future
protocol extension and cannot reorder program sequences or acknowledge a
transaction before its own commit decision is included.

Required diagnostics:

- prepared/committed/clean sequence and transaction id;
- strict block and journal barrier latency;
- recovery transaction count and duration;
- cleanup backlog, retained bytes, and oldest pending age;
- journal/checkpoint bytes and compaction duration;
- replay no-op/conflict counts; and
- typed fail-closed reason with the affected transaction and store.

## Acceptance Gate

Crash-safe mode is not releasable until:

- every failure-matrix row in scope passes with a peerless fresh-process reopen;
- recovery can itself be killed at every mutating phase and subsequently finish;
- all existing clean-restart, shared-log native conformance, document native
  conformance, and full package tests remain green;
- native network end-to-end copy/fusion counters remain at their established
  zero-copy values;
- a 1,000-iteration Node `SIGKILL` soak reports no acknowledged loss, phantom
  head, stale resurrection, or live-CID deletion; and
- each strict storage adapter documents and tests the platform primitive behind
  its barrier.

## Open Questions

- Should a caller-facing idempotency key be part of the first API, or should the
  first release expose transaction ids only in results and typed errors?
- What retention window is required for committed transaction results after
  checkpoint compaction?
- Which OPFS contexts can provide a barrier strong enough for the same claim as
  Node, and what weaker mode should other browser contexts expose?
- After measuring group commit, should the new crash-safe contract become the
  meaning of `appendDurability: "strict"` or remain an explicit third mode?

## Related Implementation Paths

- Native write-through block wrapper and shared-log coordination:
  `packages/programs/data/shared-log/src/index.ts`
- Current clean-restart and coordinate persistence tests:
  `packages/programs/data/shared-log/test/durable-native-restart.spec.ts` and
  `packages/programs/data/shared-log/test/coordinate-persistence-restart.spec.ts`
- Log append durability, native commit, publication, and rollback:
  `packages/log/src/log.ts` and `packages/log/src/entry-index.ts`
- Native append plans, graph/block commit, coordinate/document/signer journals,
  and persistence adapters: `packages/utils/native-backbone/src/index.ts` and
  `packages/utils/native-backbone/src/append_tx/`
- Current mutating native block/graph append implementation:
  `packages/log/rust/src/append.rs`
- Block transport capability delegation: `packages/transport/blocks/src/remote.ts`
- Native any-store durability modes and WAL persistence:
  `packages/utils/any-store/rust/src/`
- Non-replicating native document restart gate:
  `packages/programs/data/document/document/test/durable-native-nonreplicating-restart.spec.ts`
