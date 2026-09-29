# Log history compaction: admission boundary

Status: end-to-end implementation in progress; not a released checkpoint feature
and no history deletion.

Tracking: [#1286](https://github.com/dao-xyz/peerbit/issues/1286). Regression
coverage: [#1468](https://github.com/dao-xyz/peerbit/pull/1468).

## Why physical checkpoints are insufficient

Journal compaction removes obsolete physical records while retaining the same
logical log. Logical compaction would also remove old entries and CUT records.
A receiver must still distinguish a retired operation from a legitimate delayed
write after that information has been removed.

The 100k CUT lifecycle census retained 50,000 CUT heads and 100,000 log rows
despite an empty Documents projection. Its fresh 2k-operation control retained
1,000 CUT heads and 2,000 rows. Logical disk was 84.46 MB versus 2.62 MB and
separate-process reopen was 16.83 s versus 0.386 s. These are one-run baseline
measurements with different history lengths, not a compaction improvement.

## Current evidence and required concurrency control

Consider signed entries where an arrow points from a parent to its child:

```text
A -> B -> CUT(next = B)
```

The source removes A and B. A receiver which knows only the CUT can reject B by
its exact hash. It does not have B's signed parent links, so it cannot establish
that A belonged to the deleted ancestry. The JavaScript and native graph paths
currently admit a separately supplied A. A local Documents diagnostic also
observes the old value becoming visible after CUT-only receive and reopen.
That diagnostic uses disconnected peers, signed entry admission and compat
Documents with each graph implementation; it does not establish production
incidence or behavior of every backend.

The same prefix can have a valid concurrent branch:

```text
     -> B -> CUT(next = B)
A --|
     -> C
```

Here the source retains A because C still references it. A cold receiver must
be able to admit C with A as its ancestry. Both A and C can share the CUT's gid
and have earlier clocks. Gid/clock rejection therefore cannot replace causal
evidence. The regression suite covers direct-victim rejection after reopen and
this surviving-branch control in both graph implementations.

The bounded EntryV0 causal query can prove ancestry only while the required
CID-verified blocks are available. An incomplete walk is not proof of absence,
and a successful walk does not itself establish authorization or stable
replica custody. Remembering the parents of a rejected B would also leave the
A-before-B delivery order unresolved.

## Proposed first profile

Deliver a supported, versioned authority-governed Documents resource. Reuse the
configured owner authority model from
[TrustedNetwork v2](./trusted-network-v2-protocol.md), but define a separate,
narrow checkpoint profile: a fixed writer set, plain document values and an
identity index. Do not silently assign checkpoint authority to any log writer.
This is not activation of full TrustedNetwork v2, dynamic writer revocation or
reader confidentiality. Arbitrary history-dependent `canPerform` predicates,
Program-valued documents and transformed indexes are outside the first profile.

The current v2 operation classifier is not itself a bounded bootstrap adapter:
it can require historical policy/fence/causal blocks. Its envelope also does not
encode ordinary Documents PUT/CUT operations. The checkpoint profile must bind
its own operation format into a new serialized resource version; an open-time
option on an unchanged v1 resource is insufficient.

The version boundary is a compatibility contract, not an experimental runtime
flag. Checkpoint admission and recovery must be mandatory for this resource;
failure must not fall back to legacy replay. Existing resources remain readable
with their existing semantics. Migrating one requires an explicit, verified
state transfer preserving signed heads and Context, not re-signing its values.
Do not release disconnected helpers as the completed feature.

The authority closes an admission epoch over an exact accepted frontier and a
verified application snapshot. Here an epoch means the checkpoint interval
bound into an operation's admission context; it is distinct from v2's content
encryption epoch. Sealing history under the same policy need not rotate keys.
The checkpoint must bind:

- protocol version, immutable resource/log scope and authority;
- checkpoint sequence, predecessor checkpoint and exact policy/resource fence;
- a canonical commitment to the retained frontier and application projection;
- the snapshot block closure needed to rebuild that projection; and
- the operation/projection format and the epoch required for later writes.

A signed hash authenticates a claim, not its completeness. This profile gives
the configured authority the explicit power to finalize the included state.
It does not infer agreement from whichever heads happen to be locally visible.
Multiple writers may contribute to that state; they do not independently seal
conflicting checkpoints. Reuse v2's durable fail-closed fork state when the
authority signs conflicting successors. After truncation, the resource must
halt unless it retained an authenticated common-predecessor snapshot from
which recovery is possible. It must not promise a rewind using deleted history.

### Delayed writes and concurrent branches

Before a seal, retain the existing concurrent-branch behavior. The verified
snapshot includes the effects of all branches accepted into the sealed state,
including C above. After a seal, old-epoch operations omitted from that state
cannot later change the projection. New operations must bind the active fence
and descend from the admitted checkpoint context.

This explicitly changes the treatment of an offline writer whose work was
omitted at cutover. Its work needs application conflict handling and an
authorized new-epoch operation; Peerbit must not silently re-sign or replay it.
Do not activate this policy in existing v1 logs. A new resource version and
explicit migration are required. Applications requiring arbitrary delayed
legacy writes to remain admissible must retain the evidence those writes need.

### Freshness and restart

A brand-new cold peer needs a trusted checkpoint sequence/digest floor supplied
by its bootstrap configuration or an authenticated response bound to a fresh
request to the authority. Merely
receiving a signed checkpoint from a storage peer cannot prove that a newer one
does not exist. A pinned floor proves only that floor; an offline peer must not
report itself current without the selected freshness evidence.

Persist the highest accepted checkpoint and projection watermark before making
the checkpointed state available. Reuse the crash-safe two-slot storage helper
for local publication. Its checksum/generation chain detects incomplete local
publication, but cannot detect restoration of a coherent older directory.
Rollback resistance against that case requires the external trusted floor.

### Admission, custody and deletion are separate gates

1. Authenticate the checkpoint scope, authority, sequence and freshness floor.
2. Verify the complete snapshot closure and reconstruct the exact projection.
3. Publish its watermark durably while serializing admission against the seal.
4. Prove the required replica custody of that closure and handle ownership
   transfer before releasing the old representation.
5. Delete only history for which the new admission rules no longer require
   causal, authorization, projection or ownership evidence.

Existing persisted receipts can contribute exact-block persistence evidence.
They do not themselves prove future custody, closure completeness or agreement
to retire history. A snapshot Merkle root likewise does not establish that its
blocks remain available. Legacy peers cannot count as checkpoint-capable
replicas or be promised catch-up after the history they require is removed.

## Checkpoint cold-join milestone

The outcome is a fresh Documents peer using an authenticated snapshot plus a
bounded suffix, with ordinary reads, inserts, updates and deletes. Preserve
the original visible document heads and `Context`, not just key/value contents.
Do not substitute a newly signed copy of the state in a successor resource:
that would reset document identities, authorship and timestamps.

The internal protected-resource recovery test in #1473 establishes a different
prerequisite: independent retained-history recovery with a projection watermark.
It is not mutable Documents integration or a snapshot importer. Keep the
following work together until the cold-join outcome has executable evidence:

1. Reject wrong admission contexts before resolving parents. The optional
   reject-only log `canJoin` hook is not authorization: `canAppend`/`canPerform`
   and signature checks remain mandatory. In particular, a rejected parent
   does not automatically reject its child under existing log semantics; the
   checkpoint adapter must enforce admitted parent membership itself.
2. Verify and register exact signed boundary entries, and restore their values
   and original Documents context. Certified boundary entries terminate the
   importer's ancestry walk; do not rewrite their signed parent links. The
   snapshot must include surviving branches needed for future behavior, but
   must not require importing every historical CUT head just to represent an
   empty projection. A frontier commitment alone does not supply missing data.
3. Retain and validate the current suffix's causal evidence, including deletes.
   New epochs do not fix `A -> B -> CUT(B)` replay within the active epoch.
   Missing evidence leaves a projection unavailable, not authoritatively empty.
4. Publish snapshot identity, boundary and projection watermark under an
   admission barrier. A verified blob followed by several independent index
   writes is not atomic publication. Reopen must repair or reject an interrupted
   import before serving any view.

No standalone import helper, passing fixture or early-rejection hook completes
this milestone. Require fresh empty-store import, post-import writes and
separate-process crash/reopen to match full replay while every omitted-prefix
lookup is forbidden and counted. Retain source history throughout. Physical
history retirement is a later milestone requiring custody and old-peer tests.

### Prototype findings

The test-only Documents snapshot fixture preserves original signed heads and
`Context`. With `P -> A` represented by a snapshot at A, a complete `B -> CUT(B)`
suffix can remove A without reading omitted P. This required fixing JavaScript
CUT cleanup to traverse admitted index metadata, like the native planner,
instead of loading blocks beyond an unindexed predecessor. It does not change
ordinary join authorization or certify an import boundary.

A concurrent `A -> C` replayed after that CUT still needs to restore A; ordinary
joining then tries to resolve P. Supplying A's exact signed bytes and sorting the
complete suffix are insufficient. A CUT received without its victim evidence
also cannot establish deletion from the snapshot alone. The diagnostic checks
retained graph entries and heads as well as serialized values/context: matching
visible values can otherwise hide different accepted history. The production
profile needs an authenticated terminal boundary and independently retained
suffix evidence, not a new ordering rule silently applied to legacy Documents.

These are disconnected, compat-backend fixtures, not a public importer,
crash-safe publication proof, or measured cold-join improvement.

### Finite-bundle recovery proof

The internal `checkpoint-recovery` fixture now authenticates one owner-certified
snapshot, exact suffix inventory and replay order before mutating private
Documents staging. It pins an externally supplied owner, resource ID and exact
manifest digest. This is a fixed plain-document, single-Ed25519-owner profile,
not general writer authorization. Verification bounds manifest bytes, entry
bytes/count, parent links and total input; it reuses the canonical public EntryV0
scanner from Log before Borsh decoding and checks CIDs, signatures and same-key
causal closure separately. Every supplied block must belong to the inventory.

After `B -> CUT(B)`, the importer can reinstall the exact certified A entry and
its original row/context immediately before joining C. Restoring only A's graph
entry loses the original creation time. Both restores happen while the view is
inaccessible; C still goes through ordinary signature checking and admission.
Only missing certified direct parents of a PUT can be restored. Missing pruned
suffix dependencies are rejected, not silently treated as checkpoint boundaries.
The completed view must match the owner's certified rows, all Context fields,
remaining graph and frontier. The reference is full-history replay, minus
omitted P. Reordered block inventories do not change the certified replay order;
this is not evidence for arbitrary live delivery orders.

The fixture retains the complete authenticated bundle with the existing two-slot
checkpoint store **before** projection. A second commit records the same bundle
and completed-view watermark. Every reopen revalidates and reconstructs fresh
private staging, even when a published watermark exists. A copied read-only view
is returned only after successful replay, publication and scratch cleanup.

Separate-process tests SIGKILL the importer after input retention, after CUT,
before publication and after publication. The original ingress file is deleted
before the fresh reader starts. Recovery matches the exact view with zero
omitted-prefix reads and no view exposure during any phase. Error injection
before and after both atomic replacements also keeps the gate closed; ambiguous
checkpoint instances cannot be reused before reopen.

This proves **first-bundle recovery**, not atomic updates to persistent Documents
indexes. Staging is disposable and the returned result is not a mutable Documents
handle. Later ordinary writes are not durably retained by this fixture. Live
epoch admission/sealing, writer roles, incremental suffix retention, active
checkpoint replacement/fork handling, general document/index schemas, and large
frontiers remain outside its scope. This fixture does not activate a checkpoint
runtime API, retire history, or establish a performance improvement.

The next integration gate is the supported versioned resource: retain
post-import operations before acknowledging them, preserve the read gate across
restart, and test writes concurrent with sealing and omitted offline writes.
Then measure bounded cold join and reopen against the retained-history baseline.

### Runtime integration constraints

Keep the resource descriptor immutable: owner, fixed writer set, document/key
profile and protocol-specific log scope. The changing epoch and checkpoint
watermark belong in durable state, not serialized Program fields that determine
its address. A new Program variant alone does not isolate replication: SharedLog
topics use the lower log ID. Derive and validate the new scope from the resource
descriptor so legacy traffic cannot share it accidentally.

Do not register a decorated subclass of the existing concrete `Documents` or
`PutOperation` classes. Borsh's discriminator tree is shared with legacy readers;
adding children can change their decoding and interacts with serializer caches.
Use sibling wire types and preserve golden legacy bytes and address-based loading
under both module-import orders. Reuse runtime implementation without moving
decorated fields ahead of the legacy discriminator.

An operation needs a signed resource and epoch envelope. SharedLog owns
`meta.data` for replica metadata and overwrites it during append, so it cannot
carry that envelope. Reject a foreign/stale envelope before parent resolution
and validate it again at authoritative admission. Native raw-commit paths must
not bypass the same contract.

The active suffix needs retained causal evidence. Logical deletes in this new
profile can use APPEND records, leaving physical evidence retirement to the
seal; changing CUT to APPEND alone is insufficient. The projection must retain
delete/frontier information, preserve a concurrent branch's original creation
context, and converge independently of receive order. The existing mutable
Documents wall-time/delete projection does not establish these properties.

Recovery must complete before query or replication services advertise the
resource, not merely before the public `open()` promise resolves. Reuse existing
incremental log/block storage for suffix operations and a small two-slot record
for checkpoint publication; do not rewrite the entire snapshot on every put.
Advance a seal under an admission barrier that drains writes and receives.
An interrupted transition must recover from durable evidence before reopening
either service. Persisted delivery still proves exact entry persistence, not
checkpoint completeness or permission to retire the source history.

Required gates include direct and ancestor replay, A-before-B delivery,
surviving branch C, writes concurrent with sealing, omitted offline writes,
same-sequence authority forks, stale/foreign checkpoints, missing snapshot
blocks, bounded verification work, and interruption at every durable publication
boundary. Re-run the matched 100k lifecycle census only once a runtime change
can affect the measured bound; progress to 1M after correctness and resource
budgets pass.

## Runtime integration on the unreleased checkpoint branch

`CheckpointDocuments` is now a separate Program wire variant, not an option on
legacy `Documents`. Its immutable descriptor binds an owner, up to 32 fixed
Ed25519 writers and a resource nonce. The current implementation uses bounded
public JSON values with string `id` keys. `CheckpointDocument.decode()` returns
the plain value; the identity index contains `id` and canonical value bytes,
not automatically indexed nested JSON fields. Transforms and Program values
remain outside this profile.

Every operation signs the resource, epoch and accepted checkpoint digest.
Logical deletes retain APPEND evidence. Current-epoch parents must be admitted,
same-key, same-gid and causally earlier. A first operation in an epoch inherits
that key's certified boundary context. Concurrent tips are retained; visible
state uses a deterministic timestamp/CID order, including tombstones. A PUT
inherits creation time from its live PUT parents; recreation after only DELETE
parents starts a new lifetime, consistently before and after sealing. Bounded
32-parent writes always include the earliest-created live branch when one
exists. The legacy Documents projection rules are unchanged.

Seals omit keys whose entire frontier is DELETE, but retain **every** tip of
a mixed PUT/DELETE frontier, even when a DELETE is the visible winner. Mandatory
epoch-bound admission prevents old operations from resurrecting omitted keys.
Import rejects noncanonical tombstone-only checkpoint groups. Repeated empty
churn can therefore seal to zero records. This bounds logical replay; source
blocks and old epoch logs are not physically reclaimed.

Each checkpoint gets a separate deterministic SharedLog scope. Recovery reads
only authenticated terminal frontier blocks and that scope's active suffix;
it does not traverse terminal parents or enumerate older source logs. Exact
signed heads and inherited creation Context survive import. Both receive and
local-write projection cross physical block/log-index barriers before becoming
visible. The current backend requires actual crash-safe storage capabilities;
an in-memory or unsupported store cannot silently substitute for them.

The transition is deliberately explicit:

1. `createGenesis(blocks, ownerIdentity)` creates an empty initial anchor. It is
   not evidence that genesis is still the latest checkpoint.
2. `peer.open(resource, { args: { checkpoint } })` verifies and retains that
   externally authenticated anchor. Later offline reopen uses the durable floor.
3. `put()` and `del()` append to the active epoch. They acknowledge **local**
   persistence, not remote durability; extra delivery options are rejected.
   `index` exposes local-only queries over admitted data, not unauthenticated
   remote query projections. Its unused generic query RPC is never started.
4. Every writer calls `freezeCheckpoint()`. It fences new writes, drains
   admitted work and network receives, closes the resource, and persists a
   frozen floor. It exports the active epoch's signed causal closure through
   the node block service and durably pins one signed freeze-manifest CID.
   Repeated calls, including after restart, return that same manifest.
5. The owner calls `prepareCheckpoint(freezeCids)` with every fixed writer's
   manifest. It authenticates and durably binds the complete collection, merges
   missing admitted operations locally before starting any network service,
   and proposes the reconciled retained live-key frontier. A single writer may
   call `prepareCheckpoint()` directly as shorthand for its own freeze.
6. Every fixed writer calls `approveCheckpoint(proposal)`. Each reconstructs
   the same manifest collection and checks the exact retained live-key frontier.
   A proposal omitting an accepted live branch cannot gain approval. The owner
   then calls `publishCheckpoint(proposal, approvals)`.
7. Reopen on the returned certificate to start the successor epoch. Observers
   may adopt an authenticated direct successor without signing it. Frozen
   writers recover locally without advertising queries or replication and can
   resume approval after restart.

There is no automatic latest-checkpoint discovery. A fresh client still needs
an externally authenticated current anchor. Ordinary signatures alone cannot
prove freshness. Same-epoch certificate conflicts poison the durable floor
rather than selecting an arrival winner. Query handles and iterators expire
across freeze, failure and same-instance reopen.

Sealing requires exclusive Program ownership; releasing just one of several
owners keeps the resource usable for the others. `drop()` is rejected before
any mutation because history retirement has not been implemented. An unavailable
fixed writer prevents a new seal, by design: silence cannot prove that writer
has no unpublished accepted operations.

### Remaining release gates

This is an **unreleased integration**, not a finished compaction release.
Real network import, successor writes and offline reopen are tested. Separate
processes also verify SIGKILL recovery after committed writes, durable prepare,
certificate publication, successor writes, and a partially committed
multi-writer reconciliation. A fresh process resumes that merge from the pinned
writer collection without receiving an input bundle or expected data. Run the
matched 100k/1M cold-join/reopen census, including peak memory and active suffix
size, and complete cross-platform validation. Raw transfer is processed one
bounded block at a time, but active-epoch facts remain in memory and the
iterative traversal stack scales with causal depth. The suffix continues
growing until a seal. No constant-memory or end-to-end performance gain is yet
claimed.
The original legacy-resource migration gate also remains: creating a new
resource and copying values is not an authenticated migration preserving old
signed heads. Physical deletion of the old epoch logs is intentionally a
separate custody-safe retirement milestone.
