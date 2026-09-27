# Log history compaction: admission boundary

Status: proposed; no runtime activation or history deletion.

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

Use an explicitly opted-in, authority-governed Documents resource. Reuse the
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

Required gates include direct and ancestor replay, A-before-B delivery,
surviving branch C, writes concurrent with sealing, omitted offline writes,
same-sequence authority forks, stale/foreign checkpoints, missing snapshot
blocks, bounded verification work, and interruption at every durable publication
boundary. Re-run the matched 100k lifecycle census only once a runtime change
can affect the measured bound; progress to 1M after correctness and resource
budgets pass.
