# TrustedNetwork v2 policy and revocation protocol

Status: **Proposed**

Tracking issue: [#1346](https://github.com/dao-xyz/peerbit/issues/1346)

This document selects the security model and implementation boundaries for a
future `TrustedNetwork` v2. It is an engineering contract, not a description of
guarantees provided by current releases. Current releases provide the v1 trust
graph and self-owned outgoing-edge revocation while that owner remains trusted,
as described by `@peerbit/trusted-network`. They do not provide deterministic
revocation epochs, roles, historical revalidation, or future-write
confidentiality.

---

## Decision

Peerbit will use:

1. a distinct, version-fenced v2 program and record family;
2. a root-signed chain of sequential, complete policy snapshots;
3. a signed causal fence in each protected resource to activate a policy head;
4. policy-bound application operations, with revocation winning over operations
   concurrent with an activation fence; and
5. per-resource encryption epochs for future-write confidentiality.

The v1 graph will not be upgraded in place. The v2 program has a distinct
serialized variant, address, records, and consumer integration. A peer that
does not understand v2 might cache or serve requested opaque blocks through
generic block transport, but it cannot validate v2 policy, authorize writes,
receive content keys, or count as an authorized replicator.

## Why both a policy chain and a resource fence are required

A monotonic membership sequence orders policy changes, but it does not by
itself order a policy change against entries in another append-only log. A
revoked writer can continue signing entries against an old policy, and a
replica that was offline cannot distinguish an entry signed before revocation
from one signed afterward.

Each protected resource therefore carries a policy fence in its own causal
history. The fence is signed by the policy authority, references an accepted
policy head, and descends from the resource heads included at cutover.

- An old-policy operation that is an ancestor of the fence remains historically
  valid state.
- An old-policy operation concurrent with the fence loses, even if it was
  created earlier in wall-clock time.
- A new-policy operation is valid only when it descends from the fence.

This is deterministic from signed data. It does not depend on message arrival
order, local clocks, or which replica first observed the race. A delayed
operation that was not included in the cutover frontier may be rejected; that
is the explicit availability cost of a convergent revocation boundary.

## Goals

- Express `ADMIN`, `WRITER`, `READER`, and `REPLICATOR` independently.
- Give every replica the same eventual authorization verdict for the same set
  of policy and resource entries.
- Preserve valid pre-revocation history while rejecting old-authority writes
  concurrent with or causally after the revocation fence.
- Prevent a removed reader from decrypting content written under later
  encryption epochs.
- Reject unknown policy versions, role bits, and encryption profiles.
- Converge after reordered delivery, same-storage crash/reopen, and long-offline
  catch-up.
- Keep ordinary operation proofs `O(1)`, avoid a full membership scan, and avoid
  a direct per-member send on each operation.

## Non-goals

- Consensus between mutually distrustful policy authorities.
- Availability during every partition. An isolated replica cannot prove that a
  newer policy head does not exist.
- Erasing plaintext or keys already obtained by an authorized reader.
- Preventing an authorized reader from copying data to a revoked reader.
- Using `canRead`, provider selection, or address secrecy as a confidentiality
  boundary.
- Physically deleting every invalid or superseded block immediately.
- Transparently converting an existing v1 address into v2.
- Resource-scoped role bindings in the initial v2 profile. One network defines
  one authorization domain; use separate descriptors for disjoint role sets.
- Rotating the policy authority inside an existing v2 network. Changing the
  authority requires an explicit new descriptor and migration until a separate
  recovery protocol is specified.
- Reintroducing the former custody/rebalance implementation.

## Terms

- **Policy authority**: the immutable public key configured at genesis that
  signs the sequence of policy snapshots.
- **Policy snapshot**: one root-signed, complete network-wide membership and
  role state in the global policy chain.
- **Policy head**: the accepted snapshot at the greatest contiguous sequence.
- **Resource fence**: a policy-authority-signed entry in a protected resource
  that activates one policy head and encryption epoch for that resource.
- **Policy-bound operation**: an application operation that references the
  exact resource fence and policy digest under which it is authorized.
- **Provisional operation**: an operation exposed before a later accepted fence
  closes its interval.
- **Policy-final projection**: application state after applying the accepted
  non-forked policy and fence prefixes and the race rules in this document.

## Threat model

### Attackers considered

- Untrusted peers may forge malformed objects, replay old valid objects,
  withhold or reorder messages, advertise false availability, and create Sybil
  identities.
- A revoked writer or reader retains all signing keys, ciphertext, plaintext,
  and epoch keys it possessed before revocation. It may remain offline and
  produce entries against an old policy.
- A currently authorized writer may be malicious but does not possess admin or
  policy-authority keys.
- A subject holding a non-admin role may be malicious. The immutable root is
  the sole `ADMIN` in the initial v2 profile; delegated or multi-admin
  finalization is a later protocol extension.
- A replica may crash between receiving policy, fencing a resource, updating a
  materialized index, and persisting the new projection.

### Trust assumptions

- The signature, hash, key-encapsulation, and authenticated-encryption
  primitives remain secure.
- At least one honest path eventually delivers all relevant valid policy,
  fences, and resource entries to a catching-up replica.
- The policy authority protects its key and never intentionally signs two
  children for one parent. Authority-key compromise permits arbitrary grants,
  revocations, and epoch-key distribution and is outside the authorization
  guarantee.
- A strict persistence claim is made only by a storage backend that advertises
  and passes Peerbit's corresponding durability barrier contract.

### Limits under partition

An offline replica cannot know that it has the latest policy. It may
provisionally accept an old-policy operation while isolated. After it receives
the accepted fence, that operation is deterministically removed from the
policy-final projection if it is concurrent with or follows the fence.

APIs must not describe provisional acceptance as globally final revocation
safety. A caller may require named policy and fence digests plus persisted
delivery on replicas advertising that context. A separate future authority
attestation protocol could add freshness evidence. Peerbit still would not
provide distributed consensus that excludes an unseen future policy snapshot.

## Roles

Roles are a fixed-width bitset in v2. Initial assignments are:

| Bit    | Role         | Permission                                                                                                     |
| ------ | ------------ | -------------------------------------------------------------------------------------------------------------- |
| `0x01` | `ADMIN`      | Sign policy snapshots and resource fences; only the immutable root may hold this bit in the initial v2 profile |
| `0x02` | `WRITER`     | Produce policy-bound application mutations for a protected resource                                            |
| `0x04` | `READER`     | Receive the private material needed to decrypt a protected resource's current content epoch                    |
| `0x08` | `REPLICATOR` | Be selected and counted as an authorized replica for protected-resource availability guarantees                |

Roles apply network-wide to every resource that explicitly opts into the
descriptor. They do not imply one another. In particular, a writer need not be
a reader, and a replicator need not receive decryption material. Unknown set
bits fail closed until a later protocol version defines them.

The policy authority is the only normal snapshot finalizer. It is an explicit
protocol principal, not an implicit grant obtained through graph reachability.
Recovery from authority equivocation is deliberately outside that normal path.

The first implementation accepts policy snapshots signed by the root authority
only and rejects `ADMIN` on another subject. A later delegated-admin protocol
requires its own versioned proof and fork rules.

## Protocol objects

The v2 codec scaffold assigns the following Borsh variants: descriptor
`[2, 0]`, policy snapshot `[2, 1]`, resource fence `[2, 2]`, and operation
policy proof `[2, 3]`. Golden-byte fixtures pin their field order. These codecs
remain internal and non-activatable; pinning bytes does not yet expose a
protected-resource API.

### `NetworkDescriptorV2`

The immutable descriptor contains:

- protocol version `2`;
- an explicit 32-byte network nonce;
- the policy-authority public key;
- the genesis policy digest;
- the fixed policy hash identifier; and
- the fixed Peerbit entry-signature profile.

The network ID is SHA-256 over a domain separator, protocol version, network
nonce, and authority key. It does not include the genesis digest, avoiding a
circular commitment. Genesis has sequence `0`, an all-zero previous-policy
digest, the derived network ID, and a sorted binding that assigns `ADMIN` to the
authority. The authority signs its enclosing Peerbit entry, and the descriptor
pins the resulting canonical body digest.

Initial authority-record signature profile `1` is `EntryV0 authority-only` for
policy snapshots and resource fences: the entry must use the normal
`EntryV0.toSignable()` coverage (including causal metadata), contain exactly
one successfully verified signature, and that signer's canonical public-key
bytes must equal the descriptor authority. A different entry type, signature
cardinality, coverage rule, or signer requires a new profile identifier. This
profile does not govern writer operations. The internal operation profile
below is separately pinned; protected-resource adapters must explicitly opt
into it and validate their application payload before use.

Profile `1` also fixes the maximum canonical serialized policy-entry size at
131,072 bytes. Replicas apply that ceiling to direct admissions, resolved
parents, restored anchors, pending candidates, and fork observations before
copying, decoding, or verifying the entry. The ceiling is a consensus rule,
not a replica-local resource setting; changing it requires a new signature
profile or protocol version.

The resource-fence envelope uses a separate fixed ceiling of 8,192 bytes and at
most 64 direct predecessors. Both bounds are checked from captured raw storage
before generic entry decoding; they are likewise protocol constants rather than
resource-adapter settings.

The top-level program uses a new variant such as `trusted_network_v2`.
`"trusted_network"`, `"relations"`, `IdentityRelation`, and existing v1
addresses retain their present meaning.

### `PolicySnapshotBodyV2`

The unsigned canonical body contains:

- network ID;
- `sequence: u64`;
- previous policy digest, or the genesis sentinel;
- sorted, unique subject bindings containing the signing key, role bitset, and
  a reserved versioned encryption-key commitment.

Bindings are sorted by canonical signing-key bytes and contain only subjects
with a non-zero role bitset. Every snapshot contains the root with `ADMIN`; no
other subject may hold that bit in the initial profile.

Each snapshot is the complete current state, not an arrival-ordered delta. V2
does not use transitive trust edges: current authority is derived directly from
the signed subject bindings. Regranting a former admin therefore cannot
resurrect an orphaned v1-style outgoing edge.

The policy digest is SHA-256 over domain-separated canonical body bytes and
excludes signatures. The body is carried by a Peerbit entry whose sole verified
signer is the root authority; the existing entry signature covers the payload
and causal metadata. `previousPolicyDigest` always names the body digest, so
signature encoding cannot create several policy identities for one body.

The implementation must reject non-canonical encodings, duplicate subjects,
non-sorted bindings, `ADMIN` on a non-root subject, unknown role bits,
wrong-network replay, and invalid authority signatures. A structurally valid
snapshot whose parent is missing remains pending and triggers a bounded parent
fetch. Once the parent is known, any sequence other than exactly
`parent.sequence + 1` is invalid.

### `ResourceFenceV2`

A fence is an entry in the protected resource's causal log and contains:

- network and resource IDs;
- monotonically increasing resource-fence sequence;
- previous resource-fence digest;
- accepted policy sequence and digest; and
- content-encryption epoch and epoch-manifest digest.

The fence is a normal resource-log entry whose sole verified signer is the root
authority. Its entry signature covers the fence payload and causal links; the
resource-fence digest is the 32-byte raw digest from that canonical signed
entry's CIDv1 using SHA-256. `resourceId` is a 32-byte value whose derivation
must be fixed by the future protected-resource adapter from its canonical
manifest. No second signature is embedded in the payload.

The signed EntryV0 envelope must be a canonical raw APPEND block with public
metadata, payload, and sole authority signature, four zero reserved bytes, and
no embedded hash. Its `meta.clock.id` is the canonical authority key. It limits
`meta.next` to 64 unique canonical CIDv1/raw/SHA-256 direct predecessors and
fails closed rather than truncating them. Canonical `[2, 2]` resource-fence
bodies are exactly 186 bytes and any other length fails before generic decode.
For secp256k1, signature data is the signer-produced 132-byte ASCII form: `0x`,
128 lowercase hex digits, and recovery byte `1b` or `1c`; `s` must be low
(`<= 0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0`).
This removes textual, recovery-byte, and high-S signature malleability from raw
fence identity.

The previous fence must be a causal ancestor, but it need not consume a direct
`meta.next` slot when an intervening resource entry already proves that
ancestry. Lifting the limit requires a separately specified generic bounded
merge or authenticated-frontier primitive. Constructing a fence may inspect
`O(current heads)` at a policy transition, but ordinary writes must not carry
that cost.

TrustedNetwork v2 now contains an internal resource-fence reducer for one
immutable network, resource, and log scope. It authenticates
policy-authority-signed EntryV0 fence bytes, enforces the exact predecessor and
monotone fence, policy, and content-epoch transitions, verifies bounded
CID-linked causal descent, preserves the exact signed `meta.next` projection
(including an explicitly empty initial frontier), and records a canonically
ordered pair of observed authority-fork proofs. Replicas converge on the same
terminal state and common parent, but if the authority signs more than two
siblings, the retained pair depends on which conflict is observed first and is
not a complete equivocation set. Its durable wrapper ties every mutating
admission to the fence's exact accepted-policy-prefix lease. This does not make
either component a protected resource API: they do not interpret
application operations, revalidate their projections across a fence, prove
that an offline peer has seen the newest fence, or provide encryption-epoch key
rotation.

### `OperationPolicyProofV2`

Every protected mutation commits to:

- network and resource IDs;
- active policy sequence and digest;
- active resource-fence digest;
- content epoch when the payload is encrypted.

These fields are part of the signed operation. Supplying them only as unsigned
transport metadata is invalid. An operation is role-authorized when at least
one verified Peerbit entry signer has `WRITER` in the referenced snapshot.
Additional signers do not grant authority and remain available for an
application's own multisignature policy.

The proof, authorizing entry signatures, and role snapshot are public and can
be validated over opaque ciphertext. A `REPLICATOR` without `READER` cannot run
an application predicate that depends on plaintext. Such a predicate requires
a reader-validator or a future zero-knowledge proof; a blind replicator's
receipt proves storage, not plaintext-dependent semantic admission.

Canonical `[2, 3]` operation proofs are exactly 146 bytes and any other length
fails before decode. This pins the signed payload's wire fields only. Entry
authentication, accepted-policy provenance, and causal validation remain future
integration work.

### Encryption-epoch requirement

The later encryption slice must finalize the reserved subject encryption-key
commitment, define one fixed encryption suite, rotate a fresh per-resource
epoch secret whenever the reader set changes, and seal that secret to current
readers in `O(readers)`. Activating `READER` for a confidential resource
requires a supported policy-authenticated encryption key. The manifest and
encrypted payload bind the network, resource, policy, fence, and epoch
identifiers as authenticated data. The exact manifest and envelope codec is
intentionally not fixed before that crypto review, so preliminary v2 codecs
remain internal and non-activatable.

## Policy-chain validation and forks

A policy snapshot is valid only when:

1. its previous digest is the current parent under evaluation;
2. its sequence is exactly the parent sequence plus one;
3. bindings are complete, sorted, unique, and understood;
4. the network and protocol domains match; and
5. the policy-authority signature is valid.

Parent-resolution attempts are locally deadline-bounded and receive a
lifecycle cancellation signal. A timeout, cancellation, resolver failure,
malformed response, oversized response, or response for the wrong digest is
dependency unavailability rather than proof that the candidate is invalid. A
candidate whose own ancestry cannot be resolved stays in the bounded pending
set with an exact parent-fetch hint. Failure to resolve ancestry needed to
compare against the already accepted head makes authorization `UNAVAILABLE`
and fail closed until an explicit retry succeeds. Late resolver completion
cannot mutate reducer state.

The authority must sign at most one child per parent. Two otherwise valid
authority-signed children are durable evidence of equivocation. The network
enters `FORKED` at their common parent: replicas persist both proofs, stop
advancing accepted policy, and reject policy-final publication of dependent
writes until an explicit recovery protocol or a new network genesis is
selected.

V2 does not choose a branch by arrival order, HLC, wall-clock time, branch
length, or lowest hash. Those rules would let a policy fork silently rewrite
authority. A replica that provisionally followed one child must roll back to
the common parent after learning the fork. Recovery from authority
equivocation is a separate protocol and is not invented by a local branch
choice.

## Resource activation and write validation

Policy changes do not silently alter a separate application log. They become
active for a resource only through its next accepted resource fence.

Structural admission and policy projection are separate. A known-version
object with a missing policy, fence, or frontier proof is quarantined in a
bounded pending set while its exact dependencies are fetched. It is neither
projected nor permanently rejected merely because messages arrived out of
order. Bad signatures, malformed chains, unsupported versions, and known forks
fail closed.

For each operation, a validator must:

1. validate the referenced policy chain and accepted resource fence;
2. require the operation to descend causally from that fence;
3. require its policy proof to exactly match the fence;
4. require at least one verified entry signer to hold `WRITER` in the
   referenced policy snapshot;
5. validate the application's additional signature policy, if any; and
6. validate encryption-profile and content-epoch bindings when content is
   protected.

When replacing fence `F(n)` with `F(n+1)`:

- entries under `F(n)` that are ancestors of `F(n+1)` remain valid history;
- entries under `F(n)` concurrent with `F(n+1)` are absent from the policy-final
  projection;
- entries under `F(n)` that causally descend from `F(n+1)` are invalid;
- entries claiming `F(n+1)` that do not descend from it are invalid; and
- entries whose referenced policy or fence is not yet available locally are
  quarantined while that context is fetched, not permanently arrival-time
  rejected.

If multiple valid authority-signed fences compete for the same previous fence,
the resource enters a persisted fail-closed `FORKED` state at the previous
fence. Neither child is selected by arrival order or digest.

A fence is valid only when its sequence is exactly its parent's sequence plus
one, its policy sequence does not decrease, and its content epoch does not
decrease. Reusing a policy sequence requires the same policy digest. Every
reader-set grant or revocation requires a strictly newer content epoch; a fence
may also advance the epoch without changing readers. A same-policy,
same-content-epoch fence may be emitted solely to close a provisional operation
interval.

## Revocation procedure

Revocation of one or more roles is complete for a resource only after:

1. the authority signs the next role-removal policy snapshot;
2. reader removal, when applicable, creates a fresh content epoch and manifest
   without the removed reader;
3. the authority appends a resource fence referencing that policy and epoch;
4. the policy snapshot, epoch manifest and envelopes, cutover-frontier
   dependencies, and fence cross the requested local persistence barriers;
5. the local projection publishes the exact fence digest; and
6. when requested, there is one cohort of at least `N` distinct, currently
   authorized and capable replicas advertising the same policy/fence context,
   and every replica in that cohort acknowledged every exact-hash dependency in
   the closure.

Per-object receipt counts from disjoint peer sets cannot be combined into that
closure guarantee. The implementation therefore needs peer-correlated batch or
closure evidence; independent single-entry receipt totals are insufficient.

Resources opt into the network independently, and there is no global resource
registry in the initial profile. An API reports evidence only for the resources
named by the caller. It must not claim network-wide revocation or infer the
state of an unlisted resource.

Concurrent old-policy writes lose unless they are ancestors of the signed
fence. The authority serializes policy changes into snapshots; competing
root-signed successors trigger `FORKED` instead of an arrival-order choice.
Regranting a subject later creates a new policy state and fence; it does not
validate operations rejected at the earlier boundary.

Persisted-replica receipts prove that exact bytes crossed the advertised
storage barriers. They do not prove that a policy branch can never later be
shown to have forked, and they do not turn provisional authorization into
consensus finality. They also do not prove that every remote replica has
published its revalidated application projection; that convergence remains
observable separately.

## Revalidation, rollback, and events

Authorization is not an ingest-only predicate in v2. A replica maintains the
policy and fence digest used by each materialized projection. When it learns a
new accepted fence or fork evidence, it revalidates the affected interval and
removes or reapplies rows deterministically.

Revalidation starts at the last fence on the stable common policy-and-fence
prefix. Under normal policy advance this is the previous fence; late fork
evidence can require a longer rewind. Implementations should enforce a maximum
open interval by emitting same-policy seal fences, but this protocol does not
claim bounded worst-case recovery until a generic authenticated resource
projection checkpoint exists.

Provisional user-visible events cannot be made unseen. A v2 consumer must
choose one of two explicit feeds:

- a policy-final feed that emits an operation only after a named later fence
  includes it in the closing frontier; or
- a provisional feed that can emit a corresponding retraction.

Policy-final means final relative to that accepted closing fence. Later
authority-equivocation evidence still places the resource in `FORKED` and may
require rollback to the common predecessor; this protocol does not call that
consensus or irreversible global finality.

Silently retaining an operation in one replica's materialized index after it
loses the policy race is invalid.

## Crash recovery and offline catch-up

On reopen, a replica verifies the persisted policy high-water mark, resource
fence, fork evidence, epoch-manifest digest, and materialized projection
watermark before serving policy-final reads or accepting operations under the
persisted fence. If the projection watermark is torn, the replica rebuilds from
a verified resource projection checkpoint when one exists, otherwise from the
retained stable prefix. The checkpoint format is an implementation prerequisite
for any bounded-recovery claim.

An offline replica may hold an old policy and old epoch keys. On catch-up it
must:

1. validate the contiguous policy and resource-fence chains;
2. halt at persisted policy or resource-fence fork evidence;
3. revalidate entries since the last common fence;
4. retract stale materialized rows; and
5. refuse to distribute or use a superseded epoch key for new content.

Old-policy entries learned from that replica remain invalid when they are not
ancestors of the accepted cutover fence. Catch-up therefore cannot restore
revoked authority, although the isolated replica may have exposed provisional
state before learning the revocation.

The highest accepted contiguous sequence, its accepted policy and fence
digests, and every fork proof admitted before the declared fail-stop boundary
must be stored durably. Pending objects with missing context do not advance this
rollback anchor. This prevents an ordinary reopen from forgetting a known
revocation or fork. Restoring an older complete storage snapshot can still roll
back that local anchor; preventing operator-level snapshot rollback requires an
external monotonic anchor and is not claimed by this protocol.

The initial internal policy anchor uses Peerbit's generic crash-safe two-slot
checkpoint over a dedicated descriptor-scoped store or sublevel. It fails
closed unless the backend advertises atomic durable replacement; `persisted()`
or a flush barrier alone is not sufficient. The reducer constructs and
validates a complete replacement snapshot, then publishes its new authorization
projection only after the atomic replacement resolves. Reopen crosses a fresh
physical barrier, validates both present linked slots, re-authenticates the
exact retained policy-entry bytes, and restores `UNAVAILABLE` or `FORKED`
without serving authorization in either state. Candidate-only missing-parent
work remains intentionally ephemeral.

The application checkpoint scope binds the policy-anchor owner, payload-format
version, and exact network descriptor. A `FORKED` payload contains its common
parent plus the complete canonical set of at most 66 exact child observations
in one atomic replacement. A crash can therefore expose only the complete
previous payload or the complete fork payload, never a committed partial proof
prefix. Malformed, mixed, unrelated, oversized, duplicate, reordered, or
semantically invalid evidence fails closed. Earlier append-generation records
are explicitly rejected; there is no migration fallback while v2 remains
non-activatable.

This checkpoint assumes one writer owns the dedicated store. It does not claim
multi-process compare-and-swap, writer authentication, protection from replay
of a coherent older whole-storage snapshot, or a protocol bound on physical
LevelDB files before compaction. Backends that do not advertise atomic durable
replacement remain unsupported instead of receiving a weaker
TrustedNetwork-specific persistence fallback.

The pending-policy ceiling is protocol-fixed at 64. The transition to `FORKED`
can therefore retain at most those 64 children plus the accepted and triggering
children: 66 exact child proofs in total. The common parent and complete proof
set occupy one self-contained payload bounded at 8,782,137 bytes, including
canonical framing. After that replacement is durable and `FORKED` publishes,
the reducer returns a constant-size halted result before entry capture, head
projection, authentication, resolution, core mutation, or store access. Reopen
uses temporary bounded evidence to verify and derive the canonical two
children, then releases the full checkpoint payload before returning the
terminal reducer.

Before `FORKED`, the durable wrapper also bounds operations retained behind a
blocked store barrier or callback. At most 64 calls and 256 KiB of captured
queue-input bytes may be unsettled at once. Policy admissions account for their
entry bytes, accepted-prefix leases account for their 32-byte reference digest,
and zero-byte retry calls share the same count bound. Count and byte capacity
are reserved synchronously before retaining caller bytes or callbacks. A lease
that expires or is cancelled while queued settles outward immediately, but its
internal queue slot and reservation remain until that closure reaches the head,
skips the callback, and dequeues; every internal completion path then releases
the reservation. Oversized or incompatible typed-array inputs return
`rejected`; well-formed submissions over the wrapper limit return `capacity`.
Neither path enters the core or retains the caller buffer, and
`pendingCount`/`pendingBytes` continue to describe only the core pending-policy
set.
Boundary length checks use the intrinsic typed-array byte extent and copying
uses the intrinsic typed-array set operation, so subclasses, shadowed
`byteLength`, custom iterators, proxies, and detached views cannot bypass either
the 128 KiB entry ceiling or the wrapper accounting.

The same internal wrapper provides a callback-scoped accepted-policy-prefix
lease for the later resource-checkpoint reducer. The caller names one policy
sequence and digest. In one serialized queue slot, the wrapper proves that
reference is the current durable head or an authenticated historical prefix,
then invokes the callback with independent copies of the referenced policy and
the accepted head. Historical lookup streams backward through the resolver and
retains no policy history or ancestry cache. A future or unavailable reference
returns `unavailable`; a digest conclusively outside the accepted prefix returns
`rejected`; a forked, aborted, or publication-ambiguous anchor returns `halted`.
Only a callback that fulfills returns `completed`; a callback rejection is
propagated unchanged and does not halt or modify the anchor.

Historical lookup work is linear in the distance from the accepted head. Each
lease may now set an exact authenticated-parent step cap, an absolute deadline,
a relative timeout that includes queue wait, and a caller cancellation signal.
The resource-fence wrapper fixes those bounds for every acquisition. Exceeding
one returns `unavailable` without invoking the callback; a conclusive prefix
mismatch remains `rejected`.

The anchor also has an internal `withExactPolicyHead` readiness lease for the
next protected-operation slice. Its requirement names one canonical
CIDv1/raw/SHA-256 policy-entry block. A dedicated CID resolver must return that
exact bounded block; the anchor hashes it, re-authenticates its authority
signature and network-bound policy body once, and binds the authenticated bytes
to an opaque reducer-owned prepared token. Under one serialized authorization
fence, the reducer evaluates only that named candidate with one aggregate
64-parent-edge ceiling, admits it without draining later pending successors,
and publishes any changed state through the crash-safe checkpoint. Only after
commit does the anchor assert that the named sequence/body-digest identity is
the durable current head and invoke the callback. A same-body alternate wrapper
for an already-current head performs no checkpoint replacement. Hashless and
hash-bearing canonical EntryV0 wrappers may therefore have different CIDs while
representing the same policy identity. Missing CID resolution fails
`unavailable` with no fallback to the digest resolver. One fixed ten-second
relative ceiling (which a caller may only shorten), cancellation signal,
operation-queue reservation, and single background-authentication slot cover
the complete fetch/evaluate/checkpoint/acquisition path.

Cancellation, deadline, or ancestry-budget exhaustion before projection is
transient and does not durably poison the reducer. An accepted-ancestry
dependency failure can still enter durable fail-closed `UNAVAILABLE`. Once core
admission may have mutated state, checkpoint replacement is indivisible: the
caller may settle promptly, but the internal queue reservation and authorization
fence remain until commit or terminal failure. No callback or in-memory
publication occurs for a stale lifecycle. A successful commit may nevertheless
be recovered by a fresh opener, and callbacks are not replayed.

This exact-head promotion remains absent from the package root and is not a
complete owner-revocation API. It does not establish a resource fence, operation
replay, TrustedNetwork v2 activation, or the accepted-remote-entry/frontier
callback, and it does not close the remaining work in issue #1417.

Lease acquisition linearizes immediately before callback invocation. Caller
cancellation or deadline expiry before that point returns `unavailable` without
invoking the callback; lifecycle abort or authority equivocation returns
`halted`. Caller cancellation or deadline expiry after invocation does not
revoke the lease or halt the anchor: the wrapper holds the queue until that
callback fulfills or rejects. A lifecycle abort after invocation likewise does
not revoke the durable fact already proved, but later operations observe the
halted lifecycle after the callback releases the queue. A lease callback must
not await another operation on the same anchor because it already owns that
anchor's queue slot. The lease proves only the accepted prefix known to this
anchor at acquisition; later authority-fork evidence can still require resource
rollback. It does not make the policy and resource stores atomically commit
together.

The internal resource-fence wrapper serializes each reducer mutation inside
such a lease and publishes a changed fence head only after committing a
scope-bound crash-safe two-slot checkpoint. It re-authenticates retained fence
bytes, accepted-policy references, and immediate causal transitions on reopen;
persists comparison unavailability and canonical two-child fork evidence; and
uses fixed count, byte, ancestry, dependency-time, and admission-work bounds.
The deadline does not cancel an atomic checkpoint replacement after mutation;
the wrapper keeps the policy lease until that durable operation settles.
After ordinary ingest it deterministically retries the bounded set of
already policy-leased candidates, acquiring a separate exact lease for each so
that a resolver-loaded unleased ancestor cannot become fork evidence.

Its internal `withExactResourceFenceHead` lease resolves one canonical fence
CID through the existing exact-entry resolver and authenticates the bounded
signed EntryV0 before mutation. It admits only that named candidate under an
accepted-policy lease, commits the resource checkpoint, requires the durable
current fence CID to equal the requested CID, and holds policy-then-resource
serialization through the callback. A historical duplicate cannot satisfy the
current-head check. Later pending fences are not automatically drained by this
acquisition. The callback receives copies of the exact signed frontier,
including an empty initial frontier, referenced policy, and accepted policy
head; it does not establish complete resource replay or distributed freshness.

Acquisition shares one queue-inclusive ten-second ceiling that callers can only
shorten. Every call reserves the maximum 8 KiB entry response plus its bounded
CID against the existing 32 KiB/64-operation wrapper budget before resolution.
One preparation lane prevents signal-ignoring resolution or authentication from
accumulating after outward cancellation. Cancelled calls retain reservations
until their internal work actually settles; expired queued calls skip work.
Before callback entry, cancellation settles promptly and prevents the callback.
An in-flight checkpoint still completes under both queue slots, and only a
fresh opener may publish its result if the old lifecycle was aborted. Once the
callback begins, cancellation no longer retracts its lease; callback rejection
propagates without poisoning the anchor. Callbacks must not await another
operation on either anchor because they hold both serialized queues.

Pending candidates that have not reached a durable comparison remain replay
work, not a durable authorization fact. A future protected-resource adapter
must replay its complete retained log and resolve or fail closed on that work
before serving policy-final reads. It must also bind signed application entries
to the accepted fence, implement deterministic revalidation/retractions, and
define the accepted-remote-entry/frontier callback. Consequently the resource
reducer and wrapper remain absent from the package root and are omitted from
the published npm artifact while non-activatable. They do not provide a public
owner-revocation primitive, a cold-peer non-resurrection guarantee, physical
block reclamation, tombstone compaction, or content-encryption key rotation.

This bounds ordinary `ACTIVE` and `UNAVAILABLE` anchor history to two logical
keys as well as bounding authority-equivocation evidence. It does not truncate
resource logs, compact tombstones, or provide the wider authenticated
projection checkpoint required by protected resources. The implementation
therefore remains internal and non-activatable while those resource-enforcement
gates are incomplete.

### Internal protected-operation classification

The internal `ResourceOperationEnvelopeV2` uses variant `[2, 4]` and operation
profile `1`. Its signed public payload contains the fixed 146-byte `[2, 3]`
proof, the exact 32-byte epoch-manifest digest, and opaque application bytes.
The envelope has 185 bytes of fixed overhead. This profile requires canonical
public `EntryV0` APPEND storage with no embedded hash, zero reserved bytes,
canonical unique CIDv1/raw/sha2-256 parents, and one to eight unique verified
Ed25519 or canonical low-s secp256k1 signatures in signature-byte order. The
clock principal must be one verified signer. At least one signer must have
WRITER in the exact named historical policy; the role lookup uses the policy's
sorted bindings. Signatures cover the policy, resource, fence, content epoch,
manifest, application payload, and causal metadata together.

The internal operation classifier acquires the exact accepted historical
policy lease before the resource queue and holds both through classification.
It walks backward from the durably committed fence head to the named fence,
retaining its first closing child. The operation must descend from its named
fence. An ancestor of the closing fence is `policy-final` relative to that
accepted local prefix; a concurrent operation or one descending from the
closing fence is rejected. An operation in an open interval is `provisional`.
Later regrant does not change the first closing fence. Missing historical
policy/fence/causal blocks, an unseen named fence, cancellation, and exhausted
work budgets are `unavailable`, not permanent operation rejection. Retrying
after exact dependencies arrive computes a fresh verdict; there is no verdict
cache, retained replay queue, history truncation, or materialized projection.

Operation envelopes are capped at 128 KiB, application payloads at 64 KiB,
metadata at 16 KiB, and direct predecessors at 64. Each classification uses
at most 64 fence predecessor steps, 4,096 policy steps, and three bounded
causal walks. Each causal walk permits 1,024 entries, 16 MiB, and 65,536 links.
One queue-inclusive deadline covers classification. The operation engine
retains at most 64 calls and 512 KiB of captured input, and at most 64 unsettled
causal resolver calls even if a resolver ignores cancellation. Dependency
hints are capped at 64. Missing history remains necessary for retry; none of
these work ceilings grants permission to discard it.

These modules are excluded from the package root and published artifact.
The generic signed envelope does not interpret or authorize an application's
payload semantics. A public protected-resource adapter, durable pending-context
replay, projection revalidation and retractions, accepted-remote-entry/frontier
callbacks, and end-to-end long-offline synchronization remain required before
public revocation guarantees. The classifier does not rotate or release keys.

## Confidentiality boundary

Replication and routing operate on ciphertext and public policy metadata.
`canRead` may reduce accidental disclosure or unnecessary transfer, but it is
not relied upon to protect content.

Every grant or removal of `READER` rotates the resource epoch. A removed reader
can retain and decrypt old epochs for which it already has key material, but
receives no envelope for a new epoch. Writers and replicators do not receive
epoch private material solely by holding their respective roles.

The default history policy is forward-only. A newly added reader receives the
new epoch, not prior epoch private material. Granting historical access requires
an explicit, separately audited rekey or key-release operation.

The guarantee does not survive compromise of the policy authority, compromise
of a currently authorized reader, plaintext leakage by an application, or
deliberate reader collusion.

## Version fencing and migration

V2 is side-by-side with v1:

- use a new top-level program variant and address;
- use new tagged policy, fence, proof, and epoch records;
- pin exact variants, field order, domain separators, role bits, and golden
  bytes in tests;
- reject unknown versions, role bits, snapshot fields, and encryption profiles;
- never fall back to v1 authorization when a v2 controller cannot be opened;
  and
- require protected consumer programs to serialize an explicit v2 controller
  commitment.

Existing consumers that serialize `TrustedNetwork` directly need explicit v2
consumer variants. Substituting v2 inside their existing manifest would change
program addresses and mixed-version behavior implicitly.

The v1 owner-delete hardening was schema-compatible but tightened validator
semantics; older validators can still accept a trusted non-owner delete. That
coordinated-upgrade requirement is one reason v2 cannot share a v1 address.

Migration creates a new v2 policy genesis and a new v2 protected-consumer
program/address. The operator imports a reviewed v1 membership snapshot,
assigns roles, distributes the first epoch, and coordinates cutover. There is
no in-place migration fence and no automatic interpretation of v1 graph
reachability as live v2 policy in the initial profile.

## Bounded state and work

The implementation target is:

- active policy state: `O(current members)`;
- each complete policy snapshot: `O(current members)`;
- ordinary operation proof size: `O(1)` aside from application payload;
- role lookup: `O(1)` or `O(log current members)`;
- active key distribution across resources:
  `O(sum of current reader assignments)`;
- reader rotation: `O(current readers for that resource)`;
- direct publisher fanout: bounded, while total dissemination still scales
  with recipients;
- policy-fork evidence: at most 66 exact child proofs in one complete bounded
  checkpoint payload, followed by constant-time, constant-size rejection with
  no entry, projection, authentication, resolver, core, store, or retained-state
  work;
- durable policy operation buffering: at most 64 unsettled calls and 256 KiB of
  captured queue-input bytes;
- fence creation: `O(current heads)` until a generic bounded merge/frontier
  primitive exists; and
- revalidation: entries since the last stable common fence, which is not a
  worst-case constant bound.

The policy anchor is now logically bounded by the generic two-slot checkpoint,
but durable resource history remains unbounded until safe log truncation exists.
Older resource snapshots may be removed only after all retained fences and
historical operations that reference them are covered by an authenticated
projection checkpoint and the storage layer proves the prefix is safe to
remove. V2 must use that generic primitive rather than add a
TrustedNetwork-specific history store.

## Required implementation gates

### 1. Codec and version fence

- Pin published-v1 decoder fixtures and golden addresses for `TrustedNetwork`
  and consumers that serialize it directly, including
  `IdentityAccessController` and `ClockService`.
- Pin every v2 variant, field order, role bit, and domain separator.
- Prove a legacy decoder rejects a v2-controlled program instead of opening it
  with permissive v1 semantics.
- Prove unknown v2 records and role bits fail closed.

### 2. Policy engine

- Exhaust bounded-fixture delivery permutations and use seeded or
  property-based reordered schedules for larger policy chains; all runs must
  project the same role state.
- Keep a known-version snapshot with a missing parent bounded-pending and fetch
  that parent; reject a non-contiguous sequence once its parent is known.
- Reject invalid authority signatures, non-canonical snapshots, rollback, and
  wrong-network replay.
- Reject `ADMIN` on a non-root subject and cover entries with additional
  non-authorizing signatures.
- Persist `FORKED` and halt when two root-signed successors share a parent.
- Prove a later regrant does not resurrect a rejected pre-revocation mutation.

### 3. Resource fence and revalidation

- Deliver revoke, fence, and writes in opposite orders to two replicas and
  prove identical policy-final state.
- Cover grant/write and revoke/write concurrency, delayed pre-fence writes,
  missing-context quarantine, fence forks, and provisional retractions.
- Reject fence-sequence, policy-sequence, and content-epoch rollback.
- Enforce the direct-frontier bound without silently omitting a head.
- Require one replica cohort to acknowledge the entire persisted dependency
  closure; reject disjoint per-object receipt cohorts.
- Prove crash/reopen and long-offline catch-up preserve the same verdict.

The internal `[2, 2]` fence-body and `[2, 3]` operation-proof codecs are pinned.
Together, the internal reducer and durable wrapper now cover signed EntryV0
authentication, exact accepted-policy provenance, bounded causal ancestry,
monotone transition and fork reduction, bounded canonical dependency hints,
and crash-safe persistence of accepted, unavailable-comparison, and fork states. The policy
lease and fence-admission walks have aggregate bounds. The bounded internal
operation envelope now authenticates signed application bytes and classifies
their fence-relative authorization. Application payload semantics, durable
replay of all pending context, materialized projection
revalidation and retractions, the public accepted-entry/frontier callback, and
end-to-end long-offline catch-up remain open. This foundation does not complete
the gate or provide a usable authorization path.

### 4. Role enforcement and encryption

- Exercise every role independently for policy mutation, append, key receipt,
  plaintext read, replication selection, and persisted-replica counting.
- Prove a revoked reader can decrypt retained old content but cannot decrypt a
  new epoch.
- Prove a newly granted reader receives no historical epoch by default, and
  that explicit historical release is separately authorized.
- Prove writer-only and replicator-only identities receive no epoch private
  material.
- Prove a blind replicator can validate the public role envelope but cannot
  claim plaintext-dependent semantic admission.

### 5. Bounds and fault injection

- Benchmark policy-state size, snapshot and fence replay, reader rotation,
  steady-state write overhead, frontier size, and provider fanout.
- Hard-kill between policy persistence, fence persistence, epoch-key
  persistence, index rollback, and projection publication.
- Keep partial v2 codecs and reducers internal and non-activatable. Do not
  advertise a stable protected path while a supported operation can bypass the
  version fence, causal fence, revalidation, or required key rotation.

## Delivery order

1. Land this protocol and threat-model decision.
2. Add side-by-side v2 codecs and fail-closed compatibility tests, without
   changing v1 behavior.
3. Implement the sequential policy engine and role projection.
4. Integrate one protected resource with causal fences and deterministic
   revalidation.
5. Add epoch-key encryption and reader rotation.
6. Extend generic checkpoints and safe truncation to protected-resource
   projections, add bounded-state benchmarks, and widen consumer integrations.

Each implementation step should be independently reviewable and measured. No
step should revive or merge the historical custody/rebalance stack wholesale.
