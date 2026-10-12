# Proof-backed document deletes

A new persisted store format using the existing `Documents` class. This is not
an in-place change to existing stores. The implementation is under local
validation, **not yet published or cross-platform CI qualified**.

## Public API and supported profile

```ts
const documents = new Documents<Document>({
	format: {
		version: "owner-proofs-v1",
		owner: peer.identity.publicKey,
		keyType: "string", // or "bytes"
	},
});
await peer.open(documents, { args: { type: Document, replicate: 1 } });
await documents.put(value);
await documents.del(value.id);
```

Returned entries use the store's operation format. Inspect a decoded payload with
`inspectDocumentOperation(await result.entry.getPayloadValue())`; its `format`
and `type` discriminate legacy/proof and put/delete without changing signed bytes.
This is inspection, not authentication. For explicit encoding/decoding use
`documents.log.log.encoding`; `BORSH_ENCODING_OPERATION` and the existing
`isPutOperation`/`isDeleteOperation` predicates remain legacy-only.

The first profile supports ordinary serializable values, one immutable Ed25519
owner, strict crash-safe disk storage and full replication. JS and native graph
storage use the same authenticated admission and recovery path. Mandatory native
document mode and the native-backbone fast path are unsupported for this format.

Custom/mutable authorization callbacks, custom ID resolvers, transformed indexes,
encrypted operations, adaptive replication and `putMany` reject explicitly. There
is no silently ignored callback, weakened durability or sequential batch fallback.
Standard index field selection and local/remote queries remain available. Values
that are themselves Programs are unsupported by this first profile.

Existing stores retain their serialized format and delete behavior. New stores
have a distinct address; old readers reject the new manifest. Copying visible
values is not a resurrection-safe migration of deleted history. There is no
runtime flag that reinterprets an existing store's CUT entries.

## What deletion means

`del(id)` retires the selected signed version and its continuous authenticated
same-document ancestors. It does not retire independent replacements or follow a
causal link through another document. A cross-document parent is authenticated
only to establish the boundary, then traversal stops.

Every target needs its original canonical signed bytes. The delete carries this
bounded evidence inline and is validated independently of the currently visible
replacement. Replaying a retired original cannot make it visible again, including
after a later replacement is also deleted.

This addresses two separate legacy problems: current-head-dependent admission
can reject a legitimate out-of-order delete, while recursive CUT ancestry can
cross document boundaries. An admission-only relaxation is insufficient. The
legacy reproducer is preserved separately from new-format tests; this feature
does **not** claim that old store behavior has changed. At base commit
`2375d4b723f1bfc17601120bc996bfe6ae840c5f`, its unchanged assertions produce eight
passes and seven failures. After building, invoke it explicitly from the repo root:

```sh
pnpm exec mocha --no-config --no-package --require chai-extend/chai-global.js \
  --require source-map-support/register --timeout 60000 \
  packages/programs/data/document/document/dist/test/cut-admission-order.repro.js
```

It deliberately remains red; `proof-documents-order.spec.ts` tests the new format.

Version vectors describe causal knowledge, not deletion authority. Exact
retirement follows the observed-remove principle, with signed original identities
supplying evidence. This is not a novel CRDT claim or a compaction protocol.

## Authenticated identity and scope

The immutable descriptor contains version, a 32-byte nonce, owner and typed-key
kind. Separate domain-separated hashes derive the operation scope and lower log
ID. Presentation-only outer subclass fields do not create a new authority over
the same lower store.

The existing Documents discriminator and field order remain. Only its log-field
codec distinguishes ordinary SharedLog bytes from a versioned manifest wrapper;
runtime `.log` remains SharedLog. Legacy bytes/addresses and inherited application
fields are tested unchanged. Unknown nested discriminators reject before invoking
unrelated manifest decoders.

Every new PUT signs the scope, typed key and application payload. A delete signs
that scope/key, selected target and flat evidence bundle. The decoded value's ID
must agree with the authenticated key before mutation. Equivalent application
decoders are required; incompatible schemas are not claimed to converge.

Admission requires canonical plaintext EntryV0 bytes, recomputed CID, exactly one
Ed25519 owner signature and no prehash. The general entry signature excludes the
signature list: adding/reordering signatures can otherwise produce different CIDs
for the same signed content. Ordinary admission and witnesses enforce the same
profile; no alias registry is required.

A CUT's signed `next` list must equal the sorted exact target set. It cannot include
a cross-key boundary or extra target. Those indexed edges let replayed originals
find retained proofs even when the marker is not a head. Candidate rows alone do
not authorize retirement: original signed marker bytes are validated.

## Commit, receipt and recovery boundary

One private entry owner handles local append, remote joins and recovery before
generic recursive CUT processing or native fast paths. It reuses lower physical
mutation admission and exclusive index ownership.

1. Authenticate the operation and complete evidence; prepare exact coordinates.
2. Persist original bytes and the lower row, recording exact local commit.
3. Retire only verified targets and their coordinates.
4. Select the remaining signed winner and update the derived projection.
5. Complete block, row, coordinate and projection barriers, then notify observers.

A post-mutation failure makes the store unavailable until close/reopen; it never
pretends to roll back a committed entry. Persisted delivery errors expose exact
locally committed hashes. Readiness is preflight; a successful remote persisted
receipt remains the durability proof.

Reopen validates indexed originals, redoes CUT obligations, seeds the signed
clock and rebuilds the view before activating query RPC. Nonwinning values are
validated too. Context creation/modification timestamps derive from the selected
signed winner, not arrival order or projection-rebuild time.

Protected full replicas retain proof markers. Adaptive pruning, manual removal,
trim and unsupported batch/fast paths cannot bypass the owner. Close/drop drains
physical work before index teardown. A projection callback cannot queue teardown
of its own active mutation.

A node-scoped namespace reservation is acquired before opening derived indexes.
It rejects mixed-format aliases and multiple protected projections while allowing
ordinary legacy sharing. Only successful terminal cleanup releases a reservation.
This is lifecycle isolation, not protection against direct offline database edits
or unsupported mixed-generation library copies that lack the ownership protocol.

## Bounded costs and explicit limits

- The complete encoded entry is limited to 5,000,000 bytes. A proof is also limited
  to 256 evidence blocks and 1,024 raw parent edges, including cross-key boundaries.
- Missing evidence or an exceeded bound rejects the whole delete before mutation.
  There is no target-only or partial-prefix fallback.
- Retained-state lookup uses per-admission byte/candidate budgets and paged scans.
  Ordinary remote history uses a bounded traversal window, committing progress and
  revisiting evicted prefixes rather than accumulating an unbounded ancestry cache.
  This can trade extra reads for bounded memory.
- Original signed evidence is payload-proportional. Long histories can exceed the
  delete budget even if every individual put was legal. Arbitrarily long history
  deletion is not supported by this first profile.

There is no second persistent tombstone database, alternate public document class,
per-entry timer or background proof-fetch service. This does not provide compact
tombstones, secure erasure, physical reclamation, authenticated mutable revocation
history, or shared-fs's changing trust-graph policy. Those remain separate work.

## Validation and release gate

Local public tests use disk-backed Documents with JS and native graphs: arrival
permutations, late/duplicate replay, same-key history, cross-key boundaries, byte
keys, rejected aliases, unsupported options, close races and offline reopen.

Real-peer tests cover cold joins and exact-entry persisted PUT/DELETE receipts.
A child-process fault fixture is killed after the marker block/row are durable
but before target retirement; independent offline reopen completes retirement and
permits subsequent writes. Tests exit naturally; only the faulted child is killed.

Private tests cover malformed/cross-store/cross-owner evidence, signature aliases,
budgets, cancellation, nonhead markers and persistent-boundary faults. Historical
reference models remain test-only and do not confer callback/migration support.

Before merge/publication, require affected-package regression coverage and fresh
cross-platform CI. Local passing tests are not publication evidence. Publishing
remains held; this branch does not imply a downstream upgrade.
