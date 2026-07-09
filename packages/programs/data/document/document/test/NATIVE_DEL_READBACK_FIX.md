# Native block-store `del` read-back fix (Class-A conformance)

This note documents a real `@peerbit/document` product fix and how it relates to
the opt-in document `[default, native]` conformance leg (introduced separately by
#1024, not yet on `master`).

## The bug (Class-A: `del` read-back)

On a peer with a **native block store + native rust indexer** but a `Documents`
store opened in the DEFAULT `mode: "auto"` (i.e. NOT `mode: "native"` and no
`nativeBackbone`), calling `del` threw `Error("Missing data")`.

This is a real configuration: any peer built with
`createRustPeerbitOptions()` (or a `TestSession` peer under
`PEERBIT_SHARED_LOG_RUST_CORE=1`) that opens a plain `Documents` store hits it.
Minimal repro: single peer, `put(doc)` then `del(doc.id)`.

### Root cause (read-path incompatibility, not a missing block)

The delete read-back runs through
`Documents.handleChanges` -> `Documents.getAppendOperation`
(`src/program.ts`). In auto mode `getAppendOperation` resolved the prior put's
operation via the in-memory JS entry (`Entry.getPayloadValue`).

Under a native block store the `EntryV0` materialized in the entry index is a
**hollow shell**: its `_payload` is a `DecryptedThing` whose `_data` was never
loaded onto the JS object (the native store keeps the block bytes at the storage
layer). `getPayloadValue` -> `DecryptedThing.getValue` then throws
`"Missing data"`. Crucially the block **is** present — `blocks.get(hash)` returns
the full raw block — so this is a read-path incompatibility, not a missing block.
`entry.getStorageBytes()` (`serialize(this)`) also fails on the hollow entry, so
the payload can only be recovered from the raw block held by the block store.

## The fix

`getAppendOperation` (auto mode) now tries `getPayloadValue()` first (unchanged
JS behaviour) and, only when it throws the hollow-payload `"Missing data"` error,
falls back to `getPlainEntryOperationFromStorage`. That helper now reads the raw
block from the block store by the entry hash
(`getEntryStorageBytesFromBlocks`) whenever the entry object cannot yield its own
storage bytes, then extracts the plain operation payload from those bytes via the
native `entryV0PlainPayloadDataFromStorage` path.

- **Pure-JS backend: unchanged.** `getPayloadValue` succeeds there, so the
  fallback never runs.
- **Native-storage auto mode: fixed.** The hollow-entry read-back now resolves
  the operation from the block store.
- Document mode semantics and the `isNativeMode()` gate are untouched. The fix is
  contained to `@peerbit/document`'s read path — no native-backbone change.

Regression coverage: `test/native-del-readback.spec.ts` (native-storage peer,
auto mode, put -> del asserts deleted + no throw). A/B confirmed: revert the fix
=> `"Missing data"`; apply => pass.

## Fold-in follow-up for the document conformance leg (#1024)

The document `[default, native]` conformance leg (#1024) currently excludes the
Class-A `del`-path tests from its `test:document-rust-core` allowlist grep:

- `can add and delete`
- `delete permanently`
- `reload after delete`
- `returns approximate count with deletions`

With this fix these four are green under `PEERBIT_SHARED_LOG_RUST_CORE=1` and are
ready to fold into the leg allowlist once both #1024 and this PR land. The grep is
intentionally **not** edited here because #1024 is not yet on `master` (there is
no leg grep on this PR's base to widen); the allowlist widening is a follow-up.

Note: a fifth title the leg lists under the del exclusions,
`can delete without being replicator`, is **not** a Class-A read-back failure. It
is a separate 2-peer native-wire sync divergence (the non-replicator peer never
receives/indexes the doc, so `del` raises `NotFoundError` — "No entry with key").
That failure is identical with and without this fix and is out of scope here.
