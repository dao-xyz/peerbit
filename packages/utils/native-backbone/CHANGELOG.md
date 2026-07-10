# @peerbit/native-backbone

## 0.1.4

### Patch Changes

- [#1014](https://github.com/dao-xyz/peerbit/pull/1014) [`6294dd4`](https://github.com/dao-xyz/peerbit/commit/6294dd4201aa0d2bab4290fbce459ffc13dab851) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Typed native error paths for the backbone core (final slice)

  Eliminates the last `Result<_, JsValue>` error surface in the crate: the four
  `document_index_*_append_commit` builders and
  `validate_document_index_required_previous_signer` in documents.rs now report a
  typed `BackboneError` instead of constructing `JsValue`s. These were the only
  paths still pinned to `JsValue` by the frozen
  `make_document_index_commit` closure contract in
  `append_tx/committed_latest`; that contract is retyped to
  `-> Result<DocumentIndexAppendCommit, BackboneError>` in the same change, so the
  builders and their closures now type-check end to end without a JsValue seam.

  The required-previous-signer validator's two error literals become dedicated
  variants (`PreviousDocumentSignerPublicKeyUnavailable` and
  `PreviousDocumentSignerPublicKeyPolicyMismatch`) rendering their historical
  strings byte-for-byte. With every caller now typed, the local
  `js_wrapper_error` verbatim-forward seam in committed_latest.rs is dead and is
  removed. Every `#[wasm_bindgen]` export keeps its exact signature and reaches JS
  only through the single `From<BackboneError> for JsValue` conversion.

## 0.1.3

### Patch Changes

- [#1010](https://github.com/dao-xyz/peerbit/pull/1010) [`9d6c006`](https://github.com/dao-xyz/peerbit/commit/9d6c006f0d43a4568f91e34836a414148a269e3d) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Typed native error paths for the backbone core (part 2)

  Completes the JsValue→typed-error refactor started in the previous release:
  the append transaction modules (append_tx/storage, facts, committed_no_next,
  committed_latest, mod), the document projection/query/index paths
  (documents.rs) and the raw-receive verify/commit hot path (raw_receive.rs)
  now report a typed `BackboneError` internally instead of constructing
  `JsValue`s, so the crate no longer aborts on error when consumed as a native
  rlib. Every `#[wasm_bindgen]` export keeps its exact signature and every
  error message string is reproduced byte-for-byte.

  Notable non-mechanical changes, each behavior-preserving:
  - Four append dispatch paths now call log-rust's typed `_core` builders
    directly instead of its JsValue wrappers, rebuilding the frozen result-row
    layouts locally (facts rows via the pre-existing `committed_entry_facts_to_row`,
    trim rows via a byte-identical replica of `log_trim_entries_to_rows` fed by
    the same `trim_oldest_log_entries_core`).
  - The pending latest-batch append state no longer captures `js_sys::Array`
    handles: it holds owned facts and trimmed entries, and the JS rows are built
    at the emit boundary. The log append/commit/trim side effects still happen at
    the same point; only row construction is deferred (skipped entirely when a
    later fallible planning step aborts the append).
  - Two `expect()` calls that trapped the whole wasm instance (in wire-sync, and
    a partial-verify-hashes invariant in raw-receive) became typed errors.
  - The duplicate `js_error`/`decode_error` funnels in documents.rs were deleted
    once all their call sites were typed.

  The two Ed25519 verification `.ok()` fallbacks in raw-receive are intentionally
  preserved as documented, control-flow-unchanged swallows: their only error is a
  signature-slice parse failure (a non-Ed25519 scheme or malformed bytes), and
  deferring to the TypeScript verification fallback is correct for mixed-scheme
  verification. The swallow is now explicit and commented rather than a bare
  `.ok()`.

- [#1008](https://github.com/dao-xyz/peerbit/pull/1008) [`9fc576f`](https://github.com/dao-xyz/peerbit/commit/9fc576f3e7c357fe840433a73aeb2ba3225cc1e2) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Typed native error paths for the backbone core (part 1)
  - native-backbone: new `BackboneError` enum (Display reproduces the exact
    message strings historically thrown across the wasm boundary; single
    `From<BackboneError> for JsValue` touchpoint). The js_interop helpers,
    leaf modules (coordinates, sync_send, wire_sync), graph/profile paths and
    the shared-log planner glue now report typed errors internally; every
    `#[wasm_bindgen]` export keeps its exact signature. All 159
    `js_sys::Date::now()` profiling sites now go through a
    `cfg(target_arch)` clock shim so the crate can compile natively.
  - Deliberate validation hardening in the JS marshaling helpers: byte
    fields reject non-Uint8Array values instead of coercing garbage, f64
    integer conversions reject non-finite/negative/fractional/out-of-range
    values (including the 2^64 rounding trap) instead of truncating, and
    present-but-non-string optional fields error instead of reading as
    absent. Two `expect()` aborts in wire-sync became typed errors.
  - shared-log-rust: new `SharedLogError` enum following the same pattern;
    internal planner/parsing helpers are typed, wasm surface unchanged, and
    a typed `put_entry_coordinates_core` lets dependants skip the
    string/Array round-trip.

## 0.1.2

### Patch Changes

- [#1001](https://github.com/dao-xyz/peerbit/pull/1001) [`8e672ca`](https://github.com/dao-xyz/peerbit/commit/8e672ca92fd2b2d42a407b0947d04508ae5166eb) Thanks [@Faolain](https://github.com/Faolain)! - Serialize wasm init to fix a double-init race under concurrent loads (browser use-after-free).

## 0.1.1

### Patch Changes

- [#1000](https://github.com/dao-xyz/peerbit/pull/1000) [`81b5a4c`](https://github.com/dao-xyz/peerbit/commit/81b5a4c60813e49986663e8dbe3718a11937f8c3) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Use non-literal specifiers for the node-only fs/path dynamic imports so browser bundlers (esbuild `--platform=browser`) no longer fail resolving `node:fs/promises` and `node:path` when bundling the package

## 0.1.0

### Minor Changes

- Native performance stack: Rust/WASM-backed log append and sync kernel.

  Adds an optional native execution path that offloads hot log, sync, and
  document code paths to Rust/WASM backends while keeping the existing
  JavaScript implementations as the default. The change is fully additive —
  no wire-format, storage-format, or existing public API changes — so nodes
  running the JS path and nodes running the native path remain interoperable.

  New packages:
  - `@peerbit/native-backbone` — shared native (Rust/WASM) runtime backbone
    used by the log/shared-log/document native paths.
  - `@peerbit/shared-log-rust` — Rust-backed shared-log sync/replication kernel.
  - `@peerbit/document-rust` — Rust-backed document indexing/transform helpers.
  - `@peerbit/any-store-rust` — Rust-backed `any-store` batch storage backend.

  Additive public API on existing packages:
  - `@peerbit/log-rust` — large additive native-entry API surface (batch
    Ed25519 verification, EntryV0 encode/prepare helpers, `NativeLogBlockStore`,
    raw-CID batch helpers, native head/join entry types).
  - `@peerbit/log` — new exported entry types (`CanAppend`, `PreparedAppendFacts`,
    `PreparedNativeLogEntry`, `ShallowOrFullEntry`) and optional native
    prepare/append options plus batched block hooks (`putMany`/`rmMany`).
  - `@peerbit/shared-log` — new optional native-backbone options (deferred/
    batched signature verification, native prepare hooks) on the existing
    public option types.
  - `@peerbit/document` — new `policy` and `transform` public exports.
  - `peerbit` — new `./rust` subpath export wiring the native client path;
    optional native package dependencies.
  - `@peerbit/blocks-interface` — new optional batch methods on the `Blocks`
    interface (`putMany`, `putKnown`, `putKnownMany`, `hasMany`, `getMany`).
  - `@peerbit/blocks` — `DirectBlock` now implements the batch block methods
    (`putMany`, `putKnown`, `putKnownMany`, `getMany`).
  - `@peerbit/cache` — new `addMany` batch-insert method on `Cache`.
  - `@peerbit/test-utils` — new optional `storage` factory options on the
    test session for pluggable block/keychain/indexer stores.

  Internal changes:
  - `@peerbit/crypto` — `DecryptedThing.getValue` now memoizes the deserialized
    value and treats `undefined` correctly.
  - `@peerbit/indexer-rust` — internal storage/planner refactor (no public API
    change).

- [#988](https://github.com/dao-xyz/peerbit/pull/988) [`8f5bac1`](https://github.com/dao-xyz/peerbit/commit/8f5bac19d936ec5a9a0d0b926d8d9ddab2a41270) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Native network plane: Rust/WASM-backed stream, pubsub, and block-exchange path.

  Adds an optional native execution path that offloads the hot network code
  paths (direct-stream routing, fanout-tree/topic control, block exchange, and
  fused send/receive) to a Rust/WASM backbone while keeping the existing
  JavaScript implementations as the default. The native path is opt-in and
  defaults off; with it disabled every flag-off TS path, wire format, protocol
  id, and observable behavior is byte- and semantics-identical, so JS-path and
  native-path nodes remain interoperable.

  New package:
  - `@peerbit/network-rust` — Rust/WASM network backbone (`peerbit_wire` crate):
    native direct-stream, fanout-tree, topic-control, and block-exchange kernels
    with their TS bindings, consumed via the new `rustCore` option on the
    transport packages.

  Additive public API on existing packages:
  - `@peerbit/native-backbone` — new `NativeBackboneWireSyncSession` receive-fusion
    session plus wire-sync counter/meta types and native raw-receive / sync-send
    wire helpers.
  - `@peerbit/shared-log` — new fused send/receive path and sync capability
    handshake: exported `SyncCapabilitiesMessage` and `RawExchangeHeadsMessage`,
    a `SharedLogNativeDefaults` type, and additive `nativeBackbone` / `nativeGraph`
    / raw-exchange `sync` options. Also routes `onChange` dispatch through the
    fused receive path.
  - `@peerbit/pubsub` — rust-core `TopicControlPlane` and `FanoutTree` via a new
    `FanoutWireCodec` seam (`fanoutWire` / `fanoutParentUpgrade` exports); the
    native directory adoption and `subscribeShouldReplace` route through the
    native core when `rustCore` is enabled.
  - `@peerbit/stream` — new `rustCore` option and exported `RustCoreStream` /
    `PushableLanes` types, plus public `routes` and `wireCounters` surface for the
    native lane scheduler and out-of-band envelope verification.
  - `@peerbit/blocks` — `DirectBlock` gains an optional `rustCore` option and a
    `getBlockResponsePayload` hook so block exchange can run through the native
    core.
  - `@peerbit/rpc` — new optional `resolveRequest` hook that lets a caller supply
    a pre-resolved request (used by the native path), falling back to decode when
    it is absent or throws.
  - `peerbit` — new `NativeNetworkRuntime` client wiring (`nativeNetwork` option)
    for selecting the native network preset.
  - `@peerbit/log` — new `Entry.prepareMultihashBytesLazy` static that defers
    materializing prepared entry-block bytes, so the native commit path does not
    force stash-backed heads to build block bytes it never reads.
  - `@peerbit/test-utils` — new optional `nativeWire` option on the test session
    `CreateOptions`, threaded into the DirectStream setup for native-path tests.

### Patch Changes

- Updated dependencies []:
  - @peerbit/blocks-interface@2.1.0
