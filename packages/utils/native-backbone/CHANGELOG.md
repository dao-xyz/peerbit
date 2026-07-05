# @peerbit/native-backbone

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
