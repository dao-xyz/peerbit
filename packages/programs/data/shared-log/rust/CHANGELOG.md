# @peerbit/shared-log-rust

## 0.1.2

### Patch Changes

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

## 0.1.1

### Patch Changes

- [#1001](https://github.com/dao-xyz/peerbit/pull/1001) [`8e672ca`](https://github.com/dao-xyz/peerbit/commit/8e672ca92fd2b2d42a407b0947d04508ae5166eb) Thanks [@Faolain](https://github.com/Faolain)! - Serialize wasm init to fix a double-init race under concurrent loads (browser use-after-free).

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
