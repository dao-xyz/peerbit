# Changelog

## 1.1.2

### Patch Changes

- [#1001](https://github.com/dao-xyz/peerbit/pull/1001) [`8e672ca`](https://github.com/dao-xyz/peerbit/commit/8e672ca92fd2b2d42a407b0947d04508ae5166eb) Thanks [@Faolain](https://github.com/Faolain)! - Serialize wasm init to fix a double-init race under concurrent loads (browser use-after-free).

- [#1002](https://github.com/dao-xyz/peerbit/pull/1002) [`5ae64b2`](https://github.com/dao-xyz/peerbit/commit/5ae64b2b95c6638f8f034df5f7a80343a26e5949) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Harden the entry codec against hostile length prefixes: bound the next-hash count read from untrusted `EntryV0` meta bytes against the input actually remaining before allocating. Previously a malformed entry declaring a huge count could trigger a multi-gigabyte allocation and abort a native node (remote DoS); it now returns a catchable error. No change to valid decoding and the wasm API is byte-identical.

## 1.1.1

### Patch Changes

- [#998](https://github.com/dao-xyz/peerbit/pull/998) [`6646c8b`](https://github.com/dao-xyz/peerbit/commit/6646c8b43c8f9b919c333a6c93a462bac55cc4b1) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Refactor the crate into native-safe cores with a thin wasm surface: core logic now returns a real `LogError` type instead of `Result<_, JsValue>`, so error paths (malformed entries, CID mismatches, bad signatures) return catchable errors on native targets instead of aborting the process. The published wasm API and all error messages reaching JS are byte-identical.

## 1.1.0

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

## 1.0.0 (2026-05-26)

### Features

- add rust log graph index package ([b22ca0b](https://github.com/dao-xyz/peerbit/commit/b22ca0b9f818756497ec6b2af508110cbd7b4eac))
