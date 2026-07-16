# @peerbit/any-store-rust

## 0.1.2

### Patch Changes

- [#1040](https://github.com/dao-xyz/peerbit/pull/1040) [`2e145c3`](https://github.com/dao-xyz/peerbit/commit/2e145c316ccc275006b5daa160f2165ca1c9f1a6) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Avoid rewriting the complete native durable block snapshot on every program close. Rust-backed sublevels can now defer close-time compaction below an explicit journal threshold while preserving crash-safe WAL recovery, generic store defaults, and immutable cached-sublevel policies.

- [#1063](https://github.com/dao-xyz/peerbit/pull/1063) [`74dd442`](https://github.com/dao-xyz/peerbit/commit/74dd4424a9634446b2823ffea382d2fde6c3d82c) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Make Node and OPFS journal appends crash-safe across short writes by rolling rejected records back to their original offset and poisoning the open store after journal failure until a verified reopen. Strict mutations already queued behind the failed append now reject with the same sticky first error before changing memory or the WAL.

  Repair torn journals by durably truncating only their verified prefix instead of implicitly rewriting a checkpoint. Strict stores now remain WAL-backed even when close compaction or a threshold is forced, and OPFS checkpoint writes loop until every byte is written before publishing their manifest.

  Only a structurally incomplete final frame is treated as a recoverable crash tail. A complete frame with invalid magic, checksum, or payload now fails closed without applying a partial replay or rewriting the WAL, and failed-open persistence handles are closed before a retry.

- Updated dependencies [[`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33)]:
  - @peerbit/any-store-interface@1.1.1

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
