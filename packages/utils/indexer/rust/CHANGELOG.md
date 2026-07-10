# Changelog

## 1.0.3

### Patch Changes

- [#1019](https://github.com/dao-xyz/peerbit/pull/1019) [`c917835`](https://github.com/dao-xyz/peerbit/commit/c9178355cef55c3af983f8bf3b8abe11cf8af4e0) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Restart cached indices on reopen (close -> reopen lifecycle fix)

  The node-level indexer `Indices` scope is cached per node and outlives a program close. When a program closed it stopped its own indices (state -> "closed") while the scope stayed alive; on reopen, `Indices.init` hit the existing-index early-return branch and handed back the still-stopped index without restarting it. The next synchronous read on open (e.g. shared-log's `replicationIndex.count(...)` / `iterate(...)` during hydrate) then threw `NotStartedError`.

  `init`'s existing-index branch now calls `index.start()` (idempotent; no-op when already open) before returning the cached index whenever the scope is open, mirroring the restart the freshly-created path already performs. This matches the sqlite3 backend, which already recovers because its `scope()` restarts cached indices via a start cascade before `init` runs.

  Fixes `@peerbit/indexer-rust` (`RustIndices`) and the same latent gap in `@peerbit/indexer-simple` (`HashmapIndices`); sqlite3 was already correct and is unchanged.

## 1.0.2

### Patch Changes

- [#1001](https://github.com/dao-xyz/peerbit/pull/1001) [`8e672ca`](https://github.com/dao-xyz/peerbit/commit/8e672ca92fd2b2d42a407b0947d04508ae5166eb) Thanks [@Faolain](https://github.com/Faolain)! - Serialize wasm init to fix a double-init race under concurrent loads (browser use-after-free).

## 1.0.1

### Patch Changes

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

- Updated dependencies []:
  - @peerbit/indexer-interface@3.0.5

## 1.0.0 (2026-05-26)

### Bug Fixes

- **indexer-rust:** honor closed lifecycle contract ([0c3fc07](https://github.com/dao-xyz/peerbit/commit/0c3fc0775d63275aa280fde208fe8090e1af5e37))
- **indexer-sqlite3:** prevent crashes during and after shutdown ([00a3185](https://github.com/dao-xyz/peerbit/commit/00a318585e7ec5441859c874f55e46f6b2d2d959))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.4
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.4
