# Changelog

## 1.0.5

### Patch Changes

- [#1063](https://github.com/dao-xyz/peerbit/pull/1063) [`74dd442`](https://github.com/dao-xyz/peerbit/commit/74dd4424a9634446b2823ffea382d2fde6c3d82c) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Require native local-append acknowledgements to wait for their durable block mirror. Durable mirror failures now raise a typed, unsafe-to-retry error and poison further mutations on that program instance.

  Propagate native trim deletions to the durable mirror with staged tombstones and same-CID generation guards. Failed old-block deletion remains retryable cleanup debt so a fully durable replacement can publish one coherent new index/head state, while ownership-aware compensation preserves acknowledged, restored, shared, and otherwise uncertain content-addressed bytes. Retained orphans remain part of physical store size and therefore continue to count toward hard storage budgets until cleanup succeeds.

  Publish strict native lower-index facts through an operation-scoped generation token before consuming trim results. An index write failure now retracts only that append, cancels its deferred publication, and restores the authoritative graph, document, and coordinate state without erasing concurrent same-CID facts.

  Serialize lower-log close and drop with native append finalizers, retry incomplete rollback/index teardown stages, and erase blocks only after acknowledgements or compensation settle. Uncontended native hash mutation leases retain the synchronous commit-only fast path without recursive public bookkeeping.

  Close and drop now fail before changing lifecycle state while an internal or user mutation callback is still running; callers must retry after that callback completes.

  Advertise whether an indexer preserves rows across stop/start so ordinary close avoids duplicating every block hash for persistent or data-preserving backends, while destructive and unknown backends retain the exact drop set before stopping.

  Persist strict native recovery intent in alternating checksummed generations so an interrupted journal write cannot erase the last recoverable state. A committed lower marker remains authoritative, later mutations are blocked until failed intent retirement is recovered, and committed trim block cleanup resumes from the durable intent after restart.

  Make native coordinate, document, and signer acknowledgements wait for an explicit physical durability barrier, retain pending records after failures, reject torn or corrupt recovered WAL tails, and fail closed after ambiguous or short appends. Node barriers require `FileHandle.sync`; OPFS barriers require sync-access `flush`; buffered/custom adapters without the capability fail before a durable acknowledgement.

  Native persistence drop is now tombstone-backed and resumable, with explicit underlying-removal and terminal-drop capabilities checked before lower state is mutated. Hydration, recovery, validation, and native loads share one lifecycle queue so close waits and drop rejects before erasure. Ordinary custom close is never invoked after terminal drop, and unsafe custom compaction thresholds are rejected even on memory-only nodes. Built-in snapshot compaction remains disabled until it can use a crash-atomic generation protocol.

- Updated dependencies [[`74dd442`](https://github.com/dao-xyz/peerbit/commit/74dd4424a9634446b2823ffea382d2fde6c3d82c), [`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33)]:
  - @peerbit/indexer-interface@3.0.7

## 1.0.4

### Patch Changes

- [#957](https://github.com/dao-xyz/peerbit/pull/957) [`4f7c098`](https://github.com/dao-xyz/peerbit/commit/4f7c0989c161ea0f85ad07f9b7be5f4cebd647a8) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Keep paginated sorted iterators complete and duplicate-free when indexed rows are inserted, updated, or deleted between pages.

  After observing a mutation, an iterator keeps the ids it has already yielded and rescans the current result set. This costs O(N) query work per subsequent page and O(yielded ids) memory; consuming a large changing result set in many small pages can therefore approach O(N²) work.

  Allow live-query layers to mark externally delivered ids as yielded so mutable index iterators do not count or emit the same update twice.

- Updated dependencies [[`4f7c098`](https://github.com/dao-xyz/peerbit/commit/4f7c0989c161ea0f85ad07f9b7be5f4cebd647a8)]:
  - @peerbit/indexer-interface@3.0.6

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
