# @peerbit/shared-log-rust

## 0.1.7

### Patch Changes

- [#1357](https://github.com/dao-xyz/peerbit/pull/1357) [`f19a1c0`](https://github.com/dao-xyz/peerbit/commit/f19a1c0cbf612eaa85d908e2220516cdc4316fb0) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Add session-bound persisted delivery receipts so document and shared-log writers
  can wait for an exact entry to be crash-safe on a requested number of current
  remote replicas before retiring. Level-backed block stores and SQLite index
  stores now expose the durability barriers required to issue these receipts;
  backends without the complete barrier set fail closed. Independent
  payload-only append batches also retain their replication metadata, so remote
  peers can admit and persist every entry in the batch. Persisted delivery also
  reuses one native full-replica routing plan across gid-independent batches.
  Multi-block provider discovery now publishes one renewable log-wide lease while
  retaining batched, wire-compatible per-CID announcements for mixed-version
  readers.

## 0.1.6

### Patch Changes

- [#1309](https://github.com/dao-xyz/peerbit/pull/1309) [`81ca42b`](https://github.com/dao-xyz/peerbit/commit/81ca42b707c34114979f7d87c42045ea0af00d1c) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Bound receive-side rateless range materialization in JavaScript and both native
  backends through an explicit limited-range capability, fall back to paged index
  iteration when an older native package lacks that capability, fall back to
  simple sync on overflow, and page simple coordinate lookups without using
  `iterator.all()`.

## 0.1.5

### Patch Changes

- [#1307](https://github.com/dao-xyz/peerbit/pull/1307) [`5ab6073`](https://github.com/dao-xyz/peerbit/commit/5ab607322d438a432a0724020dab93aa7cbab9a1) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Bound native shared-log containment indexing for wide replication ranges while
  preserving exact owner-sampling membership and ordering.

- [#1285](https://github.com/dao-xyz/peerbit/pull/1285) [`96b6dc3`](https://github.com/dao-xyz/peerbit/commit/96b6dc33d62000021ff8950ec102f1ce84de6492) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Preserve the native receiver when committing entry coordinate batches.

- [#1307](https://github.com/dao-xyz/peerbit/pull/1307) [`b776562`](https://github.com/dao-xyz/peerbit/commit/b7765624ab661dc826eafeb163495e8ae426b542) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Bound native shared-log fallback sampling to linear endpoint-index work per
  cursor while preserving deterministic owner selection. Replace the synthetic
  maximum metadata sentinel with coordinate-only partitions so maximum-timestamp,
  high-Unicode identifiers remain in their correct fallback segment, and apply
  metadata tie-breaking across circularly aliased zero/MAX endpoints. Split the
  TypeScript directional fallback into disjoint monotone phases, treat exact
  directional equality as zero distance, and exclude the current range from
  same-owner adjacency queries. Keep prefetched join rows visible to iterator
  completion and pending counts so batched fallback scans cannot truncate them,
  drain `all()` through the ordered merge, and release buffered rows on close.

- [#1284](https://github.com/dao-xyz/peerbit/pull/1284) [`b8eb614`](https://github.com/dao-xyz/peerbit/commit/b8eb614e8715d2e179304c910beec235a385f197) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Avoid scanning every replication range during native append when the exact
  resident owner count proves full-replica delivery cannot apply.

- [#1307](https://github.com/dao-xyz/peerbit/pull/1307) [`a0b992e`](https://github.com/dao-xyz/peerbit/commit/a0b992eafaf84b789f76c4e1893fe68f5314b920) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Make shared-log owner sampling independent of mixed-maturity range order and
  align TypeScript fallback tie-breaking with the native deterministic order.

## 0.1.4

### Patch Changes

- [#1165](https://github.com/dao-xyz/peerbit/pull/1165) [`fca4485`](https://github.com/dao-xyz/peerbit/commit/fca4485aeb16b9a4640048bff88175f75fe9f37b) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Stop shipping compiled benchmark suites in npm tarballs, and slim @peerbit/indexer-sqlite3 by 65% (drop an unreferenced bundle, unused sqlite main-thread loaders and worker1 helpers, and the broken ./sqlite.org export whose target was never published).

## 0.1.3

### Patch Changes

- [#1051](https://github.com/dao-xyz/peerbit/pull/1051) [`03494dc`](https://github.com/dao-xyz/peerbit/commit/03494dc8d5631ee73a5c76b6cc94161ebb431cbe) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Let browser bundlers emit the lazy wasm-bindgen glue chunk so the native shared-log planner loads in Vite applications instead of requesting a missing `/wasm/shared_log_rust.js` path.

- [#1052](https://github.com/dao-xyz/peerbit/pull/1052) [`8f14ebb`](https://github.com/dao-xyz/peerbit/commit/8f14ebbbb2ee529317e27e1f810d5541bb17cf05) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Route new entries to strict range replicators when they intersect the entry coordinates, even when strict ranges are excluded from the full-replica fallback. This restores live document-stream delivery without broadcasting each append to every peer.

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
