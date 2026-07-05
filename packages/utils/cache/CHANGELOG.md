# Changelog

## 3.1.0

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

## [3.0.0](https://github.com/dao-xyz/peerbit/compare/cache-v2.2.0...cache-v3.0.0) (2026-03-04)

### ⚠ BREAKING CHANGES

- fanout tree protocol + large-network sims + interactive sandbox ([#582](https://github.com/dao-xyz/peerbit/issues/582))

### Features

- fanout tree protocol + large-network sims + interactive sandbox ([#582](https://github.com/dao-xyz/peerbit/issues/582)) ([3f16953](https://github.com/dao-xyz/peerbit/commit/3f16953f1048e6f6dda7229fb30de6d3e7e0476b))

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped to 3.0.0

## [2.2.0](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.5...cache-v2.2.0) (2025-11-25)

### Features

- migrate to borsh 6 and Typescript Stage 3 decorators ([86caba4](https://github.com/dao-xyz/peerbit/commit/86caba4f2128d3b1e2d274bea1b537722b5ec1c7))

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped to 2.3.0

## [2.1.5](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.4...cache-v2.1.5) (2025-10-03)

### Bug Fixes

- add missing deps ([cf45de8](https://github.com/dao-xyz/peerbit/commit/cf45de831c5e0d3d1d97441a9e952537cd708f58))

## [2.1.4](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.3...cache-v2.1.4) (2025-08-08)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped from 2.1.0 to 2.2.0

## [2.1.3](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.2...cache-v2.1.3) (2025-04-03)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped from 2.0.8 to 2.1.0

## [2.1.2](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.1...cache-v2.1.2) (2025-02-20)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped from 2.0.7 to 2.0.8

## [2.1.1](https://github.com/dao-xyz/peerbit/compare/cache-v2.1.0...cache-v2.1.1) (2025-01-17)

### Bug Fixes

- improve error message ([d39737a](https://github.com/dao-xyz/peerbit/commit/d39737a4d089356a9fc9ba3bfd4be021fb7b387d))

## [2.1.0](https://github.com/dao-xyz/peerbit/compare/cache-v2.0.6...cache-v2.1.0) (2024-07-20)

### Features

- support more keytypes ([0b9ab42](https://github.com/dao-xyz/peerbit/commit/0b9ab42bdf64b41a9704812ff99e6768b06cee8e))

### Bug Fixes

- fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
- peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/time bumped from 2.0.6 to 2.0.7
