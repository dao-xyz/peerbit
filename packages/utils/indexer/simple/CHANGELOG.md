# Changelog

## 1.2.12

### Patch Changes

- Updated dependencies []:
  - @peerbit/indexer-interface@3.0.8

## 1.2.11

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

## 1.2.10

### Patch Changes

- Updated dependencies [[`4f7c098`](https://github.com/dao-xyz/peerbit/commit/4f7c0989c161ea0f85ad07f9b7be5f4cebd647a8)]:
  - @peerbit/indexer-interface@3.0.6

## 1.2.9

### Patch Changes

- [#1019](https://github.com/dao-xyz/peerbit/pull/1019) [`c917835`](https://github.com/dao-xyz/peerbit/commit/c9178355cef55c3af983f8bf3b8abe11cf8af4e0) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Restart cached indices on reopen (close -> reopen lifecycle fix)

  The node-level indexer `Indices` scope is cached per node and outlives a program close. When a program closed it stopped its own indices (state -> "closed") while the scope stayed alive; on reopen, `Indices.init` hit the existing-index early-return branch and handed back the still-stopped index without restarting it. The next synchronous read on open (e.g. shared-log's `replicationIndex.count(...)` / `iterate(...)` during hydrate) then threw `NotStartedError`.

  `init`'s existing-index branch now calls `index.start()` (idempotent; no-op when already open) before returning the cached index whenever the scope is open, mirroring the restart the freshly-created path already performs. This matches the sqlite3 backend, which already recovers because its `scope()` restarts cached indices via a start cascade before `init` runs.

  Fixes `@peerbit/indexer-rust` (`RustIndices`) and the same latent gap in `@peerbit/indexer-simple` (`HashmapIndices`); sqlite3 was already correct and is unchanged.

## 1.2.8

### Patch Changes

- Updated dependencies []:
  - @peerbit/indexer-interface@3.0.5

## [1.2.7](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.6...indexer-simple-v1.2.7) (2026-05-26)

### Bug Fixes

- **indexer-sqlite3:** prevent crashes during and after shutdown ([00a3185](https://github.com/dao-xyz/peerbit/commit/00a318585e7ec5441859c874f55e46f6b2d2d959))
- **indexer:** distinguish closing from closed APIs ([c59300f](https://github.com/dao-xyz/peerbit/commit/c59300f3e47c5c390ff63b14d1a4d8edbba1bf68))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.4
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.4

## [1.2.6](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.5...indexer-simple-v1.2.6) (2026-03-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.3
    - @peerbit/logger bumped to 2.0.1
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.3

## [1.2.5](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.4...indexer-simple-v1.2.5) (2026-03-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.2
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.2

## [1.2.4](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.3...indexer-simple-v1.2.4) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.1
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.1

## [1.2.3](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.2...indexer-simple-v1.2.3) (2026-03-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.0
  - devDependencies
    - @peerbit/indexer-tests bumped to 3.0.0

## [1.2.2](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.1...indexer-simple-v1.2.2) (2025-12-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 2.1.1
  - devDependencies
    - @peerbit/indexer-tests bumped to 2.0.1

## [1.2.1](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.2.0...indexer-simple-v1.2.1) (2025-12-23)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped to 2.0.0

## [1.2.0](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.21...indexer-simple-v1.2.0) (2025-11-25)

### Features

- add react tests ([42b3923](https://github.com/dao-xyz/peerbit/commit/42b3923c4ff551a691ab2e2c1e605a84ec55d059))
- migrate to borsh 6 and Typescript Stage 3 decorators ([86caba4](https://github.com/dao-xyz/peerbit/commit/86caba4f2128d3b1e2d274bea1b537722b5ec1c7))

### Bug Fixes

- use libp2p based logger ([5ffd22b](https://github.com/dao-xyz/peerbit/commit/5ffd22b2ddcfcc133fe025fcfb399461ef2fe266))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 2.1.0
    - @peerbit/logger bumped to 2.0.0
  - devDependencies
    - @peerbit/indexer-tests bumped to 1.2.0

## [1.1.21](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.20...indexer-simple-v1.1.21) (2025-10-03)

### Bug Fixes

- restore deps versions ([5d6b35a](https://github.com/dao-xyz/peerbit/commit/5d6b35a01a08f87bd17ad63eacb70b4b8a44b1db))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 2.0.14
  - devDependencies
    - @peerbit/indexer-tests bumped to 1.1.20

## [1.1.20](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.19...indexer-simple-v1.1.20) (2025-10-03)

### Bug Fixes

- add missing deps ([cf45de8](https://github.com/dao-xyz/peerbit/commit/cf45de831c5e0d3d1d97441a9e952537cd708f58))
- clearup vfs pool on drop ([e5a07a4](https://github.com/dao-xyz/peerbit/commit/e5a07a403330656ab1210b460dbf04596eff5257))
- pnpm package manager ([a6e95de](https://github.com/dao-xyz/peerbit/commit/a6e95de9a4fb418acd73f68639bec66fe6747856))
- rever preserveDbFile flag ([4dac27a](https://github.com/dao-xyz/peerbit/commit/4dac27ad0d31fba0b31a0f53d734ad1d38ad5e3b))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 2.0.13
  - devDependencies
    - @peerbit/indexer-tests bumped to 1.1.19

## [1.1.19](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.18...indexer-simple-v1.1.19) (2025-08-26)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.17 to ^1.1.18

## [1.1.18](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.17...indexer-simple-v1.1.18) (2025-08-19)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.16 to ^1.1.17

## [1.1.17](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.16...indexer-simple-v1.1.17) (2025-08-19)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.11 to ^2.0.12
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.15 to ^1.1.16

## [1.1.16](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.15...indexer-simple-v1.1.16) (2025-08-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.10 to ^2.0.11
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.14 to ^1.1.15

## [1.1.15](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.14...indexer-simple-v1.1.15) (2025-06-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.9 to ^2.0.10
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.13 to ^1.1.14

## [1.1.14](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.13...indexer-simple-v1.1.14) (2025-04-19)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.8 to ^2.0.9
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.12 to ^1.1.13

## [1.1.13](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.12...indexer-simple-v1.1.13) (2025-04-03)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.7 to ^2.0.8
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.11 to ^1.1.12

## [1.1.12](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.11...indexer-simple-v1.1.12) (2025-03-29)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.10 to ^1.1.11

## [1.1.11](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.10...indexer-simple-v1.1.11) (2025-03-13)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.6 to ^2.0.7
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.9 to ^1.1.10

## [1.1.10](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.9...indexer-simple-v1.1.10) (2025-03-10)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.8 to ^1.1.9

## [1.1.9](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.8...indexer-simple-v1.1.9) (2025-02-20)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.5 to ^2.0.6
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.7 to ^1.1.8

## [1.1.8](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.7...indexer-simple-v1.1.8) (2025-02-20)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.4 to ^2.0.5
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.6 to ^1.1.7

## [1.1.7](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.6...indexer-simple-v1.1.7) (2025-01-23)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.3 to ^2.0.4
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.5 to ^1.1.6

## [1.1.6](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.5...indexer-simple-v1.1.6) (2025-01-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.2 to ^2.0.3
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.4 to ^1.1.5

## [1.1.5](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.4...indexer-simple-v1.1.5) (2024-12-28)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.1 to ^2.0.2
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.3 to ^1.1.4

## [1.1.4](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.3...indexer-simple-v1.1.4) (2024-12-28)

### Bug Fixes

- correct array handling for inner hits ([d283a50](https://github.com/dao-xyz/peerbit/commit/d283a50a134589617269563fde51fb4c34ed2260))

## [1.1.3](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.2...indexer-simple-v1.1.3) (2024-11-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^2.0.0 to ^2.0.1
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.2 to ^1.1.3

## [1.1.2](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.1...indexer-simple-v1.1.2) (2024-11-07)

### Bug Fixes

- iterate query optional ([e6a267a](https://github.com/dao-xyz/peerbit/commit/e6a267a6ccb7dbc34c33b30a19c0a31d5d5318fd))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.1.1 to ^2.0.0
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.1 to ^1.1.2

## [1.1.1](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.1.0...indexer-simple-v1.1.1) (2024-10-19)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.1.0 to ^1.1.1
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.1.0 to ^1.1.1

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.0.4...indexer-simple-v1.1.0) (2024-10-11)

### Features

- skip calculating iterator sizes on next calls ([a87469d](https://github.com/dao-xyz/peerbit/commit/a87469d4cadb8b8ec988e609ea39f97e40033c4e))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.0.3 to ^1.1.0
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.0.4 to ^1.1.0

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.0.3...indexer-simple-v1.0.4) (2024-09-01)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.0.2 to ^1.0.3
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.0.3 to ^1.0.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.0.2...indexer-simple-v1.0.3) (2024-08-12)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.0.1 to ^1.0.2
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.0.2 to ^1.0.3

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.0.1...indexer-simple-v1.0.2) (2024-08-12)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^1.0.0 to ^1.0.1
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.0.1 to ^1.0.2

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/indexer-simple-v1.0.0...indexer-simple-v1.0.1) (2024-08-11)

### Bug Fixes

- support polymorphism at root level ([99834e5](https://github.com/dao-xyz/peerbit/commit/99834e501009cb22455bba663f6d42b9a28b018e))

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-tests bumped from ^1.0.0 to ^1.0.1

## 1.0.0 (2024-07-20)

### ⚠ BREAKING CHANGES

- add indexer implementations

### Features

- add indexer implementations ([b53c08a](https://github.com/dao-xyz/peerbit/commit/b53c08a01bcf24cf1832619b469b0f9f564f669d))

### Bug Fixes

- fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
- peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped from ^0.0.1 to ^1.0.0
  - devDependencies
    - @peerbit/indexer-tests bumped from ^0.0.1 to ^1.0.0
