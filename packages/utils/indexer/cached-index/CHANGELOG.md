# Changelog

## 0.2.10

### Patch Changes

- Updated dependencies [[`74dd442`](https://github.com/dao-xyz/peerbit/commit/74dd4424a9634446b2823ffea382d2fde6c3d82c), [`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33), [`0a5a9a0`](https://github.com/dao-xyz/peerbit/commit/0a5a9a0c0690a310e141b80bcb84ba04fd48b329)]:
  - @peerbit/indexer-interface@3.0.7
  - @peerbit/crypto@3.1.3
  - @peerbit/time@3.0.1

## 0.2.9

### Patch Changes

- [#957](https://github.com/dao-xyz/peerbit/pull/957) [`4f7c098`](https://github.com/dao-xyz/peerbit/commit/4f7c0989c161ea0f85ad07f9b7be5f4cebd647a8) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Keep paginated sorted iterators complete and duplicate-free when indexed rows are inserted, updated, or deleted between pages.

  After observing a mutation, an iterator keeps the ids it has already yielded and rescans the current result set. This costs O(N) query work per subsequent page and O(yielded ids) memory; consuming a large changing result set in many small pages can therefore approach O(N²) work.

  Allow live-query layers to mark externally delivered ids as yielded so mutable index iterators do not count or emit the same update twice.

- Updated dependencies [[`4f7c098`](https://github.com/dao-xyz/peerbit/commit/4f7c0989c161ea0f85ad07f9b7be5f4cebd647a8)]:
  - @peerbit/indexer-interface@3.0.6

## 0.2.8

### Patch Changes

- Updated dependencies []:
  - @peerbit/crypto@3.1.2
  - @peerbit/indexer-interface@3.0.5

## [0.2.7](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.6...indexer-cache-v0.2.7) (2026-05-26)

### Bug Fixes

- **document:** start temporary hashmap indexes ([aebbe20](https://github.com/dao-xyz/peerbit/commit/aebbe20284654d82f65f3f28c8969d149ae21cdb))
- **indexer-sqlite3:** prevent crashes during and after shutdown ([00a3185](https://github.com/dao-xyz/peerbit/commit/00a318585e7ec5441859c874f55e46f6b2d2d959))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/indexer-interface bumped to 3.0.4
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.7

## [0.2.6](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.5...indexer-cache-v0.2.6) (2026-03-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.1.1
    - @peerbit/indexer-interface bumped to 3.0.3
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.6

## [0.2.5](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.4...indexer-cache-v0.2.5) (2026-03-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.1.0
    - @peerbit/indexer-interface bumped to 3.0.2
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.5

## [0.2.4](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.3...indexer-cache-v0.2.4) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.0.1
    - @peerbit/indexer-interface bumped to 3.0.1
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.4

## [0.2.3](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.2...indexer-cache-v0.2.3) (2026-03-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.0.0
    - @peerbit/indexer-interface bumped to 3.0.0
    - @peerbit/time bumped to 3.0.0
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.3

## [0.2.2](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.1...indexer-cache-v0.2.2) (2025-12-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 2.4.1
    - @peerbit/indexer-interface bumped to 2.1.1
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.2

## [0.2.1](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.2.0...indexer-cache-v0.2.1) (2025-12-23)

### Dependencies

- The following workspace dependencies were updated
  - devDependencies
    - @peerbit/indexer-simple bumped to 1.2.1

## [0.2.0](https://github.com/dao-xyz/peerbit/compare/indexer-cache-v0.1.0...indexer-cache-v0.2.0) (2025-11-26)

### Features

- migrate to borsh 6 and Typescript Stage 3 decorators ([86caba4](https://github.com/dao-xyz/peerbit/commit/86caba4f2128d3b1e2d274bea1b537722b5ec1c7))

### Bug Fixes

- add missing deps ([cf45de8](https://github.com/dao-xyz/peerbit/commit/cf45de831c5e0d3d1d97441a9e952537cd708f58))
- relase config and restore versions ([04ba9f6](https://github.com/dao-xyz/peerbit/commit/04ba9f6942a8aed24fc8c7f26637599c0595d621))
