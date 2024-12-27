# Changelog

## [1.1.4](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.1.3...indexer-sqlite3-v1.1.4) (2024-11-08)


### Bug Fixes

* revert i64  -&gt; u64 shifting changes ([4d7d8ba](https://github.com/dao-xyz/peerbit/commit/4d7d8ba0a90e147ed1c8bffbf55219db521dc853))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^2.0.0 to ^2.0.1
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.1.2 to ^1.1.3

## [1.1.3](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.1.2...indexer-sqlite3-v1.1.3) (2024-11-07)


### Bug Fixes

* apply default sorting to make iterators stable ([d6b4d16](https://github.com/dao-xyz/peerbit/commit/d6b4d1642ff30b0e40065397349f0f7bd0600aa5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.1.1 to ^2.0.0
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.1.1 to ^1.1.2

## [1.1.2](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.1.1...indexer-sqlite3-v1.1.2) (2024-10-28)


### Bug Fixes

* update sqlite ([9aa0186](https://github.com/dao-xyz/peerbit/commit/9aa018610e2c9d49680173a7be430d7e4e2a03d2))

## [1.1.1](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.1.0...indexer-sqlite3-v1.1.1) (2024-10-19)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.1.0 to ^1.1.1
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.1.0 to ^1.1.1

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.7...indexer-sqlite3-v1.1.0) (2024-10-11)


### Features

* skip calculating iterator sizes on next calls ([a87469d](https://github.com/dao-xyz/peerbit/commit/a87469d4cadb8b8ec988e609ea39f97e40033c4e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.0.3 to ^1.1.0
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.0.4 to ^1.1.0

## [1.0.7](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.6...indexer-sqlite3-v1.0.7) (2024-09-26)


### Bug Fixes

* update sqlite3 dependency version ([88f45c3](https://github.com/dao-xyz/peerbit/commit/88f45c3da090ad6ed05b43479e4695fc6b6c8e3c))

## [1.0.6](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.5...indexer-sqlite3-v1.0.6) (2024-09-01)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.0.2 to ^1.0.3
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.0.3 to ^1.0.4

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.4...indexer-sqlite3-v1.0.5) (2024-08-12)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.0.1 to ^1.0.2
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.0.2 to ^1.0.3

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.3...indexer-sqlite3-v1.0.4) (2024-08-12)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^1.0.0 to ^1.0.1
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.0.1 to ^1.0.2

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.2...indexer-sqlite3-v1.0.3) (2024-08-11)


### Bug Fixes

* support polymorphism at root level ([99834e5](https://github.com/dao-xyz/peerbit/commit/99834e501009cb22455bba663f6d42b9a28b018e))


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @peerbit/indexer-tests bumped from ^1.0.0 to ^1.0.1

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.1...indexer-sqlite3-v1.0.2) (2024-07-21)


### Documentation

* add sqlite3 topic ([1e96df1](https://github.com/dao-xyz/peerbit/commit/1e96df1b11a84a0a98050eeabc5b17960caa0286))
* rm comment ([f29c5ec](https://github.com/dao-xyz/peerbit/commit/f29c5ecef13c8e993f2e487e32af0d4d433c5a2d))

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/indexer-sqlite3-v1.0.0...indexer-sqlite3-v1.0.1) (2024-07-21)


### Bug Fixes

* make statement unique in worker ([2480ea7](https://github.com/dao-xyz/peerbit/commit/2480ea7a12061c650a0bf19a4469e1a5528e5e1e))

## 1.0.0 (2024-07-20)


### ⚠ BREAKING CHANGES

* add indexer implementations

### Features

* add indexer implementations ([b53c08a](https://github.com/dao-xyz/peerbit/commit/b53c08a01bcf24cf1832619b469b0f9f564f669d))


### Bug Fixes

* allow to provide a custom indexer ([ba924c5](https://github.com/dao-xyz/peerbit/commit/ba924c5317a32c7a85ace963a92ba3c1965d52f9))
* fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
* handle fixed size uint8arrays correctly ([1f263d0](https://github.com/dao-xyz/peerbit/commit/1f263d0a3fae50bb9d7f9d3f9fc28c9904b7b0ad))
* increase iterator timeout ([cffbe25](https://github.com/dao-xyz/peerbit/commit/cffbe25b55639555f7ab94832bbccf09f6bf54d4))
* make dir before copy ([a26a57e](https://github.com/dao-xyz/peerbit/commit/a26a57e47e9452deebe260da77abe96db12950c6))
* peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))
* remove try catch and logging from table creation ([47bed56](https://github.com/dao-xyz/peerbit/commit/47bed56e835bb02ff6e97fd4a118846013536742))
* support reserved column names ([3eb6132](https://github.com/dao-xyz/peerbit/commit/3eb6132322be55c0dd00a29a4a5b2115df3a2b00))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/indexer-interface bumped from ^0.0.1 to ^1.0.0
  * devDependencies
    * @peerbit/indexer-tests bumped from ^0.0.1 to ^1.0.0
