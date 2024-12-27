# Changelog

## [2.0.1](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v2.0.0...indexer-interface-v2.0.1) (2024-11-08)


### Bug Fixes

* revert i64  -&gt; u64 shifting changes ([4d7d8ba](https://github.com/dao-xyz/peerbit/commit/4d7d8ba0a90e147ed1c8bffbf55219db521dc853))

## [2.0.0](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.1.1...indexer-interface-v2.0.0) (2024-11-07)


### ⚠ BREAKING CHANGES

* support u64 integer keys

### Features

* support u64 integer keys ([b0ef425](https://github.com/dao-xyz/peerbit/commit/b0ef4251c727eca8ab93155b0d458a5853667bf4))

## [1.1.1](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.1.0...indexer-interface-v1.1.1) (2024-10-19)


### Bug Fixes

* make sure IndexIterator generic value extends Record ([451b9f2](https://github.com/dao-xyz/peerbit/commit/451b9f2f77c3d7efb532fa80bd25adc881548666))

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.0.3...indexer-interface-v1.1.0) (2024-10-11)


### Features

* skip calculating iterator sizes on next calls ([a87469d](https://github.com/dao-xyz/peerbit/commit/a87469d4cadb8b8ec988e609ea39f97e40033c4e))


### Bug Fixes

* update libp2p ([946a904](https://github.com/dao-xyz/peerbit/commit/946a904ea6cade2bf3de47f014a3fb96ed99e727))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.3.1 to ^2.3.2

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.0.2...indexer-interface-v1.0.3) (2024-09-01)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.3.0 to ^2.3.1

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.0.1...indexer-interface-v1.0.2) (2024-08-12)


### Bug Fixes

* add documentaion for Nested experimental query ([61d3cec](https://github.com/dao-xyz/peerbit/commit/61d3cec4ed802ac59f3c77482855a6cb9b8360b5))

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/indexer-interface-v1.0.0...indexer-interface-v1.0.1) (2024-08-12)


### Bug Fixes

* revert AbstractSearchRequest variants to v6 ([88b54af](https://github.com/dao-xyz/peerbit/commit/88b54af1f946e96d696d76b387d44cb173548e9b))
* rm unused import ([fe71c8e](https://github.com/dao-xyz/peerbit/commit/fe71c8efbf40edd591c9ddef986561db3b8c1191))

## 1.0.0 (2024-07-20)


### ⚠ BREAKING CHANGES

* add indexer implementations

### Features

* add indexer implementations ([b53c08a](https://github.com/dao-xyz/peerbit/commit/b53c08a01bcf24cf1832619b469b0f9f564f669d))


### Bug Fixes

* fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
* peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))
* set default fetch size to 10 ([aa77510](https://github.com/dao-xyz/peerbit/commit/aa77510ff232ae850523335907e92b40e5d75c56))
* simply toIdeable ([aa0b5b0](https://github.com/dao-xyz/peerbit/commit/aa0b5b0ebca0818fe12d4c5175ef6565b83d7604))
* toIdeable handle unint8array correctly ([e7b31fe](https://github.com/dao-xyz/peerbit/commit/e7b31feeef12a79337d5fa4ea814c066877fc906))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.2.0 to ^2.3.0
