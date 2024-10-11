# Changelog

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.2 to ^2.1.3
    * @peerbit/time bumped from 2.0.2 to 2.0.3

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.4 to ^2.1.5
    * @peerbit/time bumped from 2.0.4 to 2.0.5

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.5 to ^2.1.6
    * @peerbit/time bumped from 2.0.5 to 2.0.6

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.6 to ^2.1.7

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.7 to ^2.2.0

## [2.1.2](https://github.com/dao-xyz/peerbit/compare/any-store-v2.1.1...any-store-v2.1.2) (2024-10-11)


### Bug Fixes

* update uuid ([5f7f16b](https://github.com/dao-xyz/peerbit/commit/5f7f16bc9e0c8b769e4d3c7bd1050701f58c1187))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.3.1 to ^2.3.2
    * @peerbit/any-store-opfs bumped from ^1.0.1 to ^1.0.2

## [2.1.1](https://github.com/dao-xyz/peerbit/compare/any-store-v2.1.0...any-store-v2.1.1) (2024-09-01)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.3.0 to ^2.3.1
    * @peerbit/any-store-opfs bumped from ^1.0.0 to ^1.0.1

## [2.1.0](https://github.com/dao-xyz/peerbit/compare/any-store-v2.0.2...any-store-v2.1.0) (2024-07-20)


### Features

* add api for determining if persistant ([bc9e218](https://github.com/dao-xyz/peerbit/commit/bc9e218651a086ded8e7eaebaf15f3ce0db176d0))


### Bug Fixes

* disable flaky concurrency test ([5662d3e](https://github.com/dao-xyz/peerbit/commit/5662d3e6a1844f7ecf672d4ddbfdad71c4a61759))
* fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
* peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))
* rm debug parameter ([3135994](https://github.com/dao-xyz/peerbit/commit/3135994abc4ce7256671b9e91457ddacd4c8fe57))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.2.0 to ^2.3.0
    * @peerbit/logger bumped from 1.0.2 to 1.0.3
    * @peerbit/any-store-opfs bumped from ^2.0.2 to ^1.0.0
    * @peerbit/any-store-interface bumped from ^2.0.2 to ^1.0.0
    * @peerbit/time bumped from 2.0.6 to 2.0.7

## [2.0.1](https://github.com/dao-xyz/peerbit/compare/any-store-v2.0.0...any-store-v2.0.1) (2024-02-02)


### Bug Fixes

* prevent vite optimizeDeps from optimizing worker path ([1630244](https://github.com/dao-xyz/peerbit/commit/1630244a4aa45a3582d5c5ddb146ed8766abcd44))

## [2.0.0](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.15...any-store-v2.0.0) (2024-02-02)


### ⚠ BREAKING CHANGES

* This will make AnyStore in the browser only work with Vite builds as default

### Bug Fixes

* import worker url with vite url format ([8770b91](https://github.com/dao-xyz/peerbit/commit/8770b91b66a3023400dfed57bc3f87e602403966))

## [1.0.15](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.14...any-store-v1.0.15) (2024-02-02)


### Bug Fixes

* set sizeCache to 0 before initialization ([38dd8d2](https://github.com/dao-xyz/peerbit/commit/38dd8d2884e5bf17d53aae5530963d73374c9790))

## [1.0.14](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.13...any-store-v1.0.14) (2024-02-02)


### Bug Fixes

* cache size calculations ([39b4a4e](https://github.com/dao-xyz/peerbit/commit/39b4a4e15c1d2352c8ef8fd9597a3e4de5d3f761))

## [1.0.12](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.11...any-store-v1.0.12) (2024-01-24)


### Bug Fixes

* create a webworker for each OPFSStore directory ([1b9c17a](https://github.com/dao-xyz/peerbit/commit/1b9c17ae52299c73d1935203e0860024858ede08))

## [1.0.11](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.10...any-store-v1.0.11) (2024-01-23)


### Bug Fixes

* opfs open in a non-root directory ([bb5570b](https://github.com/dao-xyz/peerbit/commit/bb5570be627b0cd1635006bd62b339bfe26d7edd))

## [1.0.8](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.7...any-store-v1.0.8) (2024-01-16)


### Bug Fixes

* move movering average tracker to time package ([0376928](https://github.com/dao-xyz/peerbit/commit/0376928b6929e97366e993ca3e927348d804ae32))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.3 to ^2.1.4
    * @peerbit/time bumped from 2.0.3 to 2.0.4

## [1.0.6](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.5...any-store-v1.0.6) (2024-01-12)


### Bug Fixes

* don't reuse existing vite server ([7cbb34e](https://github.com/dao-xyz/peerbit/commit/7cbb34eb099fb5f34233ce3fbb99f32acf2e47bb))
* exclude any-store from optimizeDeps ([a14751e](https://github.com/dao-xyz/peerbit/commit/a14751e6e8ede7a96537ca353af6be416958029a))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.1 to ^2.1.2
    * @peerbit/time bumped from 2.0.1 to 2.0.2

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.4...any-store-v1.0.5) (2024-01-08)


### Bug Fixes

* OPFS disable createWritable ([a6d2a00](https://github.com/dao-xyz/peerbit/commit/a6d2a009165943d844aa11fe07bb90b3ab2fe5bc))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.1.0 to ^2.1.1
    * @peerbit/time bumped from 2.0.0 to 2.0.1

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.3...any-store-v1.0.4) (2024-01-03)


### Bug Fixes

* make OPFS worker compatible with Safari ([8b11a44](https://github.com/dao-xyz/peerbit/commit/8b11a44f29e61f429ccea5928b1aad1d909b6f11))
* OPFS use createWritable when available ([c18a930](https://github.com/dao-xyz/peerbit/commit/c18a930bb58886c1c8e3d1b0fad4dcc593fe7339))

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.2...any-store-v1.0.3) (2024-01-02)


### Bug Fixes

* OPFS allow concurrent put ([e833b02](https://github.com/dao-xyz/peerbit/commit/e833b02e129c8f74981877ec764743452ff2c37e))

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.1...any-store-v1.0.2) (2024-01-01)


### Bug Fixes

* correctly espace illegal filename characters for OPFS ([5592761](https://github.com/dao-xyz/peerbit/commit/5592761d7b33b824655fd5a0b6deaae88eb11ccd))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^2.0.0 to ^2.1.0

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/any-store-v1.0.0...any-store-v1.0.1) (2023-12-31)


### Bug Fixes

* add wildcard dependency on test lib ([17ee002](https://github.com/dao-xyz/peerbit/commit/17ee002b7417e45a7c45dba280d02d07e5a14c27))
* remove keychain dep ([73f622f](https://github.com/dao-xyz/peerbit/commit/73f622f9a766bb562eb427cce5fc6c6c10e47bce))

## 1.0.0 (2023-12-31)


### ⚠ BREAKING CHANGES

* modularize keychain
* lazy stream routing protocol
* File storage abstraction

### Features

* File storage abstraction ([65e0024](https://github.com/dao-xyz/peerbit/commit/65e0024216812498a00ac7922fcf30e25a357d86))
* get store size function ([87931ca](https://github.com/dao-xyz/peerbit/commit/87931ca9d20f2316426c01ee83d8ef4dd21197c1))
* lazy stream routing protocol ([d12eb28](https://github.com/dao-xyz/peerbit/commit/d12eb2843b46c33fcbda5c97422cb263ab9f79a0))
* modularize keychain ([c10f10e](https://github.com/dao-xyz/peerbit/commit/c10f10e0beb58e38fa95d465962f43ab1aee75ef))


### Bug Fixes

* 'lazy-level' to 'any-store' ([ef97f4d](https://github.com/dao-xyz/peerbit/commit/ef97f4d0f9f4c6c0684126938983d030ef04d1a0))
* update imports ([94e4f93](https://github.com/dao-xyz/peerbit/commit/94e4f93449a15e76b8d03a6459a7304ab4257ec4))
* update vite ([371bb8b](https://github.com/dao-xyz/peerbit/commit/371bb8b089873df36ff9e591b67046a7e8dab6ea))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from ^1.0.10 to ^2.0.0
    * @peerbit/logger bumped from 1.0.1 to 1.0.2
    * @peerbit/time bumped from 1.0.4 to 2.0.0
