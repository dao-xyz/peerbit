# Changelog

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/lazy-level bumped from ^1.0.2 to ^1.0.3

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/blocks-interface bumped from ^1.0.6 to ^1.0.7

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/lazy-level bumped from ^1.1.0 to ^1.1.1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/blocks-interface bumped from ^1.0.8 to ^1.0.9

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.8 to 1.0.9
    * @peerbit/blocks-interface bumped from ^1.1.1 to ^1.1.2
    * @peerbit/pubsub-interface bumped from ^1.1.3 to ^1.1.4
    * @peerbit/lazy-level bumped from ^1.2.0 to ^1.2.1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/keychain bumped from ^0.0.1 to ^1.0.0
    * @peerbit/any-store bumped from ^1.0.0 to ^1.0.1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 2.0.0 to 2.1.0
    * @peerbit/keychain bumped from ^1.0.0 to ^1.0.1
    * @peerbit/blocks-interface bumped from ^1.2.0 to ^1.2.1
    * @peerbit/pubsub-interface bumped from ^2.0.0 to ^2.0.1
    * @peerbit/any-store bumped from ^1.0.1 to ^1.0.2

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/keychain bumped from ^1.0.1 to ^1.0.2
    * @peerbit/any-store bumped from ^1.0.2 to ^1.0.3

## [3.0.2](https://github.com/dao-xyz/peerbit/compare/program-v3.0.1...program-v3.0.2) (2024-01-01)


### Bug Fixes

* simply OpenOptions generics ([8f83c7d](https://github.com/dao-xyz/peerbit/commit/8f83c7db577f5c10a672c7603e78e40c5123d522))

## [3.0.0](https://github.com/dao-xyz/peerbit/compare/program-v2.4.2...program-v3.0.0) (2023-12-31)


### ⚠ BREAKING CHANGES

* getReady returns a list of publickeys instead of hashes
* modularize keychain
* lazy stream routing protocol
* File storage abstraction

### Features

* File storage abstraction ([65e0024](https://github.com/dao-xyz/peerbit/commit/65e0024216812498a00ac7922fcf30e25a357d86))
* getReady returns a list of publickeys instead of hashes ([061fb61](https://github.com/dao-xyz/peerbit/commit/061fb6107922d184ca46e5f2e42a4be1b43175ab))
* lazy stream routing protocol ([d12eb28](https://github.com/dao-xyz/peerbit/commit/d12eb2843b46c33fcbda5c97422cb263ab9f79a0))
* modularize keychain ([c10f10e](https://github.com/dao-xyz/peerbit/commit/c10f10e0beb58e38fa95d465962f43ab1aee75ef))


### Bug Fixes

* don't process messages if closed ([0888f53](https://github.com/dao-xyz/peerbit/commit/0888f53509864ead2c9addcbff9f546acc685e5d))
* getReady dont throw when not subscribing ([09488e6](https://github.com/dao-xyz/peerbit/commit/09488e6dc5a53d0a8fe7332d45fe69aa02cc09c7))
* program types and argument type inference ([309b7b3](https://github.com/dao-xyz/peerbit/commit/309b7b3db0d903e3be5e7882d14dc4acce2f62fa))
* rm redundant emit self property ([77dc3c7](https://github.com/dao-xyz/peerbit/commit/77dc3c7402ae6a3b5c67296b834e398f6a06d4a5))
* rm unused imports ([89837fe](https://github.com/dao-xyz/peerbit/commit/89837fe869ad14e322c74389cdd6a35f4622c4c7))
* update libp2p dep ([f69c01a](https://github.com/dao-xyz/peerbit/commit/f69c01aeae10c6712eed0154fc3094c0af0108c2))
* update libp2p dependencies ([743db18](https://github.com/dao-xyz/peerbit/commit/743db18839de3e09904b50384aa389a4b660fe06))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.10 to 2.0.0
    * @peerbit/blocks-interface bumped from ^1.1.3 to ^1.2.0
    * @peerbit/pubsub-interface bumped from ^1.1.5 to ^2.0.0
    * @peerbit/any-store bumped from ^0.0.1 to ^1.0.0

## [2.4.2](https://github.com/dao-xyz/peerbit/compare/program-v2.4.1...program-v2.4.2) (2023-09-24)


### Bug Fixes

* correctly reopen as subprogram ([34183ac](https://github.com/dao-xyz/peerbit/commit/34183ac4aceb2635ea05e21a5946da41fb642a21))

## [2.4.1](https://github.com/dao-xyz/peerbit/compare/program-v2.4.0...program-v2.4.1) (2023-09-24)


### Bug Fixes

* can-open program that was opened as a subprogram ([5e73a20](https://github.com/dao-xyz/peerbit/commit/5e73a2021029506dbca2452ff2080e773c07cc2f))

## [2.4.0](https://github.com/dao-xyz/peerbit/compare/program-v2.3.2...program-v2.4.0) (2023-09-21)


### Features

* open programs concurrently ([d8242c4](https://github.com/dao-xyz/peerbit/commit/d8242c440d0bbd819e1d1fc7b36663889ef67280))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.9 to 1.0.10
    * @peerbit/blocks-interface bumped from ^1.1.2 to ^1.1.3
    * @peerbit/pubsub-interface bumped from ^1.1.4 to ^1.1.5

## [2.3.1](https://github.com/dao-xyz/peerbit/compare/program-v2.3.0...program-v2.3.1) (2023-09-06)


### Bug Fixes

* handle overflow from invalid payload decoding ([d19b2e7](https://github.com/dao-xyz/peerbit/commit/d19b2e79597111cc47592e85d577d8456571c4b2))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.7 to 1.0.8
    * @peerbit/blocks-interface bumped from ^1.1.0 to ^1.1.1
    * @peerbit/pubsub-interface bumped from ^1.1.2 to ^1.1.3

## [2.3.0](https://github.com/dao-xyz/peerbit/compare/program-v2.2.5...program-v2.3.0) (2023-09-06)


### Features

* support recovery of heads ([968b780](https://github.com/dao-xyz/peerbit/commit/968b780f315454f8e18d81f37f3e8a5c885b272d))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.6 to 1.0.7
    * @peerbit/blocks-interface bumped from ^1.0.9 to ^1.1.0
    * @peerbit/pubsub-interface bumped from ^1.1.1 to ^1.1.2
    * @peerbit/lazy-level bumped from ^1.1.1 to ^1.2.0

## [2.2.3](https://github.com/dao-xyz/peerbit/compare/program-v2.2.2...program-v2.2.3) (2023-09-02)


### Bug Fixes

* trailing comma formatting ([80a679c](https://github.com/dao-xyz/peerbit/commit/80a679c0dc0e7c8ac01538cb11458299fdb334d5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.5 to 1.0.6
    * @peerbit/blocks-interface bumped from ^1.0.7 to ^1.0.8
    * @peerbit/pubsub-interface bumped from ^1.1.0 to ^1.1.1
    * @peerbit/lazy-level bumped from ^1.0.3 to ^1.1.0

## [2.2.0](https://github.com/dao-xyz/peerbit/compare/program-v2.1.0...program-v2.2.0) (2023-08-06)


### Features

* support for canReplicate filter ([432e6a5](https://github.com/dao-xyz/peerbit/commit/432e6a55b88eac5dd2d036338bf2e51cef2670f3))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.4 to 1.0.5
    * @peerbit/blocks-interface bumped from ^1.0.5 to ^1.0.6
    * @peerbit/pubsub-interface bumped from ^1.0.5 to ^1.1.0

## [2.1.0](https://github.com/dao-xyz/peerbit/compare/program-v2.0.0...program-v2.1.0) (2023-07-28)


### Features

* add utility methods for listing available programs ([ec4b8d7](https://github.com/dao-xyz/peerbit/commit/ec4b8d79926987dc742cb12583efd1c91b893556))

## [2.0.0](https://github.com/dao-xyz/peerbit/compare/program-v1.0.6...program-v2.0.0) (2023-07-18)


### ⚠ BREAKING CHANGES

* remove ComposableProgram type

### Features

* remove ComposableProgram type ([4ccf6c2](https://github.com/dao-xyz/peerbit/commit/4ccf6c2ce07d7edfe1608e9bd5adfa03cf587dd4))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.3 to 1.0.4
    * @peerbit/blocks-interface bumped from ^1.0.4 to ^1.0.5
    * @peerbit/pubsub-interface bumped from ^1.0.4 to ^1.0.5
    * @peerbit/lazy-level bumped from ^1.0.1 to ^1.0.2
