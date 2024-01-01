# Changelog

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.0 to 1.0.1
    * @peerbit/stream-interface bumped from ^1.0.0 to ^1.0.1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.1 to 1.0.2
    * @peerbit/stream-interface bumped from ^1.0.1 to ^1.0.2

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.6 to 1.0.7
    * @peerbit/stream-interface bumped from ^1.0.7 to ^1.0.8

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.8 to 1.0.9
    * @peerbit/stream-interface bumped from ^1.0.9 to ^1.0.10

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.9 to 1.0.10
    * @peerbit/stream-interface bumped from ^1.0.10 to ^1.0.11

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 2.0.0 to 2.1.0
    * @peerbit/stream-interface bumped from ^2.0.0 to ^2.0.1

## [2.0.0](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.1.5...pubsub-interface-v2.0.0) (2023-12-31)


### ⚠ BREAKING CHANGES

* refactor delivery modes
* simplify subscribe/unsubscribe messages
* lazy stream routing protocol

### Features

* lazy stream routing protocol ([d12eb28](https://github.com/dao-xyz/peerbit/commit/d12eb2843b46c33fcbda5c97422cb263ab9f79a0))
* refactor delivery modes ([9b366c0](https://github.com/dao-xyz/peerbit/commit/9b366c037521ddd9f80315836585e8d8fe587a09))


### Bug Fixes

* replace emitSelf property with PublishEvent ([8c080bf](https://github.com/dao-xyz/peerbit/commit/8c080bfe892d40bdd19ba951268c612cd57cf04f))
* simplify subscribe/unsubscribe messages ([47577fe](https://github.com/dao-xyz/peerbit/commit/47577fed7dd943d748ded3d00a6e54fefeb2cff5))
* update libp2p dependencies ([743db18](https://github.com/dao-xyz/peerbit/commit/743db18839de3e09904b50384aa389a4b660fe06))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.10 to 2.0.0
    * @peerbit/stream-interface bumped from ^1.0.11 to ^2.0.0

## [1.1.3](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.1.2...pubsub-interface-v1.1.3) (2023-09-06)


### Bug Fixes

* handle overflow from invalid payload decoding ([d19b2e7](https://github.com/dao-xyz/peerbit/commit/d19b2e79597111cc47592e85d577d8456571c4b2))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.7 to 1.0.8
    * @peerbit/stream-interface bumped from ^1.0.8 to ^1.0.9

## [1.1.1](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.1.0...pubsub-interface-v1.1.1) (2023-09-02)


### Bug Fixes

* trailing comma formatting ([80a679c](https://github.com/dao-xyz/peerbit/commit/80a679c0dc0e7c8ac01538cb11458299fdb334d5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.5 to 1.0.6
    * @peerbit/stream-interface bumped from ^1.0.6 to ^1.0.7

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.0.5...pubsub-interface-v1.1.0) (2023-08-06)


### Features

* support for canReplicate filter ([432e6a5](https://github.com/dao-xyz/peerbit/commit/432e6a55b88eac5dd2d036338bf2e51cef2670f3))


### Bug Fixes

* typo change recieve to receive ([9b05cfc](https://github.com/dao-xyz/peerbit/commit/9b05cfc9220f6d8206626f5208724e3d0f34abe2))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.4 to 1.0.5
    * @peerbit/stream-interface bumped from ^1.0.5 to ^1.0.6

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.0.4...pubsub-interface-v1.0.5) (2023-07-18)


### Bug Fixes

* refactor ([751a3f3](https://github.com/dao-xyz/peerbit/commit/751a3f365f405b332a227203f65d4b3e278ca49d))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.3 to 1.0.4
    * @peerbit/stream-interface bumped from ^1.0.4 to ^1.0.5

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.0.3...pubsub-interface-v1.0.4) (2023-07-04)


### Bug Fixes

* rm postbuild script ([b627bf0](https://github.com/dao-xyz/peerbit/commit/b627bf0dcdb99d24ac8c9055586e72ea2d174fcc))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.2 to 1.0.3
    * @peerbit/stream-interface bumped from ^1.0.3 to ^1.0.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/pubsub-interface-v1.0.2...pubsub-interface-v1.0.3) (2023-06-29)


### Bug Fixes

* peer stream event types ([7607d7d](https://github.com/dao-xyz/peerbit/commit/7607d7de837813441a81f477b91ceeaba65a108f))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/stream-interface bumped from ^1.0.2 to ^1.0.3

## 1.0.0 (2023-06-28)


### ⚠ BREAKING CHANGES

* reuse pubsub message id on rpc messages
* client abstraction

### Features

* client abstraction ([6a1226d](https://github.com/dao-xyz/peerbit/commit/6a1226d4f8fc6deb167bff86cf7bdd6227c01a6b))
* reuse pubsub message id on rpc messages ([57bede7](https://github.com/dao-xyz/peerbit/commit/57bede71cd822c71b439bd8011b6f25bff1da5cb))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.4 to 1.0.0
