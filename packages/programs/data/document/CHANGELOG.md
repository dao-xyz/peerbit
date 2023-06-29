# Changelog

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.0-alpha1 to 1.0.1-alpha1
    * @peerbit/rpc bumped from 1.0.0-alpha1 to 1.0.1-alpha1
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.0-alpha1 to 1.0.1-alpha1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.3 to 1.0.4
    * @peerbit/rpc bumped from 1.0.3 to 1.0.4
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.3 to 1.0.4
    * @peerbit/time bumped from 0.0.24 to 1.0.0

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.0 to 1.0.1
    * @peerbit/rpc bumped from 1.0.0 to 1.0.1
    * @peerbit/shared-log bumped from 1.0.0 to 1.0.1
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.0 to 1.0.1

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/rpc bumped from 1.0.1 to 1.0.2
    * @peerbit/shared-log bumped from 1.0.1 to 1.0.2
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.1 to 1.0.2

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/rpc bumped from 1.0.2 to 1.0.3
    * @peerbit/shared-log bumped from 1.0.2 to 1.0.3
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.2 to 1.0.3

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.1 to 1.0.2
    * @peerbit/rpc bumped from 1.0.3 to 1.0.4
    * @peerbit/shared-log bumped from 1.0.3 to 1.0.4
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.3 to 1.0.4

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/document-v1.0.5...document-v1.1.0) (2023-06-29)


### Features

* don't rely on replicator until minAge threshold ([a097bd0](https://github.com/dao-xyz/peerbit/commit/a097bd0ab97f132568042ee1af162077f1ce20bd))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/shared-log bumped from 1.0.5 to 1.1.0

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/document-v1.0.4...document-v1.0.5) (2023-06-29)


### Bug Fixes

* keep references to values of the value is a Program ([7b3ce39](https://github.com/dao-xyz/peerbit/commit/7b3ce3981e7b96f825431c0602f118f4019cb5f7))
* re-export Role in document store ([54ee879](https://github.com/dao-xyz/peerbit/commit/54ee879e22573e9426487900b451a2a33f8719e2))
* rn SubscriptionType to Role ([c92c83f](https://github.com/dao-xyz/peerbit/commit/c92c83f8a991995744401c56018d2a800d9b235e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.2 to 1.0.3
    * @peerbit/rpc bumped from 1.0.4 to 1.0.5
    * @peerbit/shared-log bumped from 1.0.4 to 1.0.5
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.4 to 1.0.5

## 1.0.0 (2023-06-28)


### ⚠ BREAKING CHANGES

* rename org on utility modules
* client abstraction
* indexBy as dynamic property of document store
* simplify search api
* program identity derived from libp2p
* throw error if variant is unspecified
* simplify role names
* simplified identity handling

### Features

* client abstraction ([6a1226d](https://github.com/dao-xyz/peerbit/commit/6a1226d4f8fc6deb167bff86cf7bdd6227c01a6b))
* indexBy as dynamic property of document store ([b239d70](https://github.com/dao-xyz/peerbit/commit/b239d70bae1f6fd004ce9154238f58b8face1ad6))
* program identity derived from libp2p ([e7802f8](https://github.com/dao-xyz/peerbit/commit/e7802f816eb3e06c14cc57b193d2bde2b5005cef))
* simplified identity handling ([1ae2416](https://github.com/dao-xyz/peerbit/commit/1ae24168a5c8629b8f9d1c57eceed6abd4a15020))
* simplify search api ([380e08d](https://github.com/dao-xyz/peerbit/commit/380e08da9285ec4aae51bc757ce3167dc9ffa949))
* throw error if variant is unspecified ([f4aef0e](https://github.com/dao-xyz/peerbit/commit/f4aef0ea5713eb37a0dfcf251fe6233e6a54dbd7))


### Bug Fixes

* rename org on utility modules ([0e09c8a](https://github.com/dao-xyz/peerbit/commit/0e09c8a29487205e02e45cc7f1e214450f96cb38))
* rm  redudan onResponse callback for search and iterator ([41a6098](https://github.com/dao-xyz/peerbit/commit/41a6098f3b031a89b85777856337d38f1ae66434))
* rm comment ([209e835](https://github.com/dao-xyz/peerbit/commit/209e8354e2328c00303cd07f122a586b5ece64bd))
* simplify role names ([f2bfd65](https://github.com/dao-xyz/peerbit/commit/f2bfd65422d0d7066cbc34693bfeafecb508004d))
* update doc ([4e0f567](https://github.com/dao-xyz/peerbit/commit/4e0f5671f6acece81cdf5475b8c0572a7932cec8))
* update invalid versions from prerelease release-please ([e2f6411](https://github.com/dao-xyz/peerbit/commit/e2f6411d46edf6d36723ca1ea81d1e55a09d3cd4))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.4 to 1.0.0
    * @peerbit/rpc bumped from 1.0.4 to 1.0.0
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.4 to 1.0.0

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/peerbit-document-v1.0.4...peerbit-document-v1.0.5) (2023-06-16)


### Bug Fixes

* rm  redudan onResponse callback for search and iterator ([41a6098](https://github.com/dao-xyz/peerbit/commit/41a6098f3b031a89b85777856337d38f1ae66434))

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/peerbit-document-v1.0.4...peerbit-document-v1.0.5) (2023-06-16)


### Bug Fixes

* rm  redudan onResponse callback for search and iterator ([41a6098](https://github.com/dao-xyz/peerbit/commit/41a6098f3b031a89b85777856337d38f1ae66434))

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/peerbit-document-v1.0.1-alpha1...peerbit-document-v1.0.3) (2023-06-14)


### Bug Fixes

* update invalid versions from prerelease release-please ([e2f6411](https://github.com/dao-xyz/peerbit/commit/e2f6411d46edf6d36723ca1ea81d1e55a09d3cd4))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.2 to 1.0.3
    * @peerbit/rpc bumped from 1.0.2 to 1.0.3
  * devDependencies
    * @peerbit/test-utils bumped from 1.0.2 to 1.0.3

## 1.0.0-alpha1 (2023-06-14)


### ⚠ BREAKING CHANGES

* indexBy as dynamic property of document store
* simplify search api
* program identity derived from libp2p
* throw error if variant is unspecified
* simplify role names
* simplified identity handling

### Features

* indexBy as dynamic property of document store ([b239d70](https://github.com/dao-xyz/peerbit/commit/b239d70bae1f6fd004ce9154238f58b8face1ad6))
* program identity derived from libp2p ([e7802f8](https://github.com/dao-xyz/peerbit/commit/e7802f816eb3e06c14cc57b193d2bde2b5005cef))
* simplified identity handling ([1ae2416](https://github.com/dao-xyz/peerbit/commit/1ae24168a5c8629b8f9d1c57eceed6abd4a15020))
* simplify search api ([380e08d](https://github.com/dao-xyz/peerbit/commit/380e08da9285ec4aae51bc757ce3167dc9ffa949))
* throw error if variant is unspecified ([f4aef0e](https://github.com/dao-xyz/peerbit/commit/f4aef0ea5713eb37a0dfcf251fe6233e6a54dbd7))


### Bug Fixes

* simplify role names ([f2bfd65](https://github.com/dao-xyz/peerbit/commit/f2bfd65422d0d7066cbc34693bfeafecb508004d))
* update doc ([4e0f567](https://github.com/dao-xyz/peerbit/commit/4e0f5671f6acece81cdf5475b8c0572a7932cec8))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 0.9.1 to 1.0.0-alpha1
    * @peerbit/rpc bumped from 0.9.1 to 1.0.0-alpha1
  * devDependencies
    * @peerbit/test-utils bumped from 0.4.3 to 1.0.0-alpha1

## @peerbit/document [0.9.1](https://github.com/dao-xyz/peerbit/compare/@peerbit/document@0.9.0...@peerbit/document@0.9.1) (2023-06-07)


### Bug Fixes

* add release cfg ([de76654](https://github.com/dao-xyz/peerbit/commit/de766548f8106804d319e8b51e9607f2a3f60726))





### Dependencies

* **@peerbit/program:** upgraded to 0.9.1
* **@peerbit/rpc:** upgraded to 0.9.1
* **@peerbit/test-utils:** upgraded to 0.4.3
* **@peerbit/time:** upgraded to 0.0.24
