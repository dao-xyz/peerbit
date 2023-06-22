# Changelog

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/program bumped from 1.0.0-alpha1 to 1.0.1-alpha1
  * devDependencies
    * @peerbit/document bumped from 1.0.0-alpha1 to 1.0.1-alpha1
    * @peerbit/test-utils bumped from 1.0.0-alpha1 to 1.0.1-alpha1

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/peerbit-v1.0.3...peerbit-v1.0.4) (2023-06-15)


### Bug Fixes

* bump dependencies ([8a8fd44](https://github.com/dao-xyz/peerbit/commit/8a8fd440149a966337382db77afe1071141e5c74))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @dao-xyz/lazy-level bumped from 0.0.8 to 0.0.9
    * @peerbit/crypto bumped from 1.0.3 to 1.0.4
    * @peerbit/logger bumped from 0.0.7 to 1.0.0
    * @peerbit/program bumped from 1.0.3 to 1.0.4
    * @dao-xyz/uint8arrays bumped from 0.0.4 to 1.0.0
  * devDependencies
    * @peerbit/document bumped from 1.0.3 to 1.0.4
    * @peerbit/test-utils bumped from 1.0.3 to 1.0.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/peerbit-v1.0.1-alpha1...peerbit-v1.0.3) (2023-06-14)


### Bug Fixes

* update invalid versions from prerelease release-please ([e2f6411](https://github.com/dao-xyz/peerbit/commit/e2f6411d46edf6d36723ca1ea81d1e55a09d3cd4))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.2 to 1.0.3
    * @peerbit/program bumped from 1.0.2 to 1.0.3
  * devDependencies
    * @peerbit/document bumped from 1.0.2 to 1.0.3
    * @peerbit/test-utils bumped from 1.0.2 to 1.0.3

## 1.0.0-alpha1 (2023-06-14)


### ⚠ BREAKING CHANGES

* Default encryption keypair is the same the libp2p PeerId
* only allow Ed25519 PeerIds
* remove disconnect fn
* indexBy as dynamic property of document store
* program identity derived from libp2p
* simplify role names
* simplified identity handling

### Features

* Default encryption keypair is the same the libp2p PeerId ([43a3711](https://github.com/dao-xyz/peerbit/commit/43a3711525ceb1f24c10e1d8924c15cddb5928bc))
* indexBy as dynamic property of document store ([b239d70](https://github.com/dao-xyz/peerbit/commit/b239d70bae1f6fd004ce9154238f58b8face1ad6))
* only allow Ed25519 PeerIds ([532c8b3](https://github.com/dao-xyz/peerbit/commit/532c8b35bc4e85719669db47639ec5ffd11c8eab))
* program identity derived from libp2p ([e7802f8](https://github.com/dao-xyz/peerbit/commit/e7802f816eb3e06c14cc57b193d2bde2b5005cef))
* remove disconnect fn ([58e0cea](https://github.com/dao-xyz/peerbit/commit/58e0cea6df27c1d14a7edeb9b05050b1036e1db4))
* simplified identity handling ([1ae2416](https://github.com/dao-xyz/peerbit/commit/1ae24168a5c8629b8f9d1c57eceed6abd4a15020))


### Bug Fixes

* refactor loop ([cf25045](https://github.com/dao-xyz/peerbit/commit/cf250453dbfe4dd64dbabe9ed922bdde12b92864))
* rm import ([ddfc873](https://github.com/dao-xyz/peerbit/commit/ddfc873b532ea8bb32b482f40916d0f2f0e2c9a2))
* simplify role names ([f2bfd65](https://github.com/dao-xyz/peerbit/commit/f2bfd65422d0d7066cbc34693bfeafecb508004d))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 0.1.16 to 1.0.0-alpha1
    * @peerbit/program bumped from 0.9.1 to 1.0.0-alpha1
  * devDependencies
    * @peerbit/document bumped from 0.9.1 to 1.0.0-alpha1
    * @peerbit/test-utils bumped from 0.4.3 to 1.0.0-alpha1

## peerbit [0.9.1](https://github.com/dao-xyz/peerbit/compare/peerbit@0.9.0...peerbit@0.9.1) (2023-06-07)


### Bug Fixes

* add release cfg ([de76654](https://github.com/dao-xyz/peerbit/commit/de766548f8106804d319e8b51e9607f2a3f60726))





### Dependencies

* **@dao-xyz/lazy-level:** upgraded to 0.0.8
* **@peerbit/crypto:** upgraded to 0.1.16
* **peerbit-keystore:** upgraded to 0.2.12
* **@peerbit/logger:** upgraded to 0.0.7
* **@peerbit/program:** upgraded to 0.9.1
* **@dao-xyz/uint8arrays:** upgraded to 0.0.4
* **@peerbit/document:** upgraded to 0.9.1
* **@peerbit/test-utils:** upgraded to 0.4.3
