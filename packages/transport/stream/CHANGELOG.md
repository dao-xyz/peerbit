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
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.2 to 1.0.3

### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 2.0.0 to 2.1.0
    * @peerbit/stream-interface bumped from ^2.0.0 to ^2.0.1

## [2.0.6](https://github.com/dao-xyz/peerbit/compare/stream-v2.0.5...stream-v2.0.6) (2024-01-08)


### Bug Fixes

* add p-queue dep ([587427f](https://github.com/dao-xyz/peerbit/commit/587427f8194e74664e3318722d8c9af36b3f94cf))

## [2.0.5](https://github.com/dao-xyz/peerbit/compare/stream-v2.0.4...stream-v2.0.5) (2024-01-08)


### Bug Fixes

* continously update and invalidate routes ([26b424b](https://github.com/dao-xyz/peerbit/commit/26b424b3616869f6c10260a04817167b03d431a1))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 2.0.0 to 2.0.1
    * @peerbit/crypto bumped from 2.1.0 to 2.1.1
    * @peerbit/stream-interface bumped from ^2.0.1 to ^2.0.2
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 2.0.0 to 2.0.1

## [2.0.4](https://github.com/dao-xyz/peerbit/compare/stream-v2.0.3...stream-v2.0.4) (2024-01-03)


### Bug Fixes

* canRelayTrue by default ([6af9d9f](https://github.com/dao-xyz/peerbit/commit/6af9d9fc3af10fab936da6e2c4677f429796a9e1))

## [2.0.3](https://github.com/dao-xyz/peerbit/compare/stream-v2.0.2...stream-v2.0.3) (2024-01-02)


### Bug Fixes

* waitFor behaviour, only wait for reachable ([9935618](https://github.com/dao-xyz/peerbit/commit/9935618a8969e448775f7baab2773dc6e1034ace))

## [2.0.1](https://github.com/dao-xyz/peerbit/compare/stream-v2.0.0...stream-v2.0.1) (2023-12-31)


### Bug Fixes

* add wildcard dependency on test lib ([17ee002](https://github.com/dao-xyz/peerbit/commit/17ee002b7417e45a7c45dba280d02d07e5a14c27))
* remove stream dep ([c632688](https://github.com/dao-xyz/peerbit/commit/c6326885b27ddabe5d9dca436ab3f238bcc87820))

## [2.0.0](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.20...stream-v2.0.0) (2023-12-31)


### ⚠ BREAKING CHANGES

* modularize keychain
* refactor delivery modes
* lazy stream routing protocol

### Features

* add connection pruner in pubsub ([8b4c095](https://github.com/dao-xyz/peerbit/commit/8b4c095b6073ebb454be3370420d372ab77dcaf9))
* add countAll method for routes ([e5d19a3](https://github.com/dao-xyz/peerbit/commit/e5d19a3e6fe700a2aa4cc108e01d2ae9c8df268d))
* lazy stream routing protocol ([d12eb28](https://github.com/dao-xyz/peerbit/commit/d12eb2843b46c33fcbda5c97422cb263ab9f79a0))
* modularize keychain ([c10f10e](https://github.com/dao-xyz/peerbit/commit/c10f10e0beb58e38fa95d465962f43ab1aee75ef))
* refactor delivery modes ([9b366c0](https://github.com/dao-xyz/peerbit/commit/9b366c037521ddd9f80315836585e8d8fe587a09))


### Bug Fixes

* assert paths ([4f35dd2](https://github.com/dao-xyz/peerbit/commit/4f35dd2bc5ba2941a7270e7f931751338d156724))
* cleanup comments ([21cca12](https://github.com/dao-xyz/peerbit/commit/21cca1216499a4db430de7e093e6f4c31e0fcef6))
* clear healtcheck on reconnect ([035d47c](https://github.com/dao-xyz/peerbit/commit/035d47cc446293b0d9de3ce1c7eb58b66d4e75e7))
* collect uniqueAcks by message id ([3e6976b](https://github.com/dao-xyz/peerbit/commit/3e6976bc636e08d4b1221f141f8fe1d6564f214e))
* correctly handle ack cache cb ([ab1f8ce](https://github.com/dao-xyz/peerbit/commit/ab1f8ce9a456955afd9503578b4b5861c23a4512))
* correctly handle routing when doing ack delivery ([a22021c](https://github.com/dao-xyz/peerbit/commit/a22021c85bd6c7cb9f831f9a8fa54161c1095dc5))
* correctly ignore already seen messages ([3bf4fec](https://github.com/dao-xyz/peerbit/commit/3bf4fec81da428874259b0b1fe0344b76fd867f0))
* disable route updates for redundance message checks ([241d009](https://github.com/dao-xyz/peerbit/commit/241d00916338c67a1ce9f1e2a565fafc18abb4af))
* don't process messages if closed ([0888f53](https://github.com/dao-xyz/peerbit/commit/0888f53509864ead2c9addcbff9f546acc685e5d))
* don't wait for readable ([854ced3](https://github.com/dao-xyz/peerbit/commit/854ced33799c84dea54c888d73c63e29289353a0))
* dont process messages if not started ([6275062](https://github.com/dao-xyz/peerbit/commit/6275062b6a4e1425b48ed9dc3cde8e6e21df75bf))
* force messages to be provessed slowly to ensure topology ([8a2b69e](https://github.com/dao-xyz/peerbit/commit/8a2b69e8c0707a72aac0906c506b82918569d6fb))
* increase seek timeout ([d4cf164](https://github.com/dao-xyz/peerbit/commit/d4cf1641774f1f559c5da6e564bfb17c47fedd1c))
* prevent route loss on commit on target route ([087e38b](https://github.com/dao-xyz/peerbit/commit/087e38b82b44489dd0454eb4ab09b01e8a7c92be))
* remove log ([7768e13](https://github.com/dao-xyz/peerbit/commit/7768e139913ab03fc429ebc4c1fcfcd499e81a51))
* remove log ([b6e92fe](https://github.com/dao-xyz/peerbit/commit/b6e92fed444fadad38e00b0950b8995b7165b559))
* rm comment ([9bde1a8](https://github.com/dao-xyz/peerbit/commit/9bde1a86f71d3e8e5e8d93be3a5ac4d75c05840f))
* test add delay ([410be43](https://github.com/dao-xyz/peerbit/commit/410be43bcec5de21051d39713a26aa23dbd0ff45))
* test add delay ([daa9a13](https://github.com/dao-xyz/peerbit/commit/daa9a13a782271e059adb3a807ea188c97572ce2))
* try to dial directly through all neighbours ([63802e4](https://github.com/dao-xyz/peerbit/commit/63802e4a133f1f4065e6320d0d4d95dc1ea7e906))
* update libp2p ([d555de1](https://github.com/dao-xyz/peerbit/commit/d555de1e3c3f306277cb1cdc22b69a9c3ffd3f86))
* update libp2p dep ([f69c01a](https://github.com/dao-xyz/peerbit/commit/f69c01aeae10c6712eed0154fc3094c0af0108c2))
* update libp2p dependencies ([743db18](https://github.com/dao-xyz/peerbit/commit/743db18839de3e09904b50384aa389a4b660fe06))
* update routing periodically ([e4f52df](https://github.com/dao-xyz/peerbit/commit/e4f52dfb9364e7ad251299f942ab3756bbdc6708))
* update vite ([371bb8b](https://github.com/dao-xyz/peerbit/commit/371bb8b089873df36ff9e591b67046a7e8dab6ea))
* use yamux ([8dd2dac](https://github.com/dao-xyz/peerbit/commit/8dd2dac5bf19e5fa6cbe2fb3ed89197af896ffc3))
* wait for peer check ([a6a48e9](https://github.com/dao-xyz/peerbit/commit/a6a48e9d3456cdc2c42313d815f2a7a5ab62d5ef))
* wait for routes ([e744096](https://github.com/dao-xyz/peerbit/commit/e7440963bd8b37e88599ccffbd261e34db58a52b))
* wait until timeout for relayed ACKs ([398105e](https://github.com/dao-xyz/peerbit/commit/398105e7b39d56da2cf503f1f31365e91e47b72c))
* waitFor timeout ([5407016](https://github.com/dao-xyz/peerbit/commit/540701683aed5227e165442440d15766641d057a))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 1.1.1 to 2.0.0
    * @peerbit/crypto bumped from 1.0.10 to 2.0.0
    * @peerbit/stream-interface bumped from ^1.0.11 to ^2.0.0
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.8 to 2.0.0

## [1.0.20](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.19...stream-v1.0.20) (2023-09-24)


### Bug Fixes

* don't wait for direct dials ([89fd6ba](https://github.com/dao-xyz/peerbit/commit/89fd6ba557806fc8a8229006099b9ca654eb9fe4))

## [1.0.19](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.18...stream-v1.0.19) (2023-09-21)


### Bug Fixes

* cleanup test code ([9fa9266](https://github.com/dao-xyz/peerbit/commit/9fa9266eb423083b5e81b7a492ef3c6ca990366f))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.9 to 1.0.10
    * @peerbit/stream-interface bumped from ^1.0.10 to ^1.0.11
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.7 to 1.0.8

## [1.0.18](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.17...stream-v1.0.18) (2023-09-13)


### Bug Fixes

* correctly return on missing protocols ([105bc24](https://github.com/dao-xyz/peerbit/commit/105bc2476b661e02d3e7fab8d5a11ac0c11c37f1))
* refactor test ([39ce150](https://github.com/dao-xyz/peerbit/commit/39ce150222f760707cb690b7e7784ac3a33b6c28))

## [1.0.17](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.16...stream-v1.0.17) (2023-09-12)


### Bug Fixes

* only listen to webrtc connection-open events ([8c8718a](https://github.com/dao-xyz/peerbit/commit/8c8718a81ff44fb03a948bc284429123a05945dd))
* wait for webrtc directions to support protocol ([987c457](https://github.com/dao-xyz/peerbit/commit/987c457707cf7e6c7e4239f67720dab358ac0815))

## [1.0.16](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.15...stream-v1.0.16) (2023-09-10)


### Bug Fixes

* listen for new connections outside topology to capture webrtc connection ([5a50682](https://github.com/dao-xyz/peerbit/commit/5a50682e5b1e9fd1d77c1d2bfc1d29bea908d608))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 1.1.0 to 1.1.1
    * @peerbit/crypto bumped from 1.0.8 to 1.0.9
    * @peerbit/stream-interface bumped from ^1.0.9 to ^1.0.10
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.6 to 1.0.7

## [1.0.15](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.14...stream-v1.0.15) (2023-09-07)


### Bug Fixes

* allow incoming streams to run on transient connection ([ece5005](https://github.com/dao-xyz/peerbit/commit/ece5005fbaaf32fe82cb0456f56b05d841f494b9))

## [1.0.14](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.13...stream-v1.0.14) (2023-09-06)


### Bug Fixes

* handle overflow from invalid payload decoding ([d19b2e7](https://github.com/dao-xyz/peerbit/commit/d19b2e79597111cc47592e85d577d8456571c4b2))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.7 to 1.0.8
    * @peerbit/stream-interface bumped from ^1.0.8 to ^1.0.9

## [1.0.13](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.12...stream-v1.0.13) (2023-09-06)


### Bug Fixes

* update to 0.46.9 ([f6bf439](https://github.com/dao-xyz/peerbit/commit/f6bf4398e4caf7472cdfa4296990d0518c295e4c))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.6 to 1.0.7
    * @peerbit/stream-interface bumped from ^1.0.7 to ^1.0.8
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.5 to 1.0.6

## [1.0.12](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.11...stream-v1.0.12) (2023-09-03)


### Bug Fixes

* prevent slow writes to block fast writes ([b01eecc](https://github.com/dao-xyz/peerbit/commit/b01eeccf992bbda45886644df352e7accf66c819))

## [1.0.11](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.10...stream-v1.0.11) (2023-09-03)


### Bug Fixes

* downgrade to libp2p 0.46.6 ([bd7418e](https://github.com/dao-xyz/peerbit/commit/bd7418e0f36867ea5995abde98ecfd3880ccfaaf))


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.4 to 1.0.5

## [1.0.10](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.9...stream-v1.0.10) (2023-09-02)


### Bug Fixes

* trailing comma formatting ([80a679c](https://github.com/dao-xyz/peerbit/commit/80a679c0dc0e7c8ac01538cb11458299fdb334d5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 1.0.2 to 1.1.0
    * @peerbit/crypto bumped from 1.0.5 to 1.0.6
    * @peerbit/stream-interface bumped from ^1.0.6 to ^1.0.7
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.3 to 1.0.4

## [1.0.8](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.7...stream-v1.0.8) (2023-08-06)


### Bug Fixes

* typo change recieve to receive ([9b05cfc](https://github.com/dao-xyz/peerbit/commit/9b05cfc9220f6d8206626f5208724e3d0f34abe2))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.4 to 1.0.5
    * @peerbit/stream-interface bumped from ^1.0.5 to ^1.0.6

## [1.0.7](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.6...stream-v1.0.7) (2023-07-28)


### Bug Fixes

* fix graphology version ([aa549c9](https://github.com/dao-xyz/peerbit/commit/aa549c9a1fcfb0b78ba30a9a555e5e952634681b))

## [1.0.6](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.5...stream-v1.0.6) (2023-07-18)


### Bug Fixes

* correctly ignore undefined stream ([b297c19](https://github.com/dao-xyz/peerbit/commit/b297c190dde46617a158e8bd5bb182ac5dbe71af))
* refactor ([751a3f3](https://github.com/dao-xyz/peerbit/commit/751a3f365f405b332a227203f65d4b3e278ca49d))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 1.0.1 to 1.0.2
    * @peerbit/crypto bumped from 1.0.3 to 1.0.4
    * @peerbit/stream-interface bumped from ^1.0.4 to ^1.0.5
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.1 to 1.0.2

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.4...stream-v1.0.5) (2023-07-04)


### Bug Fixes

* rm postbuild script ([b627bf0](https://github.com/dao-xyz/peerbit/commit/b627bf0dcdb99d24ac8c9055586e72ea2d174fcc))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 1.0.0 to 1.0.1
    * @peerbit/crypto bumped from 1.0.2 to 1.0.3
    * @peerbit/stream-interface bumped from ^1.0.3 to ^1.0.4
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.0 to 1.0.1

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.3...stream-v1.0.4) (2023-06-30)


### Bug Fixes

* purge old hellos ([46da9dc](https://github.com/dao-xyz/peerbit/commit/46da9dc22e7c94d12c61cc0b5ffc4d1eff487300))

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/stream-v1.0.2...stream-v1.0.3) (2023-06-29)


### Bug Fixes

* peer stream event types ([7607d7d](https://github.com/dao-xyz/peerbit/commit/7607d7de837813441a81f477b91ceeaba65a108f))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/stream-interface bumped from ^1.0.2 to ^1.0.3

## 1.0.0 (2023-06-28)


### ⚠ BREAKING CHANGES

* rename org on utility modules
* reuse pubsub message id on rpc messages
* client abstraction

### Features

* client abstraction ([6a1226d](https://github.com/dao-xyz/peerbit/commit/6a1226d4f8fc6deb167bff86cf7bdd6227c01a6b))
* reuse pubsub message id on rpc messages ([57bede7](https://github.com/dao-xyz/peerbit/commit/57bede71cd822c71b439bd8011b6f25bff1da5cb))


### Bug Fixes

* rename org on utility modules ([0e09c8a](https://github.com/dao-xyz/peerbit/commit/0e09c8a29487205e02e45cc7f1e214450f96cb38))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 0.0.7 to 1.0.0
    * @peerbit/crypto bumped from 1.0.4 to 1.0.0
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.4 to 1.0.0

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/libp2p-direct-stream-v1.0.3...libp2p-direct-stream-v1.0.4) (2023-06-15)


### Bug Fixes

* bump dependencies ([8a8fd44](https://github.com/dao-xyz/peerbit/commit/8a8fd440149a966337382db77afe1071141e5c74))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/cache bumped from 0.0.6 to 0.0.7
    * @peerbit/crypto bumped from 1.0.3 to 1.0.4
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.3 to 1.0.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/libp2p-direct-stream-v1.0.0-alpha1...libp2p-direct-stream-v1.0.3) (2023-06-14)


### Bug Fixes

* update invalid versions from prerelease release-please ([e2f6411](https://github.com/dao-xyz/peerbit/commit/e2f6411d46edf6d36723ca1ea81d1e55a09d3cd4))
* update to libp2p 0.45.9 ([0420543](https://github.com/dao-xyz/peerbit/commit/0420543084d82ab08084894f24c1dff340ba6c9b))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 1.0.2 to 1.0.3
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 1.0.2 to 1.0.3

## [1.0.0-alpha1](https://github.com/dao-xyz/peerbit/compare/libp2p-direct-stream-v1.0.0-alpha1...libp2p-direct-stream-v1.0.0-alpha1) (2023-06-14)


### Bug Fixes

* update to libp2p 0.45.9 ([0420543](https://github.com/dao-xyz/peerbit/commit/0420543084d82ab08084894f24c1dff340ba6c9b))

## 1.0.0-alpha1 (2023-06-14)


### ⚠ BREAKING CHANGES

* simplified identity handling

### Features

* simplified identity handling ([1ae2416](https://github.com/dao-xyz/peerbit/commit/1ae24168a5c8629b8f9d1c57eceed6abd4a15020))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @peerbit/crypto bumped from 0.1.16 to 1.0.0-alpha1
  * devDependencies
    * @peerbit/libp2p-test-utils bumped from 0.5.3 to 1.0.0-alpha1

## @peerbit/stream [0.5.3](https://github.com/dao-xyz/peerbit/compare/@peerbit/stream@0.5.2...@peerbit/stream@0.5.3) (2023-06-07)


### Bug Fixes

* add release cfg ([de76654](https://github.com/dao-xyz/peerbit/commit/de766548f8106804d319e8b51e9607f2a3f60726))





### Dependencies

* **@peerbit/cache:** upgraded to 0.0.6
* **@peerbit/crypto:** upgraded to 0.1.16
* **@peerbit/libp2p-test-utils:** upgraded to 0.5.3
