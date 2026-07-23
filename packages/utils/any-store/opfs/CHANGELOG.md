# Changelog

## 1.1.11

### Patch Changes

- Updated dependencies [[`d39179d`](https://github.com/dao-xyz/peerbit/commit/d39179d938ef55bbed7c9ca319d72c2a41583a30)]:
  - @peerbit/any-store-interface@1.1.2

## 1.1.10

### Patch Changes

- Updated dependencies [[`a5e15b4`](https://github.com/dao-xyz/peerbit/commit/a5e15b421f39824a87322f4b88a51df120b8700b)]:
  - @peerbit/crypto@3.1.4

## 1.1.9

### Patch Changes

- [#1056](https://github.com/dao-xyz/peerbit/pull/1056) [`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Raise vulnerable direct runtime dependency floors and replace the legacy
  elliptic secp256k1 implementation with the maintained noble-curves
  implementation. Raw (`PreHash.NONE`) secp256k1 signing and recovery now require
  an exact 32-byte prepared digest; verification rejects every other length.
  The package's direct `@noble/curves` edge stays on the secure Node 18-compatible
  1.9.7 line. The wider libp2p graph still carries an upstream transitive noble
  2.0.1 engine constraint; removing that separate dependency debt is outside this
  direct crypto replacement.

  Repository development-tool pins are scoped to their compatible parent lines.
  Those root `pnpm` overrides are not published, so applications upgrading these
  packages should refresh their own lockfiles to pick up the patched transitive
  versions.

- Updated dependencies [[`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33), [`0a5a9a0`](https://github.com/dao-xyz/peerbit/commit/0a5a9a0c0690a310e141b80bcb84ba04fd48b329)]:
  - @peerbit/any-store-interface@1.1.1
  - @peerbit/crypto@3.1.3
  - @peerbit/time@3.0.1

## 1.1.8

### Patch Changes

- Updated dependencies []:
  - @peerbit/crypto@3.1.2

## [1.1.7](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.6...any-store-opfs-v1.1.7) (2026-03-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.1.1
    - @peerbit/logger bumped to 2.0.1

## [1.1.6](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.5...any-store-opfs-v1.1.6) (2026-03-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.1.0

## [1.1.5](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.4...any-store-opfs-v1.1.5) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.0.1

## [1.1.4](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.3...any-store-opfs-v1.1.4) (2026-03-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 3.0.0
    - @peerbit/time bumped to 3.0.0

## [1.1.3](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.2...any-store-opfs-v1.1.3) (2025-12-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 2.4.1

## [1.1.2](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.1...any-store-opfs-v1.1.2) (2025-12-02)

### Bug Fixes

- bundle as classic worker ([20fc6e6](https://github.com/dao-xyz/peerbit/commit/20fc6e6571676defbd0a7a1c18eb14ce423a7ff2))

## [1.1.1](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.1.0...any-store-opfs-v1.1.1) (2025-11-26)

### Bug Fixes

- module bundling ([3797bbc](https://github.com/dao-xyz/peerbit/commit/3797bbc3782717a85bd26790924a49730f1d1076))

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.15...any-store-opfs-v1.1.0) (2025-11-25)

### Features

- migrate to borsh 6 and Typescript Stage 3 decorators ([86caba4](https://github.com/dao-xyz/peerbit/commit/86caba4f2128d3b1e2d274bea1b537722b5ec1c7))
- unify asset bundling into dist/assets for asset generating packages ([5d6612c](https://github.com/dao-xyz/peerbit/commit/5d6612c726f5eebbf5e05cc082a1fca16831e9e2))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 2.4.0
    - @peerbit/logger bumped to 2.0.0
    - @peerbit/time bumped to 2.3.0
    - @peerbit/any-store-interface bumped to 1.1.0

## [1.0.15](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.14...any-store-opfs-v1.0.15) (2025-10-03)

### Bug Fixes

- restore deps versions ([5d6b35a](https://github.com/dao-xyz/peerbit/commit/5d6b35a01a08f87bd17ad63eacb70b4b8a44b1db))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 2.3.13

## [1.0.14](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.13...any-store-opfs-v1.0.14) (2025-10-03)

### Bug Fixes

- add missing deps ([cf45de8](https://github.com/dao-xyz/peerbit/commit/cf45de831c5e0d3d1d97441a9e952537cd708f58))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped to 2.3.12
    - @peerbit/any-store-interface bumped to 1.0.1

## [1.0.13](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.12...any-store-opfs-v1.0.13) (2025-09-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/logger bumped from 1.0.3 to 1.0.4

## [1.0.12](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.11...any-store-opfs-v1.0.12) (2025-08-19)

### Bug Fixes

- Uint8array inner generic type ([db9a39b](https://github.com/dao-xyz/peerbit/commit/db9a39bed8501a45212d6130ffeed455422fa613))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.10 to ^2.3.11

## [1.0.11](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.10...any-store-opfs-v1.0.11) (2025-08-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.9 to ^2.3.10
    - @peerbit/time bumped from 2.1.0 to 2.2.0

## [1.0.10](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.9...any-store-opfs-v1.0.10) (2025-06-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.8 to ^2.3.9

## [1.0.9](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.8...any-store-opfs-v1.0.9) (2025-04-19)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.7 to ^2.3.8

## [1.0.8](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.7...any-store-opfs-v1.0.8) (2025-04-03)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.6 to ^2.3.7
    - @peerbit/time bumped from 2.0.8 to 2.1.0

## [1.0.7](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.6...any-store-opfs-v1.0.7) (2025-03-13)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.5 to ^2.3.6

## [1.0.6](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.5...any-store-opfs-v1.0.6) (2025-02-25)

### Bug Fixes

- make sure size caches are calcualted in multi-level scenarios ([6dbebf0](https://github.com/dao-xyz/peerbit/commit/6dbebf0cc092663b1c5a367ebbdfe066fbe1861c))

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.4...any-store-opfs-v1.0.5) (2025-02-20)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.4 to ^2.3.5
    - @peerbit/time bumped from 2.0.7 to 2.0.8

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.3...any-store-opfs-v1.0.4) (2025-02-20)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.3 to ^2.3.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.2...any-store-opfs-v1.0.3) (2025-01-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.2 to ^2.3.3

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.1...any-store-opfs-v1.0.2) (2024-10-11)

### Bug Fixes

- update uuid ([5f7f16b](https://github.com/dao-xyz/peerbit/commit/5f7f16bc9e0c8b769e4d3c7bd1050701f58c1187))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.1 to ^2.3.2

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/any-store-opfs-v1.0.0...any-store-opfs-v1.0.1) (2024-09-01)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.3.0 to ^2.3.1

## 1.0.0 (2024-07-20)

### Features

- add api for determining if persistant ([bc9e218](https://github.com/dao-xyz/peerbit/commit/bc9e218651a086ded8e7eaebaf15f3ce0db176d0))

### Bug Fixes

- fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
- peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/crypto bumped from ^2.2.0 to ^2.3.0
    - @peerbit/logger bumped from 1.0.2 to 1.0.3
    - @peerbit/time bumped from 2.0.6 to 2.0.7
    - @peerbit/any-store-interface bumped from ^2.0.2 to ^1.0.0
