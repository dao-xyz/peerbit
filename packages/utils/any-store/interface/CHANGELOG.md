# Changelog

## 1.1.2

### Patch Changes

- [#1115](https://github.com/dao-xyz/peerbit/pull/1115) [`d39179d`](https://github.com/dao-xyz/peerbit/commit/d39179d938ef55bbed7c9ca319d72c2a41583a30) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Track insertion-time MemoryStore byte size during mutations so repeated size checks stay constant-time, reject aggregate counter overflow atomically, and document AnyStore's backend-accounted size contract, including caller-owned buffers that are later resized or detached.

## 1.1.1

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

## [1.1.0](https://github.com/dao-xyz/peerbit/compare/any-store-interface-v1.0.1...any-store-interface-v1.1.0) (2025-11-25)

### Features

- migrate to borsh 6 and Typescript Stage 3 decorators ([86caba4](https://github.com/dao-xyz/peerbit/commit/86caba4f2128d3b1e2d274bea1b537722b5ec1c7))

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/any-store-interface-v1.0.0...any-store-interface-v1.0.1) (2025-10-03)

### Bug Fixes

- add missing deps ([cf45de8](https://github.com/dao-xyz/peerbit/commit/cf45de831c5e0d3d1d97441a9e952537cd708f58))

## 1.0.0 (2024-07-20)

### Features

- add api for determining if persistant ([bc9e218](https://github.com/dao-xyz/peerbit/commit/bc9e218651a086ded8e7eaebaf15f3ce0db176d0))

### Bug Fixes

- fmt ([bdee4f4](https://github.com/dao-xyz/peerbit/commit/bdee4f4943fcabd21c53a4f37dba17d04cea2577))
- peerbit eslint rules ([5056694](https://github.com/dao-xyz/peerbit/commit/5056694f90ad03c0c5ba1e47c6ac57387d85aba9))
- remove invalid change logs ([a9206a8](https://github.com/dao-xyz/peerbit/commit/a9206a802e97e08caf8f187e2b033046bab0ba7c))
