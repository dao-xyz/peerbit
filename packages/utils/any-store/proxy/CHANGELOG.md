# Changelog

## 1.0.53

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.53

## 1.0.52

### Patch Changes

- Updated dependencies [[`d39179d`](https://github.com/dao-xyz/peerbit/commit/d39179d938ef55bbed7c9ca319d72c2a41583a30)]:
  - @peerbit/any-store-interface@1.1.2
  - @peerbit/canonical-host@1.0.52
  - @peerbit/canonical-client@1.1.43

## 1.0.51

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.51
  - @peerbit/canonical-client@1.1.42

## 1.0.50

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.50
  - @peerbit/canonical-client@1.1.41

## 1.0.49

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-client@1.1.40
  - @peerbit/canonical-host@1.0.49

## 1.0.48

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

- Updated dependencies [[`b0442bb`](https://github.com/dao-xyz/peerbit/commit/b0442bb95d4807acca64bd68c2223ecf8edc4f33)]:
  - @peerbit/any-store-interface@1.1.1
  - @peerbit/canonical-host@1.0.48
  - @peerbit/canonical-client@1.1.39

## 1.0.47

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.47
  - @peerbit/canonical-client@1.1.38

## 1.0.46

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-client@1.1.37
  - @peerbit/canonical-host@1.0.46

## 1.0.45

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.45

## 1.0.44

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.44
  - @peerbit/canonical-client@1.1.36

## 1.0.43

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.43

## 1.0.42

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.42

## 1.0.41

### Patch Changes

- Updated dependencies []:
  - @peerbit/canonical-host@1.0.41
  - @peerbit/canonical-client@1.1.35

## [1.0.40](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.39...any-store-proxy-v1.0.40) (2026-05-28)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.34
    - @peerbit/canonical-host bumped to 1.0.40

## [1.0.39](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.38...any-store-proxy-v1.0.39) (2026-05-26)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.33
    - @peerbit/canonical-host bumped to 1.0.39

## [1.0.38](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.37...any-store-proxy-v1.0.38) (2026-05-05)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.32
    - @peerbit/canonical-host bumped to 1.0.38

## [1.0.37](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.36...any-store-proxy-v1.0.37) (2026-05-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.31
    - @peerbit/canonical-host bumped to 1.0.37

## [1.0.36](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.35...any-store-proxy-v1.0.36) (2026-05-03)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.30
    - @peerbit/canonical-host bumped to 1.0.36

## [1.0.35](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.34...any-store-proxy-v1.0.35) (2026-05-02)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.29
    - @peerbit/canonical-host bumped to 1.0.35

## [1.0.34](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.33...any-store-proxy-v1.0.34) (2026-05-01)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.28
    - @peerbit/canonical-host bumped to 1.0.34

## [1.0.33](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.32...any-store-proxy-v1.0.33) (2026-04-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.27
    - @peerbit/canonical-host bumped to 1.0.33

## [1.0.32](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.31...any-store-proxy-v1.0.32) (2026-04-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.26
    - @peerbit/canonical-host bumped to 1.0.32

## [1.0.31](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.30...any-store-proxy-v1.0.31) (2026-04-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.25
    - @peerbit/canonical-host bumped to 1.0.31

## [1.0.30](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.29...any-store-proxy-v1.0.30) (2026-04-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.24
    - @peerbit/canonical-host bumped to 1.0.30

## [1.0.29](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.28...any-store-proxy-v1.0.29) (2026-04-29)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.23
    - @peerbit/canonical-host bumped to 1.0.29

## [1.0.28](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.27...any-store-proxy-v1.0.28) (2026-04-28)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.22
    - @peerbit/canonical-host bumped to 1.0.28

## [1.0.27](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.26...any-store-proxy-v1.0.27) (2026-04-14)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.21
    - @peerbit/canonical-host bumped to 1.0.27

## [1.0.26](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.25...any-store-proxy-v1.0.26) (2026-04-03)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.20
    - @peerbit/canonical-host bumped to 1.0.26

## [1.0.25](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.24...any-store-proxy-v1.0.25) (2026-03-30)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.19
    - @peerbit/canonical-host bumped to 1.0.25

## [1.0.24](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.23...any-store-proxy-v1.0.24) (2026-03-29)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.18
    - @peerbit/canonical-host bumped to 1.0.24

## [1.0.23](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.22...any-store-proxy-v1.0.23) (2026-03-27)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.17
    - @peerbit/canonical-host bumped to 1.0.23

## [1.0.22](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.21...any-store-proxy-v1.0.22) (2026-03-27)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.16
    - @peerbit/canonical-host bumped to 1.0.22

## [1.0.21](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.20...any-store-proxy-v1.0.21) (2026-03-22)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.15
    - @peerbit/canonical-host bumped to 1.0.21

## [1.0.20](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.19...any-store-proxy-v1.0.20) (2026-03-18)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.14
    - @peerbit/canonical-host bumped to 1.0.20

## [1.0.19](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.18...any-store-proxy-v1.0.19) (2026-03-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.13
    - @peerbit/canonical-host bumped to 1.0.19

## [1.0.18](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.17...any-store-proxy-v1.0.18) (2026-03-17)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.12
    - @peerbit/canonical-host bumped to 1.0.18

## [1.0.17](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.16...any-store-proxy-v1.0.17) (2026-03-16)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-host bumped to 1.0.17

## [1.0.16](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.15...any-store-proxy-v1.0.16) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.11
    - @peerbit/canonical-host bumped to 1.0.16

## [1.0.15](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.14...any-store-proxy-v1.0.15) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.10
    - @peerbit/canonical-host bumped to 1.0.15

## [1.0.14](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.13...any-store-proxy-v1.0.14) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.9
    - @peerbit/canonical-host bumped to 1.0.14

## [1.0.13](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.12...any-store-proxy-v1.0.13) (2026-03-15)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-host bumped to 1.0.13

## [1.0.12](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.11...any-store-proxy-v1.0.12) (2026-03-09)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.8
    - @peerbit/canonical-host bumped to 1.0.12

## [1.0.11](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.10...any-store-proxy-v1.0.11) (2026-03-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.7
    - @peerbit/canonical-host bumped to 1.0.11

## [1.0.10](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.9...any-store-proxy-v1.0.10) (2026-03-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.6
    - @peerbit/canonical-host bumped to 1.0.10

## [1.0.9](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.8...any-store-proxy-v1.0.9) (2026-03-08)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.5
    - @peerbit/canonical-host bumped to 1.0.9

## [1.0.8](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.7...any-store-proxy-v1.0.8) (2026-03-07)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.4
    - @peerbit/canonical-host bumped to 1.0.8

## [1.0.7](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.6...any-store-proxy-v1.0.7) (2026-03-05)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-host bumped to 1.0.7

## [1.0.6](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.5...any-store-proxy-v1.0.6) (2026-03-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-host bumped to 1.0.6

## [1.0.5](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.4...any-store-proxy-v1.0.5) (2026-03-04)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.3
    - @peerbit/canonical-host bumped to 1.0.5

## [1.0.4](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.3...any-store-proxy-v1.0.4) (2026-01-27)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.2
    - @peerbit/canonical-host bumped to 1.0.4

## [1.0.3](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.2...any-store-proxy-v1.0.3) (2026-01-24)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.1
    - @peerbit/canonical-host bumped to 1.0.3

## [1.0.2](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.1...any-store-proxy-v1.0.2) (2026-01-23)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-host bumped to 1.0.2

## [1.0.1](https://github.com/dao-xyz/peerbit/compare/any-store-proxy-v1.0.0...any-store-proxy-v1.0.1) (2026-01-22)

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.1.0
    - @peerbit/canonical-host bumped to 1.0.1

## 1.0.0 (2026-01-17)

### Features

- **canonical:** add proxy packages and e2e suites ([ad5b802](https://github.com/dao-xyz/peerbit/commit/ad5b802fd57546cc1757852d449e7616e32ff097))

### Dependencies

- The following workspace dependencies were updated
  - dependencies
    - @peerbit/canonical-client bumped to 1.0.0
    - @peerbit/canonical-host bumped to 1.0.0
