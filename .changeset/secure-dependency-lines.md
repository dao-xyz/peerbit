---
"@peerbit/any-store": patch
"@peerbit/any-store-interface": patch
"@peerbit/any-store-opfs": patch
"@peerbit/any-store-proxy": patch
"@peerbit/crypto": patch
"@peerbit/document-react": patch
"@peerbit/indexer-interface": patch
"@peerbit/indexer-sqlite3": patch
"@peerbit/indexer-tests": patch
"@peerbit/log": patch
"@peerbit/react": patch
"@peerbit/server": patch
"@peerbit/vite": patch
---

Raise vulnerable direct runtime dependency floors and replace the legacy
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
