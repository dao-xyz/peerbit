---
"@peerbit/blocks": patch
"@peerbit/canonical-client": patch
"@peerbit/canonical-host": patch
"@peerbit/crypto": patch
"@peerbit/document": patch
"@peerbit/identity-access-controller": patch
"@peerbit/keychain": patch
"@peerbit/libp2p-test-utils": patch
"@peerbit/log": patch
"@peerbit/logger": patch
"@peerbit/program": patch
"@peerbit/pubsub": patch
"@peerbit/pubsub-interface": patch
"@peerbit/react": patch
"@peerbit/server": patch
"@peerbit/shared-log": patch
"@peerbit/stream-interface": patch
"@peerbit/test-utils": patch
"@peerbit/trusted-network": patch
"peerbit": patch
---

Refresh the libp2p stack to a single deduplicated resolution.

The lockfile held every libp2p package at the floor of its own caret range, so
`@libp2p/interface` sat at 3.1.1 while the current stack is on 3.2.5. Updating
the family in one resolution moves `libp2p` to 3.3.8, `@libp2p/webrtc` to
6.0.29, and `@libp2p/interface` to a single 3.2.5 copy, which removes the
duplicate branded `Transport` type that made an isolated webrtc bump fail.

`uint8arraylist` and `it-length-prefixed` move to their v3/v11 lines to match,
and the `node-datachannel` override is dropped: webrtc 6.0.29 requires a
prebuilt `^0.32.3` on its own, so the workspace now resolves what a consumer of
the published manifests already resolved.

No source or public API changes.
