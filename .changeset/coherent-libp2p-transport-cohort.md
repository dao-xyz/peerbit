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
"@peerbit/stream": patch
"@peerbit/stream-interface": patch
"@peerbit/test-utils": patch
"@peerbit/trusted-network": patch
"peerbit": patch
---

Update the coordinated libp2p dependency cohort and use the maintained Noise and Yamux package names. Preserve Peerbit's transport configuration, custom Yamux protocol and window sizes, and standard Yamux fallback. Refresh browser fixture dependencies so transport imports do not rely on workspace hoisting.
