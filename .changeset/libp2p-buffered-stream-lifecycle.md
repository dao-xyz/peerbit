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

Update the coordinated libp2p dependency cohort to include buffered-stream close/reset and listener cleanup fixes. Preserve Peerbit's transport configuration, custom Yamux profile and standard Yamux fallback; WebRTC Direct is not enabled by this update.
