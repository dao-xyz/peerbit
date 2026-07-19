---
"peerbit": patch
"@peerbit/react": patch
"@peerbit/shared-log": patch
---

Expose a validated `pubsubUploadLimitBps` runtime option through `Peerbit.create` and the browser node `PeerProvider`, applying it to root and node pubsub shard channels and as the overridable default for opted-in SharedLog fanout channels.
