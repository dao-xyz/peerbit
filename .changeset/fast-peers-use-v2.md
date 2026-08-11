---
"@peerbit/shared-log": patch
"@peerbit/pubsub": patch
"@peerbit/pubsub-interface": patch
"@peerbit/document": patch
---

Use authenticated replication-info V2 by default while retaining legacy fallback for explicit pre-v10 compatibility opens, bind subscription events to their signed transport generation so reconnect handshakes cannot inherit stale capabilities, and establish both document-index and shared-log topics before reporting peer readiness.
