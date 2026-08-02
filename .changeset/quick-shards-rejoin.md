---
"@peerbit/pubsub": patch
---

Cancel stale fanout shard joins when automatic topic-root candidates change so subscription reconciliation can retry the current root without waiting for the old join timeout.
