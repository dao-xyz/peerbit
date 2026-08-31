---
"@peerbit/any-store-interface": patch
"@peerbit/any-store": patch
"@peerbit/indexer-interface": patch
"@peerbit/indexer-sqlite3": patch
"@peerbit/blocks-interface": patch
"@peerbit/blocks": patch
"@peerbit/log": patch
"@peerbit/shared-log-rust": patch
"@peerbit/shared-log": patch
"@peerbit/document": patch
"@peerbit/native-backbone": patch
"@peerbit/pubsub": patch
---

Add session-bound persisted delivery receipts so document and shared-log writers
can wait for an exact entry to be crash-safe on a requested number of current
remote replicas before retiring. Directory-backed block and index stores now
expose the durability barriers required to issue these receipts. Independent
payload-only append batches also retain their replication metadata, so remote
peers can admit and persist every entry in the batch. Persisted delivery also
reuses one native full-replica routing plan across gid-independent batches.
Multi-block provider discovery now publishes one renewable log-wide lease while
retaining batched, wire-compatible per-CID announcements for mixed-version
readers.
