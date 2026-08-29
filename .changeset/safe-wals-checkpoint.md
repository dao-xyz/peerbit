---
"@peerbit/native-backbone": patch
---

Add opt-in crash-safe checkpoint compaction for the Node coordinate,
document-value, and document-signer WALs. Legacy directories migrate when their
first checkpoint is published, and migrated directories fail closed on older
package versions to prevent stale replay.
