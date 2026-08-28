---
"@peerbit/log-rust": patch
"@peerbit/log": patch
"@peerbit/native-backbone": patch
"@peerbit/shared-log": patch
---

Bound shared-log gid peer history after trimming by carrying compact removed-gid facts through general native append transactions and reclaiming dead rows in bounded batches. Specialized document-index facts fallbacks remain unchanged for a follow-up slice.
