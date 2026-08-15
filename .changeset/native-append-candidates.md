---
"@peerbit/shared-log-rust": patch
"@peerbit/native-backbone": patch
"@peerbit/shared-log": patch
---

Avoid scanning every replication range during native append when the exact
resident owner count proves full-replica delivery cannot apply.
