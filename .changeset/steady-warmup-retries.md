---
"@peerbit/shared-log": patch
---

Serialize and coalesce delayed join-warmup retries per peer so large late-join synchronization does not accumulate overlapping full-backlog sends. The retry window and wire protocol remain unchanged. This releases the Peerbit-side fix from #1117.
