---
"@peerbit/shared-log": patch
---

Compare current head metadata with replication coordinates before loading entry blocks, resolving only heads whose coordinates need repair. Preserve current-head ownership, lifecycle fencing, and persisted-receipt validation. Already-indexed blocks are no longer read for this metadata reconciliation; it is not an integrity scan or a durability proof.
