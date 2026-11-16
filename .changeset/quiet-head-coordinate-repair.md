---
"@peerbit/shared-log": patch
---

Opening a replicating log, and every later announcement of its replication range, no longer reads every current head's entry block to reconcile replication coordinates. Head hashes now come from the entry index (intersected with the native graph's heads when native heads are on), and only heads without a coordinate are loaded and planned. Lifecycle fencing, stale-coordinate cleanup and persisted-receipt checks are unchanged. This reconciliation is not an integrity scan: blocks of heads that already have a coordinate are neither read nor checked.
