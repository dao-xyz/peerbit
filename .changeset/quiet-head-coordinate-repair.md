---
"@peerbit/shared-log": patch
---

Opening a replicating log, and every later announcement of its replication range, no longer reads every current head's entry block to reconcile replication coordinates. Head hashes now come from a hash-only scan of the entry index, intersected with the native graph's heads when native heads are on, and only heads without a coordinate are loaded and planned. Lifecycle fencing and persisted-receipt checks are unchanged. This reconciliation is not an integrity scan: blocks of heads that already have a coordinate are neither read nor checked.

Behavior changes:

- A head counts as current here only while its entry-index row says so, as in coordinate cleanup and leader planning. The coordinate of a head that only the native graph lists (for example a parent promoted by native pruning) is now cleaned up, even when its block is present.
- Head blocks are no longer read here, so a head whose block is missing no longer makes `open` or `replicate` fail once it has a coordinate.
- With native heads, the repair now flushes deferred entry-index writes before it scans (`EntryIndex.iterate` calls `flushPendingWrites`). That flush is what makes pending heads visible to the scan.
