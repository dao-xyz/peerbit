---
"@peerbit/indexer-sqlite3": patch
---

Commit an index value and its nested rows in one SQLite transaction. A failed insert or replacement rolls back completely, so a replacement leaves the previous value intact, and the rollback waits for nested writes that have already started. A value and its nested rows now share one durable commit instead of one commit per nested write; the configured durability mode is unchanged.

The crash-safe durability barrier now runs after the writes already queued on the connection. Its checkpoint fails inside an open write transaction, which every put now holds while it writes its nested rows.
