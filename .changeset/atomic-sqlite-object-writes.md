---
"@peerbit/indexer-sqlite3": patch
---

Commit an index value and its nested rows in one SQLite transaction. Failed inserts and replacements roll back together, and rollback waits for already-started nested writes. This also avoids separate durable commits for each child row without changing the configured durability mode.
