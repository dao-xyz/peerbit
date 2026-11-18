---
"@peerbit/indexer-sqlite3": patch
---

Add an internal change journal to the SQLite index, for the paged iterators that will re-check only the rows changed between two calls. While a reader is registered, it records the root table and primary key of every committed put, putBatch chunk, ordered write session row and delete, and drops the keys of writes that roll back. Readers are told to rebase when they fall behind its bounded window or when the index stops and starts. No behaviour change: nothing registers a reader yet, so nothing is recorded, and query results, counts and write paths are unchanged.
