---
"@peerbit/indexer-sqlite3": patch
---

Keep each SQLite iterator read or pending-count rescan under one database admission so concurrent writes cannot shift internal OFFSET pages and silently skip results. Iterators still observe changes between calls, and failed rescans do not consume results they never returned.
