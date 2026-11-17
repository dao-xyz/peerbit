---
"@peerbit/indexer-sqlite3": patch
---

Add internal statement builders that read a search in its order key (the sort terms, then the primary key) without OFFSET: a head page, its continuation after a cursor, a key list, a re-check of listed keys, and rows by key. Nothing uses them yet, so the SQL the index runs and the results it returns are unchanged.
