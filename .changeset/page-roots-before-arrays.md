---
"@peerbit/indexer-sqlite3": patch
---

Reduce repeated array reconstruction during paginated queries by selecting matching root IDs before hydrating their complete values. The optimization applies to single-root, direct-child queries with default primary-key ordering; other query shapes retain their existing path. Pagination, mutable-iterator behavior, and returned array contents are unchanged.
