---
"@peerbit/indexer-sqlite3": patch
---

Preserve requested sorting when a query skips an unavailable OR arm or root schema. Use the sort alias from a participating SELECT without changing missing-field query behavior.
