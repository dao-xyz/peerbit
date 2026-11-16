---
"@peerbit/indexer-sqlite3": patch
---

Reset browser and worker SQLite statements after failed reads or writes so cached statements remain reusable and their prior execution errors do not prevent database shutdown. Preserve the original execution error and report distinct reset failures.
