---
"@peerbit/indexer-sqlite3": patch
---

Load SQLite's initialization module as a published browser asset so Vite
consumers no longer resolve the unused and unpublished `sqlite3-worker1.js`
asset from the upstream worker-promiser side effect.
