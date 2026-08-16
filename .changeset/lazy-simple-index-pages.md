---
"@peerbit/indexer-simple": patch
---

Page unsorted in-memory index iteration from a bounded forward cursor instead
of materializing every query match on the first `next()` call. Sorted iteration
keeps its eager snapshot behavior; unsorted iteration has finite weak-snapshot
semantics when rows mutate after consumption starts.
