---
"@peerbit/indexer-sqlite3": patch
---

Select matching root IDs before reconstructing arrays in grouped, root-sorted
queries with direct-child predicates. This avoids multiplying hydrated elements
by matching predicate elements while retaining the complete predicate and outer
pagination. Deeper joins and child-sorted queries keep their existing path.
