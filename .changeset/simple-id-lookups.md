---
"@peerbit/indexer-simple": patch
---

A query that matches the id exactly together with other predicates now looks up that row and evaluates the other predicates on it alone, instead of evaluating every row. A delete of one named version, such as `DocumentIndex.delIfHead`, no longer scans the whole index.
