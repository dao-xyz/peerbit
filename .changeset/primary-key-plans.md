---
"@peerbit/indexer-sqlite3": patch
---

Create no planner indexes for a query that matches the primary key by equality. The primary key already reaches the row, so such indexes only added storage and write cost: a delete of one named version (`DocumentIndex.delIfHead`) created two composite indexes on the document table.
