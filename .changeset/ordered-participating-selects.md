---
"@peerbit/indexer-sqlite3": patch
---

Keep the requested sort when a query skips an OR arm, or a root table of a polymorphic schema, that lacks a queried field. Skipping such an arm cleared the ORDER BY, so `all()` and every `next()` page came back unsorted. The ORDER BY now uses the sort column of the first SELECT that remains in the query. Queries that skip no arm build the same SQL as before, and missing-field errors are unchanged.

Ordering between rows of different root tables is unchanged: when the selected fields include the sort key, the ORDER BY binds to the first table's column, and the other tables' rows sort as NULL.
