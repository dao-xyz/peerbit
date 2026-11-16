---
"@peerbit/indexer-sqlite3": patch
---

Keep the ORDER BY when a query skips an OR arm, or a root table of a polymorphic schema, that lacks a queried field. A skipped arm could drop the ORDER BY (always with one root table), so `all()` and every `next()` page came back unsorted, and paged reads lost their default id order. The ORDER BY now uses the sort column of the first SELECT that remains in the query. Queries that skip no arm build the same SQL as before, and missing-field errors are unchanged.

This does not fix ordering between rows of different root tables: when the selected fields include the sort key, the ORDER BY binds to the first participating table's column and the other tables' rows sort as NULL, as they already did in queries that skip no arm. The id is always selected, so this also applies to the default id order of paged reads.
