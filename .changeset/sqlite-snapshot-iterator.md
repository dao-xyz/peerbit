---
"@peerbit/indexer-sqlite3": patch
---

SQLite iterators no longer page with OFFSET. An iterator reads its search in the search's order with the primary key as the tiebreak: it lists the keys of the matching rows once and reads each page's rows by key. When rows change between two calls, the next call re-checks only the changed rows, instead of reading every earlier row again on each page. Each call (`next`, `pending`, `all`) runs in one database admission, so a delete that was queued before the next page can no longer make that page skip a live row. Also:

- `done()` is true right after the call that returns the last row, without one more empty call;
- `next(Infinity)` works on a fresh iterator (it threw "datatype mismatch");
- a search over several root tables (a polymorphic schema) is ordered by key across the tables, and a key present in more than one of them is returned once.

The mutable iterator contract is unchanged: rows inserted or moved behind the cursor come at the front of the next call, an exhausted iterator sees later writes, and `markYielded`, `pending()` and `close()` behave as before.
