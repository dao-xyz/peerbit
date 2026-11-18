---
"@peerbit/indexer-sqlite3": patch
---

SQLite iterators no longer page with OFFSET. Short reads stay as cheap as before: a search over one table reads its first page with one statement and one extra row (which tells whether more remain), and in primary-key order the next pages continue after the last key returned, while nothing is written or marked, for up to 2,048 rows. Every other read lists the keys of the matching rows once, in the search's order with the primary key as the tiebreak, and reads each page's rows by key. After rows change between two calls, each call re-checks only the changed rows (a short read first lists the remaining keys once), instead of reading every earlier row again on each page. Each call (`next`, `pending`, `all`) runs in one database admission, so a delete that was queued before the next page can no longer make that page skip a live row. Also:

- `done()` is true right after the call that returns the last row, without one more empty call;
- `next(Infinity)` works on a fresh iterator (it threw "datatype mismatch");
- a search over several root tables (a polymorphic schema), a sorted `all()` included, is ordered by key across the tables, and a key present in more than one of them is returned once;
- on Node 22, a string with a lone surrogate (an unpaired UTF-16 code unit) is now stored with U+FFFD in its place, as on Node 24 and in the browser. better-sqlite3 stored the surrogate's own three bytes on Node 22, which read back as three U+FFFD, so an iterator could not find such a row again by its key. A row stored that way before this release is no longer found by the original string, as on Node 24.

The mutable iterator contract is unchanged: rows inserted or moved behind the cursor come at the front of the next call, an exhausted iterator sees later writes, and `markYielded`, `pending()` and `close()` behave as before.

A key list costs memory and time per open iterator: each iterator that lists keys holds its own list until it is done, closed or expires. After one write, 20 open iterators over 100,000 rows with 46-character keys held 7.3 MB each (147 MB in all, against 0.5 MB before this release), and listing their keys took about 26 ms each. A budget per database that drops idle key lists is planned.
