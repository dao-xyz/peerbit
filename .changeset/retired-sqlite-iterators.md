---
"@peerbit/indexer-sqlite3": patch
---

Keep explicitly closed iterators terminal while a page or pending count is in flight. Discard late results and stop further page reads at cooperative await boundaries; already-admitted database work still settles under its existing barrier. Preserve normal iteration, query errors on open iterators, and existing pagination semantics.
