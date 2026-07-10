---
"@peerbit/indexer-rust": patch
"@peerbit/indexer-sqlite3": patch
---

Keep paginated sorted iterators complete and duplicate-free when indexed rows are inserted, updated, or deleted between pages.

After observing a mutation, an iterator keeps the ids it has already yielded and rescans the current result set. This costs O(N) query work per subsequent page and O(yielded ids) memory; consuming a large changing result set in many small pages can therefore approach O(N²) work.
