---
"@peerbit/document": patch
---

fix: prevent stale local results from shadowing newer remote versions of the same document in the search iterator. The iterator dedupes merged results by document id, which let a first-seen stale head (e.g. a local write still propagating) permanently suppress a newer version returned by a remote peer. Now, when a strictly newer version (different head, later modified timestamp) arrives for an id whose result is still buffered, the stale buffered entry is evicted and replaced. Same-version duplicates from multiple sources still dedupe, and results already delivered to the consumer are unaffected. The eviction direction follows the store's conflict rule: newest wins for mutable stores, oldest wins for `immutable: true` stores.
