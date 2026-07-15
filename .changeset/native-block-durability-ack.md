---
"@peerbit/log": patch
"@peerbit/shared-log": patch
"@peerbit/blocks": patch
"@peerbit/document": patch
"@peerbit/native-backbone": patch
"@peerbit/indexer-interface": patch
"@peerbit/indexer-simple": patch
"@peerbit/indexer-rust": patch
"@peerbit/indexer-sqlite3": patch
---

Require native local-append acknowledgements to wait for their durable block mirror. Durable mirror failures now raise a typed, unsafe-to-retry error and poison further mutations on that program instance.

Propagate native trim deletions to the durable mirror with staged tombstones and same-CID generation guards. Failed old-block deletion remains retryable cleanup debt so a fully durable replacement can publish one coherent new index/head state, while ownership-aware compensation preserves acknowledged, restored, shared, and otherwise uncertain content-addressed bytes. Retained orphans remain part of physical store size and therefore continue to count toward hard storage budgets until cleanup succeeds.

Publish strict native lower-index facts through an operation-scoped generation token before consuming trim results. An index write failure now retracts only that append, cancels its deferred publication, and restores the authoritative graph, document, and coordinate state without erasing concurrent same-CID facts.

Serialize lower-log close and drop with native append finalizers, retry incomplete rollback/index teardown stages, and erase blocks only after acknowledgements or compensation settle. Uncontended native hash mutation leases retain the synchronous commit-only fast path without recursive public bookkeeping.

Close and drop now fail before changing lifecycle state while an internal or user mutation callback is still running; callers must retry after that callback completes.

Advertise whether an indexer preserves rows across stop/start so ordinary close avoids duplicating every block hash for persistent or data-preserving backends, while destructive and unknown backends retain the exact drop set before stopping.

Persist strict native recovery intent in alternating checksummed generations so an interrupted journal write cannot erase the last recoverable state. A committed lower marker remains authoritative, later mutations are blocked until failed intent retirement is recovered, and committed trim block cleanup resumes from the durable intent after restart.

Make native coordinate, document, and signer acknowledgements wait for an explicit physical durability barrier, retain pending records after failures, reject torn or corrupt recovered WAL tails, and fail closed after ambiguous or short appends. Node barriers require `FileHandle.sync`; OPFS barriers require sync-access `flush`; buffered/custom adapters without the capability fail before a durable acknowledgement.

Native persistence drop is now tombstone-backed and resumable, with explicit underlying-removal and terminal-drop capabilities checked before lower state is mutated. Hydration, recovery, validation, and native loads share one lifecycle queue so close waits and drop rejects before erasure. Ordinary custom close is never invoked after terminal drop, and unsafe custom compaction thresholds are rejected even on memory-only nodes. Built-in snapshot compaction remains disabled until it can use a crash-atomic generation protocol.
