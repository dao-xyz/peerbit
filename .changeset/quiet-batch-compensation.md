---
"@peerbit/log": patch
---

When a multi-entry `appendMany` batch, which `SharedLog.appendMany` also uses, fails partway through writing its rows to the index, the entry index now restores the exact prior rows before it publishes the batch's length, cache or native graph entries. Before, the batch counted and cached every entry first, so after a caught failure the in-memory length and cache included entries that were not on disk. Now the log matches what a reopen reads, and a retry writes each entry once. If restoring the rows fails as well, the batch rejects with an `AggregateError` holding both errors, and the log rejects later writes; `close()` retries the restore.

Entry-index mutations now always take per-hash mutation leases; before, only logs with a native graph and logs opened by SharedLog did. On other logs, deletes of the same entry now run one at a time, and `close()` waits for mutations in flight. A multi-entry append also reads the rows it will change before writing them, which adds a little time per batch.

Unchanged: a one-entry `appendMany` and logs with an `onGidRemoved` hook, which still write row by row and publish length and cache first; the prepared-facts join batch; and batches whose index write a native graph defers or commits itself. This corrects caught errors; it does not make a batch crash-atomic.
