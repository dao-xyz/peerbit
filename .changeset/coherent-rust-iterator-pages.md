---
"@peerbit/indexer-rust": patch
---

Read all OFFSET pages of one iterator `next()` or `pending()` call without awaiting between them. A mutation queued during the call could land between two page reads and shift the rows under the offset, so `next()` could skip a row and `pending()` could miss or double count one. A call whose page read or result cloning throws no longer consumes rows it did not return; before, a failed later page lost the earlier pages of the call, and a cloning failure on the ordinary path advanced the cursor past its rows. Iterators still see mutations committed between calls.
