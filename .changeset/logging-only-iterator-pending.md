---
"@peerbit/document": patch
---

Stop counting an iterator's pending results only to log them. When a query iterator returned its next page, the document store also asked the index how many results were left, only for the `iterate:next` debug line, even with logging off. On the SQLite indexer, once anything has been written to the index since the iterator opened, that count reads every matching row again, so each page after a write read the whole result set one extra time. The count is now taken only while the `peerbit:document:index:iterate` logger is enabled, and the line is unchanged. The pending count that remote peers receive (`kept`) is still computed after every page, so results and counts do not change.
