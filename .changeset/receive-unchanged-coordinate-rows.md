---
"@peerbit/shared-log": patch
---

A receive no longer writes a head's coordinate row again when an identical row is already stored and none of the head's predecessor rows is. This happens when repair sends heads again while an earlier copy of them is still being joined. Rows that are missing or differ are written as before, and with a native backbone or the Rust indexer (`@peerbit/indexer-rust`) every row is still written.
