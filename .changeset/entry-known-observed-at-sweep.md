---
"@peerbit/shared-log": patch
---

Stop the entry recency map from growing without bound. `_entryKnownPeerObservedAt` records when each peer was last seen to know an entry, but it is only ever read through a 30-second window, and nothing removed rows on the entry dimension — so a node accumulated one row per (gossiped hash, peer) for its whole uptime, including hashes it never held. Rows past the longest read horizon are now dropped on the write path. Behaviour is unchanged: the sole reader already treats an over-age row and an absent row identically.
