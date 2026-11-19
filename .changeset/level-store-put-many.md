---
"@peerbit/any-store": patch
---

LevelStore gains putMany, which writes many keys as one synced LevelDB write batch (split at 4 MiB of values) instead of one synced write per key. Block stores on LevelDB use it when they store several blocks at once.
