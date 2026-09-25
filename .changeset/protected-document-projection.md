---
"@peerbit/trusted-network": patch
---

Prepare an internal immutable-document projection with fresh authorization replay
and crash-safe replacement of document rows and their policy/fence watermark.
The adapter is excluded from published artifacts; public V1 semantics, mutable
Documents conflict handling and history retention are unchanged.
