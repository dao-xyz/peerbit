---
"@peerbit/shared-log": patch
---

Keep global synchronization work limits charged until non-abortable lookups and response shipments settle, while releasing disconnected peers' per-generation quota and lifecycle state immediately.
