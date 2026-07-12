---
"@peerbit/react": patch
---

Use OPFS-backed browser storage even when persistent-storage permission is denied. The permission controls eviction protection, while the explicit `inMemory` option continues to select memory-only storage.
