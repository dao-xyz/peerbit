---
"@peerbit/document": patch
---

Propagate errors processing received remote continuation pages when `remote.throwOnMissing` is true, instead of silently returning an incomplete or empty batch. Default and explicit best-effort behavior remain unchanged.
