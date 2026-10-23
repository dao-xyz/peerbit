---
"@peerbit/document": patch
---

Propagate errors processing received remote continuation pages when `remote.throwOnMissing` is true, instead of silently returning an incomplete or empty batch. Default and explicit best-effort behavior remain unchanged.

Under the same option, reject query-level `NoAccess` responses on initial and continuation pages with the exported `AccessDeniedError`, including observed denying peer hashes. This does not change per-row filtering, cursor-loss handling or wire formats.
