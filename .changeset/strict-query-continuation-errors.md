---
"@peerbit/document": patch
---

Propagate errors processing received remote continuation pages when `remote.throwOnMissing` is true, instead of silently returning an incomplete or empty batch. Default and explicit best-effort behavior remain unchanged.

Under the same option, reject query-level `NoAccess` responses on initial and continuation pages with the newly exported `AccessDeniedError`. Its `peers` holds the hash of the first denying peer processed, not necessarily every denying peer. Callers that already set `throwOnMissing: true` now get this rejection where a denied query used to resolve with an empty or partial result, including when other peers answered. This does not change per-row filtering, cursor-loss handling or wire formats.
