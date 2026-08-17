---
"@peerbit/shared-log": patch
---

Add an internal, unwired durable pending-visit bridge that mirrors one canonical signed custody manifest before effects and advances the scan cursor only after a paired strict source receipt or an exact side-effect-free skip. This adds no prune, release, or production transfer authority.
