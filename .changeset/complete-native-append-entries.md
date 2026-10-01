---
"@peerbit/log": patch
---

Preserve the signed native entry image in ordinary append results so returned entries can be verified, serialized, joined and read independently of the source store's lifetime. Initialize their payload encoding and keep the separate commit-only fast path unchanged.
