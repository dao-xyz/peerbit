---
"@peerbit/indexer-rust": patch
---

Preserve encoded native runtime failures instead of masking them with a fallback mutation, while retaining the field-encoder fallback for ordinary bridge extraction rejections. Failed durable restores now reject concurrent initializers, evict the partial index, and allow a clean retry.
