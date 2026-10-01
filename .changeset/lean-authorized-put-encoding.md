---
"@peerbit/document": patch
---

Avoid eager operation-envelope encoding on the compatibility put path, including JavaScript-authorized writes. Reuse dedicated payload buffers while preserving pooled-buffer isolation, signed bytes, authorization and durable write semantics.
