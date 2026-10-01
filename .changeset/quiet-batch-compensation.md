---
"@peerbit/log": patch
---

Restore exact prior entry metadata when an immediate indexed append batch fails, before publishing its cache, length, or native graph state. Coordinate competing mutations and retain failed compensation for recovery instead of silently continuing with a partial index.

This corrects caught metadata errors; it does not add crash-atomic batching or change buffered/native-committed append semantics.
