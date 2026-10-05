---
"@peerbit/document": patch
---

Prevent blocking remote-query readiness from losing replication events during cover lookups. Observe cancellation and bound the initial lookup by the configured readiness deadline, without changing timeout defaults or the candidate-only readiness guarantee. Handle eager iterator warmup rejections before the first read without suppressing errors returned to the reader.
