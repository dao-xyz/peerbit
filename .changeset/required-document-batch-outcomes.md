---
"@peerbit/document": minor
---

Add opt-in required putMany batching with captured invocation inputs, no sequential fallback, and immutable input-index/hash commit evidence on failure. Distinguish pre-append rejection, confirmed local append, and indeterminate recovery-required outcomes independently of remote persisted receipts; this is not an all-or-none transaction guarantee.
