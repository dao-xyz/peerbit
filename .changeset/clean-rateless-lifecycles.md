---
"@peerbit/shared-log": patch
---

Isolate Rateless IBLT synchronization by target and sender, bound process admissions, sequence gaps, symbol work, response inspection, and active response delivery, fall back to Simple sync on exhaustion or timeout, and reliably release native encoder and decoder resources. Oversized coded-symbol batches are rejected during deserialization before allocation.
