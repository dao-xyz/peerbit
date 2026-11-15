---
"@peerbit/shared-log": patch
---

Keep replication-confirmation transport work bounded when an RPC ignores cancellation. Logical timeout, close, or session replacement no longer releases the physical query's charge before its send settles. Permit one successor session, authenticated recovery, or committed revision while preserving exact confirmation binding and fail-closed limits.
