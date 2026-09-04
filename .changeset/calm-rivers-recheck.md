---
"@peerbit/shared-log": patch
---

Keep `waitForReplicator` checking authoritative membership after bounded network recovery attempts so a transiently empty transition read cannot strand a ready waiter, and reject promptly with cleanup when the log closes.
