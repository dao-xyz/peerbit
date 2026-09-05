---
"@peerbit/shared-log": patch
---

Cancel cooperative synchronization work owned by an admitted peer receive generation before draining it during removal or subscription replacement. Keep cancellation scoped to that receive snapshot, preserve exact response authorization rollback, and wait for physical send and decoder work to settle before cleanup. Custom synchronizers remain compatible through an optional receive signal.
