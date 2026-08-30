---
"@peerbit/any-store-rust": patch
"@peerbit/shared-log": patch
---

Bound strict native entry-block WAL history on POSIX Node with crash-atomic,
legacy-compatible live-state checkpoints and adaptive rewrite thresholds. CUT
heads and other live tombstones remain unchanged.
