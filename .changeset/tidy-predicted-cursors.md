---
"@peerbit/document": patch
---

Retire remote cursors when ordinary unconsumed query predictions are replaced. Preserve already-consumed cursors, same-ID duplicates, and active push or keepalive snapshots, and bound best-effort cleanup without delaying acceptance of the replacement.
