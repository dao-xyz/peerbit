---
"@peerbit/log": patch
"@peerbit/shared-log": patch
---

Complete receipt-authorizing coordinate metadata for already-admitted entries even when the remaining receive is cancelled. Own native append mutations before preparation and retain ownership through coordinate completion or rollback. Serialize overlapping coordinate writes and deletes, reject stale rollback owners, and fail closed when committed metadata completion is uncertain. Keep disjoint exact-hash writes concurrent, serialize native preparations with unknown mutation sets, and release ownership before change callbacks.

Bound generic coordinate-deletion queries while retaining one mutation owner across all chunks, and avoid repeating completed cleanup after a newer mutation has promoted the same head.
