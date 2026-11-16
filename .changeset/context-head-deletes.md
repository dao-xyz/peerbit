---
"@peerbit/indexer-rust": patch
---

Add `delIdIfContextHead`, which removes a native backbone document only while its context head is the named head. The head is checked when the document is removed: in the same step, or inside the queued write when the index persists to a snapshot file, so a put queued ahead of the delete is checked, not removed. Without a native document context it returns `undefined` and leaves the delete to the caller.
