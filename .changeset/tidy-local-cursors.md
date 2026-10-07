---
"@peerbit/document": patch
---

Keep local document queries resumable when a page has no resolvable documents but the cursor still has entries. Preserve remote fallback and close exhausted cursors normally.
