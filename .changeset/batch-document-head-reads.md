---
"@peerbit/document": patch
"@peerbit/log": patch
---

Batch document result head reads through a new ordered `Log.getMany()` API.

Document search, queued iterator, and pushed-update result construction now resolve each result page's log heads in one block-store batch while preserving result order, duplicate hashes, missing/pruned entries, and resolved-versus-indexed behavior. Full log reads already materialize native entries at the entry-index boundary, so result construction no longer probes serialization or retries the same block through a second decode path.
