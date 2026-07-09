---
"@peerbit/indexer-rust": patch
"@peerbit/indexer-simple": patch
---

Restart cached indices on reopen (close -> reopen lifecycle fix)

The node-level indexer `Indices` scope is cached per node and outlives a program close. When a program closed it stopped its own indices (state -> "closed") while the scope stayed alive; on reopen, `Indices.init` hit the existing-index early-return branch and handed back the still-stopped index without restarting it. The next synchronous read on open (e.g. shared-log's `replicationIndex.count(...)` / `iterate(...)` during hydrate) then threw `NotStartedError`.

`init`'s existing-index branch now calls `index.start()` (idempotent; no-op when already open) before returning the cached index whenever the scope is open, mirroring the restart the freshly-created path already performs. This matches the sqlite3 backend, which already recovers because its `scope()` restarts cached indices via a start cascade before `init` runs.

Fixes `@peerbit/indexer-rust` (`RustIndices`) and the same latent gap in `@peerbit/indexer-simple` (`HashmapIndices`); sqlite3 was already correct and is unchanged.
