---
"@peerbit/document": patch
---

Propagate errors processing continuation pages, received from a remote peer or read from the local index, when `remote.throwOnMissing` is true, instead of silently returning an incomplete or empty batch. Default and explicit best-effort behavior remain unchanged.

Under the same option, reject query-level `NoAccess` responses on initial and continuation pages with the newly exported `AccessDeniedError`, from `get()`, `search()` and iterators. Its `peers` holds the hash of the first denying peer processed, not necessarily every denying peer. Callers that already set `throwOnMissing: true` now get this rejection where a denied query used to resolve with an empty or partial result, including when other peers answered. With `replicate: true`, rows from peers processed before the denial may already be stored locally. This does not change per-row filtering, cursor-loss handling or wire formats.

Queries that a replicator joining later triggers, for `get(id, { waitFor })` and for iterators with a keep-open `remote.wait`, no longer leak their failures as unhandled promise rejections. Under `throwOnMissing: true` such a failure rejects `get()`, or rejects the iterator's next read and every read after it until `close()`; otherwise it is logged. Under the option, a failure merging a page that a peer pushes to an iterator with push `updates`, such as a predicted `NoAccess`, now rejects the same way instead of only being logged.
