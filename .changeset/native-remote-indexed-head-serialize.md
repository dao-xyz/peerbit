---
"@peerbit/document": patch
---

Fix a remote `resolve: false` / `SearchRequestIndexed` query timing out (requester `get`/`iterate` returns `undefined`) on a peer with a native block store.

When answering an indexed remote query, the responder embeds the head log entry into the RPC response via `ResultIndexedValue.entries` (a borsh `vec(Entry)` field). Under a native block store, `this._log.log.get(hash)` can return a hollow entry whose payload bytes were never materialized on the JS side; serializing it throws "Trying to serialize a null value to field _data", aborting the whole `Results` serialization so the response is never sent and the non-replicating requester times out.

`DocumentIndex` now routes those head lookups through a `getSerializableHead` helper: it keeps the in-memory entry when it serializes cleanly (the JS case) and, only when serialization would throw, recovers the complete entry from the block store by hash (`Entry.fromMultihash`) so a wire-serializable, joinable entry is sent. The pure-JS backend is unchanged — the serialize probe succeeds and the recovery never runs. This covers all five head sites that feed `entries` (the `processQuery` indexed branch, both `wrapPushResults` sites, and both `drainQueuedResults` sites); the unrelated `resolveDocument` payload path is untouched.
