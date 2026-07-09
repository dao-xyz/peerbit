---
"@peerbit/document": patch
---

Fix `del` throwing "Missing data" on a peer with a native block store but a Documents store in the default `mode: "auto"` (not `mode: "native"`).

On such a peer the delete read-back path (`handleChanges` -> `getAppendOperation`) resolved the prior put's operation via the in-memory JS entry (`Entry.getPayloadValue`). Under a native block store the entry materialized in the entry index is a hollow shell whose payload bytes were never loaded onto the JS object, so `getPayloadValue` threw `Error("Missing data")` even though the block itself is present in the store.

Auto mode now falls back to the storage-bytes / block-store read path when (and only when) the in-memory payload is unavailable: it recovers the entry's raw block from the block store by hash and extracts the plain operation payload from those bytes. The pure-JS backend is unchanged — there `getPayloadValue` succeeds and the fallback never runs. Document mode semantics and the `isNativeMode()` gate are untouched.
