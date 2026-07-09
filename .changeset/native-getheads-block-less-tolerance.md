---
"@peerbit/log": patch
"@peerbit/shared-log": patch
---

Tolerate a block-less native graph head in `getHeads(true)` instead of crashing.

The native log graph can list a HEAD whose block is not materialized in the store: pruning a child promotes its (possibly block-less) parent to a head (rust `LogGraphIndex.delete` -> `set_head`, which only consults the graph's entry map, never the block store). Resolving that head in full (`EntryIndex.getHeads(true)`, reached from `SharedLog.startAnnounceReplicating` -> `ensureCurrentHeadCoordinatesIndexed`) threw `Failed to load entry from head with hash: <h>` on the native backbone, where the JS path already tolerates a missing block. This was a hybrid-fleet robustness gap.

- `@peerbit/log`: the native-graph head-resolution path (`EntryIndex.iterateNativeHashes`, the resolve-in-full branch) now defaults `ignoreMissing` to `true`, mirroring `resolveMany`'s own `ignoreMissing` branch and the shallow (`getShallow`) fallback the non-full path already uses. A block-less head is skipped (left non-authoritative, not force-materialized) rather than crashing. Callers that explicitly pass `ignoreMissing: false` still opt out. The change is confined to the native-graph branch and is a no-op for the default (JS) backend, which never enters it.
- `@peerbit/shared-log`: make the native-backbone write-through block store's `has()` consistent with `getMany()`/`hasMany()` by falling back to the durable store on a native (wasm-map) miss, so presence checks and resolves agree. `Blocks.has` is declared `MaybePromise<boolean>`, so returning a promise is contract-compatible.
