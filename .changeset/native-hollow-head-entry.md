---
"@peerbit/log": patch
"@peerbit/shared-log": patch
---

Fix native-vs-default parity for live-replicated head entries.

On the native shared-log path a live-replicated HEAD is cached in the entry index as a lazy `PreparedRawExchangeEntry` wrapper (it keeps the block bytes in wasm and only exposes generic getters). Its `_meta`/`_payload`/`_signatures` fields stay undefined, so a read of that head returned a hollow object: field consumers saw `undefined`, and because `EntryV0.equals` is gated on `other instanceof EntryV0`, comparisons were asymmetric (`jsEntry.equals(head)` was `false` while `head.equals(jsEntry)` was `true`). The default backend caches heads as full `EntryV0`, so this divergence was native-only. The underlying block was always fully present and decodable — only the cached JS object was hollow.

- `@peerbit/log`: add a generic `Entry.toMaterialized()` capability (a no-op returning `this` on the concrete `EntryV0`, so the default backend is unchanged). The entry index calls it at the read/resolve cache boundary and writes the materialized entry back into the cache, so a resolved head is always a full entry and the hot head is not re-decoded on subsequent reads.
- `@peerbit/shared-log`: `PreparedRawExchangeEntry` overrides `toMaterialized()` to decode itself into its full `EntryV0`.

Materialization happens only at the read boundary; the wire/sync fusion path caches heads via `put` but never resolves them, so it stays lazy (no block-byte materialization on send/receive).
