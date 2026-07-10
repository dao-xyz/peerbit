---
"@peerbit/shared-log": patch
"@peerbit/program": patch
---

Durable persistence for native-backbone nodes: a native node started with a storage directory now survives a clean stop/restart with its replication coordinates and entry blocks intact.

- Auto-derive per-program coordinate persistence from the client `directory` (previously only active if a caller passed `coordinatePersistence` explicitly). Namespaced per program under `<directory>/coordinates/<hex(log.id)>`; Node fs and browser OPFS backends. Backward-compatible: an explicitly-passed config still wins, and memory-only nodes are unchanged.
- Make the native-backbone block store durable via a write-through wrapper over the wasm hot store and a durable `AnyBlockStore` (the same per-program `storage.sublevel("blocks")` the non-native path uses). The wasm store stays cold on open and is repopulated lazily from durable storage as log reads walk the DAG, preserving the resident-coordinate-state fast path. This closes the gap where entry blocks lived only in wasm memory when the native backbone was active, so a native node could not reopen its program after restart.
- `@peerbit/program`: expose the optional `directory` on the `Client` interface (already set by `Peerbit`), so shared-log can derive durable per-program paths type-safely.

Scope note: covers clean stop/restart. Hard-kill crash-consistency (flush ordering across the block store, heads index, and coordinate WAL) is a follow-up.
