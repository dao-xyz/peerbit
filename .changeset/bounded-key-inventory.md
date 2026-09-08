---
"@peerbit/indexer-interface": minor
"@peerbit/indexer-simple": minor
"@peerbit/indexer-cache": minor
---

Add optional bounded raw-key inventory scanning to the in-memory index, with direct capability forwarding through the cache wrapper. Scans fail closed on observed mutation, partial write failure, cancellation, or lifecycle replacement instead of treating mutable query pages as a complete inventory. Existing query iteration is unchanged.

This inventories one backend owner's canonical key primitives, not typed keys, document values, durable log closure, or a revocation checkpoint. Cursor bookkeeping and page item counts are bounded; key sizes and caller retention are not. SQLite, Rust, and custom engines remain unsupported unless they explicitly implement the capability. Callers must close abandoned scans and must not emulate the capability with ordinary iteration.
