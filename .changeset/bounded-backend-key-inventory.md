---
"@peerbit/indexer-sqlite3": patch
"@peerbit/indexer-rust": patch
---

Implement bounded key-only inventory pages through the existing `scanKeyPrimitives` capability. Scans invalidate on admitted writes or lifecycle changes and release cursor resources on completion, cancellation, or failure. SQLite uses primary-key seeks; Rust visits native keys directly without decoding documents or retaining a whole-inventory key set. SQLite multi-root schemas and non-authoritative Rust backbone mirrors remain unsupported. This is an inventory primitive, not a replication-completeness or durability guarantee.
