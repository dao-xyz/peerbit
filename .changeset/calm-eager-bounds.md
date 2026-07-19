---
"@peerbit/blocks": patch
"@peerbit/network-rust": patch
"@peerbit/shared-log": patch
"@peerbit/stream": patch
---

Bound unsolicited raw direct-block responses by retained and pending entries
and bytes, copy only the block range, and require CID integrity validation
before cache admission or provider learning. Decoding codecs such as DAG-CBOR
remain on the requested-read path and are never eagerly materialized. Add
matching TypeScript/rust-core accounting and TTL behavior plus local telemetry.
SharedLog now leaves eager retention off unless callers explicitly enable it;
the requested-read path and wire protocol are unchanged.
