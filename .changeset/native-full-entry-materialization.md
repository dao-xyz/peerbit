---
"@peerbit/log": patch
---

Materialize storage-hollow native local entries when callers request a full log entry.

Native local append can keep payload and signature bytes exclusively in the native block store while caching a lightweight concrete `EntryV0` in JavaScript. A later `Log.get()` cache hit returned that hollow object because the existing read-boundary materialization only recognized the lazy raw-exchange wrapper. Payload reads and serialization could therefore fail with `Missing data` even though the complete block was present.

The entry index now detects this concrete hollow state without serializing healthy entries and routes it through the existing block-store resolution path. Batched reads continue to use `getMany`, the materialized entry replaces the cache value, and local-origin metadata is preserved. Raw-exchange send and receive remain lazy because they do not resolve entries through this full-read boundary.
