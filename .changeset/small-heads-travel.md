---
"@peerbit/shared-log": patch
---

Bound count-based lookahead for hash-resolved full entries and raw JS blocks while preserving order and deduplication when a batch falls back. Caller-owned entries and individual entry size remain outside this count bound.
