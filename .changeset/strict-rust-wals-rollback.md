---
"@peerbit/any-store-rust": patch
---

Make Node and OPFS journal appends crash-safe across short writes by rolling rejected records back to their original offset and poisoning the open store after journal failure until a verified reopen. Strict mutations already queued behind the failed append now reject with the same sticky first error before changing memory or the WAL.

Repair torn journals by durably truncating only their verified prefix instead of implicitly rewriting a checkpoint. Strict stores now remain WAL-backed even when close compaction or a threshold is forced, and OPFS checkpoint writes loop until every byte is written before publishing their manifest.

Only a structurally incomplete final frame is treated as a recoverable crash tail. A complete frame with invalid magic, checksum, or payload now fails closed without applying a partial replay or rewriting the WAL, and failed-open persistence handles are closed before a retry.
