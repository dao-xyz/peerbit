---
"@peerbit/document": patch
---

Re-export the existing `EntryType` enum from `@peerbit/document` so callers can use `EntryType.CUT` in put metadata without importing `@peerbit/log` separately. Entry encoding and behavior are unchanged.
