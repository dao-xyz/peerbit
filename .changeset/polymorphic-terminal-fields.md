---
"@peerbit/indexer-sqlite3": patch
---

Resolve terminal fields against their declaring schema variant, excluding unrelated flattened child fields, and bind explicit nested inline-field queries to their physical table. Preserve missing-field validation.
