---
"@peerbit/document": patch
---

Fold the native-storage delete-path tests into the document `[default, native]` conformance leg now that the del read-back is fixed (#1025). The `test:document-rust-core` allowlist grows from 188 to 192 (adds `can add and delete`, `delete permanently`, `reload after delete`, and `count > approximate > returns approximate count with deletions`). Test-only.
