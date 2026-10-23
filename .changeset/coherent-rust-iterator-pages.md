---
"@peerbit/indexer-rust": patch
---

Keep synchronous native iterator pages within one `next()` or `pending()` call so queued mutations cannot shift internal OFFSET pages and skip results. Failed reads no longer consume unreturned rows or advance the ordinary cursor when cloning fails. Iterators still observe mutations between public calls.
