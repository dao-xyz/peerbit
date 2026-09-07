---
"@peerbit/document": patch
---

Apply `canSearch` to remote `SearchRequestIndexed` requests before query processing, closing their bypass of query authorization. The callback receives the actual indexed request, including its replication intent; per-result `canRead` filtering and owner-checked iterator cleanup remain unchanged.

The callback type now explicitly includes `SearchRequestIndexed`. Ordinary callbacks using shared request fields remain source-compatible, but policies that exhaustively check request classes must handle indexed requests explicitly. This is an authorization behavior correction with no persisted-data or wire-format change.
