---
"@peerbit/log": patch
"@peerbit/shared-log": patch
"@peerbit/document": patch
---

`Documents.put` reports bounded per-put phase timings through the store's existing `sync.profile` callback. Each traced put ends with one settle event after the call finishes, including any requested persisted delivery; a put that reaches its write path also reports preparation and the path it took. Puts on the compatibility path also time authorization, payload and signable encoding, signing, storage, log indexing, projection and shared-log local processing. Removing `sync.profile` after open stops `documents.put` traces for later puts, and observer failures are isolated. `@peerbit/log` and `@peerbit/shared-log` only carry the internal plumbing: direct `Log.append` and `SharedLog.append` callers see no new events. Authorization, durability and wire/storage formats are unchanged.
