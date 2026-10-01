---
"@peerbit/log": patch
"@peerbit/shared-log": patch
"@peerbit/document": patch
---

Add bounded per-put phase timings through the existing `sync.profile` callback, including authorized JavaScript append encoding, signing, storage, indexing and projection. Traces isolate observer failures and settle after the original write, including requested persisted delivery, without changing authorization, durability or wire/storage formats.
