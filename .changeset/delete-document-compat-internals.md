---
"@peerbit/document": major
---

Delete the retired internal document compatibility plumbing (B12 stage 5): the always-`undefined` residual `compatibility` fields on `Documents` and `DocumentIndex` and the `compatibility` member of the exported `OpenOptions` type are removed (the open option itself was removed and rejects with `DocumentCompatibilityRetiredError` since the B12 major), together with the compat-6 `PutWithKeyOperation` ENCODE branch at the write path and the legacy query/iteration coercion branches. Writes always encode `PutOperation`; persisted data from old compatibility-6 stores (tag-0 puts, tag-2 deletes) remains decodable and applicable forever — the operation tombstone classes, their variant registrations and delete-key coercions are unchanged and now pinned encode-dead by a source ratchet.
