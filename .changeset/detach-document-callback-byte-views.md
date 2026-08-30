---
"@peerbit/document": patch
---

Detach verified document payload bytes and delete keys before exposing them to
custom ID revalidation, `canPerform`, `keep`, and replication-domain callbacks.
Custom ID revalidation and custom `canPerform` now receive independently decoded,
signed-equivalent values. This tightens local `canPerform`: callback mutations
can no longer change the persisted operation or projected document.

Callbacks that receive an `Entry` now see an interface-compatible,
payload-byte-isolated view. Its object identity differs from the canonical
`Entry`, while `instanceof`, constructor, and `valueOf()` self semantics are
preserved. Wire/storage encodings and API type signatures are unchanged.
