---
"@peerbit/shared-log": patch
---

Drop a redundant full encoder copy per rateless-IBLT sync.

`decoderFromCachedEncoder` cloned the cached encoder before calling
`to_decoder()`, then freed the clone. `to_decoder` takes `&self` and clones the
encoder state internally, so this produced two copies of the same buffer where
one suffices. The encoder is roughly 48 bytes per entry in range, so a
100k-entry range copied ~4.8 MB per StartSync purely to discard it.

Behaviour is unchanged: the borrow already guarantees the cached encoder
survives, which is what the clone was defending.
