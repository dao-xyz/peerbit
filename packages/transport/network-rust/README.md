# @peerbit/network-rust

Native (wasm-bindgen) wire codec and batched signature verification for the
Peerbit direct-stream envelope (`peerbit_wire` crate).

The crate decodes the borsh envelope from
`@peerbit/stream-interface` (`DataMessage`/`ACK`/`Hello`/`Goodbye`, all
delivery modes, multi-signature headers) byte-identically, applies the
signable-bytes rule (delivery mode and signatures are excluded from the
signed range) and batch-verifies sha256-prehashed Ed25519 signatures with
ed25519-dalek.

`createNativeWire()` returns a module whose `decodeAndVerifyBatch` plugs into
the `nativeWire` option of `@peerbit/stream`'s `DirectStream` (default off;
the TS path is unchanged when disabled).

Golden-vector parity tests against the TS implementation live in `test/`.
