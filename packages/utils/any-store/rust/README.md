# @peerbit/any-store-rust

Rust-backed `AnyStore` implementation for Peerbit.

This package is experimental and opt-in. The first implementation keeps the hot
key/value map in Rust, persists Node stores through an append-only operation log
plus compacted snapshots, and preserves the existing `AnyStore` API surface.

## Engines

- `custom-wal` is the default. It keeps the live key/value map in Rust and uses
  a compact binary WAL plus snapshots for Node persistence.
- `redb` is available for transient benchmarking. It proves that redb compiles
  into the same WASM package, but persistent redb storage is intentionally gated
  until a byte-range Node/OPFS backend is implemented.

`RustAnyStore` also exposes `putMany`, `getMany`, and `delMany` as opt-in batch
helpers. They are not part of the shared `AnyStore` interface yet, but they let
callers collapse many KV operations into one Rust/WASM call.
