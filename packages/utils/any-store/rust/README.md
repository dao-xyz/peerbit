# @peerbit/any-store-rust

Rust-backed `AnyStore` implementation for Peerbit.

This package is experimental and opt-in. The first implementation keeps the hot
key/value map in Rust, persists stores through an append-only operation log plus
compacted snapshots where the file backend supports safe replacement, and
preserves the existing `AnyStore` API surface.

## Engines

- `custom-wal` is the default. It keeps the live key/value map in Rust and uses
  a compact binary WAL over a small persistence backend. Node uses `fs` and
  browsers use OPFS sync access handles when the store runs in a dedicated
  worker.
- `redb` is available for transient benchmarking. It proves that redb compiles
  into the same WASM package, but persistent redb storage is intentionally gated
  until a byte-range Node/OPFS backend is implemented.

The OPFS backend is conservative: when the browser does not expose an atomic
move/replace primitive for checkpoints, it keeps the WAL as the source of truth
instead of truncating it behind a potentially torn snapshot.

`RustAnyStore` also exposes `putMany`, `getMany`, and `delMany` as opt-in batch
helpers. They are not part of the shared `AnyStore` interface yet, but they let
callers collapse many KV operations into one Rust/WASM call.
