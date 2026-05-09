# @peerbit/log-rust

Rust-backed log graph indexing primitives for Peerbit.

This package is intentionally below `@peerbit/log`: it owns hot graph state such
as heads and `next` adjacency, while the TypeScript log keeps public API,
storage, signing, and orchestration until the native boundary is proven.
