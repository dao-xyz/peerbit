# @peerbit/indexer-rust

Experimental Rust-backed indexer package for the Peerbit indexer interface.

## Current shape

- Rust/WASM owns the ordered key/value entry store.
- `src/planner.rs` contains the first native query planner core: typed query AST, bitmap set operations, scalar sorting, batch updates, and vector ranking.
- The TypeScript adapter implements the existing `@peerbit/indexer-interface` query semantics.
- Persistent snapshots work on Node.js through the native filesystem and in browsers through OPFS.
- The same conformance suite runs in transient and persistent mode across Node.js, browser, and webworker targets.

This package does not use SQLite, `libsqlite3-sys`, libSQL, or Turso yet. It is the package and persistence boundary for moving more index work native without coupling that decision to the higher Peerbit modules.

## Performance direction

The current package benchmark is mostly a baseline. Point lookups and writes are cheap, but scan-heavy field queries still run through the TypeScript evaluator until the adapter is wired into the native planner. Real improvements for shared-log style multi-field/range queries require moving query planning and secondary indexes into `src/planner.rs` or a backend behind the same typed planner boundary.
