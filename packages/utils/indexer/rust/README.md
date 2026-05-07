# @peerbit/indexer-rust

Experimental Rust-backed indexer package for the Peerbit indexer interface.

## Current shape

- Rust/WASM owns the ordered key/value entry store.
- The TypeScript adapter implements the existing `@peerbit/indexer-interface` query semantics.
- Persistent snapshots work on Node.js through the native filesystem and in browsers through OPFS.
- The same conformance suite runs in transient and persistent mode across Node.js, browser, and webworker targets.

This package does not use SQLite, `libsqlite3-sys`, libSQL, or Turso yet. It is the package and persistence boundary for moving more index work native without coupling that decision to the higher Peerbit modules.

## Performance direction

The current benchmark is mostly a baseline. Point lookups and writes are cheap, but scan-heavy field queries still run through the TypeScript evaluator. Real improvements for shared-log style multi-field/range queries require moving the query planner and secondary indexes into the native side, either with a Rust-native SQL engine such as Turso or a purpose-built Rust index planner.
