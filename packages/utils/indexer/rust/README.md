# @peerbit/indexer-rust

Experimental Rust-backed indexer package for the Peerbit indexer interface.

## Current shape

- Rust/WASM owns the ordered key/value entry store.
- `src/planner.rs` contains the first native query planner core: typed query AST, bitmap set operations, scalar sorting, batch updates, and vector ranking.
- The TypeScript adapter implements the existing `@peerbit/indexer-interface` query semantics.
- Persistence uses a compacted snapshot plus an incremental operation journal. Node.js writes through the native filesystem and browser builds write through OPFS.
- The same conformance suite runs in transient and persistent mode across Node.js, browser, and webworker targets.

This package does not use SQLite, `libsqlite3-sys`, libSQL, or Turso yet. It is the package and persistence boundary for moving more index work native without coupling that decision to the higher Peerbit modules.

## Performance direction

The current package benchmark is mostly a baseline for product integration. Point lookups and writes are cheap, and supported field queries now route through the native planner. The next performance proof should compare explicit `@peerbit/indexer-rust` usage against simple/sqlite in document and shared-log style workloads.

## Durability model

Persistent indexes keep two primary files per scope/index:

- `index.bin`: compacted, checksummed Borsh snapshot of the current values.
- `index.wal`: append-only operation journal containing checksummed put/delete records.

Each successful persistent `put` or `del` appends its journal record before the operation resolves. Reopening the index loads `index.bin` and replays `index.wal`, so committed writes do not depend on a clean `stop()`. `stop()` compacts the current state back into `index.bin` and removes the journal. Long-running indexes also compact after enough journaled operations to keep startup replay bounded. Compaction writes `index.bin.tmp` first; startup can recover from that temp snapshot if a primary snapshot write is torn before the journal is removed.

The default persistence durability is `normal`, matching the practical SQLite WAL `synchronous=NORMAL` tradeoff: write operations append to the WAL and snapshots are synced during compaction, but individual writes do not force a filesystem sync. Use `create(directory, { persistence: { durability: "strict" } })` when every single write must be synced before its promise resolves.

On Node.js the journal uses filesystem append, with best-effort directory sync around compaction. In browsers it uses OPFS, preferring `createSyncAccessHandle()` when available and falling back to writable files otherwise.
