# @peerbit/any-store-rust

Rust-backed `AnyStore` implementation for Peerbit.

This package is experimental and opt-in. The first implementation keeps the hot
key/value map in Rust, persists stores through an append-only operation log plus
compacted snapshots where the file backend supports safe replacement, and
preserves the existing `AnyStore` API surface.

The implementation keeps the live key/value map in Rust and uses a compact
binary WAL over a small persistence backend. Node uses `fs` and browsers use
OPFS sync access handles when the store runs in a dedicated worker.

The OPFS backend uses two manifest slots with checksums for checkpoints. A new
snapshot and empty journal are flushed before the inactive manifest slot is
updated, so a torn manifest write falls back to the previous valid checkpoint
instead of shadowing committed WAL records.

Strict Node stores can set `compactMaxJournalBytes` to bound mutation history.
On POSIX Node, the store replaces the WAL with the existing `CLEAR` + live-PUT
journal framing behind file and directory `fsync` barriers. The configured
value is a floor: once the live checkpoint is larger, its size becomes the next
history allowance so growing maps are rewritten geometrically. Checkpoint
construction materializes one additional O(live key/value state) entry array;
it does not scale with discarded history. This option fails closed on Windows
and persistence backends without a proven crash-safe replacement barrier. An
already-over-threshold store can also checkpoint during close. If checkpoint
replacement fails after the triggering WAL record was synced, that mutation and
close reject until reopen, while the synced record remains replayable.

`RustAnyStore` also exposes `putMany`, `getMany`, and `delMany` as opt-in batch
helpers. They are not part of the shared `AnyStore` interface yet, but they let
callers collapse many KV operations into one Rust/WASM call.
