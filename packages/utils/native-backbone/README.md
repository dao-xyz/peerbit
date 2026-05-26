# @peerbit/native-backbone

Experimental native owner for Peerbit write transactions.

This package is intentionally internal-facing while the native write path is being
fused. It owns the native lower-log graph, native log block store, and shared-log
resident coordinate state in one Rust object so higher layers can move toward a
single `JS -> native -> compact facts` transaction boundary.

## Coordinate WAL persistence

Native shared-log coordinates can be persisted through a write-through WAL or a
buffered WAL. Write-through flushes every append and is the strictest persistence
mode. Buffered WAL batches coordinate bytes and flushes on threshold, explicit
flush, compact, or close, which is the high-throughput mode for strict native
document writes.

Use `createBufferedNativeBackboneCoordinatePersistence(store)` with OPFS, memory,
or custom stores, and `createBufferedNativeBackboneNodeCoordinatePersistence(dir)`
for the Node adapter.

Buffered helpers also install a bounded checkpoint policy by default. When the
coordinate journal reaches the checkpoint threshold, the adapter writes a compact
snapshot and removes the replay WAL. This keeps high-throughput strict-native
document writes from trading append speed for unbounded restart replay cost.
