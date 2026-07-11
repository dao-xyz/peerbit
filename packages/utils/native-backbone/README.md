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
for the Node adapter. The lower-level
`createNativeBackboneCoordinatePersistence({ store, buffered: true })` config
uses the same bounded checkpoint default.

Buffered helpers also install a bounded checkpoint policy by default. When the
coordinate journal reaches the checkpoint threshold, the adapter writes a compact
snapshot and removes the replay WAL. This keeps high-throughput strict-native
document writes from trading append speed for unbounded restart replay cost.

The coordinate WAL supports clean stop/restart. It is not the recovery authority
for a crash-atomic append across blocks, graph state, heads, coordinates,
documents, and signer facts.

## Local durability transaction primitives

The package also exposes the first, intentionally unwired building blocks for
the proposed local crash-safe transaction protocol. This is an on-disk recovery
format for one program directory; it is not a peer-to-peer wire protocol.

- a versioned, checksummed native journal codec that distinguishes a
  structurally incomplete final frame from complete corruption;
- transaction-private block staging and typed strict-barrier receipts;
- immutable checkpoint generations with an A/B manifest switch; and
- a Node directory lease backed by an OS-held LevelDB lock and a persistent
  fencing epoch.

These APIs do not change current append or open behavior. The Node filesystem
adapter owns its official Rust codec and crash-released directory lease, and is
the first supported strict physical backend on filesystems that support file and
directory sync. Its first open creates a synced genesis only in an otherwise
empty program directory; a nonempty legacy directory fails with
`NativeDurabilityMigrationRequiredError` and is never adopted implicitly.

Phase one retains the full journal and pins genesis as its scan base while newer
A/B checkpoints prove staging coverage. Journal compaction is deliberately
deferred until a later phase can switch the journal generation and its scan base
together. Memory storage is a non-crash-safe reference adapter, and OPFS remains
unsupported until its lifetime lease and worker-termination barriers have a
dedicated conformance gate.
