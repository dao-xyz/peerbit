# @peerbit/native-backbone

Experimental native owner for Peerbit write transactions.

This package is intentionally internal-facing while the native write path is being
fused. It owns the native lower-log graph, native log block store, and shared-log
resident coordinate state in one Rust object so higher layers can move toward a
single `JS -> native -> compact facts` transaction boundary.

## Coordinate and document-fact WAL persistence

Native shared-log coordinates, document values, and document signer facts can be
persisted through write-through or buffered WALs. Write-through flushes every
append and is the strictest persistence mode. Buffered WALs batch bytes and flush
on their pending-byte threshold, explicit flush, checkpoint, or close, which is
the high-throughput mode for strict native document writes.

Use `createBufferedNativeBackboneCoordinatePersistence(store)` with OPFS, memory,
or custom stores, and `createBufferedNativeBackboneNodeCoordinatePersistence(dir)`
for the Node adapter. Persistence flush and replay are enabled by default;
checkpoint compaction is not.

The built-in Node adapter supports explicit crash-safe checkpointing when its
filesystem implementation provides atomic replacement. Opt in by setting
`compactMaxJournalBytes` or `compactMaxJournalRecords` on
`NativeBackboneNodeCoordinatePersistence` or
`createBufferedNativeBackboneNodeCoordinatePersistence`:

```ts
const persistence = createBufferedNativeBackboneNodeCoordinatePersistence(
	programDirectory,
	{ compactMaxJournalBytes: 64 * 1024 * 1024 },
);
```

Crossing either configured threshold durably publishes one versioned, checksummed
checkpoint for all three fact sets, switches between fixed A/B WAL generations,
and records an atomic replay high-water mark before covered WAL data is retired.
Checkpointing is serialized with flushes, so recovery chooses a fully published
generation instead of combining partially replaced snapshots.

Memory and OPFS persistence, along with custom stores that do not implement the
atomic replacement, removal, and durability capabilities, continue to reject
these threshold options. They retain ordinary WAL flush and replay behavior.
There is intentionally no automatic checkpoint threshold, including for the
Node helpers.

The checkpoint bounds only the coordinate, document-value, and document-signer
WALs. The entry-block WAL has a separate owner and remains out of scope, so this
feature does not by itself bound every file in a program directory.

Existing directories using the legacy snapshots and WALs open without a manual
migration. Their first checkpoint becomes forward-only once its staged generation
and downgrade sentinel are durable. If publication is interrupted after that
boundary, current recovery validates and promotes the staged generation; before
the boundary, the complete legacy WAL remains authoritative. The sentinel makes a
pre-checkpoint package fail closed rather than replay stale legacy files. Rolling
back therefore requires restoring a pre-migration copy of the directory. This is
an opt-in, forward-only internal persistence-format migration. It removes no
public API and does not alter the peer-to-peer wire format; it does add observable
Node checkpoint capabilities and downgrade behavior.

These fact WALs support clean stop/restart. They are not the recovery authority
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

Phase one retains this separate durability-transaction journal and pins genesis
as its scan base while newer A/B checkpoints prove staging coverage. Compaction
of that journal is deliberately deferred until a later phase can switch its
generation and scan base together; the coordinate/document-fact checkpointing
described above does not alter it. Memory storage is a non-crash-safe reference
adapter, and OPFS remains unsupported until its lifetime lease and
worker-termination barriers have a dedicated conformance gate.
