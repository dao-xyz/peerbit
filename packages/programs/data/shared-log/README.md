# Shared log

A log that can be replicated

## Replication status

`SharedLog.getReplicationStatus()` returns a detached, local snapshot of the
current storage use, minimum range coverage, and active-replicator count. The
local `replication:status` event is emitted for the first snapshot and then only
when its ordered reason set changes, including recovery to an empty reason set.
Explicit calls always perform a one-shot measurement. Automatic refreshes run
only while at least one `replication:status` listener is registered, avoiding
coverage scans for applications that do not consume this telemetry. To start
monitoring reliably, register the listener first and then call
`getReplicationStatus()` once to seed the initial snapshot.

This API is advisory telemetry. It is not persisted, sent over the replication
protocol, forwarded by the canonical SharedLog proxy, or used to make placement,
repair, and pruning decisions.

`replicate.limits.storage` remains a **soft objective** for the adaptive PID
controller, not a hard storage quota. Coverage pressure, discrete entry sizes,
and delayed pruning can make actual use exceed the configured value. The
`storage-objective-exceeded` reason reports that condition; it does not enforce
the objective. `range-coverage-underfilled` reports minimum ring coverage below
`replicas.min`, while `default-replica-target-unattainable` reports fewer active
replicators than that target.
