# Shared-log bounded-state contract

This document defines the architecture boundary for scaling a shared-log
deployment to 100 million global entries. It is a design contract and work
order, not a statement that the current implementation meets the target.

The target is bounded local state. Global history may reach 100 million entries,
but an ordinary storage peer must open, repair, rebalance, and prune work that is
proportional to its assigned shards and active frontier, not global history.

## State ledger

Every locally retained entry can be represented in several stores. A move or
prune is complete only when all stores have reached the same ownership outcome.

| State                        | Current owner                                        | Durable                       | Release boundary                                    | Required bound                                 |
| ---------------------------- | ---------------------------------------------------- | ----------------------------- | --------------------------------------------------- | ---------------------------------------------- |
| Entry and payload blocks     | log block store                                      | yes                           | log prune, trim, clear, or drop                     | assigned local payload bytes                   |
| Shallow log rows             | entry index (`heads` scope)                          | yes                           | entry-index delete, clear, or drop                  | assigned local entries                         |
| Native log graph             | `LogGraphIndex`                                      | no; rebuilt from shallow rows | entry delete or graph clear                         | active frontier plus bounded hot history       |
| Replication coordinates      | shared-log coordinate index or native backbone       | yes when configured           | coordinate delete or index drop                     | assigned local entries/frontier                |
| Application query rows       | application index                                    | backend-dependent             | application deletion policy                         | paged working set, not all global rows in RAM  |
| Replication ranges           | replication index and native range planner           | yes when configured           | ownership mutation or close                         | assigned ranges and active peers               |
| Gid/peer suppression history | `_gidPeersHistory` and native mirrors                | no                            | prune, peer removal, explicit cache reset, or close | live local gids and a bounded retention window |
| Entry/peer and repair memos  | sync, repair, leader, and recently-rebalanced caches | no                            | TTL, size cap, lifecycle rotation, or close         | explicit per-peer and per-session caps         |

The current native graph and Rust query index reconstruct all locally retained
rows on open. Pruning disk blocks alone therefore does not bound startup or RSS:
the shallow row, graph node, coordinate row, and any application row must be
released as part of the same logical ownership transition.

## Ownership move protocol

The planner chooses work; an executor performs durable transfer. Keeping those
roles separate makes plans testable and prevents a controller tick from deleting
data directly.

1. Capture a membership and ownership epoch.
2. Produce a bounded move from one source assignment to one or more destination
   assignments. Include the replication policy, estimated entries and bytes,
   and a resumable cursor.
3. Transfer blocks and index metadata in capped batches with backpressure.
4. Have each destination persist the batch before acknowledging its checkpoint.
   Pubsub delivery or receipt alone is not proof of durable possession.
5. Revalidate membership, epoch, role maturity, and required replica coverage.
6. Commit the new assignment only after the required destinations have durable
   checkpoints covering the move.
7. Prune the source in capped, idempotent batches across every state-ledger row.
8. Persist progress so restart resumes transfer or prune without replaying an
   unrelated shard or losing an acknowledged batch.

Writes racing a move require one explicit policy: dual-write to old and new
owners, or record and replay a source journal after the transfer checkpoint. A
plan is not complete until the chosen policy has crossed its commit boundary.

## Safety invariants

- Never prune based only on a planned leader set. Required replicas must have
  acknowledged durable possession under the current ownership epoch.
- Stale acknowledgements, timers, and repair work from an older lifecycle cannot
  authorize a commit or delete.
- Transfer, commit, and prune operations are idempotent after process failure.
- No operation materializes an unbounded iterator with `all()` or builds an
  unbounded range/entry array. Every scan has a cursor, batch cap, cancellation,
  and backpressure.
- The planner budgets bytes, entries, frontier width, and hot-write rate. Entry
  count alone is not a capacity signal for media workloads.
- Current placement treats a gid/component as indivisible. The planner must
  preserve that rule until a separately versioned sharding semantic exists.
- Cache eviction may cause redundant repair traffic, but cannot authorize prune
  or weaken replica coverage.

## Planner contract

A planner invocation consumes an immutable snapshot:

- ownership and membership epoch;
- mature, eligible storage peers and their advertised capacities;
- current ranges and replica policy;
- per-shard estimates for entries, bytes, frontier width, and write rate;
- transfer concurrency, churn, memory, disk, CPU, and network budgets; and
- restart, catch-up, and availability SLOs.

It returns a deterministic plan with a stable plan id, ordered moves, source and
destination assignments, expected cost, dependencies, and maximum concurrency.
It does not send messages, mutate ranges, or prune data. The executor reports
durable checkpoints and terminal outcomes keyed by plan id and epoch.

The plan must be bounded by the number of peers, ranges, and selected moves. It
must not enumerate global entries. Entry enumeration belongs to the resumable
executor for one selected shard.

## Validation gates

Before claiming the 100-million-entry target:

- declare p50/p95/p99 local entries, bytes, ranges, gids, and frontier rows;
- gate fresh-process RSS and reopen time at those local percentiles;
- verify transfer and prune resume after source, destination, and coordinator
  restarts at every checkpoint;
- verify replica coverage during concurrent writes, owner churn, delayed or
  dropped messages, and partial transfer;
- demonstrate that planner memory and time depend on peers/ranges, not history;
- run 10M and 100M global soaks on dedicated infrastructure with machine-readable
  coverage and resource artifacts; and
- separate non-storage viewers from storage peers in 10k-100k-user simulations.

## Work order

There are now two measured tracks. Lifecycle work for #1286 proceeds in small,
crash-safe WAL checkpointing/compaction slices. Broader custody and ownership
work still begins with the state-ledger invariant below before changing planner
or executor behavior.

1. Land the reproducible resident, persistent, and matched lifecycle censuses
   and establish local entry/RSS/reopen/disk budgets.
2. Add a state-ledger invariant test that proves prune releases every local
   representation and survives restart between stages.
3. Extract a pure, bounded ownership planner behind the contract above while
   retaining current placement semantics.
4. Add the resumable transfer/prune executor and epoch-fenced durable
   acknowledgements.
5. Introduce versioned shardable gid semantics for hot video channels or time
   buckets if the workload cannot bound one component.
6. Progress through distributed 10M and 100M global-entry soaks.

The hardened 100k lifecycle result and a complete diagnostic 1M boundary are now
recorded. The 1M artifact predates the final full-membership checks, but it is
sufficient scaling-boundary evidence: unchanged persistence grows near-linearly
despite bounded live cardinality. Rebalancing work can therefore proceed to the
state-ledger invariant rather than another scale run; the planner has measured
local-state evidence and this lifecycle contract instead of an unconstrained
global-entry goal.
