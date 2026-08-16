# Shared-log scale census

This benchmark measures memory, disk, close, and fresh-process reconstruction
costs that grow with locally retained shared-log state. It is evidence for the
100-million-global-entry work tracked in
[#1286](https://github.com/dao-xyz/peerbit/issues/1286), not a claim that the
complete stack already supports that deployment.

## Scenarios

Resident rows load the real WASM implementation in one fresh process, establish
an empty-state baseline, populate deterministic CID-length metadata, force
garbage collection, and report the added memory:

- `native-graph-chain`: one DAG and one logical head;
- `native-graph-roots`: independent roots with distinct gids, where every entry
  is a head; and
- `coordinate-frontier`: independent native coordinate rows with distinct gids.

Persistent rows use two fresh processes and a temporary directory:

- `persistent-head-index-graph-chain`: a compacted Rust shallow-entry index,
  reopened through the real `Log.open()` native-graph reconstruction path;
- `persistent-head-index-graph-roots`: the same path for a high-frontier shape;
  and
- `persistent-coordinate-index`: a compacted Rust `EntryReplicatedU32` index,
  reopened through the real indexer initialization path.

The first process creates the exact authoritative `index.bin` format by calling
the indexer's own `SnapshotFile.compact()` implementation. This fixture step is
reported as `seed.snapshotMs`; it is not append-throughput evidence. The second
process loads an empty Rust/WASM state, records a baseline, opens the compacted
index, validates its size and endpoints, records memory, and closes it normally.
The report also verifies that close did not change the logical file footprint.

This split avoids contaminating reopen RSS with fixture allocations. It also
keeps the census focused on post-close disk and startup behavior; journal append
and crash-recovery throughput require separate workloads.

## Metrics

For resident rows, `rssDeltaBytes` and the heap/external/ArrayBuffer equivalents
measure state added after the fixed runtime, WASM module, and empty index are
loaded. `fixedRuntimeRssBytes` reports that excluded portion.

For persistent rows:

- `seed.buildMs` and `seed.snapshotMs` describe deterministic fixture creation;
- `seed.maxRssBytes` captures fixture peak memory;
- `reopen.openMs` and `reopen.openEntriesPerSecond` describe fresh startup;
- `reopen.rssDeltaBytes` and related fields describe reconstructed state;
- `reopen.openMaxRssBytes` captures peak RSS through open, while
  `reopen.maxRssBytes` also includes close compaction;
- `reopen.closeMs` measures a normal full close/compaction; and
- `disk.logicalBytes`, allocated bytes, per-entry costs, and individual files
  describe the compacted index footprint.

The shallow-entry fixtures contain graph/index metadata, not entry blocks or
media payloads. The coordinate fixtures use real schemas with deterministic
synthetic metadata. Application document indexes are not included.

## Running

Build the shared-log dependency closure first:

```bash
pnpm --filter @peerbit/shared-log... run build
pnpm run bench:shared-log-scale-census -- --counts 100000,1000000 --runs 1
```

Select a subset while investigating one path:

```bash
pnpm run bench:shared-log-scale-census -- \
  --counts 100000 \
  --scenarios persistent-head-index-graph-chain,persistent-coordinate-index
```

Machine-readable output uses a versioned JSON envelope:

```bash
BENCH_JSON=1 pnpm run bench:shared-log-scale-census > scale-census.json
```

The `Shared log re-census` manual workflow runs canonical 100k and 1M sizes and
uploads the JSON result. Ordinary pull-request CI runs the
argument/report tests and tiny persistent round trips; it does not allocate
million-entry states.

## Development-machine observation

One Node v22.23.2 run on an Apple M3 Pro produced these 100k persistent results.
They validate the harness and reveal the growth shape; they are not portable
thresholds or a substitute for the workflow artifact.

| Scenario                 | Reopen | Reopened RSS | RSS/row |     Disk | Disk/row |
| ------------------------ | -----: | -----------: | ------: | -------: | -------: |
| head index + chain graph | 8.67 s |    649.2 MiB | 6,808 B | 23.0 MiB |    241 B |
| head index + root graphs | 7.37 s |    563.8 MiB | 5,912 B | 17.0 MiB |    178 B |
| coordinate index         | 4.91 s |    631.1 MiB | 6,617 B | 24.6 MiB |    258 B |

The same machine created a valid 1M chain snapshot of 240,999,957 bytes, but the
real `Log.open()` path had not completed at a 30-minute local censor point. It
was still CPU-bound at roughly 4.3 GiB RSS. That RSS is partial, not a final
peak, and endpoint validation had not yet run. The result rules out treating one
million locally materialized rows as a healthy ordinary-peer startup target.

## Interpretation

Compare results only on the same Node major version, architecture, and machine
class. RSS is process-level and allocator/WASM growth is chunky, so one small
run is directional rather than a regression threshold. Use repeated isolated
runs before making a performance claim.

The target is bounded local state in a globally sharded deployment, not one
ordinary peer materializing all 100 million entries. Persistent files can be
compact while reopen remains expensive because the current Rust query index
deserializes all local rows and the native graph is rebuilt from every retained
shallow row.

Before claiming the global target, add range-scoped ownership-transfer and prune
profiles, gid-skew/hot-component workloads, application-index reopen rows, and
distributed 10M/100M soaks with replica-coverage checks. The lifecycle and
planner requirements are recorded in
[`SHARED_LOG_BOUNDED_STATE_CONTRACT.md`](./SHARED_LOG_BOUNDED_STATE_CONTRACT.md).
