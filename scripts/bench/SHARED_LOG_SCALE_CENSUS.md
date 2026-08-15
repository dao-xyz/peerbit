# Shared-log resident scale census

This benchmark measures the resident reconstruction costs that currently grow
with locally retained shared-log state. It is an input to the 100-million-entry
work tracked in [#1286](https://github.com/dao-xyz/peerbit/issues/1286), not a
claim that the complete stack already supports that deployment.

## Scenarios

- `native-graph-chain`: locally retained history in one DAG and one logical
  head. This exercises the native graph state rebuilt when a log opens.
- `native-graph-roots`: independent roots with distinct gids. Every entry is a
  head, modeling the high-frontier shape that stresses graph and sync state.
- `coordinate-frontier`: independent resident coordinate rows with distinct
  gids, modeling shared-log coordinate hydration and frontier-wide planning.

Every `(scenario, count, run)` executes in a fresh Node process. The worker
loads the real WASM package, creates an empty native state, forces garbage
collection, inserts deterministic CID-length hashes, then forces collection
again. `rssDeltaBytes` therefore measures state added after the fixed runtime,
WASM module, and empty index are loaded. `fixedRuntimeRssBytes` reports that
excluded fixed portion separately.

## Running

Build the native shared-log dependency closure first:

```bash
pnpm --filter @peerbit/shared-log... run build
pnpm run bench:shared-log-scale-census -- --counts 100000,1000000 --runs 1
```

Machine-readable output uses a versioned JSON envelope:

```bash
BENCH_JSON=1 pnpm run bench:shared-log-scale-census > scale-census.json
```

The `Shared log re-census` workflow runs the canonical 100k and 1M sizes on a
dedicated runner and uploads the JSON result. Ordinary pull-request CI only
tests the argument/report contract; it does not allocate million-entry states.

## Interpretation

Compare results only on the same Node major version, architecture, and machine
class. RSS is process-level and allocator/WASM growth is chunky, so a single
small run is directional rather than a regression gate. Use repeated isolated
runs before making a performance claim.

This first census deliberately excludes block payloads, disk reads, query-index
reopen, network catch-up, replication skew, and churn. Those require separate
rows before a deployment can claim 100M global entries:

1. persistent log and query-index close/reopen time, RSS, and disk bytes;
2. range-scoped catch-up and ownership-transfer throughput;
3. gid skew and hot-component partitioning;
4. distributed 10M and 100M soak runs with replica-coverage checks.

Large media bytes must be budgeted separately from graph metadata. The target
is bounded local state in a globally sharded deployment, not one ordinary peer
materializing all 100M entries in memory.
