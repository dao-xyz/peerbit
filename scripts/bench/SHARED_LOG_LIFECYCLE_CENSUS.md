# Shared-log lifecycle census

This benchmark answers a narrower question than the synthetic scale census:

> If two peers retain the same 1,000 documents, does the peer that previously
> processed 100,000 documents use materially more disk or take longer to reopen
> than a fresh 1,000-document peer?

It is evidence for [#1286](https://github.com/dao-xyz/peerbit/issues/1286).
It changes no runtime behavior, storage format, protocol, or public API.

## Matched workload

Each run creates two isolated durable Peerbit directories:

- `fresh` appends exactly the retained count; and
- `history` appends the historical count while length trim holds the live set at
  the same retained count.

Both use the real Rust Peerbit client, shared-log native graph, durable block
write-through store, native coordinate WAL, and native document index. Appends
are unique documents and therefore independent roots. The benchmark injects
one suppression-history row per new gid so trim cleanup is exercised as well as
the data indexes.

Seed and cold reopen run in separate `--expose-gc` processes. The seed process
quiesces GID cleanup, block writes, native delete cleanup, and coordinate and
document journals before it samples state and closes normally. The reopen
process starts from an empty runtime, opens the same directory, validates it,
and closes normally again.

The batch size must not exceed the retained window. This keeps every trim in a
steady-state batch: an oversized initial batch can otherwise test batch
admission behavior instead of the intended long-running retention behavior.

## Correctness gates

Before either worker can report success, it requires exact retained counts for:

- lower-log rows, native graph rows, and lower/native heads;
- membership of every retained shallow/native row and durable local block;
- native coordinate keys and values;
- document query rows and native document keys and values; and
- replication ranges and unique replicators.

It also verifies that:

- every retained `{id, name}` row matches the expected range, and the complete
  document fingerprint matches the fresh control and survives reopen;
- the oldest and newest retained documents are readable;
- the first historical document, shallow row, native graph row, and local block
  are absent after trim and after cold reopen;
- retained hash, head, coordinate, and ownership-coverage fingerprints survive
  close/reopen;
- injected GID history contains only live gids before close and is empty after
  reopen, because that suppression memo is deliberately ephemeral; and
- cleanup, repair, prune, backfill, durable-write, and native-delete queues are
  drained at the measurement boundary.

Matched durable live-block bytes must also equal the fresh control. Hot WASM
block count must equal retain in the seed process and may not exceed retain on
reopen. A cold reopen may hydrate fewer blocks than the seed process, and
probing with `get()` would artificially warm it. The census enumerates indexed
documents with `resolve: false`, reads shallow metadata with `getShallow()`, and
uses batch block presence checks, so correctness validation does not load entry
bytes.

## Performance and footprint metrics

The report records client creation, program open, append, cleanup, validation,
program close, and client stop times. It includes quiescent RSS/heap snapshots
and process peak RSS for each isolated worker.

Filesystem measurements include logical bytes, allocated bytes, individual
files, and these categories:

- entry-block store;
- coordinate WAL;
- document value and signer WALs;
- head index;
- replication indexes;
- libp2p state; and
- fixed/other files.

The matched comparison reports historical-minus-fresh disk, reopen, and RSS
deltas plus growth ratios and bytes per historical entry. RSS is reported both
as absolute process memory and relative to each worker process's own pre-open
baseline. Those values are measurements, not hard-coded pass/fail budgets.
Exact live-state equality is the correctness gate; persistent historical growth
is a finding to act on, not a reason to hide the report.

## Running

Build the workspace first because the harness imports the same built modules a
real application uses:

```bash
pnpm run build
pnpm run bench:shared-log-lifecycle-census
```

The default is the canonical matched `fresh 1k` versus `100k -> retain 1k`
run. For a quick local check:

```bash
pnpm run bench:shared-log-lifecycle-census -- \
  --history-count 1000 \
  --retain 100 \
  --batch-size 100
```

Use a checkpoint file for the canonical run:

```bash
pnpm run bench:shared-log-lifecycle-census -- \
  --history-count 100000 \
  --retain 1000 \
  --batch-size 256 \
  --output lifecycle-census.json \
  --json > /dev/null
```

The JSON envelope is named `shared-log-lifecycle-census`, schema version 1.
`progress` identifies a completed or failed matched run, and the output file is
replaced atomically at each checkpoint.

The manual `Shared log re-census` workflow exposes this as
`scale-lifecycle-trim`. Ordinary CI runs only `12 -> retain 4`; the canonical
100k workload remains manual.

## Development-machine observation

One Node v24.13.1 run on an Apple M3 Pro compared a fresh 1k store with 100k
appends trimmed to 1k. It is boundary evidence, not a portable threshold:

| Metric                 | Fresh 1k | 100k -> 1k | Historical/fresh |
| ---------------------- | -------: | ---------: | ---------------: |
| Append throughput      |  2,668/s |    2,475/s |            0.93x |
| Cold-process open      |   224 ms |   1,839 ms |            8.20x |
| Post-validation RSS    |  212 MiB |    495 MiB |            2.33x |
| Logical disk footprint | 1.66 MiB | 139.82 MiB |           84.35x |

Every retained-cardinality gate was exactly 1,000; singleton and zero-state
gates also held. The complete retained document-set fingerprint matched the
expected `doc-99000`...`doc-99999` payloads in both controls, every retained
shallow/native row and durable block was present, ownership coverage survived
reopen, all queues drained, and the first trimmed document/block remained
absent. The unresolved growth was almost entirely four append-only stores:
coordinate WAL (45.05 MB over fresh), entry-block WAL (43.93 MB),
document-value WAL (40.81 MB), and document-signer WAL (14.93 MB). Head and
replication indexes had no historical growth.

The historical append phase took 40.40 seconds with flush-on-append persistence
enabled for batches of up to 256 documents. Clean close/reopen did not compact
the four growing WALs. This is persistent-history bloat despite bounded
live-index cardinality, and it also inflates reopen time and RSS. Bounded WAL
checkpointing/compaction is therefore the next focused runtime slice.

### One-million-entry boundary observation

The same Node v24.13.1 / Apple M3 Pro machine completed one diagnostic
`1M -> retain 1k` run:

| Metric                 | Fresh 1k |  1M -> 1k | Historical/fresh |
| ---------------------- | -------: | --------: | ---------------: |
| Append throughput      |  2,820/s |   2,313/s |            0.82x |
| Cold-process open      |   230 ms | 18,207 ms |           79.02x |
| Post-validation RSS    |  210 MiB |  2.86 GiB |           13.95x |
| Logical disk footprint | 1.67 MiB |  1.37 GiB |          842.68x |

The historical append phase took 432.41 seconds. Reported retained
cardinalities remained exactly 1,000, durable live-block bytes were 298,000 in
both controls, queues were drained, retained endpoints remained readable, and
the first trimmed document, native graph row, and durable block remained absent.

Logical historical overhead was 1,471,262,822 bytes across 999,000 non-retained
entries, or 1,472.736 bytes per historical entry. That slope is only 0.64%
above the 100k result's 1,463.399 bytes per historical entry, confirming
near-linear persistent growth at the two measured scales. The dominant deltas
were coordinate WAL (454,545,000 bytes), entry-block WAL (445,331,780 bytes),
document-value WAL (418,802,230 bytes), and document-signer WAL (152,624,780
bytes). Head and replication indexes again had no historical growth.

This saved 1M artifact predates the hardened census checks that fingerprint the
complete retained document set and verify every retained shallow/native row and
durable block. It also predates the baseline-adjusted RSS fields, although its
raw state records equal durable bytes. Treat it as scaling-boundary evidence,
not a final hardened correctness artifact. It is one fresh-first run without an
operating-system page-cache reset, so its disk slope is more diagnostic than its
exact timing and RSS ratios.

## Interpretation and next gate

Compare runs only on the same Node major version and comparable hardware. RSS
and short reopen timings are noisy, so repeat runs before treating small deltas
as performance gains or regressions. File growth is more diagnostic: append-only
WAL history may grow even when every live row count is exact.

`openMs` uses a fresh Node process but does not clear the operating system page
cache. One matched run also always executes fresh before history. Alternate or
repeat the controls before treating timing ratios as precise; neither caveat
explains the per-file WAL growth.

The completed 1M boundary resolves the unchanged-runtime scaling question:
durable history grows approximately linearly even though retained cardinality
remains bounded. A larger unchanged-runtime run is not warranted. Re-run the
hardened 100k census after each compaction slice, then use 1M only after 100k
demonstrates bounded growth or exposes a new scaling question.

The next #1286 slice should investigate a crash-atomic checkpoint for the
transactionally coupled coordinate, document-value, and document-signer WALs.
Keep entry-block WAL compaction separate because it has a distinct
write-through and tombstone lifecycle. Ownership transfer and two-peer
rebalance recovery remain separate workloads; this census does not imply that
the preserved custody/rebalance stack should be merged.
