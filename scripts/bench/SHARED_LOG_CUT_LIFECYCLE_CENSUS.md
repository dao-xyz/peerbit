# Shared-log CUT lifecycle census

This generic `Documents` benchmark measures delete-history growth, not a
filesystem workload and not compaction. Each matched cohort uses isolated
`fresh` and `history` directories. Fresh puts/deletes each key once; history
repeats the same cycle. Both finish with zero visible documents. Seed and
reopen run in separate Node processes.

The versioned report validates exact lower/native log rows, resident CUT heads
and coordinates, zero document/index rows, lower/native head consistency,
replication ownership, drained observable background debt, disk rows/bytes,
timings, process-cold RSS delta, and through-close peak RSS. Paged lower heads are
matched against a transient exact native set released before RSS sampling; completed phases are checkpointed.
Unavailable private observations are named under `state.debt.unobserved`, never
treated as zero. Reopen may use a warm OS page cache, so it is process-cold,
not cold-storage.

After building `peerbit` and `@peerbit/document`, run the 200-operation smoke
with `pnpm run bench:shared-log-cut-lifecycle-census`.

Canonical 100k baseline (three order-alternating matched cohorts):

```bash
pnpm run bench:shared-log-cut-lifecycle-census -- \
  --history-operations 100000 --key-count 1000 --batch-size 250 \
  --compact-max-journal-bytes 16777216 --compact-max-journal-records 65536 --runs 3 \
  --output cut-lifecycle-100k.json \
  --json > /dev/null
```

`history-operations` must be divisible by `2 * key-count`. Compare candidates
only with the same Node major, machine, command, and checkpoint policy.
Thresholds affect coupled fact WALs only; entry blocks keep production policy.

This is deliberately a pre-compaction baseline: exact row-per-operation
assertions would reject bounded history. A compaction candidate needs a
separate mode with explicit retained-row/CUT bounds and a genuinely unseen
stale-replay proof. This census makes no anti-resurrection or safe-CUT-removal
claim. Do not extrapolate it into a one-million-operation acceptance run.
