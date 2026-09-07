# Native CUT coverage planning

Measured against base `49bb1efc8f72fc89551f98c62c06aa928c45afc2` on
2026-09-07, macOS 26.6.2 arm64 / Apple M3 Pro, rustc 1.96.1, Cargo 1.96.1.
This is a native Rust graph-planning microbenchmark, not a replication,
JavaScript/WASM, disk-reopen, or compaction benchmark.

## Change and invariant

Single planning borrows the smaller of the existing reverse-child and head
sets. Batch planning first estimates relevant adjacency work; when that exceeds
one head scan, it retains the previous amortized CUT-head map. Otherwise it
avoids visiting and materializing unrelated heads. No index, persistent state,
public API, storage format, wire format, or tombstone-removal rule is added.

Coverage still requires a current CUT head with the exact gid, a direct next
reference to the candidate, and a strictly later `(wall_time, logical)` clock.
Reverse edges must remain useful after their parent is deleted. The existing
skip/reset, unchecked-context, and missing-parent results remain identical.
Five differential tests exercise all three planner entry points, exhaustive
predicate combinations, mutations, head transitions, rebuild, and the batch
cost selector against an independent exhaustive head-scan reference.

This does not bound total retained history or worst-case graph degree. The
amortized path can still allocate a map proportional to current CUT heads when
repeated relevant adjacency is more expensive. Authenticated projection
checkpoints and anti-resurrection/custody proofs remain prerequisites for
logical history truncation.

## Reproduction

From the repository root, using cached Cargo dependencies:

```sh
cargo test --offline --release --manifest-path packages/log/rust/Cargo.toml \
  benchmark_cut_coverage_matched -- --ignored --nocapture
```

The ignored test is opt-in benchmarking, not a skipped correctness gate. It
compares the candidate with the pre-change exhaustive batch algorithm in one
optimized binary. Each row has seven samples of ten 128-entry batches, with
baseline/candidate order alternated. Fixture allocation and correctness checks
are outside timing; outputs are consumed through `black_box`. Samples are
within one process, not seven independent deployments. No network or payload
verification is included. Raw rows report milliseconds per ten batches; the
table divides their median by ten.

CUT fixtures contain half covered and half unseen candidate hashes. History
grows while the candidate batch stays fixed. APPEND controls repeat the same
missing parent with either non-head or current-head referencing branches.

| Graph shape                | Historical rows | Baseline ms / batch | Candidate ms / batch |
| -------------------------- | --------------: | ------------------: | -------------------: |
| CUT heads, same gid        |           1,000 |             0.16325 |              0.01795 |
| CUT heads, same gid        |          10,000 |             1.30433 |              0.01554 |
| CUT heads, same gid        |         100,000 |            27.84039 |              0.02273 |
| CUT heads, distinct gids   |           1,000 |             0.10460 |              0.01331 |
| CUT heads, distinct gids   |          10,000 |             0.98862 |              0.01725 |
| CUT heads, distinct gids   |         100,000 |            10.75657 |              0.02105 |
| Non-head APPEND fanout     |           1,000 |             0.00900 |              0.00883 |
| Non-head APPEND fanout     |          10,000 |             0.01010 |              0.00922 |
| Non-head APPEND fanout     |         100,000 |             0.01007 |              0.01018 |
| Current-head APPEND fanout |           1,000 |             0.02653 |              0.02754 |
| Current-head APPEND fanout |          10,000 |             0.17392 |              0.17284 |
| Current-head APPEND fanout |         100,000 |             1.77283 |              1.85355 |

The favorable rows establish independence from unrelated CUT-head growth in
these fixtures, not an end-to-end speedup. The widest current-head control is
about 4.6% slower in this run; these samples do not establish a portable
regression threshold. An initial unconditional reverse-lookup candidate was
rejected: that control took roughly 5.2 seconds versus 20 milliseconds per ten
batches. Its preserved result motivated retaining the existing amortized path.

Local raw artifacts are in `/private/tmp/peerbit-cut-coverage.urwXmu/`:

- `benchmark-final.raw.log`, SHA256
  `7b0ff499b73dec4f57988ea8e2257ddd8a67ff46e94fda4e466d1d3990208f4e`.
- `benchmark-initial.raw.log`, rejected unconditional candidate, SHA256
  `9f71810c7c23cc449a5bdf145ba79c65e34639572cb61440a93eff8e65f345a4`.
- `cargo-tests-final-v2.raw.log`: 43 passed, benchmark explicitly ignored;
  `backbone-tests-final.raw.log`: 58 passed.
- `log-wasm-check.raw.log` and `backbone-wasm-check.raw.log`: offline
  `cargo check --target wasm32-unknown-unknown` passed for both embedded paths.
  Cargo format checks passed for both packages.

## Local packaged runtime validation

Both TypeScript wrappers were rebuilt locally, and both WASM artifacts were
rebuilt with `wasm-pack --mode no-install`, Cargo offline, and cached
wasm-bindgen 0.2.121. Dependencies/generated prerequisite outputs were copied
into an isolated worktree, not linked to another worktree's package trees.

- `log-wasm-wrapper-tests.raw.log`: 42 passing.
- `backbone-wasm-wrapper-tests.raw.log`: 111 passing. The existing checkpoint
  tests emitted a Node FileHandle garbage-collection warning; the process
  still exited naturally. This observation is not attributed to this change.
- Both complete wrapper suites used strict unhandled rejection handling,
  retries zero, forbid-pending, no forced exit, and a 60-second per-test limit.
- Both packages were packed with pnpm and extracted into a separate local
  consumer without installation. `packed-consumer-isolated-final.raw.log`
  records 63 assertions covering scalar/batch CUT predicates and head
  transitions through both artifacts, plus the backbone's embedded raw-receive
  CUT retention path. This is a local graph/ingest check, not a durability proof.
- A read-only loader rejected module resolution outside the isolated task
  directory. The consumer checked exact extracted wrapper paths and binary
  equality with both rebuilt `dist/wasm` and source-runtime `wasm` copies.

Rebuilt binary SHA256 values, also verified inside the tarballs:

- `log_rust_bg.wasm` (772,184 bytes):
  `b4696ca41392ba1af790dc96948aaa4ac3dda9d8f2c98ab115ea4b67c092bd81`.
- `native_backbone_bg.wasm` (2,148,739 bytes):
  `c8f60b7c960c094774eae152c81a92eaa1617916495dfe4b3dccb2b66cd289d9`.

The graph source is embedded in both `@peerbit/log-rust` and
`@peerbit/native-backbone`; both require a patch release. No downstream
acceptance, npm publication, or cross-platform validation is implied.
