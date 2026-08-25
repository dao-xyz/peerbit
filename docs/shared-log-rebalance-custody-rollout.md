# Shared-log rebalance and custody rollout

Status: **review checkpoint; not activation authorization**

Scope: the shared-log stack in PRs #1290 through #1306

Last reviewed: 2026-08-25

This document separates the changes that are ready to validate as ordinary
runtime corrections from the durable scan and custody foundations that must
remain inactive until their end-to-end safety contract exists. It is a release
and trust-boundary record, not a claim that Peerbit currently performs durable
ownership transfer or can safely prune from custody receipts.

## Decision

Proceed as three shallow tracks instead of merging the current 17-PR dependency
chain:

1. **Runtime-ready planning:** the paged rebalance query refactor and the
   deterministic owner-selection, containment-index, and fallback-stream
   changes from #1290 and #1297-#1299.
2. **Durable scan and capture:** the work store, scan executor, persistence
   adapter, collision-bucket reads, placement view, pure owner planner, native
   snapshot, and V2 provenance work from #1291-#1296 and #1300-#1302, based on
   the runtime track. Keep this staged until the non-pruning vertical slice and
   the provenance decision below pass.
3. **Custody foundation:** the signed artifact model, keyed persistence,
   pending-visit bridge, and passive catalog from #1303-#1306, based on the scan
   track. Preserve it, but do not ship or activate it merely to clear the PR
   queue.

Keep every original branch until the replacement tracks have identical intended
patches, clean CI, and reviewed changeset coverage. Do not merge a changesets
version-package PR in the middle of the stack.

## What changes today

| Slice                                                           | Current effect                                                                                                                                                                                                        | Release posture                                                                   |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Paged rebalance query tasks                                     | Refactors an existing TypeScript planning path to page bounded work while retaining its current history-commit semantics.                                                                                             | Runtime candidate after full CI and benchmarks.                                   |
| Owner-selection semantics                                       | Counts maturity by unique owner, applies one deterministic TypeScript/Rust total order, treats exact directional equality as zero distance, and excludes the current range from adjacency. Rare decisions can change. | Runtime candidate; requires a mixed-version soak.                                 |
| Native containment and fallback                                 | Bounds wide-range containment references and replaces repeated fallback scans with four ordered endpoint streams. The documented worst case is `O(P + C * (K + 4N))`, where `N` is indexed ranges.                    | Runtime candidate; requires direct scale evidence.                                |
| Durable work, collision scans, placement views, and owner plans | Internal modules and additive native methods. The supported `@peerbit/shared-log` root does not import or export these modules, and no production coordinator constructs them.                                        | Dormant foundation; do not call it a working rebalance pipeline.                  |
| V2 remote-owner provenance                                      | The future placement-capture consumer is absent, but the coordinator is wired into `SharedLog`: an expired grant rejects the receive update and forces refresh/resync.                                                | Live behavior inside the scan track; resolve the 60-second policy before release. |
| Custody artifacts, stores, pending visits, and catalog          | Internal modules call one another, but the supported package root does not import them and production has no scheduler, transfer transport, pin provider, finalizer, release, deletion, or prune consumer.            | Dormant model only; no custody or prune authority.                                |

The intended future sequence is:

```text
complete current placement view
  -> deterministic bounded plan
  -> durable scan cursor and pending visit
  -> signed source manifest
  -> destination block + row + retention-pin barrier
  -> authenticated network receipt
  -> durable source receipt
  -> current-epoch finalizer
  -> source deletion across every state-ledger representation
```

The reviewed stack implements pieces of this sequence. It does not connect the
sequence and deliberately does not implement the final deletion authority.

## Compatibility and breaking-change assessment

- The supported `@peerbit/shared-log` root API is unchanged. The package export
  map exposes only `.`; the new TypeScript modules are not supported subpath
  APIs.
- Existing Peerbit replication message schemas and production storage formats
  are unchanged. The custody manifest, receipt, A/B work records, and SQLite
  catalog are new versioned formats, but no production path currently creates
  them.
- Shared-log Rust/WASM and native-backbone additions are additive. Direct users
  should still run their own compatibility tests because those packages expose
  lower-level APIs.
- A consumer that bypasses the export map and constructs the internal V2 receive
  coordinator directly must now provide `isOwnershipActive`; that unsupported
  deep-import use has a source-level break.
- Owner selection has observable corrections even though its data and wire
  formats do not change. During a mixed old/new rollout, peers can temporarily
  choose different owners for maturity-order and exact-tie edge cases. The
  expected failure mode is repair/ownership churn, but the soak must prove that
  it cannot create persistent under-replication or unsafe pruning.
- V2 provenance defaults to a 60,000 ms freshness window and caps configuration
  at five minutes. Expiry can turn an incremental update into a conservative
  full refresh. That is a traffic/availability behavior change, not a wire-format
  break.
- The existing changesets request patch releases. That is appropriate only
  while the custody and work formats stay internal and inactive. Once durable
  records are produced in deployment, a binary downgrade is not automatically
  safe: a compatible version must recover or drain pending records first.

## Durability and trust boundaries

| Evidence or component         | What it proves                                                                                                                                                  | What it does **not** prove                                                                                                          |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Canonical placement view      | A complete, bounded, deterministic snapshot of caller-supplied local facts, with an execution/freshness fence.                                                  | Global consensus, current authorization, durable custody, or prune permission.                                                      |
| V2 owner provenance           | Recently authenticated remote-owner evidence bound to the current receiver/session/lifecycle while its grant is fresh.                                          | Durable placement consensus, destination custody, or authority after expiry/restart.                                                |
| A/B rebalance work record     | The exact plan/cursor frame passed the selected adapter's barrier and can be recovered without accepting a torn partial frame.                                  | That any entry moved, that a destination retained it, or that source deletion is safe.                                              |
| Signed custody manifest       | The source signed one canonical entry handoff attempt, bound to log, source, destination, view, plan, and attempt generation.                                   | Truth of the source's placement view or possession by the destination.                                                              |
| Destination pin evidence      | An in-process opaque capability is intended to be issued only after an external composite block + required rows + retention-pin barrier.                        | Anything until a real pin provider implements and tests that barrier. The current model cannot observe physical storage by itself.  |
| Signed custody receipt        | The destination signed the V1 profile claims for the exact manifest and pin generation.                                                                         | Network delivery, current destination eligibility, replica coverage, finalization, release, deletion, or prune authority by itself. |
| Strict custody SQLite adapter | An exact namespace binding, single-writer lease, SQLite transaction, `synchronous=FULL`, full WAL checkpoint, and directory sync on the supported adapter path. | Portable power-loss semantics on every filesystem/OS, distributed atomicity, or acceptable throughput.                              |
| Passive custody catalog row   | A bounded discovery hint that can be point-read and revalidated against the current frame.                                                                      | Custody, a pin, a receipt, an authoritative state machine, or permission to delete.                                                 |

Signatures authenticate who made a claim; orchestration must separately prove
that the signer is authorized under a current placement epoch. A pubsub message
or signed receipt is not proof of stable destination storage. Local memory-mode
stores are reducer/test simulations and must never issue production durability
or pruning authority.

The strict Node adapter currently performs a full WAL checkpoint plus directory
sync for custody transitions. This is conservative, but it is not yet a
performance design. Group commit may be introduced only if it preserves the
same acknowledgement boundary under real process-kill tests.

For the broader native append durability contract, see
[`native-shared-log-durability.md`](./native-shared-log-durability.md). Custody
handoff must not invent a weaker publication or deletion boundary than that
transaction protocol.

## Size and maintainability review

The reviewed top of stack changes 57 files by approximately +29,756/-524 lines.
About 17,404 added lines are production code and 12,232 are tests; Rust inline
tests make the effective test share higher. At least 11,587 added production
TypeScript lines belong to intentionally unwired modules.

Dormant does not mean free. `@peerbit/shared-log` publishes both `src` and
`dist`, so the unwired TypeScript still enters the npm artifact. The review
snapshot measured roughly 1.02 MiB more unpacked package content, 215 KiB more
compressed content, and 318 KiB more emitted runtime JavaScript. Re-run the pack
measurement on each replacement track; do not merge the custody track if its
only immediate result is package weight.

The main maintainability debt is visible rather than hidden:

- `custody-record-persistence.ts`, `custody-store.ts`, and
  `rebalance-work-store.ts` are each roughly 2,000-2,800 lines. Split pure
  codecs, row/schema validation, and adapter plumbing along existing trust
  boundaries before adding more transitions. Avoid a generic state-machine
  framework until two concrete consumers demonstrate the same abstraction.
- Roughly 549 byte-equivalent validator lines are duplicated between the
  shared-log-rust and native-backbone TypeScript wrappers. Prefer one generated
  source or a canonical fixture/parity guard; a new runtime package would add
  its own dependency and packaging cost.
- The native planner's 4,096 empty containment buckets predate this stack. The
  runtime changes improve wide-range amplification; they should not be blamed
  for that pre-existing fixed allocation.

## Activation gates

None of the custody or deletion paths may be activated until all applicable
boxes are closed with artifacts linked from the release PR.

- [ ] **V2 freshness decision:** either decouple ordinary replication admission
      from custody-provenance freshness or prove proactive refresh and recovery do
      not create periodic full-resync storms. Cover monotonic-clock loss, idle
      sessions beyond 60 seconds, lifecycle rotation, and mixed old/new peers.
- [ ] **Non-pruning vertical slice:** capture one complete view, plan and scan one
      bounded page, persist a pending visit before effects, transfer one entry,
      durably pin it at the destination, return and persist the exact receipt, and
      resume correctly after restart at every boundary. Source bytes remain intact.
- [ ] **Publication outbox and admission fence:** persist committed-but-unpublished
      work, isolate storage success from callback/network failure, reject stale
      admissions, and choose a tested concurrent-write policy (dual-write or a
      durable source journal plus replay).
- [ ] **Destination and finalizer authority:** implement the exact block +
      referenced-block + required-row + retention-pin barrier, authenticated receipt
      transport, current-epoch membership/role revalidation, replica-coverage check,
      and one idempotent finalizer.
- [ ] **Deletion gate:** require the finalizer result plus the exact durable source
      receipt before deleting anything. Release every representation—payload block,
      shallow row, native graph node, coordinate row, application row, and relevant
      cache/reference state—in bounded, restartable steps. A failure retains data.
- [ ] **Catalog retention and disk budgets:** add terminal-record retention/GC,
      tombstone and migration compaction, per-log quotas, and a global disk bound.
      Bounded pages alone do not bound the number of distinct catalog rows.
- [ ] **Durability performance:** benchmark transition latency/throughput and WAL
      growth with the current full checkpoint + directory sync. Any batching design
      must retain the strict acknowledgement invariant.
- [ ] **Real crash matrix:** use separate processes and actual `SIGKILL` at every
      SQLite/A/B/pin/outbox/finalizer boundary, including a second kill during
      recovery and corrupt/torn-tail cases. Exception injection is supplemental,
      not sufficient.
- [ ] **Platform decision:** run adapter conformance on supported Node filesystems
      and make an explicit Windows support decision. Do not infer directory-sync or
      lock semantics from Linux/macOS behavior.
- [ ] **Native complexity evidence:** verify bounded containment memory and
      fallback scaling at the configured range cap in isolated release-mode workers,
      with parity hashes and retained-capacity counters in addition to wall time.
- [ ] **Mixed-version and churn soak:** cover owner maturity/tie differences,
      delayed and dropped V2 refreshes, join/leave churn, concurrent writes, restart,
      repair convergence, replica coverage, and zero unauthorized deletion.
- [ ] **Observability and kill switch:** expose pending age/count/bytes, catalog
      growth, WAL/checkpoint latency, poison state, refresh/full-resync rate,
      transfer retries, pin count, finalizer state, and deletion backlog. Operators
      must be able to disable new plans and deletion independently.
- [ ] **Packaging and modularity gate:** record per-package tarball and runtime-JS
      deltas, remove or defer unused modules, split the largest trust-boundary files,
      and prevent wrapper validators from drifting.

## Rollout and rollback

1. **Runtime-only patch.** Release the runtime track without the dormant scan or
   custody modules. Canary it on a small mixed-version cohort and monitor owner
   changes, repair traffic, CPU, latency, and replica coverage. Promote only
   after deterministic TS/Rust parity, full CI, and the performance gates pass.
2. **Keep foundations staged.** Rebase the durable-scan and custody branches on
   the released runtime track, but keep them out of the production package until
   the non-pruning vertical slice and package-size decision are reviewed.
3. **Shadow planning.** When the gates are implemented, enable capture, planning,
   cursor persistence, and diagnostics for selected logs, but perform no network
   transfer and no deletion. Compare plans with actual placement and discard
   stale views safely.
4. **Transfer-only canary.** Enable destination pins and receipts for a small
   cohort while retaining every source copy. Exercise restart and kill tests in
   production-like storage before trusting receipts operationally.
5. **Finalizer, then deletion canary.** Activate current-epoch finalization and
   source deletion independently, starting with one bounded shard/log and an
   explicit storage budget. Expand only with continuous replica-coverage and
   recovery evidence.

Before custody activation, rollback is a normal package downgrade because the
current wire and production storage formats are unchanged. After any durable
work or custody record is created, rollback means: disable new plans and
deletion, retain blocks and pins, recover or drain pending records with a
compatible binary, and only then downgrade. Never delete the new records or
force-open a leased namespace simply to make an older binary start. Safe
rollback may leak retained bytes; it must not risk data loss.

## Benchmark protocol for the runtime track

PR #1288 supplies a fresh-process resident and persistent scale census. Use the
same benchmark commits, Node major, machine, and command for both sides. The
following creates detached worktrees, so it does not rewrite either review
branch. Replace `RUNTIME_REF` if the final replacement branch has another name.

```bash
REPO=/Users/marcuspousette/git/peerbit
BENCH_REF=origin/bench/shared-log-scale-census
RUNTIME_REF=origin/peerbit-org/shared-log-runtime-planning-20260825
RESULTS_DIR="$(mktemp -d /tmp/peerbit-runtime-ab-results.XXXXXX)"
BASELINE_DIR="$(mktemp -d /tmp/peerbit-runtime-ab-base.XXXXXX)"
CANDIDATE_DIR="$(mktemp -d /tmp/peerbit-runtime-ab-candidate.XXXXXX)"

git -C "$REPO" fetch origin master bench/shared-log-scale-census \
  peerbit-org/shared-log-runtime-planning-20260825
BENCH_BASE="$(git -C "$REPO" merge-base origin/master "$BENCH_REF")"
git -C "$REPO" worktree add --detach "$BASELINE_DIR" "$BENCH_REF"
git -C "$REPO" worktree add --detach "$CANDIDATE_DIR" "$RUNTIME_REF"
git -C "$REPO" rev-list --reverse "$BENCH_BASE".."$BENCH_REF" |
  xargs git -C "$CANDIDATE_DIR" cherry-pick

for CHECKOUT in "$BASELINE_DIR" "$CANDIDATE_DIR"; do
  pnpm -C "$CHECKOUT" install --frozen-lockfile
  pnpm -C "$CHECKOUT" --filter @peerbit/shared-log... run build
done

NODE_OPTIONS=--no-warnings pnpm -C "$BASELINE_DIR" \
  run bench:shared-log-scale-census -- \
  --counts 100000 --runs 3 \
  --output "$RESULTS_DIR/baseline.json" --json > /dev/null
NODE_OPTIONS=--no-warnings pnpm -C "$CANDIDATE_DIR" \
  run bench:shared-log-scale-census -- \
  --counts 100000 --runs 3 \
  --output "$RESULTS_DIR/candidate.json" --json > /dev/null

for SIDE in baseline candidate; do
  jq -r '.rows[] | [
    .scenario,
    .count,
    .run,
    (.elapsedMs // .reopen.openMs),
    (.rssDeltaBytes // .reopen.rssDeltaBytes),
    (.reopen.disk.logicalBytes // 0)
  ] | @tsv' "$RESULTS_DIR/$SIDE.json" | sort > "$RESULTS_DIR/$SIDE.tsv"
done
paste "$RESULTS_DIR/baseline.tsv" "$RESULTS_DIR/candidate.tsv"
```

Retain the raw JSON and host metadata. Persistent logical disk bytes should be
identical because the runtime track intentionally changes no persisted schema.
Treat repeated RSS or reopen regressions above an agreed machine-local budget
(initially 10%) as a stop signal, not as proof from one noisy sample. Run the
1M census once as a boundary/censor exercise after the 100k comparison; it is
not an ordinary-peer target.

The #1288 census does **not** exercise range containment or fallback selection,
so it can detect resident/reopen bloat but cannot establish the claimed planner
speedup. Before merging the runtime track, add one isolated native range-planner
benchmark with deterministic output hashes and these rows:

- full-width and nine-bucket-width containment inserts at 256, 1,024, 4,096,
  and (candidate-only if the baseline reaches its resource limit) 16,384 ranges;
- adversarial non-strict fallback with 16,384 ranges owned by one immature peer
  and one final mature candidate, for 1, 16, and 128 cursors; and
- a representative mixed-width/mixed-owner distribution for regression
  detection, not only the adversarial win.

Run each row in a fresh Node 22 process against release-mode WASM, with five
warmups and at least twenty measured calls. Record setup time, query median/p95,
peak and retained RSS, result hash, indexed references, endpoint-key advances,
and candidate-head visits. Correctness requires identical decisions to the
reference implementation; boundedness requires at most nine containment
references per narrow range (one global order plus at most eight buckets) and
at most `4N` fallback endpoint-key advances per cursor. A speed claim should be
made only from this direct benchmark, while #1288 remains the package/state
regression guard.
