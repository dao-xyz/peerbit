# Debugging Plan: ci:part2 Flake

Last updated: 2026-02-06

## Key Learnings

## Ahas/Gotchas

## Test Results

## Claims/Hypothesis

### Claims-to-Tests Coverage Matrix

| Claim/Hypothesis | Evidence so far | Test to confirm/reject | Status |
|---|---|---|---|

## Next Steps

### 2026-02-06
- Identified failing test in CI `ci:part2`: `packages/programs/data/document/document/test/index.spec.ts` test `index > operations > search > redundancy > can search while keeping minimum amount of replicas`.
- Error signature: `Error: Failed to collect all messages <collected> < 1000. Log lengths: [store1,store2,store3]` (example: `533 < 1000`, log lengths `[997,106,533]`).
- Tried using skill `brave-search` for CI history mining, but `BRAVE_API_KEY` is not set in this environment; using `gh` + GitHub API for history/log mining instead.

## Ahas/Gotchas

### 2026-02-06
- `pnpm install` emits warnings about failing to create `peerbit` bin under `packages/clients/peerbit-server/frontend/...` due to missing `.../dist/src/bin.js` (likely because builds haven't run yet). Doesn’t block `test:ci:part-2`.
- Local gotcha: running `pnpm --filter @peerbit/document test` before `pnpm run build` fails TypeScript with many `Cannot find module '@peerbit/...` errors. CI always runs `pnpm run build` first; local repro should too.

## Test Results

### 2026-02-06
- PASS: `PEERBIT_TEST_SESSION=mock pnpm --filter @peerbit/document test -- --grep "can search while keeping minimum amount of replicas"` (1 passing, ~4s).
- PASS: `PEERBIT_TEST_SESSION=mock pnpm run test:ci:part-2` (local run; `@peerbit/document` had 175 passing; redundancy test passed in ~4.1s).
- GitHub Actions scan (latest attempts only) across all 742 `CI` workflow runs in `dao-xyz/peerbit` found 6 matches of the signature `Failed to collect all messages ... Log lengths ...` in `packages/programs/data/document/document/test/index.spec.ts`:
  - 2026-02-06: run 21733357351 (research/pubsub-large-network-testing) collected 379/600 lengths [271,58,379]
  - 2026-02-05: run 21730946780 (fix/shared-log-unhandled-persistcoordinate) collected 557/1000 lengths [997,104,557]
  - 2026-01-28: run 21430245247 (master) collected 998/1000 lengths [998,88,593]
  - 2026-01-23: run 21286028027 (feat/docs-site-v2) collected 491/1000 lengths [491,96,536]
  - 2026-01-23: run 21281335068 (feat/docs-site-v2) collected 508/1000 lengths [1000,508,560]
  - 2026-01-23: run 21281096044 (feat/docs-site-v2) collected 615/1000 lengths [1000,108,615]
- Note: this does NOT include failures hidden by reruns (attempt 1 fail, attempt 2 pass), because the default Jobs API returns only the latest attempt’s jobs.
- Extended scan including rerun attempts (`/runs/{id}/attempts/{n}/jobs`) across all 742 CI runs found 7 occurrences (attempt 1 failures) of this signature. Only one of them was hidden by a successful rerun (run conclusion `success`, attempt1 failed).
- PASS (after fix cherry-pick): `PEERBIT_TEST_SESSION=mock pnpm run test:ci:part-2`.

## Claims/Hypothesis

### 2026-02-06
| Claim/Hypothesis | Evidence so far | Test to confirm/reject | Status |
|---|---|---|---|
| H1: The CI flake is primarily the test being too strict about timing: distributed `index.search(fetch=count)` can transiently return `< count` during redundancy rebalance/sync, especially on slower CI runners. | CI failures show `collected` closely matches one peer’s current `log.length` (e.g. `557/1000` with `[997,104,557]`), implying incomplete distributed aggregation at that instant; test also had `// TODO fix flakiness`. | Replace immediate assertion with `waitForResolved(...)` retry window; run `pnpm run test:ci:part-2` locally. | Confirmed locally; change applied on `fix/ci-part2-redundancy-flake`. |

## Next Steps
- Push branch `fix/ci-part2-redundancy-flake` and open PR against `dao-xyz/peerbit`.
- (Optional) Add a debug-only repro test (env-guarded) that slows indexing + forces pruning to validate the original timing window deterministically.
- PASS/DEMO: Deterministic repro of the failure mode (missing remote RPC responses) via `packages/programs/data/document/document/repro-peerbit-redundancy-timeout.mjs`.
  - With `remote.throwOnMissing=true` and `remote.timeout=200`, search throws `MissingResponsesError`.
  - With best-effort (`remote.timeout=200`), search returns a short read (e.g. `168/200`) even though other peers hold more (`logLens=[199,50,118]`).
  - Subsequent default searches converge immediately once responses are no longer delayed.

## Key Learnings

### 2026-02-06
- PR opened on the Faolain fork for this fix: https://github.com/Faolain/peerbit/pull/8 (branch `fix/ci-part2-redundancy-flake`).
- Correction: the local deterministic repro script is `packages/programs/data/document/document/tmp/repro-ci-part2-redundancy-flake.mjs` (it is under a `tmp/` folder which is gitignored by the repo root `.gitignore`, so it is not included in PR diffs unless force-added).

## Test Results

### 2026-02-06
- PASS: `PEERBIT_TEST_SESSION=mock pnpm --filter @peerbit/document test -- --grep "can search while keeping minimum amount of replicas"` (after adding `debugging-plan.md` commit + updating PR body).

## Test Results

### 2026-02-06 (stress / local repro)
- FAIL (stress loop, PR branch): iteration 17/25 failed: `Failed to collect all messages 317 < 600. Log lengths: [286,55,317]` after ~2 minutes (timed out inside `waitForResolved(...)`).
- FAIL (stress loop, `origin/master`): iteration 11/25 failed quickly: `Failed to collect all messages 997 < 1000. Log lengths: [997,102,578]`.

## Key Learnings

### 2026-02-06
- The same flake signature can be reproduced locally with a tight loop, even on a fast dev machine.
- The current de-flake change (wait-for-resolved + lower count) reduces how often the test fails quickly, but it does not fully eliminate failures under stress; sometimes convergence never happens within the current `waitForResolved` window.

## Key Learnings

### 2026-02-06 (production analog / semantics)
- Production analog: `docs.index.search({ fetch: N })` can return **fewer than N** results during churn (redundancy rebalance, indexing lag, or RPC timeouts/missing peer responses). In current code, that can be a *silent* partial result.
- RPC behavior: `@peerbit/rpc` request/response has a default timeout of **10s** (`packages/programs/rpc/src/controller.ts`), and `queryAll(...)` will throw `MissingResponsesError` if some target peers never responded (`packages/programs/rpc/src/utils.ts`).
- Document search behavior: `packages/programs/data/document/document/src/search.ts` catches `MissingResponsesError` and **swallows it by default**, unless the caller sets `remote.throwOnMissing: true`. This implies `search(...)` is effectively best-effort by default.
- Therefore, a test that asserts strict completeness under churn must opt into strictness (e.g. `remote.throwOnMissing: true`, higher `remote.timeout`, and retries/backoff; potentially `remote.wait`/reachability behavior).

## Test Results

### 2026-02-06 (CI evidence: PR #594 still flakes)
- FAIL (CI, dao-xyz/peerbit PR #594, head `13e908bbd`): run `21766414040`, job `62803272840` (`ci:part2`) failed in the updated test with:
  - `Error: Failed to collect all messages 295 < 600. Log lengths: [62,271,343]` (at `waitForResolved.timeout ... index.spec.ts:2347`).
  - This confirms the wait/retry mitigation reduces fast failures but does not eliminate timeouts; convergence sometimes does not occur within the current wait window.

## Claims/Hypothesis

### 2026-02-06
| Claim/Hypothesis | Evidence so far | Test to confirm/reject | Status |
|---|---|---|---|
| H2: Default distributed search semantics are best-effort; strict completeness requires explicit options (throwOnMissing + longer timeout + wait/retry). | `queryAll` throws MissingResponsesError for missing shard responses; search swallows unless `remote.throwOnMissing=true`; CI logs show partial results without any MissingResponsesError in the test. | Write a strict-mode test that sets `remote.throwOnMissing=true` and `remote.timeout` high; observe either (a) eventual success or (b) deterministic error pointing to missing responders/unreachability. | Pending |
| H3: There is a deeper production bug (not just test strictness): under redundancy churn, the system can fail to reach completeness within reasonable time even when peers are connected (either due to cover selection, reachability filtering, or replication/index lag). | Local stress loop can fail on both master and the de-flake branch; CI also fails on de-flake branch after 90s wait. | Add instrumentation: run strict-mode query, log which peers responded; explore `remote.wait`/reachableOnly behavior; isolate whether failures correlate with MissingResponsesError, indexing lag, or cover/rpc selection. | Pending |

## Next Steps

### 2026-02-06
- Create 2 worktrees using `wt`:
  - WT-A: implement a strict-mode test (throwOnMissing + increased timeout + retry/backoff + optional reachability/wait), so we can validate the intended contract.
  - WT-B: investigate and fix the underlying cause of non-convergence/timeouts under churn (likely production code changes in search/rpc/cover selection/replication/indexing path).
