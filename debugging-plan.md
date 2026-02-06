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
