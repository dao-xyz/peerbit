# Fanout Tree Parent Upgrade Evidence

## Scope

This PR adds guarded, opt-in parent upgrades for `FanoutTree`. Runtime behavior
remains default-off: `parentUpgradeIntervalMs` defaults to `0`. If callers opt
in to upgrades without specifying a mode, the runtime mode resolves to `shadow`,
not direct reparenting.

Default-off is a hard contract for this PR: with `parentUpgradeIntervalMs`
unset or `0`, the node must not schedule proactive upgrade checks, send parent
probes, start shadow candidates, or replace a healthy parent just because a
better edge appears. Existing disconnect, stale-parent, and kick-driven repair
paths remain the only automatic parent changes.

The new runtime options are additive opt-in knobs. The `default-candidate`
preset is intentionally benchmark/CI-only in this PR; it is not a production
default and is not imported by runtime code.

The goal of this evidence is narrower than "enable by default now":

- Prove the mechanism can improve a settled, locally suboptimal tree.
- Prove the default-candidate policy stays quiet during active/live flows.
- Bound root pressure, probe traffic, duplicate data, and per-peer reparent
  churn.
- Preserve deterministic repro paths for future seed failures.

## Review Map

The PR is intentionally broad, but the review surface has distinct buckets:

- Runtime mechanism: `src/fanout-tree.ts` and the small option re-export in
  `src/index.ts`.
- Deterministic coverage: `test/fanout-tree.spec.ts`,
  `test/fanout-topics.spec.ts`, and the sim runner/spec wrappers.
- Evidence harness: `benchmark/fanout-tree-parent-upgrade-*.ts`,
  `benchmark/fanout-tree-sim*.ts`, and
  `scripts/fanout-parent-upgrade-default-ready.mjs`.
- Automation: the PR fanout gate in `.github/workflows/ci.yml`, plus the
  nightly parent-upgrade matrix in `.github/workflows/nightly-sims.yml`.
- Small non-fanout stabilization: `packages/programs/data/shared-log/src/index.ts`
  awaits internal index shutdown during close. This is not part of the fanout
  mechanism; it is kept because part-7 stability is a gating risk for this PR.

## Policy Under Test

The named evaluator preset is:

```bash
--parentUpgradePreset default-candidate
```

That preset represents the candidate policy for future default-on discussion. It
uses:

- `shadow` mode.
- Leaf-only proactive upgrades.
- Live probe verification for stale root tracker state.
- Deterministic low-rate stale-root sampling.
- Request-aware root reservation tokens.
- Separate root pressure and root free-slot guards.
- Branch-aware root admission through `parentUpgradeRootMinSubtreeGain`.
- Make-before-break shadow cutover with a bounded dual-path window.
- A default shadow proof requiring fresh candidate-first data and a material
  average candidate lead before promotion.

This is intentionally conservative. A no-op run under active load is valid
safety evidence. A run that sends upgrade traffic must also satisfy utility and
pressure limits.

The preset defaults live in
`benchmark/fanout-tree-parent-upgrade-preset.ts` so single evaluator, multi
evaluator, prepush, default-ready, and nightly harnesses do not drift. If future
work wants a production default policy, that should be a separate runtime
policy helper and should come with default-on evidence.

## Current Verdict

The guarded mechanism is stronger than the old passive-only behavior when it is
explicitly enabled. It can promote after learning that a candidate path is
materially better, and it rejects weak/shallow moves.

It is not default-on ready in this PR. The safe default remains no proactive
parent upgrades. The evidence supports landing the guarded mechanism and its
test/soak harness, then continuing default-readiness work with more soak data.

Default-on needs more proof across:

- Multi-writer topic pressure.
- Constrained root fanout.
- Churn, reconnect storms, delayed messages, and stale tracker announcements.
- Delivery misses, duplicate data, repair bytes, orphan area, attach latency,
  reparent churn, and root child pressure.

## Gates

The PR fanout gate runs a bounded default-readiness check plus an explicit
active dual-path mechanism check. The active mechanism check intentionally opts
out of the active data guard to prove make-before-break promotion can work; it
does not imply that active-flow upgrades are enabled by default.

The default-ready runner now passes an explicit
`--maxDataOverheadRatio 1.05` gate to the single- and multi-writer evaluators.
Those evaluators compare baseline and treatment on delivery/deadline,
control/tracker/repair bytes per delivered payload byte, data payload overhead
factor, duplicate deliveries, root children/upload pressure, orphan area,
reparent rate, and active guard skips. This keeps "better tree shape" from
masking worse data-plane redundancy.

For multi-writer idle utility runs, default-ready also allows up to two
percentage points of aggregate deadline jitter only when the evaluator has
already found a useful promotion. Delivery percentage, promoted-branch latency
regression, data overhead, control/repair cost, root pressure, and reparent
limits remain hard gates. This avoids treating saturated-runner timer noise as
a product regression while still rejecting actual delivery loss or expensive
promotion.

The PR fanout gate uploads `sim-results` artifacts. Parent-upgrade evaluators
can write compact JSON summaries through `--jsonOut`; default-ready records
single-live, multi-live, multi-idle, and slow-hotspot timing evidence so a red
gate can be inspected without scraping the raw Actions log.

`ci-loss` and the CI `fanout-tree-sim` gate cover stream data-frame loss and
churn with repair enabled. The multi-writer live-churn scenarios cover
multi-root topic pressure and require the default-candidate policy to avoid
proactive upgrade traffic on active flows except for local guard skips.

The nightly soak matrix covers:

- Single-tree live-stream no-op safety.
- Single-tree idle improvement.
- Single-tree root pressure frontier.
- Multi-writer live no-op safety.
- Multi-writer churn.
- Multi-writer idle, sparse-idle, hotspot-idle, and scale cases.

Nightly artifacts include raw logs plus summary TSVs where available.

## Local Commands

Focused fanout parent-upgrade regression cluster:

```bash
PATH=/tmp/node22/node-v22.22.3-darwin-arm64/bin:$PATH \
node ./node_modules/aegir/src/index.js run test \
  --roots ./packages/transport/pubsub -- -t node \
  --grep "late direct root edge|parent upgrades|third batch|shadow upgrades"
```

Bounded local readiness suite:

```bash
PATH=/tmp/node22/node-v22.22.3-darwin-arm64/bin:$PATH \
NODE_OPTIONS=--no-warnings \
pnpm run test:fanout:parent-upgrade -- --no-build
```

Pre-push evidence suite:

```bash
pnpm -C packages/transport/pubsub run bench -- \
  fanout-tree-parent-upgrade-prepush
```

Quick pre-push evidence suite:

```bash
pnpm -C packages/transport/pubsub run bench -- \
  fanout-tree-parent-upgrade-prepush --quick 1
```

Frontier-only evidence used by nightly:

```bash
pnpm -C packages/transport/pubsub run bench -- \
  fanout-tree-parent-upgrade-prepush --only frontier --outDir sim-results
```

## Reading Results

Important counters:

- `reparentUpgrade`: accepted proactive parent moves.
- `parentProbeReqSent`: probe control traffic.
- `parentShadowStart`: shadow candidates started.
- `parentShadowPromote`: shadow candidates promoted.
- `reparentUpgradeSkipData`, `reparentUpgradeSkipRepair`,
  `reparentUpgradeSkipQuiet`: active-flow guard evidence.
- `reparentUpgradeSkipCandidatePressure`,
  `reparentUpgradeSkipRootPressure`: pressure guard evidence.
- `parentUpgradeRootReservation*`: reservation lifecycle and stale-capacity
  evidence.

Useful outcome signals:

- Delivery and deadline delivery remain at `100%`.
- Duplicate/data overhead stays bounded.
- Root child delta and root upload percentage delta stay within configured
  limits.
- Reparent count per peer stays bounded.
- Positive idle scenarios show useful promoted trees or latency/tree-depth gain.
- Active/live scenarios show guard skips without proactive upgrade traffic.

## Review Guidance

Treat this PR as an opt-in foundation, not a default flip. The strongest reasons
to reject or revise the PR would be:

- Any default-off behavior change.
- Proactive upgrade traffic when `parentUpgradeIntervalMs` is unset or `0`.
- Direct reparenting as the implicit opt-in mode.
- Missing deterministic coverage for promotion and non-promotion.
- Evidence that shadow promotion increases root pressure or delivery misses
  beyond the configured gates.

Also treat unrelated changes as suspect. The fanout portion of this PR should
stand on opt-in behavior, deterministic regression coverage, and bounded
evidence. Any CI-stability fix outside fanout should be small and explicitly
justified, not hidden as part of the parent-upgrade mechanism.
