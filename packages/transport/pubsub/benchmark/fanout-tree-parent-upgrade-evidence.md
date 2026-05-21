# Fanout Tree Parent Upgrade Evidence

## What this PR is testing

The current fanout tree maintenance is stability-first. A peer reparents after
disconnect, stale parent state, or explicit kick, but it does not continuously
move to a better parent once the tree has formed. That avoids churn, but it can
leave a peer behind a worse relay after a better direct or lower-level parent is
available.

This PR keeps proactive parent upgrades disabled by default and adds opt-in
evidence paths for parent upgrades:

- `direct`: use ranked tracker candidates directly.
- `probe`: require a live candidate reply before attempting the reparent.
- `shadow`: repeatedly observe one probed candidate before promotion.

When parent upgrades are enabled without an explicit mode, `shadow` is now the
runtime default. Shadow cutover is make-before-break by default for
incomplete/live channels: after the candidate accepts `JOIN`, the old parent is
kept until the candidate delivers live data or the bounded dual-path window
expires. The active data guard still prevents proactive active-flow upgrades
unless callers opt into that risk.

## Current conclusion

The missing behavior is real: the tree can stay locally suboptimal because parent
choice is too passive after formation. Direct proactive upgrades are not a safe
default under constrained fanout because tracker capacity can be stale by the
time many peers try to move. Probe mode fixes part of that problem by asking the
candidate for live state, but a single healthy probe is still a weak signal under
loss, repair, and churn.

The useful candidate in this branch is bounded shadow observation: probe one
candidate, require repeated healthy observations, require a wide spare capacity
margin (`parentUpgradeMinFreeSlots`, default `8`), reject candidates that are
unrooted, full, high child-pressure
(`parentUpgradeMaxChildLoadRatio`, default `0.5`), repairing, lagging,
overloaded, or not enough of a level improvement, and only then promote. Direct
root upgrades have a separate stronger threshold
(`parentUpgradeRootMinLevelGain`, default `3`) because the root is also the
topic sender and extra direct children can increase source fanout pressure. Root
candidate spare-slot margin and child-pressure can also be tuned separately with
`parentUpgradeRootMinFreeSlots` and `parentUpgradeRootMaxChildLoadRatio`. The
root child-pressure default is now `min(parentUpgradeMaxChildLoadRatio, 0.4)`,
with an additional effective `0.2` cap when a peer is maintaining multiple local
channels. Relay candidates keep the general `parentUpgradeMinFreeSlots` and
`parentUpgradeMaxChildLoadRatio` policy. That makes the work safer and more
meaningful, but the evidence still argues against flipping the runtime upgrade
default in this PR.

The latest default-candidate hardening adds a signal-based post-promotion grace:
after shadow promotion, the previous parent remains attached until the candidate
continues to win fresh data, otherwise the peer rolls back and leaves the
candidate. The default live proof window now requires `32` fresh
candidate-first messages and a material candidate-vs-current-parent lead before
promotion. This is stronger than immediate cutover and stronger than
"candidate arrived first": a move must prove that the alternate path is
meaningfully faster during dual-path observation before it can add root fanout.
The material lead threshold is currently `128ms` average candidate-first lead for
the default shadow proof; lower-lead moves are treated as weak evidence and do
not promote.

Root admission now also has a branch-value signal:
`parentUpgradeRootMinSubtreeGain`. It admits a root candidate when
`levelGain * (1 + directChildren)` is high enough. This does not require global
membership knowledge, but it distinguishes a one-leaf shortcut from a relay move
that improves several downstream peers.

The newest guards are request-aware root reservations and rotating stale-root
sampling.
When a root answers a parent probe and has spare capacity that satisfies the
requester's spare-slot margin, it can return a short-lived reservation token. The
probing peer includes that token in the proactive `JOIN_REQ`; the root counts
active reservations against free capacity and rejects invalid-token joins. This
turns the dangerous root upgrade sequence from "many peers probe the same stale
slot and race it" into "one peer gets a bounded claim, peers below the requested
margin get no token, and the claim is consumed or expires quickly." Stale-root
verification is also sampled with a small per-peer base probability
(`parentUpgradeStaleRootProbeProbability`, default `0.015625`) so an
advertised-full root is not probed by every eligible peer at once. A branch peer
gets a bounded sample boost when this local peer carries downstream children:
multi-channel branch peers cap at `0.03125`, ordinary single-channel
branch-gain candidates cap at `0.2`, and stronger single-channel subtree-gain
candidates cap at `0.25`. Quiet, completed single-channel leaves get a `0.5`
stale-full root verification budget so a positive idle case is not lost to the
tiny base sample. Settled leaves participating in multiple writer trees require
three signals before spending default stale-root probe budget: a fresh tracker
response advertising root capacity, recent slow parent-delivery evidence from a
completed batch, and a low-rate deterministic sample. Callers can still raise
`parentUpgradeStaleRootProbeProbability` explicitly to opt into more
multi-channel pressure. Direct-root promotions also face the stricter
multi-channel root child-pressure cap. These are control-plane guards for
proactive root upgrades; they do not change ordinary tree formation semantics or
the disabled-by-default runtime posture.

The evaluator now has an explicit default-candidate preset:
`--parentUpgradePreset default-candidate`. It evaluates the policy we would
consider for a later default flip without changing runtime defaults in this PR:
shadow mode, leaf-only upgrades, stale-root live verification enabled with
`0.015625` deterministic rotating base sampling, tracker-fresh plus
outcome-gated stale-root probing for settled multi-channel leaves, branch-aware
root admission, make-before-break shadow cutover (`parentShadowDualPathMs=5000`,
`parentShadowDualPathMinMessages=32`, average candidate lead at least `128ms`),
and stricter evidence limits
(`maxProbePerUpgrade <= 8`, `maxRootChildrenDelta <= 2`, and
`maxRootUploadPctDelta <= 1` percentage point). This turns "we tried some flags"
into a named, repeatable default-candidate contract. Because default
multi-channel stale-root verification is now conservative and signal-gated, a
no-op run is valid default-readiness evidence; any run that does send proactive
upgrade traffic must still satisfy the utility and pressure limits.

Root reservation tokens now also remember the requester's spare-slot margin. A
token protects only one child slot from overfill, but the root rejects the token
at `JOIN_REQ` time if intervening joins have reduced free capacity below the
margin that justified the upgrade probe. That prevents a stale token from
turning an "upgrade only while the root is wide open" decision into a near-full
root join.

Root probe replies now count pending root reservations as child pressure. That
means the child-load guard evaluates `actual children + already-reserved upgrade
slots + this candidate`, instead of letting several concurrent probes all pass
against the same stale root child count.

After a root consumes a proactive reservation, it briefly advertises zero free
slots to trackers. This is a short-lived opt-in dampener for the upgrade path:
the root still applies its normal join rules, but other settled peers stop
chasing a root slot that was just consumed by a proactive move.

The evidence output now separates root pressure from generic slot failures.
`candidateSlots` still shows coarse capacity rejection, while
`candidatePressure`, `rootPressure`, and root reservation `marginRejected`
explain whether the guard blocked a move because the probed parent was too busy
or because a previously valid root margin disappeared before the join. This is
important for deciding defaults: a no-promotion run can now be read as guarded
root-pressure behavior instead of an opaque lack of upgrades.

Shadow observations now use observe-only root probes until the peer can actually
promote. A first shadow observation can validate root capacity without minting a
reservation token; the peer requests a reservation only on the probe that can
lead directly to a `JOIN_REQ`. This avoids holding scarce root slots for
observation windows that may never promote.

The positive case is intentionally harder now. `ci-idle-upgrade` forms a
constrained relay tree, finishes the finite data stream, then exposes a partial
better root underlay and bounded root capacity. It also publishes a second batch
after the upgrade window, so the evaluator can distinguish a prettier final tree
from a real delivery win. The current settled-topology result is positive for the
opt-in policy: stale-root shadow probing improves promoted downstream branches
and average tree depth with bounded per-peer churn. The evidence still does not
justify flipping the runtime default in this PR, because live lossy/constrained
flows remain guarded by the data/quiet policy rather than proving safe proactive
movement during active repair and churn.

`ci-live-stream` covers that guarded live-flow posture directly. It keeps the
first publish batch active for several seconds, applies the late better-root
topology while messages and churn are still active, and then requires the
default-candidate policy to do zero proactive upgrade work: no parent probes, no
shadow observations, and no proactive reparents. The run must still show
data/repair/quiet guard skips, so a pass means the policy intentionally stayed
silent under load rather than merely failing to find candidates.
The simulator now snapshots parent-upgrade counters at the end of the active
publish phase (`publishActiveParentUpgrade`) and reports deltas from the start
of publishing, so this check distinguishes
"nothing happened while data was live" from work that might happen later during
settle time. The active snapshot includes data, repair, and quiet guard skips,
plus active probes, shadow starts, shadow promotions, and proactive reparents.

Shadow mode now has a bounded active-flow cutover path. When
`parentUpgradeDataGuard` is disabled, a peer can attach the candidate as a
temporary second parent without leaving the old parent. Promotion is allowed
only after fresh candidate-first data arrives during the dual-path window
(`parentShadowDualPathMinMessages`, default `32` in shadow mode), the candidate
beats the current parent by a bounded message-count margin, and the same window
records candidate-vs-current-parent first-arrival lead samples averaging at
least `128ms`. After promotion, the previous parent remains attached as a backup
until the candidate continues to win fresh data; if the previous parent wins the
grace sample or the deadline expires, the peer rolls back to the previous parent
and sends `LEAVE` to the candidate. If no fresh candidate-first proof arrives
before the initial deadline, the peer sends `LEAVE` to the candidate, keeps the
old parent, and defers another upgrade attempt until a full fresh-data sample
window has passed.
This make-before-break-and-rollback gate is now part of the default-candidate
policy; the separate active-flow runner forces the guard open only to prove the
mechanism.

Focused local runner evidence for the active dual-path case:

```bash
node packages/transport/pubsub/dist/test/fanout-tree-sim.runner.js '{"nodes":42,"bootstraps":1,"subscribers":32,"relayFraction":0.5,"candidateScoringMode":"weighted","joinConcurrency":1,"joinPhases":true,"joinPhaseSettleMs":300,"messages":180,"msgRate":60,"msgSize":128,"streamRxDelayMs":1,"settleMs":2000,"deadlineMs":1000,"timeoutMs":60000,"trackerQueryIntervalMs":500,"repair":true,"rootUploadLimitBps":100000000,"relayUploadLimitBps":100000000,"rootMaxChildren":2,"relayMaxChildren":4,"dropDataFrameRate":0,"churnEveryMs":0,"lateRootConnectAfterMs":750,"lateRootDuringPublish":true,"lateRootMaxChildren":12,"lateRootConnectFraction":0.6,"parentUpgradeIntervalMs":250,"parentUpgradeLeafOnly":false,"parentUpgradeMinLevelGain":1,"parentUpgradeRootMinLevelGain":1,"parentUpgradeRootMinSubtreeGain":1,"parentUpgradeNonRootMinLevelGain":1,"parentUpgradeMinFreeSlots":0,"parentUpgradeRootMinFreeSlots":0,"parentUpgradeMaxChildLoadRatio":1,"parentUpgradeRootMaxChildLoadRatio":1,"parentUpgradeCooldownMs":500,"parentUpgradeQuietMs":0,"parentUpgradeRepairQuietMs":0,"parentUpgradeMaxPerPeer":1,"parentUpgradeRepairGuard":false,"parentUpgradeDataGuard":false,"parentUpgradeStaleRootProbeProbability":1,"parentProbeTimeoutMs":300,"parentProbeMaxPerRound":1,"parentProbeMaxLagMessages":100,"parentShadowObserveMs":0,"parentShadowMinObservations":1,"parentShadowDualPathMs":1000,"parentShadowDualPathMinMessages":1}'
```

The latest local readiness run produced `5` active shadow promotions, `100%`
delivery, `100%` deadline delivery, `321` duplicates, and `1.056x` data
overhead. That is a positive mechanism test, not default-readiness proof. The
ordinary
`ci-live-stream` default-candidate strict run still requires zero active probes,
zero shadow starts, zero active promotions, and zero proactive reparents.

The PR fanout gate now includes this active dual-path scenario as a bounded
mechanism check. It intentionally omits `parentUpgradeMode` and
`parentUpgradeVerifyStaleRootCapacity`; the assertions require the opt-in
runtime default to resolve to shadow mode with bounded stale-root verification.
It requires at least one proactive reparent and at least one active shadow
promotion, while still bounding delivery and data overhead. This keeps the
default-candidate safety gate separate from the forced active-flow mechanism
gate.

The shared-network multi-writer evaluator extends that default-readiness check
across several independent writer-root trees in one in-memory network. Writers
publish on distinct topics, subscribers overlap across writer trees, and the
same peers, trackers, and timers carry all trees concurrently. This matters
because per-tree behavior can look safe while aggregate writer/topic pressure
still multiplies probes, root fanout, and control traffic. The multi-writer
evidence keeps the same runtime default-off posture and evaluates the same
default-candidate policy only as an opt-in treatment.

The multi-writer evaluator now treats a promoted tree as useful only when it
shows a meaningful paired signal: at least `10ms` of promoted-branch or
second-batch p95 latency gain, or at least `0.1` average tree-level gain. This
avoids counting near-zero movements as evidence for default enablement.

For local default-readiness validation before pushing, run:

```bash
pnpm run test:fanout:parent-upgrade
```

For a heavier multi-seed soak, run:

```bash
pnpm run test:fanout:parent-upgrade:soak
```

`ci-multi-live-churn` adds the missing shared-network stress case: several
writer-root trees publish concurrently while ordinary churn and late root
connectivity happen in the same process. Its strict gate treats the run as a
no-work safety check. It requires zero parent probes, zero shadow observations,
zero proactive reparents, at least one active guard skip, and bounded active
guard wakeups (`<= 1` per subscriber slot by default). Delivery and root-shape
deltas are still printed, but they are not strict failures for this scenario
when the upgrade policy did no proactive work because reconnect timing alone can
move those numbers under churn.

`ci-multi-video-live` adds the high-payload live-stream safety case. It uses
larger messages and a constrained root upload budget, then exposes late root
connectivity while writers are actively publishing. The expected default
candidate behavior is still no proactive work: `0` probes, `0` shadows, `0`
proactive reparents, active guard skips on every seed, unchanged root children,
and bounded root-upload percentage delta.

The most important retune from the wider run is the quiet window:
`parentUpgradeQuietMs` now defaults to `5000`. A 2s or 3s live-delivery window was
too permissive under `ci-loss`: the policy could send probes or promote while
loss/repair was still settling. A 5s window makes the aggressive live-delivery
run quiet, which is safer under load but should be read as safety evidence, not
as proof of live-flow topology improvement.

Failed probe/shadow rounds apply a per-channel exponential backoff before the
peer scans again, and rejected candidates get their own adaptive cooldown. The
ordinary initial reject cooldown remains `10s`, then backs off exponentially up
to `60s`; when the same peer is maintaining multiple local channels, the first
reject cooldown is floored at `20s`. The upgrade path uses deterministic local
jitter so evidence runs do not perturb the main join RNG. This is the key
load-safety guard for constrained fanout: a peer that repeatedly finds saturated
candidates becomes quiet quickly instead of adding periodic probe churn.

Active data/repair guards now also defer the next parent-upgrade scan by the
quiet/repair window and back off repeated active-flow guard deferrals. This keeps
long live streams from waking every peer/channel at a fixed interval just to
rediscover that data is still flowing.

Upgrade checks are also phase-jittered per peer. The first opt-in upgrade check
is no longer synchronized across all joined peers, and later checks use a bounded
jitter around the configured interval. This reduces periodic probe/promote waves
without changing the default-off behavior.

Shadow observation also avoids early re-probes: once a candidate has been chosen
for observation, the peer waits until the observation window is due before
probing it again. For completed finite streams, a direct root candidate with a
fresh successful probe can be promoted immediately because there is no in-flight
data to protect. The root min-gain guard keeps this fast path limited to tail
peers that are far enough from the root to justify increasing root fanout.

Live probe rejects also feed the existing tracker-feedback path when the probe
proves a candidate is full or no longer rooted. Reserve-margin and child-pressure
rejects stay local because the tracker can only represent coarse capacity, not
the stricter proactive-upgrade admission policy.

Tracker-advertised capacity remains a protective pre-filter for ordinary
candidates. Stale-full root verification is enabled only for the shadow-mode
parent-upgrade default and remains bounded by deterministic sampling, live
capacity probes, reject cooldowns, and root reservations. Direct and probe modes
still trust tracker capacity unless `parentUpgradeVerifyStaleRootCapacity` is
explicitly enabled. The cheapest safe action under constrained fanout is still
not to join an advertised-full root directly; the default path may only sample
and promote through the shadow/probe admission path.

## Reproduce the comparison

Quick single-seed mode comparison:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-small --seeds 1 --compareModes 1 --parentUpgradeDataGuard 0
```

Constrained mode comparison:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-constrained --seeds 1 --compareModes 1 --parentUpgradeDataGuard 0
```

Multi-seed shadow candidate run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario all --seeds 1,2,3,4,5 --parentUpgradeMode shadow --strict 1
```

Positive settled-topology run only:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-idle-upgrade --seeds 1,2,3,4,5 --parentUpgradePreset default-candidate --strict 1
```

Larger settled-topology pressure run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-idle-upgrade-large --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Long-running live-stream safety run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-live-stream --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Default-candidate suite:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario all --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network multi-writer live safety run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-live --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network multi-writer live churn safety run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-live-churn --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network high-payload live-stream safety run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-video-live --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network multi-writer settled-topology run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network high writer-cardinality idle run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-sparse-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Shared-network hotspot-root idle run:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-hotspot-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1
```

Larger shared-network scale checks:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-live --seeds 1 --parentUpgradePreset default-candidate --strict 1 --nodes 80 --writers 8 --activeWriters 8 --subscribersPerTree 56
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-idle --seeds 1 --parentUpgradePreset default-candidate --strict 0 --nodes 80 --writers 8 --activeWriters 8 --subscribersPerTree 56
```

Local pre-push evidence suite:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-prepush
```

The pre-push suite runs the same bounded default-candidate evidence used for
review: single-writer `all`, live-stream safety, the large idle pressure gate
with `parentUpgradeRootMaxChildLoadRatio=0.25` and `maxRootChildrenDelta=3`,
shared-network multi-writer `all`, and the larger 80-node live/idle scale
checks. It writes raw logs plus `single-summary.tsv`, `multi-summary.tsv`,
`frontier-summary.tsv`, and `manifest.json` to
`sim-results/parent-upgrade-prepush-<timestamp>`. Use `--quick 1` for a
single-seed smoke run while iterating, and use the full command before relying
on CI to arbitrate the PR. A failing larger scale-idle run is reported as
`NON-GATING FAIL` by this wrapper for the same reason it is non-gating in
nightly: it is useful evidence, but not stable enough to block this default-off
PR.

Nightly coverage runs the default-candidate soak matrix from
`.github/workflows/nightly-sims.yml`. It keeps PR CI bounded to single-seed
gates, then runs the single-writer `all` and live-stream suites, a larger
idle-upgrade pressure variant with `parentUpgradeRootMaxChildLoadRatio=0.25`
and `maxRootChildrenDelta=3`. That larger pressure variant uses
`--maxCostRatio 1.2` because useful large-topology promotions can move very low
baseline tracker/control bpp by small absolute amounts that cross the ordinary
`1.15` default-candidate ratio. The ordinary default-candidate suites keep the
stricter `1.15` cost ratio. Nightly also runs a non-gating large-idle frontier
across root load caps `0.2`, `0.225`, `0.25`, and `0.4`. It also runs the
shared-network multi-writer live/churn/video/idle/sparse/hotspot scenarios with
seeds `1,2,3`, and bounded larger shared-network live/idle checks with `80`
nodes, `8` writers, `8` active writers, and `56` subscriber slots per tree so
all scaled writer trees contribute data and the cost denominator matches the
intended all-writers-active scenario. The larger scale-idle check is
non-gating evidence for now because global deadline and second-batch timing are
still noisy at that size even when promoted branches improve and root pressure
stays bounded. Each matrix entry uploads its own artifact so a failing scenario
preserves the seed-specific log without hiding the rest of the soak evidence.
The frontier entry writes both per-cap raw logs and a
`frontier-summary.tsv` table with viable seeds, promotions, probes, root-child
delta, root-upload delta, and failure counts.

The single-writer settled-topology run fails strict mode if p95 second-batch
latency materially regresses. The single-writer evaluator tolerates the greater
of `3ms` or `15%` second-batch p95 timing jitter with
`--maxSecondBatchLatencyP95DeltaMs 3` and
`--maxSecondBatchLatencyP95DeltaRatio 0.15`, but still requires a promoted
branch or global p95 latency improvement and still fails larger global
regressions. The shared-network multi-writer evaluator reports global p95, but
uses changed-branch p95 gain as the usefulness gate because independent writer
timers can move unrelated p95 samples by far more than the upgrade itself.

The live-stream run fails strict mode if the default-candidate policy sends any
parent probes, starts any shadow observations, or performs any proactive
reparents while the flow is active, using the active-publish counter snapshot.
It also requires at least one data-guard skip during active publishing and
preserves deadline delivery within `--maxLiveDeadlinePctDelta 2`, so the
evidence is interpreted as "guarded and quiet under load," not topology
improvement.

For shared-network multi-writer live safety runs, strict mode treats zero
probes/shadows/reparents as the primary product invariant. Delivery and cost
are still reported, but they are not hard failures when the parent-upgrade path
only performs local guard checks; otherwise async simulation jitter can fail an
evidence run even though the upgrade path sent no network traffic and made no
tree changes. If proactive upgrade traffic appears, the zero-work invariant
fails and delivery/cost comparisons are also applied.

Aggressive live-delivery experiment:

```bash
pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario all --seeds 1,2,3,4,5 --parentUpgradeMode shadow --parentUpgradeDataGuard 0 --strict 1
```

Use `--strict 1` when the run should fail CI on evidence regressions. Use
`--compareModes 1` when investigating direct versus probe versus shadow against
one shared baseline.

The evaluator now prints a final `parent-upgrade-summary` grouped by scenario
and mode. The summary compresses multi-seed runs into viable/effect counts,
total upgrades/probes, average tree-depth gain, global second-batch p95 delta,
promoted-branch p95 gain and coverage, control overhead, root fanout delta, max
root upload percentage delta, max per-peer reparent count, and failure count.
`--compareModes 1` also includes second-batch, promoted-branch, and root-upload
columns in its per-seed mode table, so direct, probe, and shadow can be compared
by actual changed-branch value and sender pressure.

The multi-writer evaluator prints `parent-upgrade-multi-summary`. Its strict
gates separate active-flow safety from settled-topology usefulness:
`ci-multi-live`, `ci-multi-live-churn`, and `ci-multi-video-live` fail if any
writer tree sends active or total proactive probes, starts a shadow observation,
or performs a proactive upgrade. In those no-proactive live runs, root-shape
deltas and guard-skip counts are still printed as observability, but they are
not hard failures because the baseline and treatment are independent async
simulations and local guard timers can legitimately fire in one run but not the
other. The churn variant additionally caps active guard wakeups per subscriber
slot so the policy cannot pass by spinning local timers under load.
`ci-multi-idle` now treats zero proactive work as a valid default-safety
outcome. If a seed does promote, aggregate probes must stay bounded by
successful upgrades, max proactive reparent per peer/channel is `1`, and every
root stays within the same per-root child and upload-pressure limits.
`ci-multi-hotspot-idle` is stricter as evidence and looser as a CI smoke gate:
it allows the same `1.2` pressure-scenario cost ratio as the large single-writer
idle pressure run, but default stale-root probing for settled multi-channel
leaves requires the fresh-tracker plus slow-parent-delivery signal, so the
expected two-batch default outcome is no proactive work unless that signal has
already been learned.
`ci-multi-sparse-idle` is a
high-cardinality pressure check: inactive writer trees must send zero
probes/shadows/upgrades, while any active-tree upgrades remain bounded by the
same cost and root-pressure gates. Global p95 latency is printed for all
multi-writer idle scenarios, but the utility gate is the changed-branch p95 or
global p95 improvement because concurrent writer timers can move unrelated p95
samples while promoted branches improve. Hotspot promoted-branch regression is
also normalized by same-run active non-promoted tree regression, because the
baseline and treatment simulations are independent and a non-upgraded writer
tree can absorb timer jitter that would otherwise be misattributed to the
promotion.
The PR Fanout Gate keeps live safety as a one-seed smoke check for
`ci-multi-live`, `ci-multi-live-churn`, and `ci-multi-video-live`, but runs the
idle default-safety gate over seeds `1,2,3`. Conservative stale-root sampling
means a multi-writer idle seed can validly do zero upgrade work; the bounded
three-seed gate catches accidental probe pressure without pushing the runtime
toward unsafe default behavior.

Latest local readiness evidence after the conservative, signal-gated
multi-channel stale-root gate and the material dual-path lead proof:

- Active shadow dual-path mechanism gate: passed with `5` active shadow
  promotions, `100%` delivery, `100%` deadline delivery, and `1.056x` data
  overhead. This is a forced opt-in mechanism check, not default-readiness
  evidence.
- Single-writer live default-candidate safety: passed as a no-op with `0`
  proactive upgrades, `0` probes, `0` shadow starts, and active data/repair guard
  skips.
- Multi-writer live default-candidate safety, seed `1`: `ci-multi-live`,
  `ci-multi-live-churn`, and `ci-multi-video-live` all passed with `0` proactive
  upgrades, `0` probes, and `0` shadow starts. Active guard skips were
  observability only.
- Multi-writer idle default-candidate safety, seeds `1,2,3`: after tightening
  the material lead proof to `128ms`, the full readiness run passed `3/3` with
  `1` useful promoted tree from `1` upgrade and `2` probes, `0` active-publish
  probes/upgrades, max root-child delta `1`, root-child delta sum `1`, max root
  upload delta `0.00` percentage points, and promoted-branch p95 gain `219ms`.
- Multi-writer sparse-idle default-candidate safety, seeds `1,2,3`: the full
  readiness run passed `3/3` as a no-op safety result with `0` probes, `0`
  shadows, `0` upgrades, inactive writer trees silent, max root-child delta `0`,
  and max root upload delta `0.00` percentage points. A separate focused strict
  repeat still showed the learned-slow-parent signal can fire usefully in this
  scenario, but the full gate's default result is intentionally conservative.
- Multi-writer hotspot-idle default-candidate safety, seeds `1,2,3`: the latest
  full readiness run passed `3/3` with `2` useful promoted trees from `2`
  upgrades and `4` probes, `0` active-publish
  probes/upgrades, max root-child delta `1`, root-child delta sum `1`, and max
  root upload delta `0.01` percentage points. This keeps the earlier bad shape
  constrained: shallow root moves must now prove learned slow-parent evidence and
  a material candidate lead before adding root pressure.
- Single-channel idle improvement evidence remains separate: the
  `ci-idle-upgrade` default-candidate suite still exercises useful promotion in
  the simpler one-writer topology, while multi-channel root-pressure improvement
  either requires a prior slow completed batch or an explicit higher stale-root
  probe probability. This is the key default tradeoff: the first batch after a
  new root shortcut appears cannot be improved without speculative probing; the
  default policy can only learn safely for later batches.

The evaluator separates `promoted` runs from `guarded` runs. A guarded run sent
probes but made no parent move, so it is useful as a safety/cost check, not as
proof of topology improvement. Topology improvement criteria are applied only
when a mode actually promotes a new parent; guarded runs are judged on delivery,
control/tracker/repair cost, maintenance churn, orphan area, and root pressure.
`ci-idle-upgrade` is stricter than the safety scenarios: strict mode fails unless
it produces at least one useful promotion, improves the promoted branch or global
second-batch p95 latency, keeps probe-to-upgrade ratio within the configured
default-candidate cap, and keeps max
reparents per peer at `<= 1`.

No-op idle runs are now reported as `effect=no-op` even when they fail the
positive-case requirement to produce a useful promotion. Second-batch latency and
deadline checks are applied only when the treatment did upgrade work. This keeps
the evidence from mislabeling independent simulation timing jitter as an upgrade
regression when the upgrade policy stayed completely quiet.

The idle evaluator reports both `promotedPeerSecondBatchLatencyP95` and
`promotedBranchSecondBatchLatencyP95`, comparing the same promoted peer hashes
and their final downstream branches in the baseline and treatment runs. This
makes the promotion-value question sharper: if promoted branches improve but
global p95 does not, the move helped too little of the tail distribution; if the
promoted branch does not improve, the candidate-selection signal itself is weak.
Idle-upgrade scenarios intentionally skip the pre-upgrade `formationScore`
failure check because that score includes initial attach-time jitter before the
late-root upgrade window. They still check final tree p95/average depth,
stretch, delivery, cost, churn, orphan area, and root fanout pressure.

## Current smoke results

Local smoke runs on this branch showed:

- Focused deterministic parent-upgrade tests pass, including late direct-root
  upgrade capability, stale-root shadow verification, candidate capacity/child
  pressure rejects, adaptive probe/shadow backoff, data/repair guards, and the
  new direct-root min-gain guard.
- Earlier `ci-small`, `ci-loss`, and `ci-constrained` multi-seed shadow runs
  stayed guarded with the default data guard and 5s quiet window. The important
  result is safety: lossy/constrained live delivery did not trigger parent
  churn.
- The stricter default-candidate suite,
  `--scenario all --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1`,
  passed. `ci-small`, `ci-loss`, and `ci-constrained` stayed no-op with `0`
  probes and `0` promotions; `ci-idle-upgrade` promoted in all `3` seeds. The
  aggregate shape was: `ci-small 3/3 no-op`, `ci-loss 3/3 no-op`,
  `ci-constrained 3/3 no-op`, and `ci-idle-upgrade 3/3 promoted` with `4`
  proactive upgrades, `4` probes, average tree-depth gain about `0.06`, average
  second-batch p95 delta about `+0.7ms`, worst second-batch p95 delta `+3ms`,
  average promoted-branch gain about `13.7ms`, average branch coverage about
  `15.7%`, max root-child delta `2`, max root upload delta about `0.02%` of cap,
  and max `1` reparent per peer.
- The latest repeat of that strict suite after the multi-channel root-pressure
  retune remained green: `ci-small`, `ci-loss`, and `ci-constrained` stayed
  no-op with `0` probes/promotions, while `ci-idle-upgrade` was `3/3` promoted
  with `4` upgrades from `4` probes, average tree-depth gain about `0.08`,
  average second-batch p95 delta `-0.3ms`, worst delta `0ms`, average
  promoted-branch gain about `15.7ms`, branch coverage about `9.3%`, max
  root-child delta `2`, and max `1` reparent per peer.
- Two-phase `ci-idle-upgrade`, seeds `1,2,3,4,5`,
  `--parentUpgradePreset default-candidate --strict 1`:
  passed. Every seed promoted, kept max `1` reparent per peer, preserved
  deadline delivery at `100%`, and improved average final tree depth. Global
  second-batch p95 deltas were within the explicit `3ms` material-regression
  tolerance (`+1ms`, `0ms`, `0ms`, `+2ms`, `-2ms`), while promoted downstream
  branches improved in every seed.
- The promoted-branch evidence from that run was the important new signal:
  seed `1` branch p95 `26ms -> 18ms` across `8` peers, seed `2`
  `34ms -> 30ms` across `12` peers, seed `3` `39ms -> 11ms` across `2` peers,
  seed `4` `34ms -> 22ms` across `7` peers, and seed `5` `36ms -> 10ms`
  across `2` peers. This is the first multi-seed evidence that the policy is
  doing useful work in the part of the tree it actually changes.
- The corresponding aggregate shape is: `5/5` viable, `10` proactive upgrades,
  `13` probes, average tree-depth gain about `0.11`, average global
  second-batch p95 delta about `+0.2ms`, worst global p95 delta `+2ms`, average
  promoted-branch gain about `16ms`, average branch coverage about `17%`, max
  root-child delta `2`, max root upload delta about `0.03%` of cap, and max `1`
  reparent per peer.
- Longer `ci-idle-upgrade`, seeds `1,2,3,4,5,6,7,8,9,10`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. The aggregate
  result was `10/10` viable, `16` proactive upgrades from `20` probes, average
  tree-depth gain about `0.07`, average global second-batch p95 delta `-0.4ms`
  with worst delta `+3ms`, average promoted-branch gain about `12ms`, branch
  coverage about `17.8%`, max root-child delta `2`, max root upload delta about
  `0.03` percentage points, and max `1` reparent per peer.
- A larger 90-node settled-topology run exposed sender-pressure sensitivity:
  stale-root sampling at `0.25` still produced useful branch gains, but allowed
  root children to grow by `4` in two seeds, failing the default-candidate
  `maxRootChildrenDelta <= 2` gate. The current candidate keeps the base
  stale-root sample rate at `0.015625`, gates settled multi-channel leaf probes
  on fresh tracker capacity plus slow parent-delivery evidence, allows a `0.2`
  ordinary single-channel branch boost and a `0.25` stronger-branch boost, gives
  quiet completed
  single-channel leaves a `0.5` stale-full root verification budget, caps
  single-channel root child pressure at `0.4`, tightens multi-channel root child
  pressure to `0.2`, and feeds root-pressure rejects back to trackers as
  short-lived no-capacity signals.
  That lets single-tree branches use up to the root-child delta evidence gate
  while preventing the boosted budget from multiplying across concurrent writer
  trees.
- The nightly 90-node pressure variant now runs the same larger topology with
  `parentUpgradeRootMaxChildLoadRatio=0.25` and `maxRootChildrenDelta=3`. A
  local seeds `1,2,3` run passed with `9` useful promotions from `13` probes,
  max root-child delta `3`, max root upload delta about `0.04` percentage
  points, average promoted-branch gain about `12ms`, and max `1` reparent per
  peer. The unmodified `0.4` root cap remains useful but too permissive for this
  stricter large-topology pressure gate.
- There is not yet one obviously safe large-idle default cap: `0.4` promotes too
  aggressively for root-child pressure, `0.2` and `0.225` keep root growth lower
  but can waste too many probes per useful upgrade, and `0.25` is the best local
  pressure compromise so far only when the large-topology root-child delta gate
  allows `+3`. Nightly therefore keeps a non-gating frontier artifact for these
  caps instead of treating one value as learned policy.
  A future default candidate should be selected only if the frontier shows
  stable useful promotions, bounded root-child/root-upload deltas, and bounded
  probes per upgrade across multiple nightly runs, not from one successful local
  seed set.
- `ci-live-stream`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. This scenario
  exposes late root connectivity during the active 300-message stream while
  churn is running. The treatment made `0` proactive upgrades, sent `0` parent
  probes, started `0` shadow observations, and the active-publish snapshot also
  reported `0` proactive upgrades, `0` parent probes, and `0` shadow starts.
  It recorded active guard skips in all seeds. Recent local runs show the active
  skips split across data and repair guards depending on where each peer is in
  the stream/repair cycle; the important invariant is still `0` active probes
  and `0` active proactive reparents.
  Deadline-delivery and repair deltas are observability for no-op live runs,
  because baseline and treatment are independent async simulations. The hard
  gate is `0` parent-upgrade traffic; any total maintenance reparents are from
  churn/disconnect handling, not proactive upgrades.
- Shared-network `ci-multi-live`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. Each run used
  `4` concurrent writer roots, `40` peers, and `112` joined subscriber slots.
  The treatment made `0` proactive upgrades, sent `0` parent probes, started
  `0` shadow observations, and active-publish counters were also `0` for
  upgrades/probes/shadow starts. Active guard skips appeared on every seed; the
  aggregate run reported `59` active guard skips, max root-child delta `0`, max
  root upload delta `0.00` percentage points, and no active proactive work.
- Shared-network churned live stream, `ci-multi-live-churn`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. The treatment
  again made `0` proactive upgrades, sent `0` parent probes, and started `0`
  shadow observations, including during the active-publish snapshot. The
  aggregate run reported `123` active guard skips and average control bpp delta
  about `-10.7%`. Root-child deltas moved under churn (`max 3`, per-seed sum
  `6`), but this run is intentionally a no-proactive-work safety gate because
  reconnect timing alone can move root shape while no upgrade probe or proactive
  reparent happened.
- Shared-network high-payload `ci-multi-video-live`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. Each run used
  `4` concurrent writer roots, `48` peers, `128` joined subscriber slots,
  `1200` byte messages, and a constrained `150000` B/s root upload budget. The
  treatment made `0` proactive upgrades, sent `0` parent probes, started `0`
  shadow observations, and active-publish counters were also `0` for
  upgrades/probes/shadow starts. Active guard skips appeared on every seed; the
  aggregate run reported `115` active guard skips, max root-child delta `0`, max
  root upload delta `0.00` percentage points, average control bpp delta about
  `-3.2%`, and no active proactive work.
- Shared-network `ci-multi-idle`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. Each run used
  the same `4` writer roots and overlapping subscriber set, but with a narrow
  initial root fanout and a bounded late-root capacity window. After adding the
  slow-parent-delivery gate and material dual-path lead proof, the focused
  full readiness default-candidate run was `3/3` viable with `1` proactive
  upgrade from `2` probes, `1` useful promoted tree, `0` active proactive work,
  max root-child delta `1`, root-child delta sum `1`, max root upload delta
  `0.00` percentage points, and promoted-branch p95 gain `219ms`.
- Larger shared-network scale checks with the earlier multi-channel stale-root
  probing behavior showed useful idle improvements but also exposed shallow
  non-useful promotions. The current default candidate keeps multi-channel
  root-pressure improvement only when a peer has learned slow parent delivery,
  receives fresh root capacity, and proves a material dual-path lead.
- High writer-cardinality idle, `ci-multi-sparse-idle`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. This run joins
  `12` writer trees but only publishes on `4`; inactive writer trees must stay
  silent. After tightening the material lead proof to `128ms`, the full
  readiness run was `3/3` viable as a no-op safety result: `0` probes, `0`
  shadows, `0` upgrades, inactive writer trees sent `0`
  probes/shadows/upgrades, max root-child delta was `0`, and max root upload
  delta was `0.00` percentage points. A separate focused strict repeat produced
  useful proactive upgrades, so this scenario still covers both useful-signal
  behavior and conservative default no-op behavior.
  Global p95 is printed here but intentionally not used as the utility gate
  because the scenario is a high-cardinality pressure/no-idle-work check.
- Hotspot-root idle, `ci-multi-hotspot-idle`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. The full
  readiness run was `3/3` viable with `2` useful proactive upgrades from `4`
  probes, max root-child delta `1`, root-child delta sum `1`, max root upload
  delta `0.01` percentage points, and average promoted-branch p95 gain
  `328.5ms`. This fixes the earlier hotspot failure shape where default
  stale-root sampling could promote shallow moves without enough changed-branch
  value.
- The simulator now reports root upload pressure separately from max relay/root
  upload pressure. This matters for streamer-like workloads: root-child fanout
  count is a useful structural signal, but root upload percentage is the direct
  pressure signal for deciding whether proactive root moves are safe.
- Late-root benchmark topology now preserves upload-derived effective child
  capacity. Earlier evidence could raise `effectiveMaxChildren` directly to the
  late root fanout value; the simulator now bounds late root effective capacity
  by the same upload budget formula used by `openChannel()`. That makes the
  positive idle cases and video live-stream safety case more realistic.
- Request-aware root reservations fixed an over-reservation issue found while
  testing stricter spare-slot margins: roots no longer mint tokens for replies
  that fall below the requester's `parentUpgradeMinFreeSlots` threshold. In the
  45-node idle run, contested seeds now report bounded reservation counters such
  as `created=3 consumed=3 blocked=2` instead of issuing unused tokens.
- Root reservations preserve the requested spare-slot margin through token
  consumption. A focused regression now rejects a token when another join arrives
  between the probe and the proactive root `JOIN_REQ`, instead of accepting the
  upgrade after the root no longer satisfies the original margin.
- Root child-pressure is now independently tunable via
  `parentUpgradeRootMaxChildLoadRatio`. Its default is capped at `0.4` for root
  candidates, with an additional effective cap of `0.2` when the local peer is
  maintaining multiple channels. Contested seeds now show explicit
  `rootPressure` and shadow capacity rejects once the root crosses that
  conservative load cap, and those rejects now feed short-lived no-capacity
  tracker updates so other peers stop chasing the same stale-full root.
- Stale-root sampling rotates over settled parent-upgrade rounds for the
  non-default multi-channel opt-in path. A fixed low sample rate reduced probe
  pressure but could leave a useful settled tree with no sampled peer in a seed.
  Rotating the deterministic sample key preserves bounded per-round pressure
  while giving explicitly opted-in quiet channels repeated chances to find a
  better root parent.
- Root spare-slot margin is independently tunable via
  `parentUpgradeRootMinFreeSlots`. This lets evidence runs require roots to be
  wider open than relays before consuming sender-side fanout capacity.
- Root level gain can now be weighted by local branch impact with
  `parentUpgradeRootMinSubtreeGain`. This is the first promotion-value guard in
  the branch: a smaller relay-level improvement can be admitted when it benefits
  the relay plus its direct children, while a single leaf still needs a strong
  individual level gain.
- The simulator now records second-batch p95 latency per peer hash, and the A/B
  evaluator compares both the exact peers that performed proactive upgrades and
  their final downstream branches. A promoted peer can improve while global p95
  is flat if it covers too little of the tail; branch metrics make that visible
  instead of hiding it inside one aggregate p95 number.
- Admission and reservation counters now distinguish generic slot rejection,
  child-pressure rejection, root-pressure rejection, and root reservation margin
  rejection. This makes guarded/no-promotion runs easier to audit under
  constrained fanout.
- `parentUpgradeRootMinSubtreeGain` threshold sweeps were useful but did not
  produce a better global threshold. `6` became too conservative and produced no
  useful promotion in one positive-case seed; `4` blocked another useful seed.
  The current evidence candidate keeps the ordinary threshold at `3`, and uses
  promoted-branch metrics plus explicit material-regression tolerance to catch
  low-value moves.
- `ci-loss`, seed `3`,
  `--parentUpgradeMode shadow --parentUpgradeDataGuard 0 --parentUpgradeQuietMs 3000 --parentUpgradeRepairQuietMs 3000 --strict 1`:
  failed with `0` promotions but `8` probes and a deadline-delivery regression.
  The issue was probe pressure during a lossy live flow, not only promotion
  churn.
- `ci-loss`, seed `3`,
  `--parentUpgradeMode shadow --parentUpgradeDataGuard 0 --strict 1`: passed
  after raising the default quiet window to 5s. The treatment sent `0` probes and
  made `0` promotions.
- `ci-small`, `ci-loss`, and `ci-constrained`, seeds `1,2,3,4,5`,
  `--parentUpgradeMode shadow --parentUpgradeDataGuard 0 --strict 1`: passed.
  In the lossy and constrained scenarios, treatment runs were no-op/guarded with
  `0` probes and `0` promotions after the 5s quiet window, spare-capacity,
  child-pressure, and stale-root verification guards. Treat this as safety
  evidence, not as topology improvement evidence; sim-level delivery/cost can
  still vary with async timing when the upgrade path does no work.

The constrained and live-loss results are intentionally interpreted
conservatively: they prove the guards can avoid unsafe movement, not that the
current shadow policy is ready as a global default. Runs are seed-controlled, but
the simulated async timers still have jitter, so decisions should come from
multi-seed direction rather than one number.

## Evidence criteria

An upgrade mode is only a candidate if it improves or preserves:

- `formationScore` in live/safety scenarios
- `treeLevelP95`
- `formationStretchP95`
- `deliveredWithinDeadlinePct`
- for live-stream scenarios, zero active-publish proactive
  probes/shadow starts/reparents while the data guard is active
- for shared-network multi-writer live scenarios, zero active and total
  proactive probes/shadow starts/reparents across all writer trees; delivery
  and cost are strict only if the treatment sends proactive upgrade traffic
- for single-writer idle-upgrade scenarios, promoted-branch or global
  second-batch p95 latency while keeping global second-batch p95 inside the
  material-regression tolerance
- for shared-network multi-writer idle scenarios, at least one useful promoted
  writer tree per seed in the positive idle scenarios, bounded probes per
  upgrade for the ordinary positive idle case, max one proactive reparent per
  peer/channel, and the same per-root fanout/upload-pressure limits. Hotspot
  idle is currently treated as a pressure/usefulness stress case rather than
  proof that the policy is ready by default.

It should be rejected or retuned if it materially worsens:

- `controlBpp`, `trackerBpp`, or `repairBpp`
- parent probes per successful proactive upgrade
- `maintReparentsPerMin`
- `maintMaxReparentsPerPeer`
- `maintOrphanArea`
- root fanout pressure, including root-child growth over baseline
- root upload pressure, including root upload percentage-point growth over
  baseline

The current default candidate, if later evidence holds across more seeds and
larger topologies, is bounded shadow upgrades with data/repair/quiet guards,
spare-capacity hysteresis, race-aware root reservations, short-lived tracker
no-capacity dampening after consumed root reservations, rotating stale-root
verification at base `0.015625`, channel-cardinality-aware branch sampling,
branch-aware root admission, and a `20s` rejected-candidate cooldown floor for
multi-channel peers. Quiet completed single-channel leaves get a `0.5`
stale-full root verification budget, but root reservations and root-pressure
gates still bound the promotion. The remaining blocker is confidence rather than
a known local failure: the latest hotspot multi-writer evidence passed, but a
default flip still needs CI plus a longer multi-seed soak across larger and
lossier topologies.
This PR intentionally leaves `parentUpgradeIntervalMs: 0` as the default.
