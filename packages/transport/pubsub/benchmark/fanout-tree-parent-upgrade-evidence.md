# Fanout Tree Parent Upgrade Evidence

## What this PR is testing

The current fanout tree maintenance is stability-first. A peer reparents after
disconnect, stale parent state, or explicit kick, but it does not continuously
move to a better parent once the tree has formed. That avoids churn, but it can
leave a peer behind a worse relay after a better direct or lower-level parent is
available.

This PR keeps that default unchanged and adds opt-in evidence paths for parent
upgrades:

- `direct`: use ranked tracker candidates directly.
- `probe`: require a live candidate reply before attempting the reparent.
- `shadow`: repeatedly observe one probed candidate before promotion.

`shadow` here is probe-observation shadowing. It does not duplicate the data
plane through a second parent.

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
while relay candidates keep the general `parentUpgradeMinFreeSlots` and
`parentUpgradeMaxChildLoadRatio` policy. That makes the work safer and more
meaningful, but the evidence still argues against flipping the runtime upgrade
default in this PR.

Root admission now also has a branch-value signal:
`parentUpgradeRootMinSubtreeGain`. It admits a root candidate when
`levelGain * (1 + directChildren)` is high enough. This does not require global
membership knowledge, but it distinguishes a one-leaf shortcut from a relay move
that improves several downstream peers.

The newest guards are request-aware root reservations and stale-root sampling.
When a root answers a parent probe and has spare capacity that satisfies the
requester's spare-slot margin, it can return a short-lived reservation token. The
probing peer includes that token in the proactive `JOIN_REQ`; the root counts
active reservations against free capacity and rejects invalid-token joins. This
turns the dangerous root upgrade sequence from "many peers probe the same stale
slot and race it" into "one peer gets a bounded claim, peers below the requested
margin get no token, and the claim is consumed or expires quickly." Stale-root
verification is also sampled per peer
(`parentUpgradeStaleRootProbeProbability`, default `0.125`) so an
advertised-full root is not probed by every eligible peer at once. These are
control-plane guards for proactive root upgrades; they do not change ordinary
tree formation semantics or the disabled-by-default runtime posture.

The evaluator now has an explicit default-candidate preset:
`--parentUpgradePreset default-candidate`. It evaluates the policy we would
consider for a later default flip without changing runtime defaults in this PR:
shadow mode, non-leaf upgrades allowed, stale-root live verification enabled
with `0.125` deterministic per-peer sampling,
branch-aware root admission, and stricter evidence limits
(`maxProbePerUpgrade <= 2`, `maxRootChildrenDelta <= 2`, and
`maxRootUploadPctDelta <= 1` percentage point). This turns "we tried some flags"
into a named, repeatable default-candidate contract.

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

The most important retune from the wider run is the quiet window:
`parentUpgradeQuietMs` now defaults to `5000`. A 2s or 3s live-delivery window was
too permissive under `ci-loss`: the policy could send probes or promote while
loss/repair was still settling. A 5s window makes the aggressive live-delivery
run quiet, which is safer under load but should be read as safety evidence, not
as proof of live-flow topology improvement.

Failed probe/shadow rounds apply a per-channel exponential backoff before the
peer scans again, and rejected candidates get their own adaptive cooldown. The
upgrade path uses deterministic local jitter so evidence runs do not perturb the
main join RNG. This is the key load-safety guard for constrained fanout: a peer
that repeatedly finds saturated candidates becomes quiet quickly instead of
adding periodic probe churn.

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
candidates. Stale-full root verification is now explicit opt-in via
`parentUpgradeVerifyStaleRootCapacity`; leaving it on by default caused herd-like
root probes in constrained fanout. Root reservations and stale-root sampling make
stale-root verification less bursty, but they do not remove the need for the
opt-in guard: the cheapest safe action under constrained fanout is still not to
probe an advertised-full root unless the scenario is explicitly testing
settled-topology improvement. The normal default trusts tracker capacity and
stays quiet when a root is advertised full.

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

The settled-topology run fails strict mode if p95 second-batch latency
materially regresses. The default evaluator tolerates the greater of `3ms` or
`15%` second-batch p95 timing jitter with
`--maxSecondBatchLatencyP95DeltaMs 3` and
`--maxSecondBatchLatencyP95DeltaRatio 0.15`, but still requires a promoted
branch or global p95 latency improvement and still fails larger global
regressions.

The live-stream run fails strict mode if the default-candidate policy sends any
parent probes, starts any shadow observations, or performs any proactive
reparents while the flow is active. It also requires at least one data-guard skip
and preserves deadline delivery within `--maxLiveDeadlinePctDelta 1`, so the
evidence is interpreted as "guarded and quiet under load," not topology
improvement.

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

The evaluator separates `promoted` runs from `guarded` runs. A guarded run sent
probes but made no parent move, so it is useful as a safety/cost check, not as
proof of topology improvement. Topology improvement criteria are applied only
when a mode actually promotes a new parent; guarded runs are judged on delivery,
control/tracker/repair cost, maintenance churn, orphan area, and root pressure.
`ci-idle-upgrade` is stricter than the safety scenarios: strict mode fails unless
it produces at least one useful promotion, improves the promoted branch or global
second-batch p95 latency, keeps probe-to-upgrade ratio at `<= 2`, and keeps max
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
- A larger 90-node settled-topology run exposed the remaining sender-pressure
  issue: stale-root sampling at `0.25` still produced useful branch gains, but
  allowed root children to grow by `4` in two seeds, failing the
  default-candidate `maxRootChildrenDelta <= 2` gate. Retuning the default
  stale-root sample rate to `0.125` passed `ci-idle-upgrade-large`, seeds
  `1,2,3`, with `3/3` promoted, `5` total upgrades, `5` probes, average global
  second-batch p95 delta about `-1.0ms`, worst global p95 delta `+2ms`, average
  promoted-branch gain about `12.3ms`, average branch coverage about `9.7%`, max
  root-child delta `2`, max root upload delta about `0.02%` of cap, and max `1`
  reparent per peer.
- `ci-live-stream`, seeds `1,2,3`,
  `--parentUpgradePreset default-candidate --strict 1`: passed. This scenario
  exposes late root connectivity during the active 300-message stream while
  churn is running. The treatment made `0` proactive upgrades, sent `0` parent
  probes, started `0` shadow observations, and recorded data-guard skips in all
  seeds (`166`, `171`, `174`). Deadline-delivery deltas were `+1.31`, `-0.44`,
  and `+0.05` percentage points, within the live-flow material-jitter gate. The
  aggregate shape was `3/3 no-op`, average control bpp delta about `-1.2%`, max
  root-child delta `1`, max root upload delta about `0.10%` of cap, and max `4`
  total maintenance reparents per peer from churn/disconnect handling, not
  proactive upgrades.
- The simulator now reports root upload pressure separately from max relay/root
  upload pressure. This matters for streamer-like workloads: root-child fanout
  count is a useful structural signal, but root upload percentage is the direct
  pressure signal for deciding whether proactive root moves are safe.
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
  candidates, which prevented the evidence run from consuming every apparently
  available root upgrade slot. Contested seeds now show explicit `rootPressure`
  and shadow capacity rejects once the root crosses that conservative load cap.
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
  produce a better current candidate. `6` became too conservative and produced
  no useful promotion in one positive-case seed; `4` still showed timing-sensitive
  global p95 movement. The current evidence candidate remains `3` with branch
  metrics and explicit material-regression tolerance.
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
- for live-stream scenarios, zero proactive probes/shadow starts/reparents
  while the data guard is active
- for idle-upgrade scenarios, promoted-branch or global second-batch p95 latency,
  while keeping global second-batch p95 inside the material-regression tolerance

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
spare-capacity hysteresis, race-aware root reservations, sampled stale-root
verification at `0.125`, and branch-aware root admission.
This PR intentionally leaves `parentUpgradeIntervalMs: 0` as the default.
