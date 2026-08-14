---
"@peerbit/shared-log": patch
---

Compute total participation exactly instead of sampling it 25 times per second.

The adaptive rebalance tick called `calculateTotalParticipation()` on its default
path, which Monte-Carlo estimates coverage with `appromixateCoverage({ samples:
25 })` — 25 sequential index `count()` queries per tick. The same function
already offers `{ sum: true }`, which totals `widthNormalized` across the
replication index in a single `iterate().all()`.

The two compute the same quantity: the mean coverage of the domain is the sum of
the range widths. The sampled form was an estimate of the exact one, so this
removes 25 of the tick's 27 index round-trips per open adaptive log — which is
every default-configured log, since `replicate: true` and `replicate: undefined`
both resolve to adaptive.

The replication controller now sees a stable input rather than a sampled one.
