---
name: Scalable fanout pubsub (parent)
about: Parent issue for scalable fanout pubsub over @peerbit/stream (tree + repair + incentives)
title: "Scalable fanout pubsub over @peerbit/stream (tree + repair + incentives)"
labels: ["pubsub", "performance"]
---

This is the parent tracker for “1 publisher → very large audience” pubsub fanout.

Full spec + acceptance criteria lives in `docs/scalable-fanout.md` (copy/paste friendly).

## Status (as of 2026-02-04)
- FanoutTree multicodec: `/peerbit/fanout-tree/0.4.0` (coordinated upgrades assumed).
- Join robustness: `JOIN_REJECT` redirects + join throttling/cooldowns to prevent join storms.
- Join scaling: `bootstrapMaxPeers` option to limit how many bootstrap trackers each node dials/queries (join + tracker announce/refresh).
- Sim harness correctness: fixed deterministic public key hash collisions for `nodes >= 256` so large sims report real join/delivery ratios.
- Data-plane perf: forwarding uses the original signed `DataMessage` bytes (no per-hop re-sign); optional `maxDataAgeMs` drop for live workloads.
- Sim observability: `fanout-tree-sim` now reports tree shape + stream queue/backpressure metrics.
- Sim speed: `fanout-tree-sim` uses mock sign/verify to avoid crypto dominating large local sims.
- Full Peerbit integration sim: `packages/clients/test-utils/benchmark/fanout-peerbit-sim.ts` (real Peerbit clients over in-memory libp2p + bootstraps).
- CI regression sims: `packages/transport/pubsub/test/fanout-tree-sim.spec.ts` and `packages/transport/pubsub/test/pubsub-topic-sim.spec.ts`.
- Nightly scale sims: `.github/workflows/nightly-sims.yml` uploads artifacts for trend tracking.

## Goal
Ship an economical + reliable broadcast mode (bounded per-node upload, measurable latency/reliability) without global membership knowledge.

## Definition of Done (high level)
- CI-friendly sims gate delivery, latency deadline delivery, and overhead factor.
- Protocol stays O(1) per node (w.r.t. total subscribers) in steady state.
- Under overload/churn, system degrades gracefully (kicks/re-parenting over unbounded queues).

## Roadmap (tracked)
- [ ] Define standard workloads + thresholds (live vs reliable)
- [ ] Join/bootstrapping hardening (capacity announcements, feedback, redirects)
- [ ] Data-plane: bounded tree fanout + upload shaping
- [ ] Reliability: bounded pull repair (parent cache) + neighbor-assisted repair (IHAVE + FETCH_REQ budgets)
- [ ] Overload policy: kick/re-parent loops + fairness controls
- [ ] Economics: earnings metrics + relay preference hooks (future settlement out-of-scope)
- [ ] Scale sims to 10k+ nodes locally (perf + memory profiling)

## Notes
- `pubsub-topic-sim` with real subscription gossip can intentionally “explode” at scale; for large local sims prefer `fanout-tree-sim` (or run topic-sim with `--subscribeModel preseed` to isolate the data plane).
