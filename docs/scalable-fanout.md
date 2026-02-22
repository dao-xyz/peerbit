# Scalable Fanout PubSub (Tree + Repair + Incentives)

This doc is both:
- a **parent GitHub issue draft** (copy/paste section), and
- a **living engineering spec** for making Peerbit’s pubsub fanout economical and reliable at large scale.

---

## Parent Issue (draft)

**Title:** Scalable fanout pubsub over `@peerbit/stream` (tree + pull-repair + incentives)

### Goal
Ship a pubsub fanout solution that can support **very large audiences** (target: 1 publisher → 1,000,000 subscribers) with bounded per-node upload, low latency, and measurable reliability — **without requiring global membership knowledge**.

### Motivation
Current pubsub subscriber discovery can “explode” because the control-plane scales poorly (subscription gossip amplification). At large scale, the system must:
- avoid `to=[all subscribers]`
- avoid global ACKs
- avoid any per-message work that grows with total subscribers

### Requirements
**Functional**
- 1 publisher can publish at 30 msg/s (configurable), and subscribers receive messages with bounded latency.
- Delivery works without global knowledge of subscribers.
- Nodes can configure **upload limits** and will not exceed them (best-effort within simulation and later production).
- Nodes can express **relay preferences** (e.g. “only relay if compensated / bid-based selection”).

**Reliability**
- Define explicit delivery goals per workload, e.g.:
  - “live”: > 99% delivered within deadline under mild churn/loss
  - “reliable”: > 99.9% delivered eventually with bounded overhead
- Repair must be bounded and local (neighbors/parent only).

**Economics / incentives**
- Simulate “relay earnings” based on forwarded bytes.
- Define a future-proof interface for bids/quotes (even if settlement is out-of-scope initially).

**Engineering**
- Provide a deterministic, local simulation harness to test 1k–10k nodes on one machine:
  - measure delivery ratio, p50/p95/p99 latency, bandwidth overhead, queue/backpressure, and “earnings” distribution.
- Add CI-friendly “small sim” tests that assert invariant thresholds.

### Non-goals (initially)
- Full on-chain settlement / proofs-of-forwarding (design hooks only).
- Byzantine security guarantees at 1M (spam/DoS protection is tracked separately).

### Proposed approach (high-level)
Adopt a **Plumtree-inspired** architecture:
- **Tree push** as the steady-state data plane (economical bandwidth).
- **Local pull repair** (and/or gossip summaries) as the reliability layer.
- **Capacity-aware admission control**: each relay accepts children within an upload budget and may prefer higher bids.

### Milestones (suggested order)
1. Benchmark harness: in-memory libp2p + real `@peerbit/stream` data-plane.
2. Tree overlay join protocol with upload caps + bid-based selection.
3. Pull repair: parent caches window, children request missing seqs, bounded budgets.
4. Re-parenting on overload/churn: detect staleness and re-attach.
5. “Cost effectiveness”: overhead factor vs ideal tree, fairness/earnings metrics.
6. CI gates: small sims with threshold assertions.
7. Productionization plan: integrate into `@peerbit/pubsub` as an optional mode.

### Acceptance criteria (simulation)
For a configurable workload (e.g. 2k nodes, 30 msg/s, 10s, 1KB):
- Connected subscribers ≥ 99% (or defined threshold).
- Delivered ≥ 99% (or defined threshold).
- Overhead factor ≤ X (defined per reliability mode).
- No node exceeds upload cap by more than Y% (or best-effort with explicit backpressure/drop policy).

---

## Spec Notes

### Current implementation status (WIP)
Tracking issues:
- `dao-xyz/peerbit#586`: churn maintenance objective metrics (CI gates)
- `dao-xyz/peerbit#587`: join formation optimization (candidate scoring + formation score)

- Simulation harnesses:
  - `packages/transport/pubsub/benchmark/pubsub-topic-sim.ts` (TopicControlPlane; includes churn + CI runner via `pubsub-topic-sim-lib.ts`)
  - `packages/transport/pubsub/benchmark/fanout-tree-sim.ts` (timeouts, loss, churn, deadline+overhead CI gates, economics metrics)
- Full Peerbit integration sim:
  - `packages/clients/test-utils/benchmark/fanout-peerbit-sim.ts` (full Peerbit clients over in-memory libp2p; uses FanoutTree + blocks + pubsub services)
- Experimental production building blocks:
  - `packages/transport/pubsub/src/fanout-tree.ts` (protocol + join/repair)
  - `packages/transport/pubsub/src/fanout-channel.ts` (convenience wrapper)
  - `peer.services.fanout` + `peer.fanoutChannel(...)` / `peer.fanoutJoin(...)` (convenience APIs)
- SharedLog `target: "all"` now uses fanout as the data plane when configured (`fanout` option); there is no publish fallback from fanout back to legacy RPC send.
- SharedLog `target: "all"` now enforces fanout-only semantics: delivery settle options are only supported for `target: "replicators"`, not fanout broadcast.
- CI regression sims (small + assertive):
  - `packages/transport/pubsub/test/fanout-tree-sim.spec.ts`
  - `packages/transport/pubsub/test/pubsub-topic-sim.spec.ts`
- Nightly scale sims (artifacted):
  - `.github/workflows/nightly-sims.yml`

### Recent progress (2026-02-03)
- Protocol multicodec bumped to `/peerbit/fanout-tree/0.5.0` (breaking, coordinated upgrades assumed).
- Added channel-local economical unicast: `JOIN_ACCEPT` carries a route token and `UNICAST` forwards via root + tree edges (no full-network flooding).
- Added channel-level targeted send API: `FanoutChannel.getRouteToken()` + `FanoutChannel.unicast(...)`.
- Added route lookup + targeted send convenience: `FanoutChannel.resolveRouteToken(...)` + `FanoutChannel.unicastTo(...)` (resolves routes via the tree control-plane when no out-of-band token is available).
- Added filtered `"unicast"` events for `(topic, root)` in `FanoutChannel`.
- Route-token cache hardening: bounded route cache (`routeCacheMaxEntries`) + TTL (`routeCacheTtlMs`) + eviction/expiry metrics.
- Route caches warm on-demand via route queries (no periodic route announce control-plane).
- Added route-query fallback search on cache misses: parent/root can recursively query subtree branches and return first valid route (keeps lookup robust after cache expiry).
- Added route-fallback observability counters: `routeProxyQueries`, `routeProxyTimeouts`, `routeProxyFanout` (also exposed in `fanout-tree-sim` output).
- `JOIN_REJECT` now optionally includes redirect candidates so bootstraps/relays can steer joiners when full/not-attached.
- Join loop throttling: cache bootstrap neighbors + tracker candidate results to avoid query storms at scale.
- Join scaling: `bootstrapMaxPeers` option to limit how many bootstrap trackers each node dials/queries (join + tracker announce/refresh).
- Join stability at scale: per-round attempt cap + candidate cooldown to avoid long sequential timeouts and hot-spotting.
- Join formation: candidate scoring mode (`candidateScoringMode`) for parent selection (ranked-shuffle|ranked-strict|weighted).
- In-memory sim correctness: fixed deterministic public key hash collisions once `nodes >= 256` (enables accurate 1k–10k sims).
- Data-plane perf: forwarding no longer re-signs at each hop (forwards the original signed `DataMessage` bytes).
- Live control: optional `maxDataAgeMs` to drop forwarding of stale data (deadline-oriented workloads).
- Sim speed: `fanout-tree-sim` uses mock signing/verification so crypto doesn’t dominate large local sims.
- Sim observability: `fanout-tree-sim` reports tree shape + stream queue/backpressure metrics.
- Control-plane split (slice 1): extracted topic-root discovery into a standalone `TopicRootControlPlane` module (explicit roots, deterministic fallback, resolver + tracker hooks) and wired `TopicControlPlane` to delegate topic-root resolution through it.
- Removed duplicate topic-root convenience methods from `TopicControlPlane`; callers now use `topicRootControlPlane` directly for topic→root concerns.

### API sketch (current)
This is intentionally **separate from FanoutTree data-plane channels** so existing “normal pubsub” use-cases (RPC, many-writers, small groups) remain unchanged under `TopicControlPlane`.

- As a Peerbit user you now have a dedicated service: `peer.services.fanout` (type: `FanoutTree`).
- Convenience: `peer.fanoutChannel(topic, root)` creates a `FanoutChannel` bound to `peer.services.fanout` and `peer.fanoutJoinAuto(topic, ...)` can resolve `root` via the topic-root control plane.
- Bootstrapping/rendezvous:
  - Call `peer.bootstrap()` (recommended) to dial bootstrap servers **and** configure the same bootstrap list for `peer.services.fanout`.
  - Or call `peer.services.fanout.setBootstraps([...multiaddrs])` directly if you want a custom rendezvous set.
  - Joiners can send best-effort feedback to trackers (e.g. “candidate full/unreachable”) so trackers converge faster than TTL alone.
- Minimal usage pattern:
  - **root** calls `openChannel(topic, rootId, { role: "root", ... })` then `publishData(...)`
  - **subscribers/relays** call `joinChannel(topic, rootId, { ... })` and listen on `"fanout:data"`
- Convenience wrapper: `FanoutChannel` wraps `(topic, root)` + filters events for that channel.
- SharedLog broadcast path (`append(..., { target: "all" })`) now requires a configured fanout channel (`log.open({ fanout: { ... } })`) and uses fanout as the transport path. `fanout.root` can be provided explicitly or resolved via the topic-root control plane if omitted.

### Breaking-phase scope (what this PR does and does not do)
- This branch/PR (`#582`) hardens the fanout tree data plane and control-plane for large fanout channels.
- Runtime defaults now wire pubsub through `TopicControlPlane`, so control-plane implementation can evolve without changing client wiring.
- Internal peer/server/test wiring now instantiates `TopicControlPlane` by default.
- Added standalone `TopicRootControlPlane` + `TopicRootDirectory` in `@peerbit/pubsub` as the explicit topic→root control-plane primitive (deterministic candidate hashing + override hooks).
- `TopicControlPlane` still carries generic pubsub/control-plane semantics because core paths depend on `PubSub` behavior today:
  - program lifecycle topic wiring (`@peerbit/program`)
  - RPC topic request/response transport (`@peerbit/rpc`)
  - non-fanout topic workflows in document/shared-log paths
- Full “fanout-only runtime defaults” still requires an explicit root-discovery/control-plane split, not just a constructor swap.
- Follow-up tracking:
  - `#586` and `#587` (fanout/scaling follow-up issues)

Current upload shaping knobs (WIP):
- `uploadLimitBps` + `maxChildren` still define *admission* capacity.
- Runtime shaping uses a per-channel token bucket in the forwarding path:
  - `uploadOverheadBytes` (default 128) tunes how conservative we are vs real framed bytes.
  - `uploadBurstMs` (default ~1 message interval) bounds burstiness.
- Under sustained overload, `allowKick` enables the relay to kick low-bid children so they can re-attach elsewhere (instead of silently dropping forever).
- `joinChannel(..., { staleAfterMs })` can be used to trigger re-parenting if a parent stalls.
- Optional neighbor-assisted repair (WIP):
  - enable `neighborRepair` to allow nodes to request missing payloads from a small set of other peers (not only the current parent).
  - when configured with a small `neighborMeshPeers`, nodes keep a tiny "lazy repair mesh" and exchange `IHAVE`-style cache summaries to pick better fetch targets.
  - `neighborRepairBudgetBps` / `neighborRepairBurstMs` can bound `FETCH_REQ` control-plane overhead.

### Design principles
- **No global membership**: delivery is via bounded fanout overlay (tree/mesh).
- **No global ACKs**: reliability via local pull repair and bounded caches.
- **Bounded per-node work**: each node has fixed-degree children + fixed repair budgets.
- **Deadline-aware**: “live” workloads may drop late data instead of repairing indefinitely.

### Metrics to report (bench + CI)
- Delivery: delivered/expected, duplicates, missing after deadline, tail loss.
- Latency: p50/p95/p99/max (with bounded sampling).
- Bandwidth: total payload forwarded, total framed bytes (transport), overhead factor vs ideal tree.
- Control-plane: tracker/join/repair bytes, and bytes-per-delivered-payload budgets (bpp).
- Backpressure: queued bytes, dropped forwards, time-in-queue.
- Economics: earnings (bid * bytes), distribution (p50/p95/p99), churn impact.

Current sim knobs worth calling out:
- **Global timeout:** `--timeoutMs` (all sims)
- **Standard workloads:** `fanout-tree-sim --preset live|reliable` (pins a comparable workload configuration)
- **Peerbit integration presets:** `fanout-peerbit-sim --preset ci-small|ci-loss|live|reliable|scale-1k`
- **Scale benchmark:** `fanout-tree-sim --preset scale-5k` (5k nodes with assertions for join/delivery/overhead)
- **Scale benchmark (bigger):** `fanout-tree-sim --preset scale-10k` (10k nodes, intended for local profiling)
- **Join scoring:** `fanout-tree-sim --candidateScoringMode ranked-shuffle|ranked-strict|weighted`
- **Live deadline metric:** `--deadlineMs` reports delivery-within-deadline (in addition to eventual delivery)
- **Profiling:** `--profile 1` collects CPU, memory, and event-loop delay stats
- **Data overhead factor:** reported as `sentPayloadBytes / idealTreePayloadBytes` (ideal = `subscribers * messages * msgSize`)
- **Control-plane budgets:** `--assertMaxControlBpp`, `--assertMaxTrackerBpp`, `--assertMaxRepairBpp`
- **Data loss:** `--dropDataFrameRate` drops low-priority stream data frames (payload), keeping control-plane reliable.
- **Churn:** `fanout-tree-sim` supports `--churnEveryMs`, `--churnDownMs`, `--churnFraction` (temporary offline peers).

Practical note:
- `pubsub-topic-sim` with real subscription gossip can intentionally “explode” at scale (lots of control/data traffic). For large local sims prefer `fanout-tree-sim`, or run topic-sim with `--subscribeModel preseed` to isolate the data plane.

### Next steps (near-term)
- Add re-parenting policy that is deadline-aware (detect “slow parent” vs “dead parent” and switch cleanly).
- Add stronger CI gates: max peak upload vs cap (`maxUploadFracPct`) and upper bounds on repair control-plane bytes.
- Run scale profiling (1k→10k nodes locally): memory growth, CPU hotspots, event-loop lag, queue/backpressure behavior.
- Expand economics hooks: configurable relay selection (by bid, trust, or explicit “premium subscribers”) + output fairness metrics.

---

## Definition of Done (long-term acceptance criteria)

The goal is not “it works on my machine”. The goal is “we can ship and be confident it scales”.

### Protocol / architecture
- **No global membership lists** in the steady-state data plane (publisher must not build `to=[all subscribers]`).
- **Bounded per-node work**: each node’s steady-state forwarding fanout is capped (degree + repair budgets).
- **Bounded repair**: repair stays local (parent/neighbor set), and does not become global ACK aggregation.
- **Degrades gracefully**: under overload, the system should prefer controlled dropping + re-parenting over unbounded queues.

### Performance targets (initial v1 “broadcast channel”)
Define and publish a small set of standard workloads. A reasonable starting set:

**Workload A (“live”)**
- Underlay: random graph degree 6 (or real-world libp2p connection manager defaults)
- Rate: 30 msg/s, 1 KiB payload, 60s duration
- Loss: 0.5%–1% per hop data drop (configurable), no byzantine assumptions
- Churn: 0.5% nodes/min leave + join (configurable)
- Acceptance:
  - Connected subscribers ≥ **99%**
  - Delivered within deadline (e.g. 2s) ≥ **99%** of connected
  - Tail delivery (eventual, e.g. 10s) ≥ **99.9%** of connected
  - Overhead factor (payload bytes / ideal tree payload bytes) ≤ **1.3**
  - No node exceeds configured upload cap by more than **+10%** sustained (allow brief bursts)

**Workload B (“reliable”)**
- Same underlay/rate as A, but stricter reliability expectations and higher allowed overhead:
  - Delivered eventual ≥ **99.99%** of connected
  - Overhead factor ≤ **2.0**

### Scalability targets
We likely cannot CI-run 1M nodes, but “1M-ready” means:
- All steady-state mechanisms are **O(1) per node** (w.r.t. total subscribers).
- Any “global-ish” behavior is strictly limited to bootstrapping/rendezvous and is not per message.
- Simulation demonstrates stable behavior at **10k** nodes on one machine and **100k** nodes in distributed CI/bench infra (as available).

### Upgradeability / future protocol evolution
- We assume **coordinated upgrades** (e.g. bump Peerbit + bootstrap servers together), so we can keep the implementation lean:
  - **Breaking changes** → bump the multicodec and drop old versions (single-version support).
  - **Additive changes** → new message types / capability flags within the same multicodec.
- Still keep a small “canary sim” that exercises the latest protocol end-to-end before release.

### Operability
- Expose counters/gauges needed to diagnose issues:
  - join/rejoin rates, reject reasons, re-parent count
  - forwarded bytes, dropped forwards, repair requests/responses
  - queue/backpressure metrics per lane (from `@peerbit/stream`)
- Provide “assert mode” for sims/benches so we can gate PRs with thresholds (non-zero exit on regression).

---

### Open questions
- How to represent bids/quotes in the real protocol (and how to prevent Sybil/cheating)?
- Should repair be parent-only or neighbor-assisted (Plumtree “lazy” gossip summaries)?
- How should “premium” subscribers be prioritized without centralization?
- How should upload caps be enforced: drop-old, drop-new, or re-parent?
