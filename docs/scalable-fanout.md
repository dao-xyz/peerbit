# Scalable Fanout PubSub (Engineering Spec)

This document defines the durable architecture and test contract for large-scale
fanout pubsub in Peerbit.

Roadmap execution, prioritization, and progress tracking live in GitHub:
`dao-xyz/peerbit#577` and its child issues.

---

## Goal

Support very large fanout channels (target: 1 publisher -> 1,000,000 subscribers)
with bounded per-node upload, low latency, and measurable reliability, without
requiring global membership knowledge.

## Motivation

At scale, pubsub control planes can become the bottleneck ("subscription gossip
amplification"). The design must avoid:
- `to=[all subscribers]`
- global ACK aggregation
- per-message work that grows with total subscribers

## Scope

This spec covers:
- fanout data-plane and overlay behavior
- reliability model and bounded repair behavior
- capacity/admission constraints
- benchmark and CI measurement contracts

This spec does not track implementation status by PR/issue.

## Non-goals

- Full on-chain settlement or proofs-of-forwarding
- Byzantine security guarantees at 1M scale

---

## Architecture Baseline

Peerbit fanout follows a Plumtree-inspired shape:
- Tree push for steady-state delivery efficiency
- Local bounded repair for reliability under loss/churn
- Capacity-aware admission (upload budgets, max children, optional policy knobs)

### Design principles

- No global membership in steady-state delivery
- No global ACKs
- Bounded per-node work
- Deadline-aware operation for live workloads

### Channel and routing model

- Fanout channels are rooted and sequence-numbered
- Route-token unicast/proxy paths provide economical targeted routing inside a
  channel
- Root discovery/control is separated from delivery forwarding

See wire-level details in:
- `/Users/admin/git/peerbit/docs/fanout-tree-protocol.md`

### Topic and delivery model

- Topic delivery uses sharded overlays (`shard = hash(topic) % shardCount`)
- Broadcast path uses fanout overlays
- Explicit-recipient path uses targeted delivery semantics
- Membership knowledge is best-effort and bounded

### Runtime/API model

- Fanout service: `peer.services.fanout`
- Channel helper: `peer.fanoutChannel(topic, root)`
- Root resolution helper: topic root control plane

Minimal channel usage pattern:
- Root: `openChannel(topic, rootId, { role: "root", ... })`, then `publishData(...)`
- Subscriber/relay: `joinChannel(topic, rootId, { ... })`, consume `"fanout:data"`

### Capacity and shaping model

- Admission capacity: `uploadLimitBps` + `maxChildren`
- Forwarding shaping: per-channel token bucket and burst bounds
- Overload policy may prefer controlled drop/re-parent behavior over unbounded
  queue growth

---

## Measurement Contract

### Metrics to report

- Delivery: delivered/expected, duplicates, missing after deadline, tail loss
- Latency: p50/p95/p99/max
- Bandwidth: payload bytes, framed bytes, overhead factor vs ideal tree
- Control-plane: tracker/join/repair bytes, bytes-per-delivered-payload budgets
- Backpressure: queued bytes, dropped forwards, time-in-queue
- Economics: forwarded-byte-based earnings distributions (if enabled)

### Standard workloads and acceptance targets

#### Workload A ("live")

- Underlay: random graph degree 6 (or equivalent default topology)
- Rate: 30 msg/s, payload 1 KiB, duration 60s
- Loss: 0.5%-1% per-hop data drop
- Churn: 0.5% nodes/min leave + join

Acceptance targets:
- Connected subscribers >= 99%
- Delivered within deadline (for example 2s) >= 99% of connected
- Tail eventual delivery (for example 10s) >= 99.9% of connected
- Overhead factor (payload bytes / ideal tree payload bytes) <= 1.3
- Sustained upload cap overage bounded (allowing short bursts)

#### Workload B ("reliable")

- Same base topology/rate as Workload A

Acceptance targets:
- Eventual delivery >= 99.99% of connected
- Overhead factor <= 2.0

### Scalability criteria

1M-ready means:
- Steady-state mechanisms are O(1) per node relative to total subscribers
- Global-ish behavior is limited to bootstrap/rendezvous, not per message
- Sim evidence scales to large local runs (10k-class) and larger distributed
  benches when available

---

## Bench and CI Harness Contract

### Harnesses

- Topic control-plane sim:
  - `packages/transport/pubsub/benchmark/pubsub-topic-sim.ts`
- Fanout overlay sim:
  - `packages/transport/pubsub/benchmark/fanout-tree-sim.ts`
- Full Peerbit integration sim:
  - `packages/clients/test-utils/benchmark/fanout-peerbit-sim.ts`

### CI and regression coverage

- `packages/transport/pubsub/test/fanout-tree-sim.spec.ts`
- `packages/transport/pubsub/test/pubsub-topic-sim.spec.ts`
- `.github/workflows/nightly-sims.yml`

### Canonical knobs (for comparable runs)

- `--timeoutMs` (global run bound)
- `--preset live|reliable` (fanout-tree-sim)
- `--preset ci-small|ci-loss|live|reliable|scale-1k` (fanout-peerbit-sim)
- `--preset scale-5k|scale-10k` (fanout-tree-sim scale runs)
- `--candidateScoringMode ranked-shuffle|ranked-strict|weighted`
- `--deadlineMs`
- `--assertMaxControlBpp --assertMaxTrackerBpp --assertMaxRepairBpp`
- `--dropDataFrameRate`
- `--churnEveryMs --churnDownMs --churnFraction`

Practical note:
- `pubsub-topic-sim` can intentionally explode at scale with full subscription
  gossip; prefer `fanout-tree-sim` for large fanout validation, or run topic-sim
  with `--subscribeModel preseed` to isolate data-plane behavior.

---

## Upgrade Policy

- Coordinated upgrades are assumed
- Breaking wire changes require a multicodec bump
- Additive compatible features can evolve within a multicodec version
- Keep a canary end-to-end sim in release validation

---

## Open Questions

- Bids/quotes representation and anti-Sybil safeguards
- Parent-only versus neighbor-assisted repair defaults
- Priority handling for premium subscribers without centralization
- Preferred upload-cap policy under sustained overload (drop-old/drop-new/re-parent)

---

## Related Docs

- Protocol: `/Users/admin/git/peerbit/docs/fanout-tree-protocol.md`
- Blog/article: `/Users/admin/git/peerbit/docs/blog/2026-02-02-interactive-fanout-visualizer.md`
