# Network V2 TODO (Unify Routing + Fanout Overlays)

Last updated: 2026-02-17

This doc captures the current state, the core design direction we want (V2), and the next concrete implementation steps so another agent can pick up the work.

## Why This Exists

We currently maintain two partially overlapping "network knowledge" systems:

- `@peerbit/stream` (DirectStream): general-purpose message transport with multi-hop routing learned via ACK traces and seek/flood behavior.
- `@peerbit/pubsub` (FanoutTree): a topic/root scoped overlay (bounded fanout tree + repair) with its own join/repair and "route token" unicast.

That duplication is starting to feel bloaty:

- Two topology managers.
- Two kinds of "route knowledge" (ACK-learned paths vs fanout route tokens).
- Two delivery semantics in shared-log (RPC/DirectStream for directed control traffic, FanoutTree for `target="all"` broadcast).

V2 goal: one coherent story where topology/routing knowledge is shared and composed, not duplicated.

## Current State (Code Pointers)

- Topic membership + forwarding baseline: `packages/transport/pubsub/src/index.ts` (`TopicControlPlane`) extends `DirectStream`.
- Direct routing knowledge: `packages/transport/stream/src/routes.ts` (ACK/trace-based route store).
- Fanout overlay: `packages/transport/pubsub/src/fanout-tree.ts` + `packages/transport/pubsub/src/fanout-channel.ts`.
- Shared-log uses both planes today:
  - FanoutTree for `append(..., { target: "all" })` broadcast, and now also for "directed delivery w/ settle" via fanout unicast ACK where available: `packages/programs/data/shared-log/src/index.ts`
  - RPC/DirectStream remains as a fallback path for directed delivery when fanout is not configured or a unicast cannot be routed.

## Observed Issues / Smells

- FanoutTree `MSG_DATA` publish proxy makes the root the transport-level origin (root signs the DATA frame). If the application needs original publisher identity, it must be carried at the application level.
- Two independent mechanisms try to solve "how do I reach peer X?":
  - DirectStream learns ephemeral routes from ACK traces.
  - FanoutTree has route tokens scoped to `(topic, root)` and a root proxy path.

## V2 North Star

Design a single routing fabric that can support:

- Broadcast at massive scale with bounded per-node cost (Fanout-style trees).
- Directed messages with "fast path if known, safe path if not" semantics.
- Multiple traffic patterns (single-writer to huge audience, many-writer to many-reader, replication graphs) without multiplying bespoke overlays.

## Core Unification Idea

Bridge with a tiny contract:

- If a sender can produce a cheap *proof of path* to the destination (or a cheap next-hop hint), use it.
- Otherwise, fall back to an overlay that guarantees reachability (tree root proxy, limited flood, or repair mesh).

In practice, "proof of path" can come from:

- DirectStream ACK traces (what worked recently).
- Fanout route tokens (tree-derived economical unicast paths).
- Any future directory/lookup system (DHT-ish, trackers, provider indices).

The key is that all proofs reduce to the same abstraction: "for destination D, here are candidate next hops with a cost/TTL and a confidence."

## Proposed Long-Term Shape

- Underlay: libp2p streams and connection management. Keep this dumb.
- One shared route knowledge store (per node) that can ingest:
  - ACK traces (observations).
  - Overlay route tokens (structured paths).
  - Connection liveness/latency (measurements).
- Overlays:
  - A small, churn-resilient membership/repair view (HyParView-like active/passive) as the "always-on" background.
  - One or more logical fanout trees (per writer, per shard, or per channel) for economical push.
- Unicast becomes a mode of the overlay, not a separate system:
  - Prefer direct/cheap proof routes.
  - Otherwise route via overlay proxy/root.

## Multi-Writer Scenarios (What We Need To Handle)

- 2 writers -> 1,000,000 readers:
  - Two logical trees (or a forest) keyed by writer/shard. Shared repair/membership substrate.
- 100 active writers -> 100 active readers:
  - Likely not "one giant tree"; prefer small clusters and/or per-shard trees with opportunistic direct edges.
- Shared-log replicators streaming by public keys:
  - Lots of directed flows; we should not require one-off routing systems per program. The unified route store should pay off here.

## Concrete V2 Milestones

1. (Done) Make fanout-delivered program messages carry origin + timestamp.
   - Shared-log now wraps fanout payloads in an application envelope: `packages/programs/data/shared-log/src/fanout-envelope.ts`
   - Shared-log reconstructs a real `RequestContext.message.header.timestamp` when receiving fanout payloads: `packages/programs/data/shared-log/src/index.ts`
2. (Done) Normalize event metadata on fanout overlays.
   - `FanoutTreeDataEvent` and `FanoutTreeUnicastEvent` now include `timestamp` and `origin`: `packages/transport/pubsub/src/fanout-tree.ts`
3. (Done, partial) Add an economical directed path inside the fanout overlay.
   - Added `MSG_UNICAST_ACK` + `FanoutTree.unicastToAck()` and `FanoutChannel.unicastToAck()`: `packages/transport/pubsub/src/fanout-tree.ts`, `packages/transport/pubsub/src/fanout-channel.ts`
   - Added tests: `packages/transport/pubsub/test/fanout-tree.spec.ts`
4. (Done) Move shared-log directed delivery (when `delivery:true`) onto fanout unicast ACK when a fanout channel is configured.
   - Tests: `packages/programs/data/shared-log/test/delivery.spec.ts`
5. (Done, opt-in) Replace subscription gossip for large topics (pubsub delivery should not require global-ish subscriber dissemination).
   - `TopicControlPlane` now supports per-topic delivery policy: "direct" (old behavior) vs "fanout topic" (new).
   - For fanout topics:
     - `subscribe(topic)` joins the fanout overlay (no `Subscribe` gossip).
     - `publish(topic)` broadcasts via fanout overlay (no subscriber set required).
     - `unsubscribe(topic)` leaves overlay (no `Unsubscribe` gossip).
     - `getSubscribers(topic)` is explicitly bounded/best-effort (overlay-local peer hints), never global membership.
   - Default policy is conservative: fanout topics are enabled only when the topic has an explicit root set in `TopicRootControlPlane`.
     - i.e. opt-in via `topicRootControlPlane.setTopicRoot(topic, rootHash)`.
   - Code: `packages/transport/pubsub/src/index.ts`, `packages/clients/peerbit/src/libp2p.ts`
   - Tests: `packages/transport/pubsub/test/fanout-topics.spec.ts`
6. (Done) Route-proof decision point + unified route store (ACK traces + fanout route tokens as inputs to one decision point).
   - Shared-log directed delivery now prefers a proven cheap direct path (connected or routed) and falls back to fanout unicast ACK, then to RPC/pubsub routing: `packages/programs/data/shared-log/src/index.ts`
   - DirectStream now shares a single node-level session + `Routes` table across co-located protocols by default (`sharedRouting=true`), so ACK-learned routes are not duplicated per protocol: `packages/transport/stream/src/index.ts`
7. (Done, partial) Reduce/delete redundant topology managers.
   - We still have distinct overlay semantics (FanoutTree vs DirectStream), but the *node-level routing knowledge* is now shared.
   - The remaining work (if we want to go further) is to decide whether to fully deprecate DirectStream multi-hop routing for fanout-backed workloads.

## Immediate Next Steps (Implementation TODO)

These are ordered to keep the PR green while moving toward the V2 "single story" architecture.

1. Replace subscription gossip for large topics
   - Status: Implemented (opt-in fanout topics via `TopicRootControlPlane` explicit roots).
   - Follow-ups:
     - Decide whether `TopicControlPlane.publish(..., { id, priority })` should be preserved for fanout topics (FanoutTree currently assigns its own ids).
     - Clarify signature semantics for fanout-backed pubsub delivery:
       - fanout transport frames are signed by the root; original publisher identity is not preserved unless carried at application-level.
     - Add a policy knob for "publish on fanout topic without subscribing" (currently requires join; consider LRU/TTL auto-join).

2. Define the unified routing decision point (route proofs)
   - Create a small shared abstraction: `RouteHint` / `RouteProof`
     - inputs: ACK traces (DirectStream), fanout route tokens (FanoutTree), underlay metrics (RTT, liveness)
     - outputs: next-hop candidates + cost + TTL/confidence
   - Status: Implemented as a single decision point for shared-log directed delivery:
     - if a cheap direct path is known, use it
     - else use fanout unicast ACK (bounded overlay)
     - else fall back to RPC/pubsub routing (may flood for discovery)
   - Node-level route store is shared across protocols via `DirectStreamOptions.sharedRouting` (default true).
   - Files: `packages/transport/stream/src/index.ts`, `packages/programs/data/shared-log/src/index.ts`

3. Abuse resistance and cost control (make scaling predictable)
   - Status: Implemented
   - Per-hop rate limiting for proxy publish / unicast relay:
     - `proxyPublishBudgetBps`/`unicastBudgetBps` token buckets (per-child ingress), with tests
   - Cap fanout tracker state:
     - bounded tracker directory per channel + bounded namespace cache (LRU)
   - Code: `packages/transport/pubsub/src/fanout-tree.ts`, tests: `packages/transport/pubsub/test/fanout-tree.spec.ts`

4. Benchmarks
   - Status: Updated
   - `pubsub-topic-sim` now supports `--fanoutTopic 1` + `--fanoutRootIndex` to exercise fanout-backed topic delivery on the real `TopicControlPlane` code path.
   - Files: `packages/transport/pubsub/benchmark/pubsub-topic-sim-lib.ts`, `packages/transport/pubsub/benchmark/pubsub-topic-sim.ts`

## Test / Verification Checklist

- `pnpm run test`
- `pnpm run build`
- Add/adjust tests for:
  - Fanout-delivered shared-log `target="all"` still replicates entries (already covered).
  - Fanout-delivered shared-log directed delivery blocks on fanout unicast ACK when configured (already covered).
  - Pubsub large-topic mode does not require subscription gossip for reachability (new).
  - Pubsub `getSubscribers()` is explicitly best-effort/bounded and callers do not assume completeness (new).
