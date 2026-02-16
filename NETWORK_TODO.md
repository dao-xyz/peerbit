# Network V2 TODO (Unify Routing + Fanout Overlays)

Last updated: 2026-02-16

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
  - RPC (DirectStream) for most directed traffic: `packages/programs/data/shared-log/src/index.ts`
  - FanoutTree only for `append(..., { target: "all" })` via ExchangeHeads broadcast.

## Observed Issues / Smells

- Shared-log currently receives fanout payloads without a proper `RequestContext.message` (it uses `{ } as any`). Anything that reads `context.message.header.timestamp` will break if we move more messages onto fanout.
- FanoutTree `MSG_DATA` publish proxy currently makes the root the transport-level origin (root re-wraps payload into a new signed `DataMessage`). If the application needs the original publisher identity, it must be carried in the payload.
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

1. Make fanout-delivered program messages carry origin + timestamp (so program semantics don't depend on transport hop identity).
2. Normalize metadata across planes:
   - Every delivered message has a meaningful `timestamp` and an origin identity.
3. Introduce a "route proof" abstraction and bridge fanout tokens and ACK traces into it.
4. Move shared-log directed traffic onto fanout unicast where it is topologically cheaper than DirectStream seek/flood.
5. Reduce or delete redundant topology managers (ultimately, DirectStream routing becomes either optional or a thin adapter over the shared route store).

## Immediate Next Steps (Implementation TODO)

These are ordered to keep the PR green and create foundations for the bigger merge.

1. Fanout events expose metadata
   - Add `timestamp` (and `origin` for data events) to `FanoutTreeDataEvent` and `FanoutTreeUnicastEvent`.
   - Files: `packages/transport/pubsub/src/fanout-tree.ts`, `packages/transport/pubsub/src/fanout-channel.ts`

2. Shared-log fanout envelope (no migration/back-compat needed)
   - Wrap shared-log fanout payloads in an envelope that carries:
     - `publisher` (public key hash)
     - `timestamp`
     - `payload` (serialized `TransportMessage`)
   - On receive, reconstruct a minimal `RequestContext.message` with a real timestamp.
   - Files: `packages/programs/data/shared-log/src/index.ts` (and a small helper file)

3. Start unifying directed delivery
   - Decide whether unicast ACK belongs in:
     - FanoutTree transport (new `MSG_UNICAST_ACK`), or
     - Program-level (shared-log ACK message).
   - Goal: stop requiring DirectStream seek/flood for directed delivery inside an already-joined fanout overlay.
   - Files: `packages/transport/pubsub/src/fanout-tree.ts`, shared-log delivery tests

4. Replace subscription gossip for large topics
   - Long term: TopicControlPlane should not need to flood subscribe state to achieve reachability.
   - Candidate: "subscribe = join fanout overlay" and `getSubscribers()` becomes overlay-member-aware (bounded, not global).
   - Files: `packages/transport/pubsub/src/index.ts` (`TopicControlPlane`)

## Test / Verification Checklist

- `pnpm run test`
- `pnpm run build`
- Add/adjust tests for:
  - Fanout-delivered shared-log `target="all"` still replicates entries.
  - Fanout-delivered messages provide a usable `context.message.header.timestamp` in shared-log receive paths.
  - FanoutTree event metadata fields are populated and consistent.

