# Network V2 TODO: Fanout Shards Everywhere

Last updated: 2026-02-17

This doc is the handoff for the "fanout shards everywhere" direction: **every pubsub topic maps to a bounded number of shard overlays**, and we build both broadcast and (some) directed control traffic on top of those overlays with explicit, testable tradeoffs.

If you are reading this as the next agent: start with `packages/transport/pubsub/src/index.ts` and run `pnpm run test` before touching anything.

## TL;DR

- We cannot afford "one overlay per topic" at scale (topic explosion).
- We can afford "one overlay per shard" where `shard = hash(topic) % shardCount` with `shardCount` fixed (e.g. 256).
- Pubsub uses **FanoutTree per shard** for broadcast + membership control.
- Directed delivery uses **DirectStream when explicit recipients are provided**, and can use fanout unicast ACK as an economical overlay path where possible.
- Deterministic shard roots require a stable candidate set (routers/bootstraps). Small ad-hoc nets use an auto-candidate fallback.

## Why This Exists

Historically we had two overlapping "network knowledge" planes:

- `@peerbit/stream` (DirectStream): targeted delivery + route learning (ACK traces + seek/flood).
- `@peerbit/pubsub` (FanoutTree): topic/root scoped overlay (bounded fanout tree + repair + route tokens).

V2 is about making the story coherent and predictable at scale:

- **Bound memory:** no global membership dissemination.
- **Bound overlays:** fixed number of shard overlays regardless of user-topic count.
- **Explicit knobs:** router vs leaf roles, shardCount, budgets, retry behavior.

## Current State (Code Pointers)

- Sharded pubsub control plane: `packages/transport/pubsub/src/index.ts` (`TopicControlPlane`)
  - Topic -> shard mapping
  - Join/leave shard overlays
  - Publish via shard overlays
  - Bounded `getSubscribers()` cache + `requestSubscribers()` snapshots
- Fanout overlay: `packages/transport/pubsub/src/fanout-tree.ts` + `packages/transport/pubsub/src/fanout-channel.ts`
- Root selection: `packages/transport/pubsub/src/topic-root-control-plane.ts` (`TopicRootControlPlane`)
- Shared-log (fanout delivery envelope + directed delivery preferences):
  - `packages/programs/data/shared-log/src/fanout-envelope.ts`
  - `packages/programs/data/shared-log/src/index.ts`
- "Two peers, no bootstraps" reliability fixes (auto candidates + re-announce): `packages/transport/pubsub/src/index.ts`

## The Design (What We Actually Implemented)

### 1. Shards Everywhere

Every user topic maps to exactly one shard overlay:

- `shard = hash32(topic) % shardCount`
- internal shard topic: `shardTopicPrefix + shardIndex`
- default values:
  - `shardCount = 256`
  - `shardTopicPrefix = /peerbit/pubsub-shard/1/`

This means:

- 1,000,000 user topics still means ~256 overlays.
- Each node joins only the shard overlays it needs, not the whole world.

### 2. Root Selection (Deterministic)

Shard overlays are `FanoutChannel(topic=shardTopic, root=rootHash)` where `rootHash` is resolved by `TopicRootControlPlane`.

Root resolution requires a consistent candidate set across peers, otherwise the overlay partitions.

We support two modes:

- Production/large net mode:
  - Candidates are seeded from bootstraps/routers (stable).
  - See `Peerbit.bootstrap()` which aligns fanout bootstraps and seeds candidates.
- Small ad-hoc mode (no bootstraps):
  - Candidate set starts as `[self]`.
  - As underlay peers connect, candidates expand to include those peers.
  - When candidates change, we reconcile shard channels and re-announce subscriptions so membership caches converge.

### 3. Router vs Leaf Behavior (Churn Control)

Shard overlay fanout options are split by role:

- `fanoutRootChannel`: applied when this node is the shard root.
- `fanoutNodeChannel`: applied when joining as a node.

This lets us do "leaf-only non-routers" cleanly:

- routers: `maxChildren > 0`, often `hostShards=true`
- leaves: `maxChildren=0` so they never become relays, reducing churn cascades

### 4. Membership Knowledge Is Bounded

We do not attempt to maintain global membership.

- `getSubscribers(topic)` returns:
  - self if subscribed
  - plus a bounded best-effort cache of remote subscribers learned from shard Subscribe messages
- `requestSubscribers(topic[, to])` sends a `GetSubscribers` control message on the shard overlay and receives best-effort snapshots from peers that overlap.

This is the scalable alternative to "every peer knows every subscriber".

### 5. Publish Semantics

Two paths:

- Explicit recipients (`options.mode.to`): use DirectStream delivery (targeted).
- Broadcast (`topics` only): publish once per shard overlay.

Fanout payloads carry a publisher-signed `DataMessage` embedded in the fanout payload, so message `id`, `priority`, and publisher signature semantics are preserved even though the fanout transport frames are root-signed.

## Verification (What's Green)

- `pnpm run build` passes.
- `@peerbit/pubsub` tests pass, including churn simulation.
- `@peerbit/program` tests pass (fixed shard-root candidate determinism + shard-root hosting for 2-peer tests).
- `peerbit` client test `dial waits for pubsub` now passes by:
  - defaulting shard-root candidates to `[self]` when not configured
  - expanding candidates on connect
  - reconciling shard channels + re-announcing subscriptions

## Remaining Work (What's Left)

### Required for This PR

- Ensure **full** `pnpm run test` is green (currently re-running after fixing `peerbit` pubsub dial test).
- Re-run `pnpm run build` if any further changes land.

### Near-Term Cleanup (Bloat Reduction)

- Decide whether we want to fully deprecate direct (non-sharded) subscription gossip paths.
  - Today, `TopicControlPlane.onDataMessage` still parses subscription control messages, but control is intended to live on shard overlays.
- Confirm `Peerbit.dial()` semantics:
  - Today it waits for pubsub + blocks neighbor reachability.
  - Consider also waiting for fanout neighbor streams if we want "pubsub is usable immediately after dial" to be strictly true.

### Performance / Frontier Knobs (Research + Bench)

- Tune `shardCount` and default fanout channel options across realistic workloads.
- Make router selection explicit and measurable:
  - number of routers
  - router capacity (maxChildren, uploadLimitBps)
  - join/repair aggressiveness
- Add metrics + dashboards for:
  - shard join latency
  - repair rate
  - per-node bandwidth
  - subscriber-cache hit rates / requestSubscribers rates

### Unification (Long-Term)

We still have two kinds of route knowledge:

- ACK-trace routes (DirectStream)
- Overlay route tokens / root-proxy semantics (FanoutTree)

The long-term direction is to unify these behind a single "route hint" interface:

- if we can prove a cheap path, use it
- otherwise, fall back to the shard overlay for reachability

## Commands

- Build: `pnpm run build`
- Full test: `pnpm run test`
- Pubsub only: `pnpm -s --filter @peerbit/pubsub test`
- Peerbit only: `pnpm -s --filter peerbit test`
