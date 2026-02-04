# Interactive fanout visualizer (tree vs gossip)

*February 2, 2026*

This post introduces a small interactive sandbox that helps build intuition for **why “subscribe gossip exploding” happens**, and why scalable broadcast designs typically separate:

- a **control-plane** (join / discovery / capacity announcements), and
- a **data-plane** (actual message delivery with bounded per-node fanout).

## Try it

This is a small browser-friendly sandbox that runs the **real code paths**:

- `@peerbit/pubsub` (`DirectSub`)
- `@peerbit/stream` (routing + connection handling)
- over an **in-memory libp2p shim** (no sockets)

Click a node to set the **writer** (red), then press **Run**.

<div class="not-prose">
  <fanout-protocol-sandbox nodes="20" degree="4" subscribers="19" messages="1" msgSize="32" intervalMs="0" seed="1"></fanout-protocol-sandbox>
</div>

## What you’re seeing

### Tree push (economical)
In a tree overlay, each node forwards to a bounded number of children. If the tree is well-formed, one message needs on the order of **N−1** transmissions (one per subscriber), which makes “1 → 1,000,000” *possible in principle*.

### Gossip flood (redundant)
In a gossip-style flood, a message is forwarded along many edges. Even with deduplication, the network can do work closer to **O(E)** per message, which becomes expensive at scale and can amplify bursts (especially when control-plane membership churn is also being gossiped).

## How this maps to Peerbit work

This sandbox runs the real `DirectSub`/`DirectStream` logic (with a fake/in-memory transport). The larger architectural direction in Peerbit is still:

- **FanoutTree** uses tracker/bootstraps for join/discovery and a bounded tree for delivery.
- Reliability is regained with **local pull repair** (and optional neighbor-assisted repair), instead of global ACKs.

If you want to go deeper:
- [Scalable fanout pubsub spec](/scalable-fanout.md)
- [Fanout tree protocol](/fanout-tree-protocol.md)
