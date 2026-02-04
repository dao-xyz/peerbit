# Interactive fanout visualizer (tree vs gossip)

*February 2, 2026*

When a topic grows from 20 peers to 20,000 peers, the failure mode is rarely “the publisher is too slow”.

It is almost always **network overhead**:
- too much control traffic (membership, subscribe announcements, retries), and
- too much redundant forwarding (messages bouncing across many links even after deduplication).

In Peerbit we started calling one common symptom **“subscribe gossip exploding”**. The exact mechanics vary by protocol, but the pattern is the same: as the audience grows, the amount of “who is interested in what?” traffic grows faster than the useful payload.

This post introduces a small interactive sandbox that helps build intuition for why that happens, and why scalable broadcast systems usually split the work into two layers:

- a **control-plane** (join / discovery / capacity announcements), and
- a **data-plane** (actual message delivery with bounded per-node fanout).

## Try it

This is a small browser-friendly sandbox that runs the **real code paths**:

- `@peerbit/pubsub` (`DirectSub`)
- `@peerbit/stream` (routing + connection handling)
- over an **in-memory libp2p shim** (no sockets)

How to use it:
1. Click a node to pick the **writer** (it turns red).
2. Press **Generate network** if you want a new random topology.
3. Press **Run** to (a) set up subscriptions and then (b) publish messages.
4. Enable **Show flow** to see “comets” moving along edges as data is forwarded.

Tip: keep **Messages = 1** and **Flow speed = 1000 ms** while you are learning. It makes the pattern much easier to see.

<div class="not-prose">
  <fanout-protocol-sandbox nodes="20" degree="4" subscribers="19" messages="1" msgSize="32" intervalMs="0" seed="1"></fanout-protocol-sandbox>
</div>

## What you’re seeing

### 1) Tree push (economical, but needs repair)
In a tree overlay, each node forwards to a bounded number of children. If the tree is well formed, a single publish uses close to **N - 1** payload transmissions (roughly one per subscriber). This is what makes “1 publisher to 1,000,000 subscribers” possible in principle.

The catch is reliability. Trees break under churn and packet loss. The common state of the art pattern is not “tree only”, it is “tree push plus bounded repair”. Plumtree is the classic example of this style. It uses:
- eager push along the tree for efficiency, and
- local recovery mechanisms so one broken edge does not break delivery. [1]

### 2) Gossip flood (robust, but redundant)
In a flood or mesh gossip design, messages are forwarded across many edges. Deduplication prevents loops, but the amount of work can still be closer to “per edge” than “per subscriber”. That cost becomes very noticeable at scale.

Gossip is excellent as a repair tool (and for small to medium topics), but it is hard to make it economical for very large audiences if it is also responsible for membership and subscription dissemination. This is where “subscribe gossip exploding” shows up.

## How this maps to Peerbit work

This sandbox runs the real `DirectSub` and `DirectStream` logic over a fake in-memory transport. It skips real sockets and expensive crypto verification so you can run hundreds or thousands of nodes locally, including in the browser.

The architectural direction in Peerbit is:

- **FanoutTree** uses tracker/bootstraps for join/discovery and a bounded tree for delivery.
- Reliability is regained with **local pull repair** (and optional neighbor-assisted repair), instead of global ACKs.

In the sandbox you can also switch the subscribe setup:
- **Preseed (no subscribe gossip)**: pre-wires the writer’s view of subscribers so you can focus on the data plane.
- **Real subscribe model**: uses actual subscribe flows so you can see the control-plane cost.

## What to look for

If you want to judge whether a design is “economic”, ignore how pretty the animation is and look for these behaviors:

- Does a single publish look mostly tree shaped (few edges active) or mesh shaped (lots of edges active)?
- Does delivery succeed when you increase the node count and keep degree bounded?
- How quickly does the writer learn enough routes to deliver beyond the first hop?
- How big is the overhead factor (total bytes sent divided by ideal tree bytes)?

## Learn more

- [Scalable fanout pubsub spec](/scalable-fanout.md)
- [Fanout tree protocol](/fanout-tree-protocol.md)

## References

1. J. Leitão, J. Pereira, L. Rodrigues. “Epidemic Broadcast Trees”. SRDS 2007. https://doi.org/10.1109/SRDS.2007.4365705
2. J. Leitão, J. Pereira, L. Rodrigues. “HyParView: A Membership Protocol for Reliable Gossip-Based Broadcast”. DSN 2007. https://doi.org/10.1109/DSN.2007.56
3. libp2p. “Gossipsub” protocol specification. https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/README.md
4. A. Demers et al. “Epidemic Algorithms for Replicated Database Maintenance”. PODC 1987. https://doi.org/10.1145/41840.41841
