# Making a leap toward large-scale P2P broadcasting with Fanout Trees

*February 2, 2026*

Sending a message to 20 peers is easy.

Sending it to 20,000 peers is where things get weird. Not because the payload is large, but because the network starts spending more effort on:
- learning who is interested,
- keeping that membership fresh under churn, and
- forwarding the same content over and over across the overlay.

In Peerbit we started calling one common symptom **subscription gossip amplification**. Different systems get there in different ways, but the smell is the same: the amount of "who is subscribed to what?" traffic grows faster than the useful payload.

This post is a more human tour of a new Peerbit network protocol we have been building: **Fanout Trees**.

It is informed by classic work like Epidemic Broadcast Trees (Plumtree-style tree + repair) [1], HyParView [2], and modern pubsub practice like libp2p Gossipsub [3], all of which build on older epidemic/gossip framing [4].
If your mental model is more "Netflix/Twitch CDN trees" or "Tor-style overlays", that is fine too. Fanout Trees live in the same design space of making large distribution networks behave politely under real constraints.

The idea is simple:
- keep delivery costs bounded per node (economic), and
- make the overlay form and heal without requiring global membership knowledge (scalable).

Below are two interactive sandboxes. One focuses on delivery (where the bytes go). The other focuses on topology formation (how the network connects in the first place).

## Demo 1: Economic broadcast delivery

Fanout Trees try to behave like a broadcast tree: each node forwards to a bounded number of children.
If the tree is healthy, one publish is close to **N - 1** payload transmissions, roughly one per subscriber.

### Try it

This sandbox runs **the real Peerbit code paths**:
- `@peerbit/pubsub` (`TopicControlPlane` + forwarding behavior)
- `@peerbit/stream` (DirectStream transport + ACK modes)
- over an in-memory libp2p shim (no sockets)

Note: this demo focuses on **forwarding behavior** (and the cost of learning "who is subscribed") on a fixed underlay graph. The actual **FanoutTree overlay protocol** (join + admission + repair) is what Demo 2 visualizes.

We skip expensive crypto verification in the demo harness so you can run hundreds or thousands of nodes locally, including in the browser. This is a measurement convenience, not a production security recommendation.

How to use it:
1. Click a node to pick the **writer** (it turns red).
2. Press **Generate network** if you want a new random topology.
3. Press **Run** to publish messages (by default the flow view focuses on the publish data plane).
4. Enable **Show flow** to see “comets” moving along edges as data is forwarded.

Tip: keep **Messages = 1** and **Flow speed = 1000 ms** while you are learning. It makes the pattern much easier to see.

<div class="not-prose">
  <fanout-protocol-sandbox nodes="20" degree="4" subscribers="19" messages="1" msgSize="32" intervalMs="0" streamRxDelayMs="1000" seed="1"></fanout-protocol-sandbox>
</div>

### What you're seeing

This is the core reason very large fanout channels are plausible in principle. If each peer only forwards to a few children, you do not melt the whole network just because the audience is huge.

As of February 2026, Peerbit validates this direction with CI-sized deterministic sims plus larger local/nightly fanout simulations (see the linked spec doc for the exact presets and thresholds). So this post should be read as an architecture + evidence-of-direction writeup, not a claim that every target scale is already production-proven.

The catch is reliability. Trees break under churn and loss. A well-established pattern in this design family is "tree push plus bounded repair":
- deliver eagerly along the tree for efficiency, and
- repair locally so one broken edge does not break delivery. [1]

What to play with:
- Try increasing `nodes` and keep `degree` bounded. Watch whether one publish lights up a small set of edges (tree-like) or lots of edges (mesh-like).
- Switch `Flow capture` to include setup and see how much traffic happens before the first publish.
- Keep `Preseed (no subscription gossip)` if you only want to study the pure delivery pattern.

If you notice that nodes receive the same payload more than once, that is not automatically "wrong". Redundancy is a dial:
- too little redundancy and a single broken edge can drop delivery,
- too much redundancy and you throttle yourself with repeated forwarding.

Fanout Trees are designed so that redundancy stays bounded. The goal is not perfect delivery in the face of arbitrary failures, it is predictable delivery cost and predictable recovery behavior.

Assumptions behind these claims:
- We are optimizing for large-scale throughput and bounded per-node cost under churn/loss, not byzantine-adversary guarantees.
- Repair is intentionally local and budgeted (parent/nearby peers), not a global ACK/repair mesh.
- Capacity limits and admission policy are part of correctness: if those are misconfigured, you can still overload specific parts of the overlay.

## Demo 2: Network formation (join + capacity)

Delivery is only half the story. Large networks need a join process that:
- converges without everyone knowing everyone, and
- respects per-node capacity (so the bootstrap node does not get crushed).

This sandbox visualizes **real FanoutTree join** over the same in-memory transport.

For simplicity, node 0 acts as both:
- the **bootstrap/tracker** (rendezvous that returns join candidates), and
- the **root** (level 0 of the tree).

In production these roles do not need to be the same node. The combined role here is a visualization simplification.

As peers join, node 0 accepts up to `rootMaxChildren`. After that it starts rejecting joins and, because it also acts as the tracker in this demo, it steers joiners toward other nodes with free slots so the tree can keep growing without everyone attaching to the root.

### Try it

How to use it:
1. Press **Step join** a few times, then press **Auto**.
2. Try `rootMaxChildren=2` and `nodeMaxChildren=3` to force deeper trees quickly.
3. Observe how capacity changes the shape and depth of the tree.
4. Click a non-root node to drop it (simulate churn) and watch orphans reattach.
5. If you cut off a whole subtree, enable **Live mode (heartbeat)** so nodes detach when data stops.

<div class="not-prose">
  <fanout-formation-sandbox nodes="80" rootMaxChildren="4" nodeMaxChildren="4" joinIntervalMs="250" seed="1"></fanout-formation-sandbox>
</div>

## Where this fits in the landscape

We did not invent broadcast trees. We are adapting well-known ideas to a Peerbit setting where nodes are untrusted, churn is real, and we want per-node costs to stay bounded.

Some helpful mental comparisons:
- **libp2p Gossipsub**: a strong baseline for decentralized pubsub, but at huge audience sizes you can still run into control-plane overhead patterns that look like subscription gossip amplification. Fanout Trees aim for a delivery pattern that is closer to "one transmission per subscriber" than "one transmission per underlay edge in a mesh". [3]
- **Netflix / Twitch**: their distribution "tree" is mostly an operator-controlled hierarchy (origin -> regional -> edge) plus caching, and the last hop is client unicast (the audience does not relay). Netflix's Open Connect is a good concrete example of this pattern: a CDN delivered by peering + appliances, with centralized control-plane decisions and an operator paying the bandwidth bill. [6] Twitch's live video system has similar centralized characteristics (ingest, processing, and distribution are run by the platform rather than by viewers relaying to each other). [8] Fanout Trees are chasing a similar bounded-fanout delivery shape, but with relays selected by a join protocol and capacity limits enforced by peers rather than by a single provider.
- **Tor**: not a broadcast system, but it is a mature overlay where relays expose capacity and policy and clients build routes through multiple relays. The overlap here is operational: dialing/keeping connections, handling churn, and making capacity-aware routing decisions without melting the network. One key connection is discovery: Tor uses directory authorities to publish and agree on network status, so clients can find relays and weight paths by capacity. [7] Fanout Trees are about high-throughput fanout delivery, so the threat model and routing goals are different, but the "directory/tracker + capacity-aware route selection" idea is very much shared.
- **Iroh**: Iroh's `iroh-gossip` explicitly builds on *Epidemic Broadcast Trees* (HyParView membership + PlumTree broadcast), so it is in the same family of "tree push + bounded repair over a partial view." [5] The main differences are scope and control: Iroh's gossip is symmetric (any peer can broadcast within a topic swarm) and does not assume a designated root, while Peerbit Fanout Trees are channel-rooted and root-sequenced (publishes can be proxied upstream to a root that assigns sequence numbers). Fanout Trees also add tracker-backed, capacity-aware admission (max children, upload budgets, optional bidding) plus route tokens for economical unicast/proxy-publish inside the same overlay. The tradeoff is explicit: a root is a coordination point, so the long-term answer is multi-root sharding and/or root rotation when you want many writers at once.

When each model tends to fit better:
- Rooted fanout channels (Peerbit style): stronger control over ordering/cost envelopes for high-fanout broadcast channels.
- Symmetric topic swarms (Iroh style): simpler many-writer symmetry when you do not want one logical coordination point.

## What we are shipping in Peerbit

This post is centered around two pieces:
- **FanoutTree**: a bounded broadcast overlay with a join process that respects capacity and can re-form under churn.
- **A split between formation and delivery**: you can change join/discovery behavior without changing how payload delivery is forwarded.

How routing works in Peerbit today (important nuance):
- User topics are sharded: `shard = hash(topic) % shardCount`, and broadcast goes over that shard's fanout overlay.
- Explicit recipient pubsub delivery uses targeted DirectStream paths (not shard broadcast).
- Fanout route-token unicast/proxy paths are used for economical targeted control inside a shard/channel, with bounded fallback behavior when route resolution misses.

Status (current vs next):
- Implemented now: rooted channel sequencing, bounded fanout forwarding, join/admission controls, local repair, route-token unicast/proxy-publish, shard-based topic mapping.
- In progress: stronger reliability/perf gates and wider workload coverage in simulation and CI.
- Planned: multi-root strategies (sharding/rotation) for heavier concurrent multi-writer workloads.

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
5. Iroh. “iroh-gossip” crate documentation. https://docs.rs/iroh-gossip/latest/iroh_gossip/
6. Netflix. “Open Connect” (CDN program and appliances). https://openconnect.netflix.com/en/
7. Tor Project. “Tor Directory Protocol, Version 3” (Directory authorities and consensus). https://spec.torproject.org/dir-spec/
8. Twitch Engineering. “Ingesting Video at Worldwide Scale”. https://blog.twitch.tv/en/2024/09/13/ingesting-video-at-worldwide-scale/
