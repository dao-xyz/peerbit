# PubSub Transport

This package contains two related building blocks that both run on top of
[`@peerbit/stream`](../stream/README.md):

## `TopicControlPlane` (`/lazysub/0.0.1`)

A DirectStream-based topic membership and forwarding layer.

This is useful for small to medium overlays and targeted messaging (explicit receiver
lists) where you can tolerate subscription gossip and do not need the strict fanout
economics of a tree.

## `FanoutTree` (`/peerbit/fanout-tree/0.5.0`)

A scalable fanout tree for 1 writer to many subscribers (and tree-local unicast using
route tokens).

This is the intended mechanism for very large audiences where you must cap per-node
upload and avoid gossip-style explosion.

`FanoutChannel` is a small convenience wrapper around `FanoutTree` for channel-scoped
usage.

## `TopicRootControlPlane`

A small helper to resolve a topic root from:

- An explicit mapping (topic -> root)
- A set of default candidates (deterministic choice per topic)
- Optional trackers (if configured by the application)
