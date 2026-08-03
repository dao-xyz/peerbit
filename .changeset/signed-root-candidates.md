---
"@peerbit/pubsub-interface": minor
"@peerbit/pubsub": minor
---

Authenticate automatic topic-root candidate provenance with bounded, expiring self-claims on the dual-stack 2.1 control protocol. Claims use a maximum 90-second signed lifetime and refresh every 30–35 seconds; the 30-second accepted clock skew makes the receiver-relative worst case 120 seconds. Expiry removes a candidate immediately instead of waiting behind candidate-update batching.

A directly connected legacy 2.0 peer can contribute only its authenticated stream identity, not transitively advertised hashes. Upgrade relays first or configure explicit candidates during rolling upgrades because sparse mixed-version auto-mode topologies cannot forward 2.1 claims through legacy relays. Claims prove key ownership, freshness, and configuration scope—not current liveness, authorization, deployment identity, or Sybil resistance—so hostile deployments should configure explicit candidates or an authority-backed resolver.
