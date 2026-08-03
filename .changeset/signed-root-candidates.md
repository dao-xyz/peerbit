---
"@peerbit/pubsub-interface": minor
"@peerbit/pubsub": minor
---

Authenticate automatic topic-root candidate provenance with bounded, expiring self-claims on the dual-stack 2.1 control protocol. A directly connected legacy 2.0 peer can contribute only its authenticated stream identity, not transitively advertised hashes; sparse mixed-version topologies must use explicit candidates until their relays are upgraded. Claims prove key ownership, freshness, and configuration scope—not liveness, authorization, or Sybil resistance—so hostile deployments should configure explicit candidates or an authority-backed resolver.
