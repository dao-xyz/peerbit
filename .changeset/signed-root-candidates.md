---
"@peerbit/pubsub-interface": minor
"@peerbit/pubsub": minor
"@peerbit/stream": minor
---

Authenticate automatic topic-root candidate provenance with bounded, expiring self-claims on the dual-stack 2.1 control protocol. Claims use a canonical 90-second signed lifetime and refresh every 30–35 seconds; the 30-second accepted clock skew makes the receiver-relative worst case 120 seconds, enforced by a monotonic deadline while the process is running. A process-lifetime replay floor retains the highest accepted timestamp for each of the lowest 64 origins, so expiry or a rolled-back wall clock cannot re-admit an identical or older claim. Lower origins deterministically displace higher floors. Once 64 floors exist, any unknown origin above the retained maximum—including a displaced origin or a higher newcomer—remains ineligible until restart, so honest churn can leave fewer than 64 active automatic candidates. This bounds memory but does not make auto mode Sybil-resistant; hostile deployments should use explicit candidates or an authority-backed resolver. Because a newer canonical claim always expires after an older one, replaying an older claim cannot extend candidate admission under a stable wall clock after process state is cleared; a restart necessarily trusts the host wall clock again. Expiry removes a candidate immediately instead of waiting behind candidate-update batching, and a connected 2.1 node never substitutes an unsigned self-candidate if local claim refresh fails.

Add `expiresInMs` to `@peerbit/stream` publish and message creation options so callers can derive a signed expiry exactly from the captured header timestamp.

A directly connected legacy 2.0 peer can contribute only its authenticated stream identity, not transitively advertised hashes. Upgrade relays first or configure explicit candidates during rolling upgrades because sparse mixed-version auto-mode topologies cannot forward 2.1 claims through legacy relays. Claims prove key ownership, freshness, and configuration scope—not current liveness, authorization, deployment identity, or Sybil resistance—so hostile deployments should configure explicit candidates or an authority-backed resolver.
