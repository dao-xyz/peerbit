---
"@peerbit/pubsub": patch
---

Bound fanout state that bootstrap trackers and relays accumulate from remote peers. Per-key control metrics for keys without an open channel now live in a capped LRU. Provider control frames, which are keyed per provider namespace (often one per block), now count into a single aggregate, so a long-running relay no longer keeps metrics for every block it ever saw. Provider watch registrations are capped and dropped when the watching peer disconnects. Provider namespace names are capped, and so are per-channel ingress token buckets on long-lived roots. Counters for open channels are unchanged and remain readable after close.
