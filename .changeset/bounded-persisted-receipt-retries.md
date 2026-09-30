---
"@peerbit/shared-log": patch
---

Keep persisted-receipt response waits bounded independently of confirmation and
transfer work, so a dropped request can retry before the delivery deadline.
Adapt the response wait only after an unanswered request, and back off repeated
valid responses that make no receipt progress without changing ingress limits,
session and ownership checks, or durable receipt requirements.
