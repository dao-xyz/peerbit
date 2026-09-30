---
"@peerbit/pubsub": patch
---

Recover idle fanout channels whose parent stops responding while its transport still appears open. Reuse the existing join loop and parent-probe protocol, requiring two consecutive missed probes of the same attachment before rejoining. Healthy full parents remain attached, and late results cannot detach replacement channels or streams.

Bound parent probes across signing, sending and reply waits; cancel outstanding work and reject replies from the wrong or superseded stream.

Ignore delayed kicks from former parents or superseded streams so they cannot detach a replacement attachment.
