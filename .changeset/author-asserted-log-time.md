---
"@peerbit/log": patch
---

Document that signed entry timestamps are author-asserted, that the default Log
clock has no future-skew policy, and that admitted future timestamps can advance
later local appends. This does not change clock or admission behavior.
