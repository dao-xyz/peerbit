---
"@peerbit/react": patch
---

Let Node exit while browser-style identity lock keep-alive timers are active,
and serialize the package's process-global storage/timer tests so coverage jobs
cannot hang after every assertion has passed.
