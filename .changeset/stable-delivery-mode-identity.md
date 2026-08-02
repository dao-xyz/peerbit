---
"@peerbit/stream-interface": patch
"@peerbit/stream": patch
"@peerbit/pubsub": patch
---

Classify delivery modes by their canonical Borsh wire schema so duplicate JavaScript module instances cannot silently drop targeted messages or misroute acknowledgements.
