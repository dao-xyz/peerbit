---
"@peerbit/shared-log": patch
---

Add a cross-backend wire conformance test: a pure-native (rust) peer and an all-default (JS) peer that sync over the frozen wire must converge to byte-identical log state (identical content-addressed entry hashes, gids, values and heads), not merely the same entry count. The existing mixed-pair test asserted `log.length` only, which would not catch a native encoder emitting a subtly different but still-valid frame. Test-only.
