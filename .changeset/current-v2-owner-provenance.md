---
"@peerbit/shared-log": patch
---

Track bounded, current-session V2 remote-owner provenance, invalidate it across lifecycle, session, transport, recovery, and clock fences, and publish it only after ownership mutations fully finalize inside the global mutation lane. This volatile evidence is for future placement capture only and carries no custody or prune authority.
