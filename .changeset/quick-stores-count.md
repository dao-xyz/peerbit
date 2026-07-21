---
"@peerbit/any-store": patch
---

Track insertion-time MemoryStore byte size during mutations so repeated size checks stay constant-time, including after caller-owned buffers are resized or detached.
