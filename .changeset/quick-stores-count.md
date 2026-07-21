---
"@peerbit/any-store": patch
"@peerbit/any-store-interface": patch
---

Track insertion-time MemoryStore byte size during mutations so repeated size checks stay constant-time, reject aggregate counter overflow atomically, and document AnyStore's backend-accounted size contract, including caller-owned buffers that are later resized or detached.
