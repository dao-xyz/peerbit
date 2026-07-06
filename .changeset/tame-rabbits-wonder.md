---
"@peerbit/log-rust": patch
---

Harden the entry codec against hostile length prefixes: bound the next-hash count read from untrusted `EntryV0` meta bytes against the input actually remaining before allocating. Previously a malformed entry declaring a huge count could trigger a multi-gigabyte allocation and abort a native node (remote DoS); it now returns a catchable error. No change to valid decoding and the wasm API is byte-identical.
