---
"@peerbit/shared-log": patch
---

Bound the native coordinate mutation-generation map with a hold-counted settle.

The map previously grew one row per distinct entry hash ever committed or
received and had no removal path, so it was monotone in lifetime throughput for
the life of a SharedLog instance. Each row is now hold-counted: taking a
rollback snapshot takes one hold per hash, and the token is settled at every
point where it can no longer be rolled back, deleting the row at zero holds.
Rollback semantics are unchanged - the generation gate still makes a superseded
rollback a strict no-op - and a token that is never settled simply retains its
row, which is the previous behavior.
