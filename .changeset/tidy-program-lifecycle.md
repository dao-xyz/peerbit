---
"@peerbit/program": patch
"@peerbit/shared-log": patch
"@peerbit/native-backbone": patch
"@peerbit/rpc": patch
"@peerbit/string": patch
"peerbit": patch
---

Make program graph open, close, drop, and handler stop race-safe and retryable
after partial failures; preserve parent/child ownership through rollback; fence
concurrent initialization and teardown; and retain cleanup ownership until all
terminal work completes. Lifecycle `onClose` and `onDrop` callbacks now run after
base child teardown and the closed-state transition, are awaited, and retry when
they reject; subclass cleanup performed after awaiting `super.close()` or
`super.drop()` can still follow those callbacks. Immediate reentry into the
owning handler stop or current terminal method now rejects, while synchronous
delegation to a captured pre-replacement wrapper is unwrapped safely only for the
same operation and owner. Cross-operation, changed-owner, and after-yield stale
wrapper cycles reject before mutation. Parent teardown also restores missing
inverse ownership edges and recognizes only validated stale-edge repair as
progress. After lifecycle code has yielded, it must schedule stop or terminal
work from its external owner rather than await its own teardown. SharedLog, RPC,
and StringIndex now preserve their resources for non-terminal owner releases and
invalid owners. RPC also becomes network-inert after a committed base close or
drop error and checkpoints subscription and listener cleanup for exact retry.
Interrupted native persistence drops can now resume their durable tombstone on
the same adapter generation. A markerless failed drop keeps ordinary native
persistence work fenced while still permitting destructive retry, and close
retries resume the first incomplete flush/store-close stage without flushing a
generation already admitted for drop.
