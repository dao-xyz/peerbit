# @peerbit/any-store

`@peerbit/any-store` provides Peerbit's in-memory and Level-backed `AnyStore`
implementations.

## Crash-safe checkpoints

The opt-in `@peerbit/any-store/checkpoint` entrypoint stores a bounded snapshot
in two alternating fixed keys:

```ts
import { createStore } from "@peerbit/any-store";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";

const store = createStore("./peerbit-data");
await store.open();

const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
	store,
	scope: new TextEncoder().encode("my-program/state-v1"),
	maxPayloadBytes: 1024 * 1024,
});

await checkpoint.commit(serializedState);
const reopenedState = checkpoint.current;
```

The helper reserves exactly two fixed keys in the supplied store or sublevel.
Callers must provide a trusted namespace dedicated to one checkpoint; unrelated
or adversarial writers must not share it. Every present slot is checksummed and
bound to its scope, generation parity, and predecessor. Opening fails closed on
a malformed, missing, unrelated, or ambiguous generation instead of falling
back to older state. A failed replacement has an indeterminate outcome, so that
checkpoint instance becomes terminal and must be reopened before any state is
trusted again.

Current limitations:

- Only the Node ClassicLevel-backed `LevelStore`, including its sublevels,
  exposes crash-safe atomic replacement. Memory and OPFS stores deliberately do
  not claim the capability.
- This is a single-writer primitive. It prevents overlapping commits through
  one instance, but it does not coordinate multiple processes or checkpoint
  instances.
- SHA-256 checks detect corruption and bind records; they do not authenticate
  the writer. Replaying a coherent older two-slot set or whole-database snapshot
  is undetectable without an external monotonic authority; applications that
  need rollback detection require one.
- `maxPayloadBytes` is capped at 64 MiB and bounds commits plus helper-owned
  copies after `AnyStore.get()` returns. It cannot limit how many bytes a store
  backend materializes while servicing `get()`. Storage that is itself hostile
  therefore remains out of scope until AnyStore gains a bounded-read
  capability.
- Two logical keys bound database growth, but LevelDB's physical LSM files may
  retain overwritten bytes until normal compaction.
- The helper stores opaque full snapshots. It does not truncate Peerbit logs,
  compact tombstones, or provide incremental checkpoints.
