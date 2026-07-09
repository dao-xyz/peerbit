# shared-log `[default, native]` conformance matrix (opt-in CI leg)

This document is the PR body / reference for the opt-in `[default, native]`
conformance leg that re-runs a curated allowlist of the **existing** shared-log
suites against the native (Rust) data plane. It is test-infra only.

## Mechanism

An env switch, `PEERBIT_SHARED_LOG_RUST_CORE=1`, flips `TestSession`
(`packages/clients/test-utils/src/session.ts`) so that every real (non
in-memory, non-external-libp2p) peer is created with the native data plane:

- **Storage + indexer** come from `createRustPeerbitOptions({ network: false })`
  (native block/any-store storage + native rust indexer), merged into the peer's
  create options **fill-only-undefined** so a spec that sets `storage`/`indexer`
  explicitly is never clobbered.
- **`sharedLogNativeDefaults`** is stamped onto the instance **after**
  `Peerbit.create`, with `nativeWireSync: undefined`. This makes a plain
  `shared-log.open()` auto-nativize the backbone + graph and use raw
  exchange-heads over the **existing JS wire** — no second (native) transport
  core. `Peerbit.create` refuses `network` together with an external libp2p, and
  a second native `DirectStream` over the same node would conflict, so the switch
  deliberately does **not** engage the native network/transport core.

A **hard native-present guard** throws (loud) if `peerbit/rust` does not resolve
to a real callable, and it sanity-probes the native indexer once (start/stop).
A missing or stubbed native build therefore surfaces as a loud error rather than
a silent false-green run on the default backend.

**Default (env-unset) runs are byte-for-byte unchanged:** `nativeMatrixConfig()`
returns `undefined` and none of the injection code executes.

The only product-ish file touched is `session.ts`, which is test infrastructure.
No shared-log `src/` product code is changed by this PR.

## What the leg covers (allowlist)

The leg is wired as `@peerbit/shared-log`'s `test:shared-log-rust-core` script
and a CI step in the `test_native` job, mirroring the stream layer's
`test:stream-rust-core`. The allowlist is a mocha `--grep` anchored to the
top-level describe titles confirmed **byte-for-byte green** under the native
backend:

| Describe (mocha title path)                                   | Native result |
| ------------------------------------------------------------- | ------------- |
| `append delivery options` (delivery.spec)                     | 11/11         |
| `join` (+ `mergeSegments`, `already but not replicated`)      | 15/15         |
| `replicate` (+ observer/replicator/mode/entry/persistance)    | 28/28         |
| `<setup> replication references` (joins-by-ref, next-blocks)  | green         |
| `<setup> replication replication one way` (1/1000/large/…)    | green*        |
| `<setup> replication replication two way` (partial synced)    | 6/6           |
| `<setup> canReplicate`                                        | 4/4           |
| `<setup> replication degree` (prune-family + commit options)  | green*        |
| `<setup> start/stop replicate on connect`                     | 2/2           |
| `<setup> sync` (manually synced entries)                      | 2/2           |

`<setup>` is `u32-simple` or `u64-iblt` (the active `testSetups`). `replicate`'s
`persistance` block includes the close→reopen restart cases, which pass thanks to
the rust-indexer reopen fix (#1019).

`start/stop replicate on connect` is the single test lifted from the otherwise
memory-only `start/stop` describe: it was native-red because a live-replicated
head was cached as a hollow lazy wrapper (its `_meta`/`_payload`/`_signatures`
stayed undefined, and `EntryV0.equals` — gated on `instanceof EntryV0` — was
asymmetric against it). Fixed by materializing the head at the entry-index read
boundary (`Entry.toMaterialized()`); see the S1 entry below. The rest of the
`start/stop` describe stays excluded for the memory-only Class-D reason (A2).

`*` = the describe is green **except** the specifically excluded tests below,
which are removed from the grep via negative lookahead (the whole
`replication degree update` sub-block is excluded as one unit).

The leg is **opt-in and non-blocking** initially (`continue-on-error: true`). It
widens as the Class-B tests below are made backend-agnostic.

## What is EXCLUDED, and why (honest catalog — no silent caps)

### Class-B — message-counter artifacts (convergence is correct)

These assert on *how many* wire messages/fetches happened, not on final
convergence. The native path converges to the same state but moves a different
number of frames (e.g. via authoritative push / different sync batching), so the
counters differ. Convergence is correct; only the counter assertion fails.

- `redundancy > only sends entries once …` (2 peers dynamic/fixed/write-after-open, 3 peers)
- `one way > it does not fetch missing entries from remotes when exchanging heads to remote`
- `replication > retries simple sync when first response is dropped`
- `leader > will consider in flight` (leader.spec — not in the allowlist describes)

**Root cause for the future widening:** `getReceivedHeads` (and the per-message
counters layered on it) is backend-coupled — it counts JS-wire receive events
that the native raw-exchange path does not emit the same way. Making these
counters backend-agnostic (or asserting on convergence instead of counts) is the
next widening step, which would fold most of Class-B into the leg.

### Class-C — over-nativization

- `replication degree > update > …` sub-block (range-rotation / prune-delay
  integration tests, e.g. `a smaller replicator join leave joins`). Under the
  full native backbone these exercise code paths the narrower native modes do not,
  and diverge. Excluded as a unit (`replication degree update`).

### Finding-B — scoping (native converges via authoritative push)

- `commit options > control per commmit put before join repairs when joiner
  request responses are dropped` (u64-iblt only). The JS path repairs by
  re-requesting dropped joiner responses; the native path instead converges via
  authoritative push, so the "responses were re-requested" assertion does not
  hold. Final state converges. All the other authoritative-repair tests in
  `commit options` are **green** and are covered by the leg.

### S1 — hollow-head parity (FIXED)

- `start/stop > replicate on connect` — **fixed and now covered by the leg.**

A HEAD entry that a native receiver live-replicates was cached in the entry
index as a lazy `PreparedRawExchangeEntry` whose `_meta`/`_payload`/`_signatures`
fields stay undefined. Reading that head (`iterator().collect()`, `getHeads(true)`,
`toArray`) returned the hollow wrapper, and `EntryV0.equals` — gated on
`other instanceof EntryV0` — was asymmetric against it (`jsEntry.equals(head)`
was `false` while `head.equals(jsEntry)` was `true`). The block itself was always
present and decodable; only the cached JS object was hollow, so the default
backend (which caches heads as full `EntryV0`) never diverged.

**Fix:** a generic `Entry.toMaterialized()` capability in `@peerbit/log` (no-op
on `EntryV0`, overridden by `PreparedRawExchangeEntry` to decode itself into its
full `EntryV0`). The entry index calls it at the read/resolve cache boundary and
writes the materialized entry back into the cache. The wire/sync fusion path
caches heads via `put` but never resolves them, so it stays lazy (the
`network-e2e-native` fusion counters — `jsEntryDecode`/`blockCopyOuts` — remain
0). The `start/stop replicate on connect` case is lifted into the allowlist; the
rest of `start/stop` stays excluded for the memory-only Class-D reason (A2).

The three prune-family tests previously lumped under this heading were a
mis-bucket — they have distinct root causes and are catalogued in their proper
classes below. They are **not** addressed by the S1 fix and remain excluded:

- `replication degree > will prune once reaching max replicas` → **A2 /
  Class-D (memory-only durability)**. See A2 below: its session
  (`TestSession.disconnected(3, …)`, line 2280) is directory-less, so the native
  block store is memory-only; the prune's durability bookkeeping cannot be
  satisfied without a persistent store. Same class as the `start/stop > can
  restart replicate` durability case, and expected to be addressed the same way
  (a directory-backed native analog) — tracked as the S2a durability follow-up.
- `replication degree > time out when pending IHave are never resolved` →
  **Class-B / Finding-B (converge-vs-timeout)**. The native path converges via
  authoritative push rather than sitting on the pending-IHave timeout the test
  asserts; final state is correct, the timing/counter assertion is not backend-
  agnostic.
- `replication degree > does not confirm checked prune from a shallow-only entry`
  → **Class-B / Finding-B (converge-vs-timeout)**. Same class: the native
  raw-exchange path confirms/converges differently than the JS wire-event counter
  the test asserts on; convergence is correct, the counter/confirmation assertion
  is backend-coupled.

Both prune divergences (the Class-D durability one and the two converge-vs-count
ones) are tracked as separate follow-ups (the S2a durability and S2b prune
issues); they are out of scope for the S1 fix.

### A2 — memory-only durability (Class-D), NOT a bug

- `start/stop > can restart replicate` fails native on reopen with
  `Failed to load entry from head`. Its `TestSession.connected(3)` session has
  **no directory** → a memory-only native block store, which loses entries when a
  store is closed and reopened.

  **Verdict (verified):** a directory-backed native analog — two peers, `db1`
  live, `db2` closed and REOPENED from an on-disk native store
  (`Peerbit.create({ directory, ...createRustPeerbitOptions() })`, the
  `durable-restart-conformance.spec.ts` pattern), then re-replicating the entry
  appended while closed (length 1→2) — **PASSES**. So the original failure is
  **expected Class-D memory-only durability**, not a real bug. Excluded from the
  allowlist and documented here.

### Also excluded

- `sync-raw` "keeps the plain live path for peers that never advertised raw" and
  restart-without-directory cases — memory-only / narrower-native-mode scope,
  same Class-C/Class-D rationale.

## Verification summary

- **Native (env set):** allowlist grep GREEN (0 failing) — see below.
- **Default (env unset):** same allowlist grep GREEN (baseline unchanged).
- **Guard:** with the native import stubbed to a non-function, the leg throws
  loudly (`… is set but the native Rust data-plane module (peerbit/rust) is not
  available. Refusing to run the "native" matrix leg …`) instead of false-green.
