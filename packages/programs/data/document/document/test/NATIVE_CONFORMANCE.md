# document `[default, native]` conformance matrix (opt-in CI leg)

This document is the PR body / reference for the opt-in `[default, native]`
conformance leg that re-runs a curated allowlist of the **existing** document
suites against the native (Rust) data plane. It is test-infra only — no
`@peerbit/document` `src/` product code is changed. It is a mechanical clone of
the shared-log conformance leg (see
`packages/programs/data/shared-log/test/NATIVE_CONFORMANCE.md`), reusing the same
env switch.

## Mechanism

The document leg **reuses the shared-log env switch**,
`PEERBIT_SHARED_LOG_RUST_CORE=1`, which flips `TestSession`
(`packages/clients/test-utils/src/session.ts`) so that every real (non
in-memory, non-external-libp2p) peer is created with the native data plane —
native block/any-store storage **and the native rust indexer** (via
`createRustPeerbitOptions({ network: false })`, merged fill-only-undefined so a
spec that sets `storage`/`indexer` explicitly is never clobbered).

The document-specific consequence: a `Documents` store opened in the **default**
`mode:"auto"` (plain args — **no** `mode:"native"`, **no** `nativeBackbone`)
builds its generic index on `@peerbit/indexer-rust`. `DocumentIndex.init`
(`src/search.ts`) constructs the index unconditionally via
`node.indexer.scope(...).init(...)`, so on the switch peer `docs.index.index` is
a **`RustIndex`** where on the default backend it is a **`SQLiteIndex`**. The
pure comparator / sort / paging / query surface is byte-identical between the two
backends, which is exactly what this leg exercises.

A **hard in-suite guard** (`test/native-conformance-guard.spec.ts`, included in
the allowlist grep) asserts `docs.index.index.constructor.name === "RustIndex"`
when the env is set and `=== "SQLiteIndex"` when it is unset. If the native build
is missing or the switch fails to engage, the leg **refuses to false-green** on
the JS backend — the document analog of the shared-log leg's hard native-present
guard. (A/B verified: flipping the expected class makes the guard fail with the
actual `RustIndex` value under the env, proving the native backend genuinely
engages.)

**Default (env-unset) runs are byte-for-byte unchanged:** the switch code in
`session.ts` is a no-op when the env is not `1`/`true`, and the guard asserts the
SQLite backend on the baseline leg.

### One test-helper fix (backend-agnostic sync suppression)

`iterate > sort` (index.spec.ts) monkey-patches
`store.docs.log.rpc._responseHandler` to DROP the exchange-heads message as a
"omit synchronization so results are always the same (HACKY)" setup step. It
keyed the drop on `msg.constructor.name === "ExchangeHeadsMessage"` (the JS
wire). Under the switch the native backend syncs via `RawExchangeHeadsMessage`,
so the class-name-keyed drop no longer suppressed sync and the setup's
`waitForReplicator` produced non-deterministic results. The suppression is now
**backend-agnostic** — it drops **both** `ExchangeHeadsMessage` and
`RawExchangeHeadsMessage` (imported from `@peerbit/shared-log`, the same pattern
the shared-log `getReceivedHeads` / `isRepairHintExchangeHeadsMessage` fix used).
This is a **no-op for the JS suite** (`RawExchangeHeadsMessage` does not occur on
the JS wire). It is the only message-type-coupled suppression HACK in the whole
document test dir (grep-confirmed). With it, `iterate > sort` runs 17/17 green on
both backends.

## What the leg covers (allowlist)

The leg is wired as `@peerbit/document`'s `test:document-rust-core` script and a
`continue-on-error` CI step in the `test_native` job, immediately after the
shared-log rust-core step. The allowlist is a mocha `--grep` anchored to the
describe titles confirmed **byte-for-byte green** under the native backend
(comparator / sort / paging / query surface), with negative-lookahead exclusions
for the native-internal, remote-indexed, and 2-peer sync cases documented below.

| Describe (mocha title path)                        | Native  | Default |
| -------------------------------------------------- | ------- | ------- |
| `native conformance guard` (leg guard)             | 1/1     | 1/1     |
| `operations > basic` (minus 5 excluded)            | 135/135 | 135/135 |
| `operations > get`                                 | 6/6     | 6/6     |
| `operations > index`                               | 3/3     | 3/3     |
| `operations > search` (incl `fields`)              | 15/15   | 15/15   |
| `iterate > sort` (incl `close`)                    | 17/17   | 17/17   |
| `count > approximate`                              | 7/7     | 7/7     |
| `query distribution`                               | 7/7     | 7/7     |
| `returnIndexed`                                     | 1/1     | 1/1     |
| `caching`                                          | 1/1     | 1/1     |
| **Total (allowlist grep)**                         | **192** | **192** |

Native and default each run **192 passing / 0 failing** under the allowlist grep.

The leg is **opt-in and non-blocking** initially (`continue-on-error: true`). It
widens as the excluded classes below are made backend-agnostic or fixed.

## What is EXCLUDED, and why (honest catalog — no silent caps)

Every native-only failure below was triaged. **None is an index
comparator / sort / paging / query-result divergence** — the pure query surface
is byte-identical between `RustIndex` and `SQLiteIndex`, exactly as the spike
predicted (`operations search` 15/15 and `iterate sort` 17/17 pass native). The
native-only failures all live in the **write / delete / block-store** path or in
**native-internal append-path plumbing**, or in the **remote-indexed
(`resolve:false` over the wire)** path — not in query ordering. Class labels
mirror the shared-log doc.

### Class-A — del-path block-store read-back (`Missing data`) — FIXED (#1025)

The default `mode:"auto"` delete path reads the prior PUT entry's payload to
determine which document was removed: `Documents.handleChanges` ->
`getAppendOperation(entry)` -> `entry.getPayloadValue()`
(`src/program.ts`). `getAppendOperation` has an `isNativeMode()` branch that
reads from storage bytes instead, but under this switch the `Documents` program
is in **auto mode** (`isNativeMode()` is `false` — only the *indexer* is
nativized, not the document mode), so it falls through to `getPayloadValue()`,
which loads the payload block from the **native** block store and throws
`Error: Missing data` (`DecryptedThing.getValue`, `EntryV0.getPayloadValue`).
A minimal repro: single-peer `put` then `del` — the `put` succeeds, the `del`
throws. Plain `put` (no del) and query-only paths are unaffected, which is why
the query-surface describes pass.

This was a real native-vs-JS divergence in the **block-store / payload
read-back** path (the document analog of the shared-log #1021 hollow-entry
class), **not** an index-comparator divergence.

**Fixed in #1025**: `getAppendOperation` now falls back — only on the
hollow-payload `Missing data` error — to reading the raw block from the block
store by entry hash and decoding the plain operation (a no-op for the JS
backend). The four del-path tests are now **covered** (folded into the
allowlist, 188 → 192): `can add and delete`, `delete permanently`,
`reload after delete`, and
`count > approximate > returns approximate count with deletions`.

Still excluded — a **separate** divergence, not the read-back bug:

- `operations > basic > can delete without being replicator` — the
  non-replicating peer never indexes the doc, so the delete resolves to a
  `No entry with key` miss. This is a 2-peer remote/sync path issue (same family
  as Class-E below); it fails identically with and without the #1025 fix.

### Class-C — over-nativization (native-internal append-path assertions)

These tests `sinon.spy` **internal append-planning methods** and assert *which*
code path a put took (`appendLocallyValidated`, `appendLocallyPrepared`,
`commitNativeDocumentAppend`, `planLocalAppendForGid`,
`createPlainPutCommitPlan`, …). Under the switch the store runs the native
indexer but in JS *append mode* (auto, not `mode:"native"`), so these
mode-internal path assertions do not hold — a smaller/different set of the
planning methods is called, or a native-only helper (`planLocalAppendForGid`) is
`undefined` on the JS-mode log. Convergence and query results are correct; only
the "which internal path ran" assertion fails. This is the document analog of the
shared-log Class-C over-nativization block. Excluded tests:

- `operations > basic > uses the validated local append path for plain puts`
  (`expected +0 to equal 1`)
- `operations > basic > uses the validated local append path for document updates`
  (`expected +0 to equal 2`)
- `operations > basic > uses the independent native prepared batch path for unique putMany`
  (`Cannot read properties of undefined (reading 'planLocalAppendForGid')`)
- `operations > basic > uses native shared-log planning for replicated target-none puts`
  (`expected undefined to exist`)
- `operations > basic > uses commit-only local puts when coordinate persistence is deferred`
  (`expected +0 to equal 1`)

### Class-E — remote-indexed (`resolve:false`) over the native wire

The **local** `get(id, { resolve: false })` and custom-transform indexed shape
are correct under native (single-peer probe: `RustIndex` returns
`{ id, nameTransformed, __context, __indexed }` identically to `SQLiteIndex`). It
is only the **2-peer remote** indexed fetch — a non-replicating peer querying the
replicator for the *indexed* (non-resolved) row over the native raw-exchange
wire — that returns `undefined`, so `.nameTransformed` / `.name` reads throw.
This is a remote-query / sync path divergence, not a local index-comparator one.
Because the affected `custom index` and `prefetch` describes are remote-heavy and
mix green local tests with red remote-indexed ones, they are **excluded whole**
(not partially cherry-picked) to keep the allowlist to cleanly-green describes:

- `custom index` (whole describe) — `get > get indexed`,
  `get > uses indexed requests for replicated resolved remote get`,
  `iterate > iterate indexed`, `iterate > iterate replicate indexed` are
  native-red; the plain (resolved) `get` / `iterate` / `get local first` tests
  are native-green.
- `prefetch` (whole describe) — `can prefetch search results` is native-red on
  the 2-peer remote prefetch (`Cannot read properties of undefined (reading
  'name')`).

Follow-up: make the remote-indexed (`SearchRequestIndexed`) fetch resolve the
indexed row over the native raw-exchange path, then fold these describes in.

### Not yet catalogued (deferred, out of scope for this leg)

The `updates`, `replication`, `remote`, `acl`, `program as value`, `migration`,
`updateIndex`, and `most-common-query-predictor` describes were not run
exhaustively for this initial leg — they are replication / sync / lifecycle
heavy rather than pure query-surface, and are the natural next widening step once
the Class-A/C/E follow-ups above land. They are neither claimed green nor
silently capped.

## Verification summary

- **Native (env set):** allowlist grep GREEN — **192 passing, 0 failing**.
- **Default (env unset):** same allowlist grep GREEN — **192 passing, 0
  failing** (baseline unchanged).
- **Guard:** `docs.index.index` is asserted `RustIndex` under the env and
  `SQLiteIndex` without it; flipping the expected class makes the guard fail with
  the real `RustIndex` value, so a missing/disengaged native build cannot
  false-green.
- **No product `src/` change:** only the test helper (backend-agnostic sync
  suppression in `iterate > sort`), the new guard spec, `package.json`
  (`test:document-rust-core` script), `ci.yml` (one `continue-on-error` step),
  and this doc.
