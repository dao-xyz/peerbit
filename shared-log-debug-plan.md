# Status (2026-02-06)

This doc started as an investigation log for CI flakes surfaced by PR #589. As of **2026-02-06**, the shared-log hardening work described in `shared-log-debug.md` has been implemented on this branch and validated locally.

## What Changed (Implemented)

- **Pubsub: make subscription discovery resilient to debounce timing**
  - Eagerly initialize per-topic state in `subscribe()` so early remote Subscribe messages are not dropped.
  - Include “pending subscribe” topics in `requestSubscribers()` responses so peers can discover us during the debounce window.
  - File: `packages/transport/pubsub/src/index.ts`

- **SharedLog: serialize replication-info application per peer**
  - Replace fire-and-forget replication-info handling with a per-peer promise chain (`replicationInfoQueue`) so `addReplicationRange()` is not invoked concurrently for the same sender.
  - Track last seen replication-info timestamp per peer (`latestReplicationInfoMessage`) and ignore older updates.
  - File: `packages/programs/data/shared-log/src/index.ts`

- **SharedLog: don’t lose replication-info during startup**
  - If applying replication-info fails with `NotStartedError`/index-not-ready, store the latest message per peer (`pendingReplicationInfo`) and retry after open.
  - Flush pending messages after startup (`flushPendingReplicationInfo()`).
  - File: `packages/programs/data/shared-log/src/index.ts`

- **SharedLog: startup backfill of subscribers**
  - Call `pubsub.requestSubscribers(this.topic)` after open to backfill missed subscribe events.
  - File: `packages/programs/data/shared-log/src/index.ts`

- **SharedLog: make `replicator:join` idempotent**
  - Join is now emitted on the transition “not known replicator -> has segments”, not on “diffs applied”.
  - Prevents duplicates during concurrent/all-state announcements and makes restart/prune semantics consistent.
  - File: `packages/programs/data/shared-log/src/index.ts`

- **Migration (v8 compatibility): always respond with a role**
  - `getRole()` no longer throws when multiple local segments exist; it selects the widest segment as best-effort.
  - Fix role timestamp propagation and ensure `ResponseRoleMessage` send path can’t fail the whole subscription handler.
  - Fix `ResponseRoleMessage -> AllReplicatingSegmentsMessage` conversion to denormalize factor/offset into u32 coordinate space.
  - Files: `packages/programs/data/shared-log/src/index.ts`, `packages/programs/data/shared-log/src/replication.ts`

- **Bug fix: RoleReplicationSegment offset encoding**
  - `RoleReplicationSegment` incorrectly used `factor` when encoding `offset`.
  - File: `packages/programs/data/shared-log/src/role.ts`

- **Tests: fix a flaky assertion and a test bug**
  - `waitForReplicator waits until maturity` now asserts on the *remaining* maturity time (segment might predate the wait start).
  - Migration test’s v8 mock now replies using the opened store (`db1.log.rpc`) instead of the unopened template instance.
  - Files: `packages/programs/data/shared-log/test/replicate.spec.ts`, `packages/programs/data/shared-log/test/migration.spec.ts`

## Verification (Local)

- `pnpm run build` (PASS)
- `node ./node_modules/aegir/src/index.js run test --roots ./packages/programs/data/shared-log -- -t node` (PASS; 1743 passing)
- Targeted regression greps (PASS):
  - `replicate:join not emitted on update`
  - `8-9, replicates database of 1 entry`
  - `9-8, replicates database of 1 entry`
  - `segments updated while offline`
  - `will re-check replication segments on restart and announce online`

## Commit

- **TBD** (filled in after commit)

---

# Learnings

## Test Results
*(To be updated as tests are run)*

| Test | Result | Notes |
|------|--------|-------|
| events.spec.ts "replicate:join not emitted on update" | KNOWN FLAKY | Duplicate replicator:join - same peer hash twice in db1JoinEvents |
| migration.spec.ts "8-9, replicates database of 1 entry" | KNOWN FLAKY | waitForResolved timeout - replication never completes within 10s |
| migration.spec.ts "9-8, replicates database of 1 entry" | KNOWN FLAKY | Same as above |
| Master CI Part 4 (last 400 runs) | 5 failures, none matching triple-failure | Different shared-log flakes in other suites |

## Key Learnings

1. **C4 is INCORRECT on current master**: The PR #589 fix (eager `initializeTopic()` in `subscribe()` and `debounceSubscribeAggregator.has(topic)` in requestSubscribers) is NOT in the current codebase. The current branch (`fix/sync`) is at master. The fix described in C4 exists only in PR #589's branch.

2. **The triple-failure signature is unique to PR #589's CI run**: Only run 21732319700 shows all three failures together. This strongly suggests the pubsub timing change exposes a latent shared-log race.

3. **PR #3 does NOT fix the TOCTOU race**: It adds shutdown guards to `persistCoordinate` -- a completely different code path from the fire-and-forget IIFE at line 2971 that causes the race.

4. **The real root cause is architectural**: The fire-and-forget `(async () => { ... })().catch(...)` pattern at line 2971 of shared-log/src/index.ts creates unguarded concurrent access to `addReplicationRange()`.

---

# Claims

## C1: DirectSub.subscribe() is debounced (subscriptionDebounceDelay ?? 50ms)
**Status**: VERIFIED
**Evidence**: packages/transport/pubsub/src/index.ts:96-98 - `debouncedAccumulatorSetCounter((set) => this._subscribe([...set.values()]), props?.subscriptionDebounceDelay ?? 50)`
Line 126-128: `subscribe()` delegates to `this.debounceSubscribeAggregator.add({ key: topic })`
Line 134-176: `_subscribe()` sets `this.subscriptions.set(topic, prev)` at line 152 and calls `this.listenForSubscribers(topic)` at line 155

## C2: Incoming Subscribe drops if topic not initialized
**Status**: VERIFIED
**Evidence**: packages/transport/pubsub/src/index.ts:686-688 - `const peers = this.topics.get(topic); if (peers == null) { return; }`

## C3: requestSubscribers responses only include topics in this.subscriptions
**Status**: VERIFIED
**Evidence**: packages/transport/pubsub/src/index.ts:727-731 calls `getSubscriptionOverlap()`, lines 451-466 filters to `this.subscriptions` only.

## C4: PR's fix eagerly calls initializeTopic() and treats pending-topics in requestSubscribers
**Status**: INCORRECT (on current master)
**Evidence**: Lines 126-128 of pubsub/src/index.ts: `subscribe()` only calls `debounceSubscribeAggregator.add()`. No `initializeTopic()` call. `debounceSubscribeAggregator.has()` appears only in `unsubscribe()` at line 179, NOT in requestSubscribers response path. **This describes the proposed fix in PR #589, not the current code.**

## C5: Replication-info processed in fire-and-forget async IIFE
**Status**: VERIFIED
**Evidence**: packages/programs/data/shared-log/src/index.ts:2954-3007 - `(async () => { ... })().catch(...)` with no await, no per-peer queue/lock.

## C6: addReplicationRange() TOCTOU on prevCount === 0
**Status**: VERIFIED
**Evidence**: Line 1200 (reset path): `isNewReplicator = prevCount === 0 && ranges.length > 0`. Line 1226 (non-reset): `isNewReplicator = prevCountForOwner === 0`. Line 1382-1387: dispatches `replicator:join` if `isNewReplicator`. Two concurrent calls can both read 0 before either writes.

## C7: handleSubscriptionChange sends both AllReplicatingSegmentsMessage and RequestReplicationInfoMessage
**Status**: VERIFIED
**Evidence**: Lines 3954-3966: sends `AllReplicatingSegmentsMessage`. Lines 3981-3985: sends `RequestReplicationInfoMessage`. Line 2919: handler responds with `AllReplicatingSegmentsMessage`.

## C8: Startup only snapshots via getSubscribers(), no requestSubscribers()
**Status**: PARTIALLY VERIFIED
**Evidence**: Lines 2236-2247: `afterOpen()` calls `getSubscribers()` and iterates results calling `handleSubscriptionChange()`. Shared-log never calls `pubsub.requestSubscribers()` (confirmed by grep). However, `handleSubscriptionChange` does send `RequestReplicationInfoMessage` (a shared-log level request), so it does actively reach out for replication info.

## C9: NotStartedError swallows replication-info
**Status**: VERIFIED
**Evidence**: Lines 2998-3001: `.catch((e) => { if (isNotStartedError(e)) { return; } })`. Lines 290-303: `isNotStartedError()` returns true for `AbortError`, `NotStartedError`, `IndexNotStartedError`, `ClosedError`.

## C10: handleSubscriptionChange sends both messages
**Status**: VERIFIED
**Evidence**: Same as C7. Confirmed at line 3916 definition.

## C11: events.spec.ts checks for duplicate replicator:join
**Status**: PARTIALLY VERIFIED
**Evidence**: Lines 103-107: `waitForResolved(() => expect(db1JoinEvents).to.have.members([...]))`. This uses `.to.have.members()` which checks exact membership. The test is named "replicate:join not emitted on update" -- it primarily tests that updates don't re-trigger join, not specifically the TOCTOU race. But duplicate join would cause it to fail.

## C12: migration.spec.ts checks replication of 1 entry with default timeout
**Status**: VERIFIED
**Evidence**: Line 130: `waitForResolved(() => expect(db2.log.log.length).equal(1))`. Line 139: same pattern. No timeout specified = default.

## C13: waitForResolved default timeout is 10s
**Status**: VERIFIED
**Evidence**: packages/utils/time/src/wait.ts:73-83 - `timeout: 10 * 1000` default.

---

# Ahas/Gotchas

1. **C4 describes the fix, not current code**: The eager `initializeTopic()` and `debounceSubscribeAggregator.has()` in requestSubscribers are PR #589 changes, NOT on master. This is critical -- the claims document mixes "what the PR does" with "what the code does".

2. **onPeerReachable() gap not covered by PR #589**: Even with PR #589's fix, `onPeerReachable()` (line 485) only announces if `this.subscriptions.size > 0` and uses `getSubscriptionOverlap()` without the pending-subscribe check. So if a new peer connects during the debounce window, onPeerReachable won't announce our pending subscription.

3. **GetSubscribers handler inconsistency**: The `GetSubscribers` handler (line 803) uses `getSubscriptionOverlap()` without pending-subscribe check. Only the inline requestSubscribers response within the Subscribe handler gets PR #589's fix.

4. **The migration test failure mechanism is two-fold**: (a) missed subscription discovery means replication-info handshake doesn't run, AND (b) if it does run but indexes aren't ready, the message is silently dropped with no retry.

5. **Two different layers of "requestSubscribers"**: There's pubsub-level `requestSubscribers()` (sends GetSubscribers message) and shared-log-level `RequestReplicationInfoMessage`. The document sometimes conflates these.

6. **`subscribe→unsubscribe` before debounce leaves stale topics entry**: `unsubscribe()` cancels the debounce aggregator but does NOT clean up the `topics` entry that was eagerly created (relevant when PR #589 fix is applied).

7. **MAJOR: Debug document's shared-log analysis is based on STALE CODE**: The fire-and-forget IIFE (C5), TOCTOU race (C6), NotStartedError loss (C9), and missing requestSubscribers on startup (C8) have ALL been fixed on current master. The per-peer queue (`enqueueReplicationInfoMessage`), pending message buffer (`pendingReplicationInfo`), and `requestSubscribers()` call in `afterOpen()` are all present. Claims verified against wrong line numbers.

8. **This repo IS dao-xyz/peerbit upstream**: `git remote -v` → `origin https://github.com/dao-xyz/peerbit.git`. Local master == remote master at `07ba57225`.

---

# Reviews

## Review 1 Analysis

### What Review 1 Gets Right

1. **Core technical story is correct.** The debounce gap, incoming Subscribe drop (topics.get undefined), and requestSubscribers overlap empty -- all verified against code.

2. **"What changed" section matches the diff.** Verified: eager initializeTopic(), pending-subscribe inclusion, 3 tests.

3. **requestSubscribers pending-subscribe not directly asserted by tests.** Correct gap in test coverage.

4. **"subscribe then unsubscribe within debounce" not covered.** Correct. unsubscribe() cancels debounce aggregator but does NOT clean up topics entry.

5. **Proposed test files (bug2, bug3) are well-designed.**

### What Review 1 Misses

1. **onPeerReachable() gap** -- not patched by PR #589 to include pending subscribes.
2. **GetSubscribers handler inconsistency** -- only Subscribe handler gets the fix.
3. **getSubscribers() self-inclusion gap** -- self excluded during debounce window.
4. **PR version nuance** -- reviews pr-589-gh (3 commits) but current code is master.

## Review 2 Analysis

### What Review 2 Gets Right

1. **Core fix description accurate.** Correctly identifies both changes.
2. **requestSubscribers fix "plausible but not directly exercised."** Same as Review 1.
3. **Relay design choice is intentional.** Correctly identifies the rejected approach.
4. **Edge case: stale topics entry after subscribe-then-unsubscribe.** Verified.
5. **initializeTopic() is idempotent.** Confirmed: lines 116-119 create empty Map/Set, won't overwrite.

### What Review 2 Misses

1. Same gaps as Review 1 (onPeerReachable, GetSubscribers handler, getSubscribers self).
2. Recommends reading initializeTopic() but doesn't perform it.
3. Doesn't note the commit history evolution (3 commits, approach changes).

## Consensus Points

1. Core race condition is real and correctly identified
2. Eager initializeTopic() fix is correct and well-tested
3. requestSubscribers pending-subscribe fix is logically consistent but NOT directly tested
4. subscribe-then-unsubscribe edge case is NOT tested
5. PR does not CAUSE shared-log CI failures -- it EXPOSES them
6. Design choice to NOT track non-subscribed topics is intentional

## Recommended Tests from Reviews (consolidated)

1. requestSubscribers + pending subscribe (block _subscribe, assert discovery)
2. subscribe-then-unsubscribe within debounce (assert cleanup)
3. Non-subscriber does not track topics from incoming Subscribe traffic
4. initializeTopic() idempotency
5. onPeerReachable during debounce window
6. GetSubscribers handler during debounce window

---

# CI Error Analysis

## PR #3 (Faolain/peerbit) - fix/rootcause-b-persistcoordinate-guard
**Status**: Open with CI failures
**CI Failures**: Same 3 shared-log failures:
- events.spec.ts "replicate:join not emitted on update" -- FAIL (duplicate join, 2 instead of 1)
- migration.spec.ts "8-9" -- FAIL (0 instead of 1)
- migration.spec.ts "9-8" -- FAIL (0 instead of 1)
**Fixes TOCTOU?**: NO -- persistCoordinate guard is a completely different code path from the fire-and-forget IIFE at line 2971.

## PR #4 (Faolain/peerbit) - More comprehensive shared-log fix
**Status**: Open with CI failures
**Expected**: Same root cause family. Without per-peer serialization of `addReplicationRange()`, the duplicate replicator:join will persist.

## PR #591 (dao-xyz/peerbit) - fix(shared-log): avoid unhandled rejection when entry.hash is missing
**Status**: Open with CI failures
**Relation**: Orthogonal fix (null entry hash). CI failures may include same TOCTOU flakes since those are latent in shared-log.

## Master CI History
- 5 Part 4 failures in last 400 master runs, but NONE with the same triple-failure signature
- The specific pattern (events + migration 8-9 + migration 9-8) is unique to PR #589's run 21732319700
- Master is clean on Part 4 (1744/1744 pass) in recent runs

---

# Proposed Tests

## Test 1: Duplicate replicator:join race reproduction (shared-log)
**File**: packages/programs/data/shared-log/test/race-duplicate-join.spec.ts
**Hypothesis**: TOCTOU race in addReplicationRange() where two concurrent async IIFEs both see prevCount === 0
**Approach**: Intercept AllReplicatingSegmentsMessage, replay it twice concurrently with different timestamps, assert only 1 replicator:join event
**Before fix**: 2 join events (FAIL)
**After fix**: 1 join event (PASS)

## Test 2: Per-peer serialization validation (shared-log)
**File**: packages/programs/data/shared-log/test/race-serialization.spec.ts
**Hypothesis**: addReplicationRange() calls from same peer can interleave
**Approach**: Monkey-patch addReplicationRange to track concurrent execution count
**Before fix**: maxConcurrency >= 2
**After fix**: maxConcurrency === 1

## Test 3: NotStartedError message loss (shared-log)
**File**: packages/programs/data/shared-log/test/race-notstartederror.spec.ts
**Hypothesis**: NotStartedError causes replication-info to be silently dropped with no retry
**Approach**: Monkey-patch addReplicationRange to throw NotStartedError on first call, verify message is lost
**Before fix**: Message dropped, peer segments empty
**After fix**: Message queued and retried after index ready

## Test 4: requestSubscribers during pending subscribe (pubsub)
**File**: packages/transport/pubsub/test/request-subscribers-pending.spec.ts
**Hypothesis**: During debounce window, topics.get(topic) is undefined, Subscribe responses are dropped
**Approach**: Use large debounce delay, check topics map before/after debounce
**Before fix**: topics map not initialized during debounce
**After fix**: topics map initialized eagerly

## Test 5: Subscribe-then-unsubscribe edge case (pubsub)
**File**: packages/transport/pubsub/test/subscribe-unsubscribe-debounce.spec.ts
**Hypothesis**: subscribe→unsubscribe within debounce may leave stale state
**Approach**: Subscribe then immediately unsubscribe, verify no stale topic entries or false advertising

---

# Claims-to-Tests Coverage Matrix

| Claim | Description | Status | Validated by Test? | Gap? |
|-------|-------------|--------|-------------------|------|
| C1 | subscribe() debounced (50ms) | VERIFIED | Test 4 (indirect) | Weak -- no test asserts debounce mechanism directly |
| C2 | Incoming Subscribe drops if topic not initialized | VERIFIED | Test 4 (direct) | No gap |
| C3 | requestSubscribers only includes this.subscriptions | VERIFIED | NOT TESTED | **Gap** -- need test for getSubscriptionOverlap during debounce |
| C4 | PR fix: eager initializeTopic + pending in requestSubscribers | INCORRECT on master | Test 4 (partial -- eager init half) | **Gap** -- requestSubscribers half untested |
| C5 | Fire-and-forget async IIFE for replication-info | VERIFIED | Test 1 + Test 2 | No gap |
| C6 | addReplicationRange() TOCTOU on prevCount === 0 | VERIFIED | Test 1 (direct) | No gap |
| C7 | handleSubscriptionChange sends both messages | VERIFIED | NOT TESTED | Low priority structural gap |
| C8 | Startup snapshots via getSubscribers() only | PARTIAL | NOT TESTED | Medium priority -- explains migration failure |
| C9 | NotStartedError swallows replication-info | VERIFIED | Test 3 (direct) | No gap |
| C10 | handleSubscriptionChange sends both messages | VERIFIED | NOT TESTED | Same as C7 |
| C11 | events.spec.ts catches duplicate join | PARTIAL | Test 1 (supersedes) | No gap |
| C12 | migration.spec.ts checks replication of 1 entry | VERIFIED | Test 3 (partial) | Existing test IS the validation |
| C13 | waitForResolved default 10s | VERIFIED | N/A | Context, not testable fix |

## Strong Coverage (5/13): C2, C5, C6, C9, C11
## Partial Coverage (3/13): C1, C4 (eager half), C12
## No Coverage (5/13): C3, C7, C8, C10, C13 (C13 needs none; C7/C10 duplicates)

## Most Critical Gap: requestSubscribers pending-subscribe path (C3/C4) -- both reviews flagged this independently

## Additional Tests Recommended

| # | Test | Priority | Covers |
|---|------|----------|--------|
| 6 | requestSubscribers includes pending-subscribe topics | HIGH | C3, C4 |
| 7 | onPeerReachable during debounce window | MEDIUM | Gotcha #2 |
| 8 | GetSubscribers handler during debounce window | MEDIUM | Gotcha #3 |
| 9 | initializeTopic() idempotency | LOW | Review rec #4 |
| 10 | Startup discovery misses debounce-window peers | MEDIUM | C8 |

---

# Attack Plan

## CRITICAL CORRECTION: Debug Document is Based on Stale Code

The debug document's analysis of the shared-log TOCTOU race (C5, C6, C9) is based on an **older version** of the code. The current master already has:

1. **Per-peer serialization queue** (`replicationInfoQueue` / `enqueueReplicationInfoMessage`) at line 2616-2650 of shared-log/src/index.ts — replaces the fire-and-forget IIFE
2. **NotStartedError message buffering** (`pendingReplicationInfo`) at line 2684-2695 — messages are stored and retried via `flushPendingReplicationInfo()` at line 2289 in `afterOpen()`
3. **`requestSubscribers()` call in `afterOpen()`** at line 2267 — backfills subscriber state on startup
4. **`uniqueReplicators` Set guard** at line 1302-1308 — `isNewReplicator = !wasKnownReplicator && hasSegmentsAfter`

This means C5 (fire-and-forget IIFE) is **STALE**, C6 (TOCTOU) is **MITIGATED** by the queue, and C9 (NotStartedError loss) is **MITIGATED** by pending message buffering.

## Root Cause Verdict

**The bug is primarily in PUBSUB. The shared-log's per-peer queue already prevents the described TOCTOU race.**

The pubsub debounce window (C1, C2, C3) is the primary remaining issue:
- `subscribe()` doesn't call `initializeTopic()` — topics.get(topic) is undefined during debounce
- Incoming Subscribe messages are silently dropped at line 687 (`if (peers == null) return`)
- `requestSubscribers` responses exclude pending subscribes (only checks `this.subscriptions`)

PR #589's faster subscription discovery EXPOSES a latent shared-log issue: `handleSubscriptionChange` is called twice for the same peer (from both the pubsub event AND `afterOpen().getSubscribers()`), sending duplicate messages. The per-peer queue serializes responses but the duplicate sends are wasteful and may trigger edge cases under CI load.

## Fix Priority Order

### P0: Must Fix (blocks CI — pubsub layer)

**Fix 1: Eager `initializeTopic()` in `subscribe()`**
- File: `packages/transport/pubsub/src/index.ts`, line 126
- Change: Add `this.initializeTopic(topic)` before the debounce aggregator add
- Why: Closes the debounce window where incoming Subscribe messages are dropped

**Fix 2: Include pending-subscribe topics in requestSubscribers response**
- File: `packages/transport/pubsub/src/index.ts`, around line 727-731
- Change: Check `this.debounceSubscribeAggregator.has(topic)` as fallback when `getSubscriptionOverlap` returns empty
- Why: Ensures remote peers learn about our subscription during debounce window

### P1: Should Fix (prevents regressions — shared-log layer)

**Fix 3: Deduplicate `handleSubscriptionChange` calls for same peer**
- File: `packages/programs/data/shared-log/src/index.ts`, line 4041
- Change: Track which peers have been processed with `subscribed=true` to skip redundant message sends
- Why: With faster pubsub, both event handler and `afterOpen().getSubscribers()` can trigger for the same peer

### P2: Nice to Have (pubsub hardening)

**Fix 4: `onPeerReachable()` during debounce window** (line 481)
**Fix 5: `GetSubscribers` handler during debounce window** (line 798-824)
**Fix 6: subscribe-then-unsubscribe cleanup** (line 178-182)

## Implementation Sequence

### Step 1: Apply PR #589 pubsub fix (P0-1 and P0-2)
```
File: packages/transport/pubsub/src/index.ts
Line 126: Add initializeTopic() before debounce
Line ~731: Add debounceSubscribeAggregator.has() check in requestSubscribers response
```
**Quick test**: `pnpm -C packages/transport/pubsub test` (~30s)
**Expected**: All pubsub tests pass + topic initialized immediately on subscribe()

### Step 2: Run targeted shared-log tests
**Quick test**: Run the 3 failing tests individually:
```bash
pnpm -C packages/programs/data/shared-log test -- --grep "replicate:join not emitted on update"
pnpm -C packages/programs/data/shared-log test -- --grep "replicates database of 1 entry"
```
**Expected**: All pass. If not, proceed to Step 3.

### Step 3: Add handleSubscriptionChange deduplication (P1-3) if Step 2 still flaky
```
File: packages/programs/data/shared-log/src/index.ts
Line 4041: Add guard Set to skip duplicate handleSubscriptionChange(subscribed=true) calls
```
**Quick test**: Same as Step 2

### Step 4: Loop validation
Run the targeted tests 10-20x to confirm stability:
```bash
for i in $(seq 1 20); do echo "Run $i"; pnpm -C packages/programs/data/shared-log test -- --grep "replicate:join not emitted|replicates database of 1 entry" || break; done
```

### Step 5: Full test suite validation
```bash
pnpm run test:part-4
```

## Quick Validation Strategy

| Fix | Command | Time |
|-----|---------|------|
| Pubsub fix | `pnpm -C packages/transport/pubsub test` | ~30s |
| Migration test | `pnpm -C packages/programs/data/shared-log test -- --grep "replicates database"` | ~30s |
| Events test | `pnpm -C packages/programs/data/shared-log test -- --grep "replicate:join"` | ~30s |
| Repeat validation | Loop 20x with `--grep` | ~10min |
| Full Part 4 | `pnpm run test:part-4` | ~20min |

## Risk Assessment

- **P0-1 (eager initializeTopic)**: LOW risk. `initializeTopic()` is idempotent (creates Map/Set only if absent). Verified at lines 116-119.
- **P0-2 (pending-subscribe response)**: LOW-MEDIUM risk. Could advertise a subscription that gets cancelled by unsubscribe-during-debounce. Mitigated by Unsubscribe message correcting state.
- **P1-3 (handleSubscriptionChange dedup)**: MEDIUM risk. Must clear dedup set on peer disconnect/unsubscribe to allow reconnect. Use timestamp-based throttle instead of hard dedup.

## Updated Claim Status (with corrected code analysis)

| Claim | Original Status | Corrected Status | Notes |
|-------|----------------|-----------------|-------|
| C5 | VERIFIED | **STALE** | Fire-and-forget IIFE replaced with `enqueueReplicationInfoMessage` queue |
| C6 | VERIFIED | **MITIGATED** | Per-peer queue serializes `addReplicationRange` calls |
| C9 | VERIFIED | **MITIGATED** | `pendingReplicationInfo` buffers and retries on `flushPendingReplicationInfo()` |
| C8 | PARTIAL | **FIXED** | `afterOpen()` now calls `requestSubscribers()` at line 2267 |
| C1-C3 | VERIFIED | **STILL VALID** | Pubsub debounce gap is the primary remaining issue |
