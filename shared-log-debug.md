You have recently made a PR here: https://github.com/dao-xyz/peerbit/pull/589 in order to address flaky connectivity within the Peerbit library for pubsub. This patch then had 3 CI test failures visible on github here https://github.com/dao-xyz/peerbit/actions/runs/21732319700/job/62689580781 (and you can inspect the history of the same PR with the other job failures on the CI page). It has been noted by some peers that this PR actually tries to improve connectivity between peers but actually more easily surfaces a "shared-log" flaky/race condition that was under the hood before, so it doesn't cause the error it just speeds up connections leading to the error surfacing (but this is not confirmed). Some make claims that it's related to a debounce whereas there are other claims that extra replications are now being caused because a debounce isn't canceled. 
  
  <Assumptions>
  These assumptions are correct in the current code:

  - DirectSub.subscribe() is debounced (subscriptionDebounceDelay ?? 50ms), and _subscribe() is where subscriptions is set and listenForSubscribers() is called.
    packages/transport/pubsub/src/index.ts:96
    packages/transport/pubsub/src/index.ts:139
    packages/transport/pubsub/src/index.ts:159
    packages/transport/pubsub/src/index.ts:270
  - Incoming Subscribe handling drops remote subscriptions if the topic hasn’t been initialized locally:
    const peers = this.topics.get(topic); if (peers == null) return;
    packages/transport/pubsub/src/index.ts:690
  - requestSubscribers responses only include topics present in this.subscriptions via getSubscriptionOverlap(), so during the debounce window overlap can be empty.
    packages/transport/pubsub/src/index.ts:456
    packages/transport/pubsub/src/index.ts:732

  The PR’s fix (eager initializeTopic() inside subscribe() and adding pending-topics via debounceSubscribeAggregator.has(topic) to requestSubscribers responses) directly addresses those gaps.
  packages/transport/pubsub/src/index.ts:126
  packages/transport/pubsub/src/index.ts:732

  Notable edge-case to sanity-check (not a blocker, but real): subscribe() then unsubscribe() before debounce fires now leaves an initialized topics entry behind (since unsubscribe only cancels the debounced subscribe). That can affect “are we subscribed” heuristics that
  use this.topics.has(topic) rather than this.subscriptions.has(topic).
  packages/transport/pubsub/src/index.ts:126
  packages/transport/pubsub/src/index.ts:183

  ## What CI Actually Failed (PR run 21732319700, job 62689580781)

  In pnpm run test:ci:part-4 (Ubuntu 22.04, Node 22), @peerbit/shared-log failed:

  - events > replicate:join not emitted on update with duplicate join entries (same peer hash twice).
    packages/programs/data/shared-log/test/events.spec.ts:103
    packages/programs/data/shared-log/test/events.spec.ts:104
  - migration-8-9 > 8-9, replicates database of 1 entry timed out and last assertion was expected +0 to equal 1.
    packages/programs/data/shared-log/test/migration.spec.ts:130
  - migration-8-9 > 9-8, replicates database of 1 entry same symptom.
    packages/programs/data/shared-log/test/migration.spec.ts:139

  (waitForResolved default timeout is 10s, so these “AssertionError” failures are consistent with a 10s timeout where replication never completed.)
  packages/utils/time/src/wait.ts:73

  ## Most Likely Origin of the shared-log Flakes

  ### 1) Duplicate replicator:join (events.spec)

  The most plausible mechanism is a race in shared-log’s replication-info handling:

  - Shared-log can receive multiple replication-info messages for the same peer during join, close together:
      - Proactive announcement on subscribe (handleSubscriptionChange sends AllReplicatingSegmentsMessage).
        packages/programs/data/shared-log/src/index.ts:3954
      - Response to RequestReplicationInfoMessage (receiver sends AllReplicatingSegmentsMessage).
        packages/programs/data/shared-log/src/index.ts:2919
        packages/programs/data/shared-log/src/index.ts:3981
  - Incoming replication-info messages are processed in a fire-and-forget async IIFE, not serialized per peer.
    packages/programs/data/shared-log/src/index.ts:2955
    packages/programs/data/shared-log/src/index.ts:2971
  - addReplicationRange() decides “is new replicator” by reading the index state (prevCount === 0), then later writes. Two concurrent calls can both see prevCount === 0 and both emit replicator:join.
    packages/programs/data/shared-log/src/index.ts:1139
    packages/programs/data/shared-log/src/index.ts:1200
    packages/programs/data/shared-log/src/index.ts:1382

  This exactly matches the CI diff: same peer hash appearing twice in db1JoinEvents.

  Why PR #589 could increase the odds (without being the root cause): pubsub timing changes can make subscription discovery and replication-info exchange happen “tighter” in time, increasing the chance the two replication-info messages overlap and hit this race window.

  ### 2) Migration tests not replicating within 10s (migration.spec)

  Two high-probability contributors (can stack):

  - Missed/late subscription discovery ⇒ replication-info handshake doesn’t run soon enough.
    Shared-log relies on pubsub subscribe events plus a single snapshot in afterOpen(). It does not call pubsub requestSubscribers(). If the subscribe event happens before the listener is attached (or the snapshot runs before pubsub has the subscriber recorded),
    handleSubscriptionChange() might not run, so RequestReplicationInfoMessage isn’t sent and roles/ranges aren’t applied.
    packages/programs/data/shared-log/src/index.ts:2138
    packages/programs/data/shared-log/src/index.ts:2219
    packages/programs/data/shared-log/src/index.ts:3916
    packages/transport/pubsub/src/index.ts:274
  - Replication-info messages can be dropped during startup if indexes aren’t ready.
    In the replication-info IIFE, errors classified as “not started” are swallowed and the message is effectively lost (no retry at this layer). If this happens at the wrong time, the peer never gets replication ranges, and replication can stall.
    packages/programs/data/shared-log/src/index.ts:290
    packages/programs/data/shared-log/src/index.ts:2999

  ## Bottom Line On “Are the suspected origins correct?”

  - “This PR doesn’t touch shared-log directly” is true, but shared-log depends on pubsub (workspace:*), so pubsub timing changes can expose or amplify existing shared-log races.
  - The replicator:join failure is strongly consistent with a shared-log concurrency bug (lack of per-peer serialization around addReplicationRange()), not with a deterministic pubsub regression.
  - The migration failures are consistent with timing/handshake issues (missed subscribe window, lost replication-info processing, or simply >10s replication under CI load). Pubsub races can be a trigger, but shared-log also has its own “one-shot snapshot” assumptions.

  ## Most Actionable Hardening (if you want to fix the flakes)
  Creating an actionable plan

  In the shared-log, we always subscribe to our RPC topic during the open process, which leads us to eventually initialize it. However, if a remote subscribe message arrives before this, it might be dropped, preventing us from learning about the subscription. To tackle this, we should incorporate a
  requestSubscribers call right after opening. Here’s the proposed minimal patch set:

  1. Immediate triage: Confirm causes by running tests with instrumentation.
  2. Code hardening: Strengthen the shared-log.
  3. Pubsub improvements: Ensure the relevant PR is merged, possibly adding tests.
  4. Test hardening: Enhance waitForResolved timeouts; implement a deterministic handshake.
  5. CI: Execute targeted tests several times, capturing debug logs and seeds.

  For the update, the latest PR was modified on Feb 6, 2026, head 61ab928..., with the failing run ID 21732319700. I believe the assumption about the CI failure is correct; it seems related to shared-log concurrency rather than pubsub issues.

• Current Evidence (PR + CI)

  1. PR dao-xyz/peerbit#589 (head 61ab928698a82de0441adca1b8580a4c4133795b, Feb 6 2026) is correctly targeting a real pubsub race: incoming Subscribe can be dropped while local subscribe() is still in the debounce window. Fix is to eagerly initializeTopic() in subscribe() and treat “pending
     subscribe” as subscribed for requestSubscribers responses (packages/transport/pubsub/src/index.ts:126, packages/transport/pubsub/src/index.ts:732).
  2. The CI failures you called out look shared-log–originated (not pubsub-test–originated): duplicate replicator:join in events.spec.ts and replication timeout in migration.spec.ts (PR run 21732319700, job 62689580781 per earlier log inspection).

  Most Likely Root Causes

  1. Duplicate replicator:join is a shared-log concurrency bug:

  - Join emits inside addReplicationRange() when isNewReplicator is computed off index counts (packages/programs/data/shared-log/src/index.ts:1139, packages/programs/data/shared-log/src/index.ts:1200, packages/programs/data/shared-log/src/index.ts:1384).
  - Replication-info messages are applied via an un-awaited async IIFE, so two replication-info messages from the same peer can run addReplicationRange() concurrently (packages/programs/data/shared-log/src/index.ts:2971).
  - On subscribe, shared-log sends both AllReplicatingSegmentsMessage and RequestReplicationInfoMessage (remote responds with another AllReplicatingSegmentsMessage), making “two near-simultaneous replication-info messages” common during joins (packages/programs/data/shared-log/src/index.ts:3954,
    packages/programs/data/shared-log/src/index.ts:3981, packages/programs/data/shared-log/src/index.ts:2919).

  2. Migration flake is likely “handshake missed” + “early message dropped”:

  - Startup only snapshots local pubsub state via getSubscribers() and does not actively backfill via requestSubscribers() (packages/programs/data/shared-log/src/index.ts:2236).
  - Replication-info processing explicitly swallows NotStartedError/IndexNotStartedError and drops the update (packages/programs/data/shared-log/src/index.ts:2999, packages/programs/data/shared-log/src/index.ts:290), which can turn into a permanent “never learned peer’s replication intent” and then
    a 10s waitForResolved() timeout (packages/utils/time/src/wait.ts:73).

  Actionable Hardening Plan (Ordered By Impact)

  1. Serialize replication-info application per peer (must-do)

  - Change the replication-info handler at packages/programs/data/shared-log/src/index.ts:2955 to enqueue work per from.hashcode() instead of fire-and-forget IIFEs.
  - Implementation shape: Map<string, Promise<void>> chaining (or a tiny per-peer mutex) so only one addReplicationRange() runs at a time for a given peer.
  - Move the “drop old message” check inside the serialized section and compare against “latest applied” (not “latest received”) to prevent older messages applying after newer ones.
  - Validation: run pnpm -C ../peerbit-pr589/repo run test:ci:part-4 in a loop (20-50 iterations) and ensure events.spec.ts no longer produces duplicate join.

  2. Make replicator:join emission idempotent across all paths

  - Guard replicator:join in addReplicationRange() with a state transition, not a racy count check. Prefer “was peer in uniqueReplicators before?” inside the same serialized section.
  - Add the same guard in pruneOfflineReplicators() before emitting join (packages/programs/data/shared-log/src/index.ts:2294), because it can overlap with join-time replication updates and currently emits unconditionally.
  - Validation: add a regression test that forces two identical replication-info messages to be handled “back-to-back” and asserts only one join event.

  3. Stop losing replication-info messages on NotStartedError

  - Replace the current “swallow and return” in the replication-info handler catch (packages/programs/data/shared-log/src/index.ts:2999) with “store latest message per peer and retry once indexes are started”.
  - Drain pending messages at the end of afterOpen() (packages/programs/data/shared-log/src/index.ts:2219) or when the replication index signals readiness.
  - Validation: stress test by injecting an artificial delay before index start (or force early inbound messages) and confirm replication still converges.

  4. Reduce duplication of replication-info messages at join

  - Adjust handleSubscriptionChange() (packages/programs/data/shared-log/src/index.ts:3916) so it does not always send both:
      - Keep the proactive AllReplicatingSegmentsMessage for fast-path.
      - Send RequestReplicationInfoMessage only as a fallback (e.g., delayed 200-500ms; cancel if we already received any replication-info from that peer).
  - This is not required once (1) is done, but it reduces traffic and reduces probability of uncovering other races.
  - Validation: keep join correctness and re-run stress loop.

  5. Backfill subscriber state on startup

  - In afterOpen() (packages/programs/data/shared-log/src/index.ts:2219), call await this.node.services.pubsub.requestSubscribers(this.topic) before the getSubscribers() snapshot.
  - This makes startup robust to missed subscribe events and to peers that subscribed while we were offline.
  - Validation: reproduce migration tests with forced “subscribe event loss” (can be simulated by dropping first Subscribe message in pubsub tests) and ensure migration.spec.ts stabilizes.

  6. Test hardening (only after code hardening above)

  - Keep existing assertions, but make them wait on explicit shared-log state rather than only time:
      - In packages/programs/data/shared-log/test/events.spec.ts:84, wait for replicationIndex to contain exactly 1 segment for peer2 before asserting join list length.
      - In packages/programs/data/shared-log/test/migration.spec.ts:124, optionally bump waitForResolved timeout to 30s while the underlying fix lands (defaults are 10s at packages/utils/time/src/wait.ts:73).
  - Add a dedicated regression test for “duplicate inbound replication-info does not duplicate join”.

  7. CI signal improvements

  - Add a “flake detector” job (nightly or non-blocking) that runs test:ci:part-4 20x and uploads logs when a failure occurs.
  - Add trace logging behind an env flag for the replication-info queue and join emission so CI failures become diagnosable without reruns.

  Next Steps

  1. Do you want this hardening plan turned into an actual patch series in a new PR against dao-xyz/peerbit (shared-log fixes), keeping PR #589 pubsub-only?
  2. If yes, tell me which worktree to implement in (../peerbit-pr589/repo master vs ../peerbit-pr589/repo.pr-589 PR branch), and I’ll implement steps 1-3 first and validate by looping test:ci:part-4.
<Assumptions>

  <Claim1>
I can’t find the same exact triple-failure signature anywhere except that one PR #589 run.

  What I checked:

  - CI pull_request runs (last 134 retained): 14 had test:ci:part-4 failures, but only run 21732319700 (PR #589, head 976ac89) contains the failure summaries:
      - events -> replicate:join not emitted on update: (assertion)
      - migration-8-9 -> 8-9, replicates database of 1 entry: (expected log length 1, got 0)
      - migration-8-9 -> 9-8, replicates database of 1 entry: (expected 1, got 0)
  - CI master push runs (last 400): 5 had test:ci:part-4 failures, 0 had those same failure summaries. Master failures were other shared-log flakes (timeouts/assertions in other suites), and these 3 tests were passing in those
    logs.

  So there’s no pattern / “started after commit X” I can point to from CI data, because it’s a single occurrence.

  Root cause (from the actual stacks in PR #589 run):

  - events.spec.ts:103-107: replicator:join events collected in db1JoinEvents ended up not matching exactly one peer as expected. The assertion failure output is consistent with a duplicate join emission (same peer hash
    appearing twice).
  - migration.spec.ts:130 and migration.spec.ts:139: after adding "hello" on one side, the other side’s log.log.length never reached 1 within waitForResolved’s timeout (so replication/handshake didn’t complete).

  Those are both symptoms of replication/membership not settling deterministically (either duplicated join signals, or replication never converging). With only one CI occurrence, it’s unclear whether that’s a shared-log flake
  that happened to hit this PR, or a regression made more likely by the pubsub changes.
<Claim1>

<Claim2>
  PR #589 (pubsub race condition fix) is complete and pushed:
  - 4 commits on fix/pubsub-initialize-topic-on-subscribe
  - All 40 pubsub tests pass + 3 new regression tests pass + 192 log/replicate tests pass
  - PR description updated with definitive root cause analysis of CI failures

  Key findings from the deep investigation:
  - The 3 shared-log CI failures (events, migration-8-9) are caused by a pre-existing TOCTOU race in SharedLog.onMessage(), not by our pubsub fix
  - Our fix makes subscriptions resolve faster (removes ~50ms debounce delay), which exposes the latent shared-log race where two concurrent async IIFEs both see prevCount === 0 and emit duplicate replicator:join events
  - Master runs clean on Part 4 (1744/1744 pass), confirming the failures only surface when subscription timing changes
  - The shared-log race should be fixed separately (serializing per-peer replication info processing)
  </Claim2>

<Claim3>
 Another expected result — the replicate timing test (waitForReplicator waits until maturity) fails because our faster subscription discovery shortens the wait time (2639ms < expected 2900ms). Same shared-log timing sensitivity we already
  documented. All background tasks from the previous session have now completed, all consistent with our analysis.

⏺ As expected — duplicate replicator:join event (same peer hash appears twice in the array instead of once). This is the shared-log TOCTOU race where two concurrent AllReplicatingSegmentsMessage handlers both see prevCount === 0 and both
  emit the event.

  All three background tasks confirm our analysis. Everything is consistent and documented in PR #589.
</Claim3>

<Claim4>
Results: PR #3 does NOT fix the TOCTOU race

  PR #3 (fix/rootcause-b-persistcoordinate-guard) adds shutdown guards to persistCoordinate — protecting against TypeError when _close() nullifies internal indices during async operations. It's a valid shutdown hardening fix, but it targets a different problem.

  Test results with PR #3 applied (on top of pubsub fix):
  ┌────────────────────────────────────────────────────┬────────┬──────────────────────────────────────────────────┐
  │                        Test                        │ Result │                      Issue                       │
  ├────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ events > replicate:join not emitted on update      │ FAIL   │ Still duplicate replicator:join (2 instead of 1) │
  ├────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ migration-8-9 > replicates database of 1 entry     │ FAIL   │ Still 0 instead of 1                             │
  ├────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ replicate > waitForReplicator waits until maturity │ PASS   │ Likely environmental timing variance             │
  └────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────┘
  Why it doesn't fix the race

  The TOCTOU race is in onMessage() (~line 2971), where two concurrent fire-and-forget async IIFEs both call addReplicationRange() and both see prevCount === 0 before either writes. The fix would need a per-peer mutex or serialization queue around addReplicationRange calls, or an atomic
  check-and-set for isNewReplicator. PR #3's persistCoordinate guard is in a completely different code path.
  </Claim4>

<Approach1>
## PR #589 Assumptions (PubSub)

  These assumptions are correct in the current code:

  - DirectSub.subscribe() is debounced (subscriptionDebounceDelay ?? 50ms), and _subscribe() is where subscriptions is set and listenForSubscribers() is called.
    packages/transport/pubsub/src/index.ts:96
    packages/transport/pubsub/src/index.ts:139
    packages/transport/pubsub/src/index.ts:159
    packages/transport/pubsub/src/index.ts:270
  - Incoming Subscribe handling drops remote subscriptions if the topic hasn’t been initialized locally:
    const peers = this.topics.get(topic); if (peers == null) return;
    packages/transport/pubsub/src/index.ts:690
  - requestSubscribers responses only include topics present in this.subscriptions via getSubscriptionOverlap(), so during the debounce window overlap can be empty.
    packages/transport/pubsub/src/index.ts:456
    packages/transport/pubsub/src/index.ts:732

  The PR’s fix (eager initializeTopic() inside subscribe() and adding pending-topics via debounceSubscribeAggregator.has(topic) to requestSubscribers responses) directly addresses those gaps.
  packages/transport/pubsub/src/index.ts:126
  packages/transport/pubsub/src/index.ts:732

  Notable edge-case to sanity-check (not a blocker, but real): subscribe() then unsubscribe() before debounce fires now leaves an initialized topics entry behind (since unsubscribe only cancels the debounced subscribe). That can affect “are we subscribed” heuristics that
  use this.topics.has(topic) rather than this.subscriptions.has(topic).
  packages/transport/pubsub/src/index.ts:126
  packages/transport/pubsub/src/index.ts:183

  ## What CI Actually Failed (PR run 21732319700, job 62689580781)

  In pnpm run test:ci:part-4 (Ubuntu 22.04, Node 22), @peerbit/shared-log failed:

  - events > replicate:join not emitted on update with duplicate join entries (same peer hash twice).
    packages/programs/data/shared-log/test/events.spec.ts:103
    packages/programs/data/shared-log/test/events.spec.ts:104
  - migration-8-9 > 8-9, replicates database of 1 entry timed out and last assertion was expected +0 to equal 1.
    packages/programs/data/shared-log/test/migration.spec.ts:130
  - migration-8-9 > 9-8, replicates database of 1 entry same symptom.
    packages/programs/data/shared-log/test/migration.spec.ts:139

  (waitForResolved default timeout is 10s, so these “AssertionError” failures are consistent with a 10s timeout where replication never completed.)
  packages/utils/time/src/wait.ts:73

  ## Most Likely Origin of the shared-log Flakes

  ### 1) Duplicate replicator:join (events.spec)

  The most plausible mechanism is a race in shared-log’s replication-info handling:

  - Shared-log can receive multiple replication-info messages for the same peer during join, close together:
      - Proactive announcement on subscribe (handleSubscriptionChange sends AllReplicatingSegmentsMessage).
        packages/programs/data/shared-log/src/index.ts:3954
      - Response to RequestReplicationInfoMessage (receiver sends AllReplicatingSegmentsMessage).
        packages/programs/data/shared-log/src/index.ts:2919
        packages/programs/data/shared-log/src/index.ts:3981
  - Incoming replication-info messages are processed in a fire-and-forget async IIFE, not serialized per peer.
    packages/programs/data/shared-log/src/index.ts:2955
    packages/programs/data/shared-log/src/index.ts:2971
  - addReplicationRange() decides “is new replicator” by reading the index state (prevCount === 0), then later writes. Two concurrent calls can both see prevCount === 0 and both emit replicator:join.
    packages/programs/data/shared-log/src/index.ts:1139
    packages/programs/data/shared-log/src/index.ts:1200
    packages/programs/data/shared-log/src/index.ts:1382

  This exactly matches the CI diff: same peer hash appearing twice in db1JoinEvents.

  Why PR #589 could increase the odds (without being the root cause): pubsub timing changes can make subscription discovery and replication-info exchange happen “tighter” in time, increasing the chance the two replication-info messages overlap and hit this race window.

  ### 2) Migration tests not replicating within 10s (migration.spec)

  Two high-probability contributors (can stack):

  - Missed/late subscription discovery ⇒ replication-info handshake doesn’t run soon enough.
    Shared-log relies on pubsub subscribe events plus a single snapshot in afterOpen(). It does not call pubsub requestSubscribers(). If the subscribe event happens before the listener is attached (or the snapshot runs before pubsub has the subscriber recorded),
    handleSubscriptionChange() might not run, so RequestReplicationInfoMessage isn’t sent and roles/ranges aren’t applied.
    packages/programs/data/shared-log/src/index.ts:2138
    packages/programs/data/shared-log/src/index.ts:2219
    packages/programs/data/shared-log/src/index.ts:3916
    packages/transport/pubsub/src/index.ts:274
  - Replication-info messages can be dropped during startup if indexes aren’t ready.
    In the replication-info IIFE, errors classified as “not started” are swallowed and the message is effectively lost (no retry at this layer). If this happens at the wrong time, the peer never gets replication ranges, and replication can stall.
    packages/programs/data/shared-log/src/index.ts:290
    packages/programs/data/shared-log/src/index.ts:2999

  ## Bottom Line On “Are the suspected origins correct?”

  - “This PR doesn’t touch shared-log directly” is true, but shared-log depends on pubsub (workspace:*), so pubsub timing changes can expose or amplify existing shared-log races.
  - The replicator:join failure is strongly consistent with a shared-log concurrency bug (lack of per-peer serialization around addReplicationRange()), not with a deterministic pubsub regression.
  - The migration failures are consistent with timing/handshake issues (missed subscribe window, lost replication-info processing, or simply >10s replication under CI load). Pubsub races can be a trigger, but shared-log also has its own “one-shot snapshot” assumptions.

  ## Most Actionable Hardening (if you want to fix the flakes)
Creating an actionable plan

  In the shared-log, we always subscribe to our RPC topic during the open process, which leads us to eventually initialize it. However, if a remote subscribe message arrives before this, it might be dropped, preventing us from learning about the subscription. To tackle this, we should incorporate a
  requestSubscribers call right after opening. Here’s the proposed minimal patch set:

  1. Immediate triage: Confirm causes by running tests with instrumentation.
  2. Code hardening: Strengthen the shared-log.
  3. Pubsub improvements: Ensure the relevant PR is merged, possibly adding tests.
  4. Test hardening: Enhance waitForResolved timeouts; implement a deterministic handshake.
  5. CI: Execute targeted tests several times, capturing debug logs and seeds.

  For the update, the latest PR was modified on Feb 6, 2026, head 61ab928..., with the failing run ID 21732319700. I believe the assumption about the CI failure is correct; it seems related to shared-log concurrency rather than pubsub issues.

• Current Evidence (PR + CI)

  1. PR dao-xyz/peerbit#589 (head 61ab928698a82de0441adca1b8580a4c4133795b, Feb 6 2026) is correctly targeting a real pubsub race: incoming Subscribe can be dropped while local subscribe() is still in the debounce window. Fix is to eagerly initializeTopic() in subscribe() and treat “pending
     subscribe” as subscribed for requestSubscribers responses (packages/transport/pubsub/src/index.ts:126, packages/transport/pubsub/src/index.ts:732).
  2. The CI failures you called out look shared-log–originated (not pubsub-test–originated): duplicate replicator:join in events.spec.ts and replication timeout in migration.spec.ts (PR run 21732319700, job 62689580781 per earlier log inspection).

  Most Likely Root Causes

  1. Duplicate replicator:join is a shared-log concurrency bug:

  - Join emits inside addReplicationRange() when isNewReplicator is computed off index counts (packages/programs/data/shared-log/src/index.ts:1139, packages/programs/data/shared-log/src/index.ts:1200, packages/programs/data/shared-log/src/index.ts:1384).
  - Replication-info messages are applied via an un-awaited async IIFE, so two replication-info messages from the same peer can run addReplicationRange() concurrently (packages/programs/data/shared-log/src/index.ts:2971).
  - On subscribe, shared-log sends both AllReplicatingSegmentsMessage and RequestReplicationInfoMessage (remote responds with another AllReplicatingSegmentsMessage), making “two near-simultaneous replication-info messages” common during joins (packages/programs/data/shared-log/src/index.ts:3954,
    packages/programs/data/shared-log/src/index.ts:3981, packages/programs/data/shared-log/src/index.ts:2919).

  2. Migration flake is likely “handshake missed” + “early message dropped”:

  - Startup only snapshots local pubsub state via getSubscribers() and does not actively backfill via requestSubscribers() (packages/programs/data/shared-log/src/index.ts:2236).
  - Replication-info processing explicitly swallows NotStartedError/IndexNotStartedError and drops the update (packages/programs/data/shared-log/src/index.ts:2999, packages/programs/data/shared-log/src/index.ts:290), which can turn into a permanent “never learned peer’s replication intent” and then
    a 10s waitForResolved() timeout (packages/utils/time/src/wait.ts:73).

  Actionable Hardening Plan (Ordered By Impact)

  1. Serialize replication-info application per peer (must-do)

  - Change the replication-info handler at packages/programs/data/shared-log/src/index.ts:2955 to enqueue work per from.hashcode() instead of fire-and-forget IIFEs.
  - Implementation shape: Map<string, Promise<void>> chaining (or a tiny per-peer mutex) so only one addReplicationRange() runs at a time for a given peer.
  - Move the “drop old message” check inside the serialized section and compare against “latest applied” (not “latest received”) to prevent older messages applying after newer ones.
  - Validation: run pnpm -C ../peerbit-pr589/repo run test:ci:part-4 in a loop (20-50 iterations) and ensure events.spec.ts no longer produces duplicate join.

  2. Make replicator:join emission idempotent across all paths

  - Guard replicator:join in addReplicationRange() with a state transition, not a racy count check. Prefer “was peer in uniqueReplicators before?” inside the same serialized section.
  - Add the same guard in pruneOfflineReplicators() before emitting join (packages/programs/data/shared-log/src/index.ts:2294), because it can overlap with join-time replication updates and currently emits unconditionally.
  - Validation: add a regression test that forces two identical replication-info messages to be handled “back-to-back” and asserts only one join event.

  3. Stop losing replication-info messages on NotStartedError

  - Replace the current “swallow and return” in the replication-info handler catch (packages/programs/data/shared-log/src/index.ts:2999) with “store latest message per peer and retry once indexes are started”.
  - Drain pending messages at the end of afterOpen() (packages/programs/data/shared-log/src/index.ts:2219) or when the replication index signals readiness.
  - Validation: stress test by injecting an artificial delay before index start (or force early inbound messages) and confirm replication still converges.

  4. Reduce duplication of replication-info messages at join

  - Adjust handleSubscriptionChange() (packages/programs/data/shared-log/src/index.ts:3916) so it does not always send both:
      - Keep the proactive AllReplicatingSegmentsMessage for fast-path.
      - Send RequestReplicationInfoMessage only as a fallback (e.g., delayed 200-500ms; cancel if we already received any replication-info from that peer).
  - This is not required once (1) is done, but it reduces traffic and reduces probability of uncovering other races.
  - Validation: keep join correctness and re-run stress loop.

  5. Backfill subscriber state on startup

  - In afterOpen() (packages/programs/data/shared-log/src/index.ts:2219), call await this.node.services.pubsub.requestSubscribers(this.topic) before the getSubscribers() snapshot.
  - This makes startup robust to missed subscribe events and to peers that subscribed while we were offline.
  - Validation: reproduce migration tests with forced “subscribe event loss” (can be simulated by dropping first Subscribe message in pubsub tests) and ensure migration.spec.ts stabilizes.

  6. Test hardening (only after code hardening above)

  - Keep existing assertions, but make them wait on explicit shared-log state rather than only time:
      - In packages/programs/data/shared-log/test/events.spec.ts:84, wait for replicationIndex to contain exactly 1 segment for peer2 before asserting join list length.
      - In packages/programs/data/shared-log/test/migration.spec.ts:124, optionally bump waitForResolved timeout to 30s while the underlying fix lands (defaults are 10s at packages/utils/time/src/wait.ts:73).
  - Add a dedicated regression test for “duplicate inbound replication-info does not duplicate join”.

  7. CI signal improvements

  - Add a “flake detector” job (nightly or non-blocking) that runs test:ci:part-4 20x and uploads logs when a failure occurs.
  - Add trace logging behind an env flag for the replication-info queue and join emission so CI failures become diagnosable without reruns.
<Approach1>

<Review1>
## What PR #589 actually does (verified against the diff + PR description)

The PR description explicitly states the root cause is a debounce window: `subscribe()` queues work via `debounceSubscribeAggregator`, while the real `_subscribe()` (which calls `initializeTopic()` and sets `subscriptions`) happens later. If a peer receives (or responds to) messages in that window, state can become asymmetric. ([GitHub][1])

Concretely, the PR implements two behavioral changes in `DirectSub`:

1. **Eager topic initialization inside `subscribe()`**

It now calls `initializeTopic(topic)` immediately if the topic doesn’t exist, before adding to the debounce aggregator. ([GitHub][2])

2. **Treat “pending subscribes” as subscribes when answering `requestSubscribers`**

When building `mySubscriptions` (overlap), it now also includes any topic that is still pending inside `debounceSubscribeAggregator` (i.e., `subscribe()` was called but the debounced `_subscribe()` hasn’t executed yet). ([GitHub][2])

And it adds 3 tests in `bug1-initializeTopic-race.spec.ts`:

* Unit: `topics.has(topic)` becomes true immediately after calling `subscribe()` (before debounce fires).
* Integration: concurrent subscribe+connect → mutual discovery.
* Integration: connect first, then subscribe → still works. ([GitHub][2])

---

## Is the review correct?

### ✅ The core technical story is correct

The review’s description of the race matches the PR author’s own explanation:

* `subscribe()` is debounced, `_subscribe()` sets up topic tracking and `subscriptions`. ([GitHub][1])
* During the debounce gap:

  * an incoming `Subscribe` may be dropped because `topics.get(topic)` is undefined/null and the handler returns early ([GitHub][1])
  * `requestSubscribers` overlap can be empty because `subscriptions` isn’t set yet ([GitHub][1])

That’s exactly what the PR claims and what the fix targets.

### ✅ The review’s “what changed” section matches the diff

* Eager `initializeTopic()` in `subscribe()` is exactly in the diff. ([GitHub][2])
* Pending-subscribe inclusion using `debounceSubscribeAggregator.has(topic)` in the `requestSubscribers` response is exactly in the diff. ([GitHub][2])
* The three tests described in the review are present. ([GitHub][2])

### ✅ The nuance “this only fixes topics you locally called subscribe() for” is also intended

The PR author explicitly says they **did not** change the incoming Subscribe handler, and they rejected an earlier approach (“always init topic on incoming Subscribe”) because it broke a “rejoin with different subscriptions” test. The current approach is intentionally targeted to topics the local node subscribes to. ([GitHub][1])

So the review’s point that this is a *design choice* (not a general relay-tracks-everything behavior) is aligned with the PR.

### ⚠️ The review is right that the “pending subscribe advertised in requestSubscribers” isn’t directly asserted

The current tests prove **eventual mutual discovery** and **eager topic init**, but they do not explicitly assert “a requestSubscribers response includes a pending subscribe” as a standalone behavior. ([GitHub][2])

That said, it *may* be indirectly exercised by the concurrent subscribe+connect integration test depending on exactly how the protocol handshake works. The review’s “less directly proven” comment is fair.

### ⚠️ “subscribe then unsubscribe within debounce” is not covered

The review’s concern here is legitimate: eager init creates a topic entry even if you later cancel before the debounced work fires, and there’s no new test that ensures cleanup/advertising is correct. The PR notes unsubscribe already uses `debounceSubscribeAggregator.has()` for pending subscribes, but doesn’t add an explicit test for the subscribe→unsubscribe edge. ([GitHub][1])

---

## Tests to add to validate the assumptions

Below are **drop-in mocha+chai tests** in the same style as the existing `bug1-...spec.ts` file. They are designed to directly validate the assumptions the review calls out.

### Test file 1: `bug2-requestSubscribers-pendingSubscribe.spec.ts`

This file adds two integration tests:

1. **Directly tests the “pending subscribe is treated as subscribed for requestSubscribers” assumption**
   To make this deterministic, it *blocks* `_subscribe()` on peer A so `subscriptions` never gets set, while the “pending” marker should still be present. Then peer B subscribes and should still discover A as a subscriber. This isolates the new “pending subscribe counts” logic. The PR description explicitly names `_subscribe()` as the place where `subscriptions` is set, and the diff shows the `debounceSubscribeAggregator.has()` fallback used to cover that gap. ([GitHub][1])

2. **Asserts the “don’t track topics you didn’t subscribe to” design**
   Peer A never subscribes; peer B subscribes. We assert A never creates `topics` entry for that topic (confirming the intended behavior and guarding against reintroducing the reverted “initializeTopic on incoming Subscribe” behavior). ([GitHub][1])

```ts
// packages/transport/pubsub/test/bug2-requestSubscribers-pendingSubscribe.spec.ts
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub } from "../src/index.js";

function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: any) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

describe("BUG 2: pending subscribe should be visible via requestSubscribers", function () {
	this.timeout(60_000);

	it("peer discovers remote subscription while remote _subscribe() is blocked (pending subscribe advertised)", async () => {
		const TOPIC = "pending-subscribe-advertised";

		const session = await TestSession.disconnected(2, {
			services: {
				pubsub: (c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						connectionManager: false,
					}),
			},
		});

		try {
			const a = session.peers[0].services.pubsub;
			const b = session.peers[1].services.pubsub;

			// Connect first (so any handshake/requestSubscribers traffic can happen)
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			// Block A._subscribe() so that:
			// - A has called subscribe(TOPIC) (so debounce aggregator has it)
			// - but A never reaches the point where it sets `subscriptions`
			// This isolates the "pending subscribe counts" logic.
			const gate = deferred<void>();
			const aAny = a as any;

			expect(aAny._subscribe, "Expected DirectSub to have a _subscribe() method").to.be.a(
				"function",
			);

			const originalSubscribeImpl = aAny._subscribe.bind(aAny);
			aAny._subscribe = async (...args: any[]) => {
				await gate.promise;
				return originalSubscribeImpl(...args);
			};

			let aSubscribeResolved = false;
			const aSubscribePromise = a.subscribe(TOPIC).then(() => {
				aSubscribeResolved = true;
			});

			// Now subscribe normally on B
			await b.subscribe(TOPIC);

			// Key assertion:
			// B should learn that A is subscribed *even though* A's subscribe hasn't resolved yet.
			// Without the "pending subscribe" inclusion in requestSubscribers, B has no basis to learn A.
			await waitForResolved(() => {
				expect(aSubscribeResolved, "A.subscribe should still be pending").to.be.false;

				const bTopicMap = b.topics.get(TOPIC);
				expect(bTopicMap).to.not.be.undefined;
				expect(
					bTopicMap!.has(a.publicKeyHash),
					"B should record A as a subscriber while A is pending",
				).to.be.true;
			});

			// Cleanup: release A and let subscribe resolve, so we don't leave dangling work.
			gate.resolve();
			await aSubscribePromise;
		} finally {
			await session.stop();
		}
	});

	it("a node that did NOT subscribe does NOT start tracking a topic just because it receives Subscribe traffic (design guard)", async () => {
		const TOPIC = "non-subscriber-should-not-track";

		const session = await TestSession.disconnected(2, {
			services: {
				pubsub: (c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						connectionManager: false,
					}),
			},
		});

		try {
			const a = session.peers[0].services.pubsub; // will NOT subscribe
			const b = session.peers[1].services.pubsub; // WILL subscribe

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			await b.subscribe(TOPIC);

			// Give a moment for any Subscribe traffic to be exchanged
			await delay(250);

			// If we ever re-introduce "initializeTopic on incoming Subscribe",
			// this would start failing.
			expect(a.topics.has(TOPIC)).to.equal(false);
			expect(a.topics.get(TOPIC)).to.equal(undefined);
		} finally {
			await session.stop();
		}
	});
});
```

---

### Test file 2: `bug3-subscribe-then-unsubscribe-before-debounce.spec.ts`

This test targets the edge case the review flagged:

* Peer A calls `subscribe(topic)` (so topic is eagerly initialized and “pending subscribe” exists).
* Peer A then immediately calls `unsubscribe(topic)` before the debounced subscribe finishes.
* Peer B subscribes and connects/handshakes.
* Assert B does **not** record A as a subscriber, and A does **not** keep tracking the topic.

This validates the assumption: “unsubscribe properly cancels pending subscribe + stops advertising it”.

```ts
// packages/transport/pubsub/test/bug3-subscribe-then-unsubscribe-before-debounce.spec.ts
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub } from "../src/index.js";

function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: any) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

describe("Edge case: subscribe then unsubscribe inside debounce window", function () {
	this.timeout(60_000);

	it("does not advertise or retain topic if unsubscribe happens before debounced subscribe executes", async () => {
		const TOPIC = "subscribe-then-unsubscribe-before-debounce";

		const session = await TestSession.disconnected(2, {
			services: {
				pubsub: (c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						connectionManager: false,
					}),
			},
		});

		try {
			const a = session.peers[0].services.pubsub;
			const b = session.peers[1].services.pubsub;

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			// Block A._subscribe so A never reaches "subscriptions set" state,
			// making this test specifically about pending-subscribe cancellation.
			const gate = deferred<void>();
			const aAny = a as any;
			expect(aAny._subscribe).to.be.a("function");
			const originalSubscribeImpl = aAny._subscribe.bind(aAny);
			aAny._subscribe = async (...args: any[]) => {
				await gate.promise;
				return originalSubscribeImpl(...args);
			};

			// Start subscribe (pending) but do not await it
			const aSubscribe = a.subscribe(TOPIC).catch(() => {
				// If unsubscribe cancels in a way that rejects the subscribe promise,
				// we don't want an unhandled rejection to fail the test.
			});

			// Immediately unsubscribe
			await a.unsubscribe(TOPIC);

			// Now B subscribes; if A still advertises "pending subscribe",
			// B may incorrectly record A as a subscriber.
			await b.subscribe(TOPIC);

			// Wait a bit for any requestSubscribers/Subscribe traffic to settle
			await delay(500);

			const bTopicMap = b.topics.get(TOPIC);
			if (bTopicMap) {
				expect(
					bTopicMap.has(a.publicKeyHash),
					"B should NOT record A as subscribed after A unsubscribed during debounce",
				).to.equal(false);
			}

			// Also assert A is not still tracking the topic (or at least not claiming it)
			// Depending on current semantics, you may want either:
			// - topics entry removed entirely, OR
			// - topics entry exists but empty and not advertised.
			//
			// This is the stricter expectation (recommended):
			await waitForResolved(() => {
				expect(a.topics.has(TOPIC)).to.equal(false);
			});

			// Cleanup: release gate and give a tick so any queued work can finish
			gate.resolve();
			await delay(50);
			await aSubscribe;
		} finally {
			await session.stop();
		}
	});
});
```

**Note:** That “topics entry removed entirely” assertion is intentionally strict. If the intended semantics are “topic map may exist but should not be advertised / should stay empty,” then change that part to check the map is empty instead of checking `has() === false`.

---

## Summary of what these added tests validate

* **Assumption: `requestSubscribers` should include pending subscribes**
  ✅ Tested by forcing `subscriptions` to remain unset while the pending marker exists, and asserting the other peer still discovers the subscriber.

* **Assumption: eager init doesn’t accidentally turn a node into a topic tracker for topics it didn’t subscribe to**
  ✅ Tested by ensuring a non-subscribing peer doesn’t create topic entries just because it sees Subscribe traffic (guards against the reverted approach that broke “rejoin”). ([GitHub][1])

* **Assumption: unsubscribe cancels pending subscribe correctly**
  ✅ Tested by subscribe→unsubscribe inside debounce and checking no advertising/retained tracking.

If you want, I can also propose a **more surgical unit test** (no libp2p session) that asserts the requestSubscribers handler includes pending topics by directly invoking the handler, but that depends on whether the handler is factored into a testable method or is currently only inside `onDataMessage()` (it looks inlined). ([GitHub][1])

[1]: https://github.com/dao-xyz/peerbit/pull/589 "https://github.com/dao-xyz/peerbit/pull/589"
[2]: https://github.com/dao-xyz/peerbit/pull/589/files "https://github.com/dao-xyz/peerbit/pull/589/files"
</Review1>

<Review2>
Here’s what PR **#589** is doing, and whether the “this fixes the race” story holds up.

## What the PR claims the bug is

In **DirectSub**, `subscribe()` doesn’t immediately run the real subscription logic; it schedules it through a debounce aggregator. The PR author says there’s a window where:

1. `this.topics.get(topic)` is still `undefined`, so an incoming **Subscribe** message gets dropped (because the handler returns early), and
2. even if the topic exists, `getSubscriptionOverlap()` can still return empty because `this.subscriptions` hasn’t been set yet, so a **requestSubscribers** response is empty and the other peer never learns about your subscription. ([GitHub][1])

That produces “peer A knows peer B, but peer B doesn’t know peer A” when both subscribe + connect concurrently. ([GitHub][1])

## What actually changed in code

### 1) Eagerly initialize the topic in `subscribe()`

They added:

```ts
if (!this.topics.has(topic)) {
  this.initializeTopic(topic);
}
return this.debounceSubscribeAggregator.add({ key: topic });
```

…so the topic entry exists immediately, even before the debounced `_subscribe()` fires. ([GitHub][2])

### 2) Treat “pending subscribe” as a subscription in `requestSubscribers`

When building the `requestSubscribers` response, they now add topics that are still pending inside the debounce aggregator:

```ts
if (!mySubscriptions.includes(topic) &&
    this.debounceSubscribeAggregator.has(topic)) {
  mySubscriptions.push(topic);
}
```

So a peer can learn you’re “about to be subscribed” during the debounce window. ([GitHub][2])

### 3) Regression tests were added

The new test file asserts:

* **Unit:** right after calling `subscribe()`, `topics.has(topic)` is already true (before debounce fires) ([GitHub][2])
* **Integration:** if both peers `subscribe()` and `connect()` concurrently, they eventually see each other recorded under the topic ([GitHub][2])
* **Integration (normal path):** connect first, then subscribe, still works ([GitHub][2])

## Does this actually fix the described race?

### Problem 1 (“incoming Subscribe dropped because topics.get is undefined”)

**Yes — but only for topics the local node has called `subscribe()` on.**

If the incoming Subscribe handler was previously doing a `topics.get(topic)` + early return when undefined (as described in the PR), then creating the topic map *immediately* in `subscribe()` removes the “undefined topic” window for that topic. ([GitHub][1])

The unit test strongly supports that `initializeTopic()` creates the expected Map entry synchronously (it checks `.get(TOPIC)` is a `Map` immediately). ([GitHub][2])

### Problem 2 (“requestSubscribers response empty because subscriptions isn’t set yet”)

**The fix is logically consistent with the claimed cause**, because it explicitly covers the same debounce window by checking `debounceSubscribeAggregator.has(topic)`. ([GitHub][1])

However: **there is no direct regression test** in the added test file that asserts the `requestSubscribers` response includes pending subscribes. The tests mainly validate topic initialization + mutual discovery during concurrent subscribe/connect. ([GitHub][2])
So this part looks reasonable, but it’s less “proven” by the new tests than part 1.

## Are the PR’s assumptions correct?

### ✅ Assumption: “`subscribe()` is debounced; `_subscribe()` sets up topic tracking and `subscriptions` later”

This is explicitly stated in the PR writeup ([GitHub][1]) and is consistent with why the added tests would fail without eager init. I can’t fully confirm without reading the full `DirectSub` implementation (outside the diff hunks), but nothing in the diff contradicts it.

### ✅ Assumption: “Calling `initializeTopic()` early is safe / idempotent”

They claim it’s idempotent and guarded. ([GitHub][1])
In the actual change they also guard with `if (!this.topics.has(topic))`, so even if `initializeTopic()` isn’t perfectly idempotent, it won’t be called repeatedly for the same topic. ([GitHub][2])
One subtle risk: using `has()` assumes the map never contains a key with an `undefined` value (rare, but possible). If that could happen, checking `get() == null` would be safer than `has()`.

### ⚠️ Assumption: “Relays should *not* track topics they don’t subscribe to”

This is a design assumption, not a mechanical one.

The PR explicitly says they *avoided* initializing topics in the incoming Subscribe handler because it broke “rejoin with different subscriptions,” and they want to preserve “only track topics you care about.” ([GitHub][1])

That’s coherent if DirectSub nodes are *not supposed* to act as full routing relays for topics they aren’t subscribed to. If your “relay” use case depends on tracking remote subscribers even when the relay itself isn’t subscribed, then this PR intentionally does **not** solve that (it fixes only the “I called subscribe but debounce hasn’t fired yet” case).

### ⚠️ New behavioral edge case introduced by eager init

Eager init means `topics` will exist **even if you unsubscribe before the debounced subscribe executes** (depending on how unsubscribe cleanup works). If topic cleanup is tied to `_subscribe()` having run, you could end up temporarily (or permanently) tracking a topic you aren’t actually subscribed to.

The PR notes `debounceSubscribeAggregator.has()` is used in `unsubscribe()` for pending subscribes ([GitHub][1]), which suggests they’ve thought about pending subscribe cancellation — but there’s no new test covering “subscribe then immediately unsubscribe” to ensure the topic entry is removed/ignored correctly.

## What I’d recommend to “verify” this PR beyond what’s here

If you want higher confidence the assumptions are correct, I’d add/confirm:

1. **A regression test for requestSubscribers + pending subscribe**, e.g.:

   * Peer B calls `subscribe(topic)` but before debounce fires,
   * Peer A triggers `requestSubscribers` to B,
   * Assert A learns B is subscribed (via the pending check).

2. **A subscribe-then-unsubscribe-within-debounce test**, asserting:

   * topic doesn’t remain tracked / doesn’t cause incorrect remote subscription tracking,
   * `requestSubscribers` does *not* advertise it once unsubscribe has been called.

3. **A quick read of `initializeTopic()`** to confirm it’s truly “create map + no heavy side effects,” since the PR relies on it being safe to do earlier. ([GitHub][1])

---

**Bottom line:**

* The **core fix (eager topic init in `subscribe()`) matches the described race and is supported by the new unit + integration tests.** ([GitHub][2])
* The **requestSubscribers pending-subscribe fix is plausible and consistent**, but it’s **not directly exercised** by the new tests. ([GitHub][2])
* The **relay/“track topics you didn’t subscribe to”** question is a **design choice**; the PR explicitly chooses “no” to avoid breaking rejoin behavior. ([GitHub][1])

[1]: https://github.com/dao-xyz/peerbit/pull/589 "fix(pubsub): eagerly initialize topic in subscribe() to prevent race by Faolain · Pull Request #589 · dao-xyz/peerbit · GitHub"
[2]: https://github.com/dao-xyz/peerbit/pull/589/files "fix(pubsub): eagerly initialize topic in subscribe() to prevent race by Faolain · Pull Request #589 · dao-xyz/peerbit · GitHub"
</Review2>

Other relevant information:
- This PR https://github.com/Faolain/peerbit/pull/3 attempts to solve the TOCTOU race within shared-log but seems to have CI failures. Find out what those are.  Did these appear on CI history for main or any other PRs or did this PR cause these regressions. 

- This PR https://github.com/Faolain/peerbit/pull/4 attempts to be a more comprehensive solution than https://github.com/Faolain/peerbit/pull/3 yet still also has CI errors. Find out what these errors are see if they appear in the CI history for main or other PRs or if this PR is the cause of it.  
- This PR https://github.com/dao-xyz/peerbit/pull/591 fix(shared-log): avoid unhandled rejection when entry.hash is missing which also has its own CI errors on Github that can be inspected. 


Instructions: 
- When running tests, for every test that either passes or fails note down the result in the shared-log-debug-plan.md learnings section along with any learnings you had from that result. 

If there are any gotchas/ahas note them in the # Ahas/Gotchas section.

There are many claims and assumptions in this document and your task is to find the root cause/causes of the connectivity bug given all the information you have available and make all tests pass. Reminder we want to use tests as our way to validate our assumptions. We want narrow tests that can be run quickly to validate our hypothesis. 

- Spawn a subagent to analyze the reviews in the context of everything above and include them in the shared-log-debug-plan.md Reviews section.
- Spawn a subagent to create tests for all relevant tests that should be created based on the below information, PR descriptions, PR diffs, and your own judgment for proposed tests that should exist. Put these all in a shared-
- Spawn a subagent to verify the veracity of the claims below, which you are to put in the shared-log-debug-plan.md in the claims section.
- Spawn a subagent to compare the produced claims to the generated tests to see if any of the tests can validate the claims, if not create tests to validate those claims. 
- Finally spawn a final agent after all of the above is done to aggregate all the information and create a definitive attack plan to solve the problem being seeing whether it's in pubsub, shared-log or elsewhere. Remember we need our tests to be quick and we need to validate all assumptions. Leave the full tests for later when we're confident (since they take over 20 minutes to run).

