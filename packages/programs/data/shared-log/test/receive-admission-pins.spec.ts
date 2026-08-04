// Stage-3 PR-3 pinning tests. These pin the two receive-path behaviors most
// likely to break subtly when the D1/D2 fences migrate onto the peer-session
// registry (stage-3 manifest, PART 5):
//
// 1. The replication-info RECOVERY EPOCH (`_replicationInfoReceiveEpochByPeer`)
//    is the ONLY fence stopping a handler that released its receive lease
//    before a committed removeReplicator from resurrecting the evicted
//    replicator when it reaches the apply lane — the peer session does NOT
//    rotate at removal (the peer stays subscribed).
//
// 2. The OPENING-BARRIER truth (`_subscriptionOpeningEpochByPeer`) is a
//    WINDOW (barrier start -> settle/unsubscribe), not a phase: a session
//    already rotated to "opening" whose barrier has not started must not
//    stash capability adverts, and a barrier superseded mid-drain must not
//    promote what it stashed.
//
// Both tests were written (and passed) against the pre-migration fences; the
// behavioral assertions are unchanged after the seam-2 migration — only the
// internal window probes were re-pointed to the fences' stage-3 homes (the
// receive-epoch map on the PeerSessionRegistry, the opening-barrier flag on
// the PeerSession).
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { SyncCapabilitiesMessage } from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { AddedReplicationSegmentMessage } from "../src/replication.js";
import { RequestMaybeSync, SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-receive-admission-pins",
};

describe("receive admission replication-info recovery epoch", () => {
	it("fences a handler parked before its apply-lane turn across a committed liveness eviction", async () => {
		const session = await TestSession.connected(2);
		const handlerParked = pDefer<void>();
		const releaseHandler = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store, {
				args: { replicate: 1, setup, timeUntilRoleMaturity: 0 },
			});
			await session.peers[1].open(store.clone(), {
				args: { replicate: 1, setup, timeUntilRoleMaturity: 0 },
			});
			const sharedLog = target.log as any;
			const sourceKey = session.peers[1].identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			let remoteRange: any;
			await waitForResolved(async () => {
				const ranges = await target.log.replicationIndex
					.iterate({ query: { hash: sourceHash } })
					.all();
				expect(ranges).to.have.length.greaterThan(0);
				remoteRange = ranges[0].value;
			});

			// A committed removal schedules fresh replication-info requests to the
			// still-subscribed peer; a genuine re-learn through those would mask
			// what this test pins, so keep them from firing.
			const scheduleRequests = sinon
				.stub(sharedLog, "scheduleReplicationInfoRequests")
				.callsFake(() => {});

			const delayedMessage = new AddedReplicationSegmentMessage({
				segments: [remoteRange.toReplicationRange()],
			});
			// Arm inside the synchronizer pass for exactly this message: from the
			// synchronizer's decline to the apply-queue call the handler runs
			// synchronously, so the next apply-queue call for this peer is its own.
			let armed = false;
			const originalSynchronizerOnMessage =
				sharedLog.syncronizer.onMessage.bind(sharedLog.syncronizer);
			const synchronizer = sinon
				.stub(sharedLog.syncronizer, "onMessage")
				.callsFake(async (message: unknown, context: unknown) => {
					if (message === delayedMessage) {
						armed = true;
						return false;
					}
					return originalSynchronizerOnMessage(message, context);
				});
			// Park the handler BETWEEN its receive-lease release and its
			// apply-lane turn: it is no longer drainable, but has not entered the
			// lane. Only the receive-epoch fence can stop it now.
			const originalApplyQueue =
				sharedLog.withReplicationInfoApplyQueue.bind(sharedLog);
			const applyQueue = sinon
				.stub(sharedLog, "withReplicationInfoApplyQueue")
				.callsFake(async (...args: unknown[]) => {
					const [peerHash, fn] = args as [string, () => Promise<void>];
					if (armed && peerHash === sourceHash) {
						armed = false;
						handlerParked.resolve();
						await releaseHandler.promise;
					}
					return originalApplyQueue(peerHash, fn);
				});

			let joins = 0;
			const onJoin = () => {
				joins += 1;
			};

			try {
				const receive = target.log.onMessage(delayedMessage, {
					from: sourceKey,
					message: { header: { timestamp: BigInt(Date.now()) } },
				} as any);
				await handlerParked.promise;

				// Drive a liveness eviction through to a committed removeReplicator,
				// exactly as the liveness monitor does it (the session does NOT
				// rotate: the peer is still subscribed).
				const liveness = sharedLog._liveness;
				await liveness.evictReplicatorFromLiveness(
					sourceHash,
					sourceKey,
					sharedLog._replicationLifecycleController,
					sharedLog._peerSessions.current(sourceHash),
					liveness._replicatorLastActivityAt.get(sourceHash),
				);

				// The eviction committed: ranges deleted, recovery epoch advanced,
				// ordering watermark reset.
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(
					sharedLog.getReplicationInfoReceiveEpoch(sourceHash),
				).to.not.equal(null);
				expect(sharedLog.latestReplicationInfoMessage.has(sourceHash)).to.be
					.false;

				target.log.events.addEventListener("replicator:join", onJoin);
				releaseHandler.resolve();
				await receive;

				// The parked handler reached its lane after the committed removal:
				// it must not restore the range, must not re-fire replicator:join,
				// and must not write a fresh ordering watermark.
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(target.log.uniqueReplicators.has(sourceHash)).to.be.false;
				expect(sharedLog.latestReplicationInfoMessage.has(sourceHash)).to.be
					.false;
				expect(joins).to.equal(0);
			} finally {
				target.log.events.removeEventListener("replicator:join", onJoin);
				releaseHandler.resolve();
				applyQueue.restore();
				synchronizer.restore();
				scheduleRequests.restore();
			}
		} finally {
			releaseHandler.resolve();
			await session.stop();
		}
	});

	it("keeps a pre-close recovery-epoch capture stale after reopen", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const db = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false, setup },
			});
			const sharedLog = db.log as any;
			const peerHash = "remote-peer-recovery-epoch";

			const captured = sharedLog.advanceReplicationInfoReceiveEpoch(peerHash);
			expect(
				sharedLog.isCurrentReplicationInfoReceiveEpoch(peerHash, captured),
			).to.be.true;

			await db.close();
			// The epoch map is cleared at close: a capture held across close
			// compares against null, never against a surviving token.
			expect(
				sharedLog.isCurrentReplicationInfoReceiveEpoch(peerHash, captured),
			).to.be.false;

			await session.peers[0].open(db, {
				args: { replicate: false, setup },
			});
			// After reopen the pre-close capture must still fail the current-check:
			// close()+open() is a hard fence for every outstanding capture.
			expect(
				sharedLog.isCurrentReplicationInfoReceiveEpoch(peerHash, captured),
			).to.be.false;

			// Sanity: the reopened instance issues fresh, current tokens.
			const fresh = sharedLog.advanceReplicationInfoReceiveEpoch(peerHash);
			expect(sharedLog.isCurrentReplicationInfoReceiveEpoch(peerHash, fresh)).to
				.be.true;
			expect(fresh).to.not.equal(captured);
		} finally {
			await session.stop();
		}
	});
});

describe("receive admission opening-barrier windows", () => {
	it("does not stash a capability advert before the reconnect barrier starts", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();

			// The opening-barrier truth is a WINDOW (map entry set at barrier
			// start), not a phase: rotate a session to "opening" WITHOUT starting
			// its barrier.
			sharedLog.advanceSubscriptionEpoch(sourceHash, "opening");

			// An advert arriving now takes the plain path: applied directly, never
			// staged in the opening stash.
			await target.log.onMessage(
				new SyncCapabilitiesMessage({ capabilities: 3 }),
				{ from: sourceKey } as any,
			);
			expect(sharedLog._openingSyncCapabilitiesByPeer.has(sourceHash)).to.be
				.false;
			expect(sharedLog._peerSyncCapabilities.get(sourceHash)).to.equal(3);

			// With the peer replication-info-blocked but still no barrier window,
			// the advert is not admitted at all — the pre-barrier "opening" phase
			// must not widen admission or stage the advert.
			sharedLog._peerSessions._replicationInfoBlockedPeers.add(sourceHash);
			try {
				await target.log.onMessage(
					new SyncCapabilitiesMessage({ capabilities: 5 }),
					{ from: sourceKey } as any,
				);
				expect(sharedLog._openingSyncCapabilitiesByPeer.has(sourceHash)).to.be
					.false;
				expect(sharedLog._peerSyncCapabilities.get(sourceHash)).to.equal(3);
			} finally {
				sharedLog._peerSessions._replicationInfoBlockedPeers.delete(sourceHash);
			}
		} finally {
			await session.stop();
		}
	});

	it("does not promote a stashed advert when the reconnect barrier aborts mid-drain", async () => {
		const session = await TestSession.disconnected(2);
		const parkEntered = pDefer<void>();
		const releasePark = pDefer<void>();
		let oldReceive: Promise<void> | undefined;
		let subscription: Promise<void> | undefined;
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const scheduleRequests = sinon
				.stub(sharedLog, "scheduleReplicationInfoRequests")
				.callsFake(() => {});
			const parkMessage = new RequestMaybeSync({ hashes: [] });
			const originalSynchronizerOnMessage =
				sharedLog.syncronizer.onMessage.bind(sharedLog.syncronizer);
			const synchronizer = sinon
				.stub(sharedLog.syncronizer, "onMessage")
				.callsFake(async (message: unknown, context: unknown) => {
					if (message === parkMessage) {
						parkEntered.resolve();
						await releasePark.promise;
						return true;
					}
					return originalSynchronizerOnMessage(message, context);
				});

			try {
				// Hold an admitted receive so the reconnect barrier parks mid-drain.
				oldReceive = target.log.onMessage(parkMessage, {
					from: sourceKey,
				} as any);
				await parkEntered.promise;

				let subscriptionSettled = false;
				subscription = sharedLog
					._onSubscription({
						detail: { from: sourceKey, topics: [target.log.topic] },
					})
					.then(() => {
						subscriptionSettled = true;
					});
				await waitForResolved(() =>
					expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
						.true,
				);
				// Window-open probe (stage-3 home: the barrier flags its session).
				const barrierSession = sharedLog._peerSessions.current(sourceHash);
				expect(barrierSession).to.exist;
				expect(barrierSession.openingBarrierActive).to.be.true;

				// An advert arriving inside the barrier window is stashed for the
				// opening generation, not applied.
				await target.log.onMessage(
					new SyncCapabilitiesMessage({ capabilities: 3 }),
					{ from: sourceKey } as any,
				);
				expect(
					sharedLog._openingSyncCapabilitiesByPeer.get(sourceHash)
						?.capabilities,
				).to.equal(3);
				expect(sharedLog._peerSyncCapabilities.has(sourceHash)).to.be.false;

				// Abort the barrier mid-drain: a newer rotation supersedes its
				// subscription epoch before the drain settles.
				sharedLog.advanceSubscriptionEpoch(sourceHash, "departing");
				expect(subscriptionSettled).to.be.false;

				releasePark.resolve();
				await Promise.all([oldReceive, subscription]);

				// The aborted barrier must not promote the stashed advert, and the
				// window state must be fully torn down.
				expect(sharedLog._peerSyncCapabilities.has(sourceHash)).to.be.false;
				expect(sharedLog._openingSyncCapabilitiesByPeer.has(sourceHash)).to.be
					.false;
				expect(barrierSession.openingBarrierActive).to.be.false;
			} finally {
				releasePark.resolve();
				await Promise.allSettled(
					[oldReceive, subscription].filter(
						(value): value is Promise<void> => value != null,
					),
				);
				synchronizer.restore();
				scheduleRequests.restore();
			}
		} finally {
			releasePark.resolve();
			await session.stop();
		}
	});
});
