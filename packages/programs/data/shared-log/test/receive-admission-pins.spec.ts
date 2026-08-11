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
import { BorshError } from "@dao-xyz/borsh";
import { AccessError } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { NativeDurableCommitError } from "../src/errors.js";
import {
	RequestIPruneV2,
	ResponseIPruneV2,
	StashBackedRawExchangeHeadsMessage,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import {
	AddedReplicationInfoV2Message,
	AllReplicatingSegmentsMessage,
	RequestReplicationInfoMessage,
} from "../src/replication.js";
import {
	ConfirmEntriesMessage,
	RequestMaybeSync,
	SimpleSyncronizer,
} from "../src/sync/simple.js";
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
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
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
			await waitForResolved(() =>
				expect(
					sharedLog._v2Receive._receiveStates.get(sourceHash)?.phase,
				).to.equal("active"),
			);
			const receiveState = sharedLog._v2Receive._receiveStates.get(sourceHash);

			// A committed removal re-solicits replication info from the
			// still-subscribed peer; a genuine re-learn through recovery would
			// mask what this test pins, so keep it from firing.
			const v2Recovery = sinon
				.stub(sharedLog, "scheduleReplicationInfoV2Recovery")
				.callsFake(() => {});
			const v2ReAdvertisement = sinon
				.stub(sharedLog._v2Receive, "reAdvertiseLocalCapabilityForRecovery")
				.returns(true);

			// The stream's next in-order mutation frame: byte-valid for the
			// CURRENT authenticated stream, so absent the fence under test it
			// would apply.
			const delayedMessage = new AddedReplicationInfoV2Message({
				receiverChallenge: receiveState.receiverBinding.slice(),
				senderEpoch: receiveState.senderEpoch.slice(),
				sequence: receiveState.lastSequence + 1n,
				segments: [remoteRange.toReplicationRange()],
			});
			// Park the handler BETWEEN its receive-lease release and its
			// apply-lane turn: the V2 handler releases its lease right after
			// reserving admission, before joining the per-peer apply lane. Only
			// the receive-epoch fence can stop it now.
			let armed = false;
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
				armed = true;
				const receive = target.log.onMessage(delayedMessage, {
					from: sourceKey,
					message: {
						header: {
							session: receiveState.senderTransportSession,
							timestamp: BigInt(Date.now()),
						},
					},
				} as any);
				await handlerParked.promise;

				// Drive a liveness eviction through to a committed removeReplicator,
				// exactly as the liveness monitor does it (the session does NOT
				// rotate: the peer is still subscribed).
				const liveness = sharedLog._liveness;
				await liveness.evictReplicatorFromLiveness(
					sourceHash,
					sourceKey,
					sharedLog._instanceLifecycle?.membershipLifecycleController,
					sharedLog._peerSessions.current(sourceHash),
					liveness._replicatorLastActivityAt.get(sourceHash),
				);

				// The eviction committed: ranges deleted, recovery epoch advanced.
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(sharedLog._peerSessions.receiveEpoch(sourceHash)).to.not.equal(
					null,
				);

				target.log.events.addEventListener("replicator:join", onJoin);
				releaseHandler.resolve();
				await receive;

				// The parked handler reached its lane after the committed removal:
				// it must not restore the range and must not re-fire
				// replicator:join.
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(target.log.uniqueReplicators.has(sourceHash)).to.be.false;
				expect(joins).to.equal(0);
			} finally {
				target.log.events.removeEventListener("replicator:join", onJoin);
				releaseHandler.resolve();
				applyQueue.restore();
				v2ReAdvertisement.restore();
				v2Recovery.restore();
			}
		} finally {
			releaseHandler.resolve();
			await session.stop();
		}
	});

	it("fences a parked handler when coherent deletion throws after durable commit", async () => {
		const session = await TestSession.connected(2);
		const handlerParked = pDefer<void>();
		const releaseHandler = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store, {
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
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
			await waitForResolved(() =>
				expect(
					sharedLog._v2Receive._receiveStates.get(sourceHash)?.phase,
				).to.equal("active"),
			);
			const receiveState = sharedLog._v2Receive._receiveStates.get(sourceHash);

			const scheduleV2Recovery = sinon
				.stub(sharedLog, "scheduleReplicationInfoV2Recovery")
				.callsFake(() => {});
			const recoveryCalls: Array<{
				gateOpen: boolean;
				receiveEpoch: object | null;
			}> = [];
			const v2Recovery = sinon
				.stub(sharedLog._v2Receive, "advanceRecovery")
				.callsFake((...args: unknown[]) => {
					const properties = args[0] as { receiveEpoch: object | null };
					recoveryCalls.push({
						gateOpen:
							sharedLog._peerSessions.isReceiveCleanupGateOpen(sourceHash),
						receiveEpoch: properties.receiveEpoch,
					});
					return true;
				});
			const reAdvertisementCalls: Array<{
				gateOpen: boolean;
				peerSession: object;
				receiveEpoch: object | null;
			}> = [];
			const v2ReAdvertisement = sinon
				.stub(sharedLog._v2Receive, "reAdvertiseLocalCapabilityForRecovery")
				.callsFake((...args: unknown[]) => {
					const properties = args[0] as {
						peerSession: object;
						receiveEpoch: object | null;
					};
					reAdvertisementCalls.push({
						gateOpen:
							sharedLog._peerSessions.isReceiveCleanupGateOpen(sourceHash),
						peerSession: properties.peerSession,
						receiveEpoch: properties.receiveEpoch,
					});
					return true;
				});
			// The stream's next in-order mutation frame: byte-valid for the
			// CURRENT authenticated stream, so absent the fence under test it
			// would apply.
			const delayedMessage = new AddedReplicationInfoV2Message({
				receiverChallenge: receiveState.receiverBinding.slice(),
				senderEpoch: receiveState.senderEpoch.slice(),
				sequence: receiveState.lastSequence + 1n,
				segments: [remoteRange.toReplicationRange()],
			});
			let armed = false;
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
			const originalDelete =
				sharedLog.deleteReplicationRangesCoherently.bind(sharedLog);
			const postCommitFailure = new Error("post-commit deletion failure");
			const coherentDelete = sinon
				.stub(sharedLog, "deleteReplicationRangesCoherently")
				.callsFake(async (...args: unknown[]) => {
					await originalDelete(...args);
					throw postCommitFailure;
				});

			try {
				armed = true;
				const receive = target.log.onMessage(delayedMessage, {
					from: sourceKey,
					message: {
						header: {
							session: receiveState.senderTransportSession,
							timestamp: BigInt(Date.now()),
						},
					},
				} as any);
				await handlerParked.promise;
				const receiveEpochBefore =
					sharedLog._peerSessions.receiveEpoch(sourceHash);

				let removalError: unknown;
				try {
					await sharedLog.removeReplicator(sourceKey, { noEvent: true });
				} catch (error) {
					removalError = error;
				}
				expect(removalError).to.equal(postCommitFailure);
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(sharedLog._peerSessions.receiveEpoch(sourceHash)).to.not.equal(
					receiveEpochBefore,
				);
				expect(recoveryCalls).to.have.length(2);
				expect(recoveryCalls.map((call) => call.gateOpen)).to.deep.equal([
					false,
					true,
				]);
				expect(recoveryCalls[0].receiveEpoch).to.equal(
					recoveryCalls[1].receiveEpoch,
				);
				expect(reAdvertisementCalls).to.have.length(1);
				expect(reAdvertisementCalls[0].gateOpen).to.be.true;
				expect(reAdvertisementCalls[0].peerSession).to.equal(
					sharedLog._peerSessions.current(sourceHash),
				);
				expect(reAdvertisementCalls[0].receiveEpoch).to.equal(
					recoveryCalls[1].receiveEpoch,
				);

				releaseHandler.resolve();
				await receive;
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(target.log.uniqueReplicators.has(sourceHash)).to.be.false;
			} finally {
				releaseHandler.resolve();
				coherentDelete.restore();
				applyQueue.restore();
				v2ReAdvertisement.restore();
				v2Recovery.restore();
				scheduleV2Recovery.restore();
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

			const captured = sharedLog._peerSessions.advanceReceiveEpoch(peerHash);
			expect(sharedLog._peerSessions.isReceiveEpochCurrent(peerHash, captured))
				.to.be.true;

			await db.close();
			// The epoch map is cleared at close: a capture held across close
			// compares against null, never against a surviving token.
			expect(sharedLog._peerSessions.isReceiveEpochCurrent(peerHash, captured))
				.to.be.false;

			await session.peers[0].open(db, {
				args: { replicate: false, setup },
			});
			// After reopen the pre-close capture must still fail the current-check:
			// close()+open() is a hard fence for every outstanding capture.
			expect(sharedLog._peerSessions.isReceiveEpochCurrent(peerHash, captured))
				.to.be.false;

			// Sanity: the reopened instance issues fresh, current tokens.
			const fresh = sharedLog._peerSessions.advanceReceiveEpoch(peerHash);
			expect(sharedLog._peerSessions.isReceiveEpochCurrent(peerHash, fresh)).to
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
			sharedLog._peerSessions.rotate(sourceHash, "opening");

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
				await waitForResolved(
					() =>
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
				sharedLog._peerSessions.rotate(sourceHash, "departing");
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

// Stage-4.5 PR-2 pinning tests (P5-P7). These pin the dispatcher facts that
// must survive the extraction of the five cold control-plane branches out of
// `onMessage` (both checked-prune protocol arms and the three
// replication-info arms):
//
// P5. DISPATCH PRECEDENCE: the control-plane arms declared ahead of the
//     synchronizer delegation (checked prune, ConfirmEntries,
//     SyncCapabilities) never reach `syncronizer.onMessage`, and the arms
//     declared behind it (RequestReplicationInfoMessage) run only when the
//     synchronizer declines.
//
// P6. LEASE ONE-SHOT: a replication-info announcement releases its receive
//     lease BEFORE joining the per-peer apply lane (a stalled lane does not
//     hold `_activeReceiveHandlersByPeer`), and the finally's release after
//     the mid-branch release has no effect on concurrently held leases.
//
// P7. ERROR ENVELOPE: control-plane handler throws traverse the shared
//     classifier (AccessError/BorshError/AbortError swallowed while
//     NativeDurableCommitError propagates), and the wire-stash release in
//     the finally runs exactly once, before the poison recheck.

describe("receive admission control-plane dispatch precedence", () => {
	it("handles prune, confirm and capability messages ahead of the synchronizer", async () => {
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
			sharedLog._peerSessions.rotate(sourceHash, "opening");

			const synchronizer = sinon.spy(sharedLog.syncronizer, "onMessage");
			const removeKnown = sinon.spy(sharedLog, "removeEntriesKnownByPeer");
			const admit = sinon
				.stub(sharedLog, "admitAndSendCheckedPruneGrants")
				.resolves({ missing: [], admitted: [] });
			const pendingDelete = sinon.spy(
				sharedLog._checkedPrune,
				"getPendingDelete",
			);
			const markKnown = sinon.spy(sharedLog, "markEntriesKnownByPeer");
			try {
				const pruneRequest = new RequestIPruneV2({
					requests: [{ hash: "hash-a", requestId: new Uint8Array(32) }],
				});
				await target.log.onMessage(pruneRequest, { from: sourceKey } as any);
				expect(removeKnown.calledOnce).to.be.true;
				expect(admit.calledOnce).to.be.true;

				const pruneResponse = new ResponseIPruneV2({
					requests: [{ hash: "hash-a", requestId: new Uint8Array(32) }],
				});
				await target.log.onMessage(pruneResponse, { from: sourceKey } as any);
				expect(pendingDelete.calledOnce).to.be.true;

				const confirm = new ConfirmEntriesMessage({ hashes: ["hash-a"] });
				await target.log.onMessage(confirm, { from: sourceKey } as any);
				expect(markKnown.calledOnce).to.be.true;

				const capabilities = new SyncCapabilitiesMessage({ capabilities: 3 });
				await target.log.onMessage(capabilities, { from: sourceKey } as any);
				expect(sharedLog._peerSyncCapabilities.get(sourceHash)).to.equal(3);

				// None of the pre-delegation arms consulted the synchronizer.
				const seen = synchronizer.getCalls().map((call) => call.args[0]);
				expect(seen).to.not.include(pruneRequest);
				expect(seen).to.not.include(pruneResponse);
				expect(seen).to.not.include(confirm);
				expect(seen).to.not.include(capabilities);
			} finally {
				synchronizer.restore();
				removeKnown.restore();
				admit.restore();
				pendingDelete.restore();
				markKnown.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("reaches the replication-info request arm only when the synchronizer declines", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { compatibility: 9, replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { compatibility: 9, replicate: 1, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			sharedLog._peerSessions.rotate(sourceHash, "opening");

			const send = sinon.stub(sharedLog.rpc, "send").resolves();
			const segments = sinon.spy(sharedLog, "getMyReplicationSegments");
			let claim = true;
			const originalSynchronizerOnMessage =
				sharedLog.syncronizer.onMessage.bind(sharedLog.syncronizer);
			const synchronizer = sinon
				.stub(sharedLog.syncronizer, "onMessage")
				.callsFake(async (message: unknown, context: unknown) => {
					if (message instanceof RequestReplicationInfoMessage) {
						return claim;
					}
					return originalSynchronizerOnMessage(message, context);
				});
			const sentSegmentsMessages = () =>
				send
					.getCalls()
					.filter(
						(call) => call.args[0] instanceof AllReplicatingSegmentsMessage,
					).length;
			try {
				// Claimed by the synchronizer: the arm behind the delegation must
				// not run.
				await target.log.onMessage(new RequestReplicationInfoMessage(), {
					from: sourceKey,
				} as any);
				expect(segments.called).to.be.false;
				expect(sentSegmentsMessages()).to.equal(0);

				// Declined: the arm answers with the local segments.
				claim = false;
				await target.log.onMessage(new RequestReplicationInfoMessage(), {
					from: sourceKey,
				} as any);
				expect(segments.called).to.be.true;
				expect(sentSegmentsMessages()).to.equal(1);
			} finally {
				synchronizer.restore();
				segments.restore();
				send.restore();
			}
		} finally {
			await session.stop();
		}
	});
});

describe("receive admission control-plane lease one-shot", () => {
	it("releases the receive lease before the apply lane and never twice", async () => {
		const session = await TestSession.connected(2);
		const laneEntered = pDefer<void>();
		const releaseLane = pDefer<void>();
		let parkedLane: Promise<void> | undefined;
		let receive: Promise<void> | undefined;
		try {
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store, {
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: 1,
					setup,
					timeUntilRoleMaturity: 0,
				},
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
			await waitForResolved(() =>
				expect(
					sharedLog._v2Receive._receiveStates.get(sourceHash)?.phase,
				).to.equal("active"),
			);
			const receiveState = sharedLog._v2Receive._receiveStates.get(sourceHash);
			try {
				// Hold a second lease in the same bucket: a lost mid-branch release
				// would keep the bucket at 2, and any extra release (e.g. a finally
				// that releases again) would drain this lease's slot too — both
				// visibly different from the pinned steady state of 1.
				const extraRelease = sharedLog.acquirePeerReceiveLease(
					sourceHash,
					sharedLog._instanceLifecycle?.membershipLifecycleController,
					sharedLog._peerSessions.current(sourceHash),
				);
				expect(extraRelease).to.exist;
				await waitForResolved(() =>
					expect(
						sharedLog._activeReceiveHandlersByPeer.get(sourceHash)?.current
							.active,
					).to.equal(1),
				);

				// Park the per-peer apply lane so the announcement's lane turn
				// queues behind it.
				parkedLane = sharedLog.withReplicationInfoApplyQueue(
					sourceHash,
					async () => {
						laneEntered.resolve();
						await releaseLane.promise;
					},
				);
				await laneEntered.promise;
				const parkedTail =
					sharedLog._replicationInfoApplyQueueByPeer.get(sourceHash);

				const timestamp = BigInt(Date.now() + 5_000);
				const announcedSequence = receiveState.lastSequence + 1n;
				let settled = false;
				receive = target.log
					.onMessage(
						new AddedReplicationInfoV2Message({
							receiverChallenge: receiveState.receiverBinding.slice(),
							senderEpoch: receiveState.senderEpoch.slice(),
							sequence: announcedSequence,
							segments: [remoteRange.toReplicationRange()],
						}),
						{
							from: sourceKey,
							message: {
								header: {
									session: receiveState.senderTransportSession,
									timestamp,
								},
							},
						} as any,
					)
					.then(() => {
						settled = true;
					});
				// The handler queued its apply-lane turn…
				await waitForResolved(() =>
					expect(
						sharedLog._replicationInfoApplyQueueByPeer.get(sourceHash),
					).to.not.equal(parkedTail),
				);
				// …and released its receive lease BEFORE that turn could run: with
				// the lane still parked, only the concurrently held lease remains.
				await waitForResolved(() =>
					expect(
						sharedLog._activeReceiveHandlersByPeer.get(sourceHash)?.current
							.active,
					).to.equal(1),
				);
				expect(settled).to.be.false;

				releaseLane.resolve();
				await receive;
				// The announcement applied in its lane turn (the stream committed
				// the forged frame's sequence)…
				expect(receiveState.lastSequence).to.equal(announcedSequence);
				// …and the finally's release after the mid-branch release had no
				// effect: the concurrently held lease still occupies its slot.
				await waitForResolved(() =>
					expect(
						sharedLog._activeReceiveHandlersByPeer.get(sourceHash)?.current
							.active,
					).to.equal(1),
				);
				extraRelease();
				await waitForResolved(
					() =>
						expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
							.false,
				);
			} finally {
				// no stubs to restore
			}
		} finally {
			releaseLane.resolve();
			await Promise.allSettled(
				[parkedLane, receive].filter(
					(value): value is Promise<void> => value != null,
				),
			);
			await session.stop();
		}
	});
});

describe("receive admission receive error envelope", () => {
	it("swallows classified control-plane errors and rethrows durable poison", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: 1, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			sharedLog._peerSessions.rotate(sourceHash, "opening");
			const send = sinon.stub(sharedLog.rpc, "send").resolves();
			try {
				// AccessError from inside the prune arm is swallowed.
				const admit = sinon
					.stub(sharedLog, "admitAndSendCheckedPruneGrants")
					.rejects(new AccessError("pinned"));
				await target.log.onMessage(
					new RequestIPruneV2({
						requests: [{ hash: "hash-a", requestId: new Uint8Array(32) }],
					}),
					{ from: sourceKey } as any,
				);
				expect(admit.calledOnce).to.be.true;
				admit.restore();

				// BorshError from the confirm arm is swallowed.
				const markKnown = sinon
					.stub(sharedLog, "markEntriesKnownByPeer")
					.throws(new BorshError("pinned"));
				await target.log.onMessage(
					new ConfirmEntriesMessage({ hashes: ["hash-a"] }),
					{ from: sourceKey } as any,
				);
				expect(markKnown.calledOnce).to.be.true;
				markKnown.restore();

				// NativeDurableCommitError is the one class the envelope rethrows.
				const removeKnown = sinon
					.stub(sharedLog, "removeEntriesKnownByPeer")
					.throws(new NativeDurableCommitError(new Error("pinned")));
				await expect(
					target.log.onMessage(
						new RequestIPruneV2({
							requests: [{ hash: "hash-b", requestId: new Uint8Array(32) }],
						}),
						{ from: sourceKey } as any,
					),
				).to.be.rejectedWith(NativeDurableCommitError);
				expect(removeKnown.calledOnce).to.be.true;
				removeKnown.restore();
			} finally {
				send.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("releases a wire-backed raw stash exactly once, before the poison recheck", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const order: string[] = [];
			const stashRelease = sinon.stub().callsFake(() => {
				order.push("stash");
				return true;
			});
			const message = new StashBackedRawExchangeHeadsMessage({
				messageId: new Uint8Array(32),
				hashes: [],
				gidRefrences: [],
				byteLengths: new Uint32Array(0),
				reserved: new Uint8Array(4),
				stash: {
					release: stashRelease,
					stashedBlocks: () => undefined,
				} as any,
			});
			const poison = sinon
				.stub(sharedLog, "throwIfNativeDurableCommitFailed")
				.callsFake(() => {
					order.push("poison");
				});
			try {
				// A context without `from` fails before any arm runs: the envelope
				// still releases the stash exactly once, then rechecks the poison
				// AFTER the release.
				await target.log.onMessage(message, {} as any);
				expect(order).to.deep.equal(["poison", "stash", "poison"]);
				// The release is one-shot at the message level too.
				expect(message.release()).to.be.false;
				expect(stashRelease.calledOnce).to.be.true;
			} finally {
				poison.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("keeps handled and rejected raw normalization inside the shared stash envelope", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const profileEvents: any[] = [];
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store.clone(), {
				args: {
					replicate: false,
					setup,
					sync: { profile: (event: any) => profileEvents.push(event) },
				},
			});
			const sharedLog = target.log as any;
			const createStashMessage = (
				release: () => boolean,
				hashes: string[] = [],
			) =>
				new StashBackedRawExchangeHeadsMessage({
					messageId: new Uint8Array(32),
					hashes,
					gidRefrences: hashes.map(() => []),
					byteLengths: Uint32Array.from(hashes, () => 1),
					reserved: new Uint8Array(4),
					stash: {
						release,
						stashedBlocks: () => undefined,
					} as any,
				});

			const handledRelease = sinon.stub().returns(true);
			const materialize = sinon.spy(sharedLog, "materializeRawReceiveMessage");
			try {
				const { entry: knownEntry } = await target.add("known", {
					meta: { next: [] },
				});
				// All-known raw receives are handled by normalization. Its early
				// return must still cross the outer wire-stash release boundary once.
				await target.log.onMessage(
					createStashMessage(handledRelease, [knownEntry.hash]),
					{
						from: target.node.identity.publicKey,
					} as any,
				);
				expect(materialize.calledOnce).to.be.true;
				expect(handledRelease.calledOnce).to.be.true;
			} finally {
				materialize.restore();
			}

			const order: string[] = [];
			const rejectedRelease = sinon.stub().callsFake(() => {
				order.push("stash");
				return true;
			});
			const rejectedMaterialize = sinon
				.stub(sharedLog, "materializeRawReceiveMessage")
				.callsFake(async () => {
					order.push("materialize");
					throw new AccessError("pinned raw normalization error");
				});
			try {
				// Helper errors retain the existing onMessage classification, and stash
				// cleanup happens after the failed helper exactly once.
				await target.log.onMessage(createStashMessage(rejectedRelease), {
					from: target.node.identity.publicKey,
				} as any);
				expect(rejectedMaterialize.calledOnce).to.be.true;
				expect(rejectedRelease.calledOnce).to.be.true;
				expect(order).to.deep.equal(["materialize", "stash"]);
			} finally {
				rejectedMaterialize.restore();
			}

			const releaseEvents = profileEvents.filter(
				(event) => event.name === "sharedLog.rawReceive.wireStashRelease",
			);
			expect(releaseEvents).to.have.length(2);
			expect(releaseEvents.map((event) => event.entries)).to.deep.equal([1, 0]);
			expect(
				releaseEvents.every((event) => event.details.bytesMaterialized === 0),
			).to.be.true;
		} finally {
			await session.stop();
		}
	});
});
