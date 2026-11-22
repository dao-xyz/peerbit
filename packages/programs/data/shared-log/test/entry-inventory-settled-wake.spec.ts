import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
} from "../src/exchange-heads.js";
import { ReplicationIntent } from "../src/ranges.js";
import { FullReplicationInfoV2Message } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("entry inventory settled-session wake", () => {
	let session: TestSession;
	let release: (() => void) | undefined;

	afterEach(async () => {
		release?.();
		release = undefined;
		sinon.restore();
		await session?.stop();
	});

	const openDisconnectedLog = async (peers = 2) => {
		session = await TestSession.disconnected(peers);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		return { db, log: db.log as any };
	};

	const makeReplicationRange = (
		log: any,
		properties: {
			id: Uint8Array;
			ownerHash: string;
			offset: number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		},
	) =>
		new log.indexableDomain.constructorRange({
			id: properties.id,
			offset: log.indexableDomain.numbers.denormalize(properties.offset),
			width: log.indexableDomain.numbers.denormalize(0.2),
			publicKeyHash: properties.ownerHash,
			mode: properties.mode ?? ReplicationIntent.NonStrict,
			timestamp: properties.timestamp ?? 1n,
		});

	const remoteSenderCapabilities =
		SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
		SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND;

	// An authenticated V2 receive stream from `remote` under a newly rotated,
	// open topic session, so a Full can commit through the onMessage path.
	const openV2ReceiveLane = async (
		log: any,
		remote: PublicSignKey,
		senderTransportSession = 4_242n,
	) => {
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);
		const receiveEpoch = log._peerSessions.receiveEpoch(remoteHash);
		log._peerSyncCapabilities.set(remoteHash, remoteSenderCapabilities);
		log._peerSyncCapabilitySessions.set(remoteHash, senderTransportSession);
		log._peerSyncCapabilityTimestamps.set(remoteHash, 1n);
		expect(
			log._v2Receive.markLocalCapabilityReady({
				peerHash: remoteHash,
				peerSession,
				receiveEpoch,
				receiverTransportSession: BigInt(log.node.services.pubsub.session),
				requestNotBeforeMs: 0,
			}),
		).to.be.true;
		expect(
			log._v2Receive.observeCapability({
				peerHash: remoteHash,
				target: remote,
				peerSession,
				receiveEpoch,
				capabilities: remoteSenderCapabilities,
				senderTransportSession,
				capabilityTimestamp: 1n,
			}),
		).to.be.true;
		const state = log._v2Receive._receiveStates.get(remoteHash)!;
		clearTimeout(state.requestTimer);
		state.requestTimer = undefined;
		return {
			peerSession,
			state,
			context: (timestamp: bigint) =>
				({
					from: remote,
					message: { header: { session: senderTransportSession, timestamp } },
				}) as any,
		};
	};

	const full = (
		lane: Awaited<ReturnType<typeof openV2ReceiveLane>>,
		segments: any[],
	) =>
		new FullReplicationInfoV2Message({
			receiverChallenge: lane.state.receiverBinding!.slice(),
			senderEpoch: randomBytes(32),
			sequence: 1n,
			segments,
		});

	// The scheduler calls `create` synchronously from `wake`, so `created` is
	// exact as soon as an edge returns. The held page keeps the job active
	// without running a real page, timer or retry.
	const instrument = (log: any, remoteKey: PublicSignKey) => {
		const pubsub = log.node.services.pubsub;
		const edge = () =>
			pubsub.dispatchEvent(
				new CustomEvent("peer:stream-ready", { detail: remoteKey }),
			);
		const wake = sinon.spy(log._entryInventoryRecovery, "wake");
		const created: Array<[string, object]> = [];
		const held = pDefer<void>();
		release = () => held.resolve();
		sinon
			.stub(log, "recoverPeerEntryInventory")
			.callsFake((peer: any, session: any, signal: any) => {
				created.push([peer, session]);
				return {
					next: () =>
						new Promise<"complete">((resolve, reject) => {
							signal.addEventListener("abort", () => reject(signal.reason), {
								once: true,
							});
							void held.promise.then(() => resolve("complete"));
						}),
					close: () => {},
				};
			});
		const sendHint = sinon.spy(log._v2Send, "resumeAfterTransportRecovery");
		const receiveHint = sinon.spy(
			log._v2Receive,
			"resumeAfterTransportRecovery",
		);
		return { edge, wake, created, sendHint, receiveHint };
	};

	// A first session whose Full committed, then a renewed session for the same
	// peer whose Full has not committed yet.
	const openRenewedSession = async () => {
		const { db, log } = await openDisconnectedLog(2);
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const range = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: remoteHash,
			offset: 0.1,
			timestamp: 1n,
		});
		await log.addReplicationRange([range], remoteKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const first = await openV2ReceiveLane(log, remoteKey, 4_242n);
		// The first Full repeats rows already in the index, as after a local
		// reopen, so it runs the unchanged-Full repair too.
		stubSuccessorRepair(log, remoteHash);
		await db.log.onMessage(
			full(first, [range.toReplicationRange()]),
			first.context(1n),
		);
		expect(first.peerSession.hasPredecessor).to.equal(false);
		expect(log._v2Receive._cutoverPeerSessions.has(first.peerSession)).to.equal(
			true,
		);
		expect(log._successorFullRepairSessions.has(first.peerSession)).to.equal(
			true,
		);
		expect(log.scheduleJoinAuthoritativeRepair.calledOnce).to.equal(true);
		log._v2Receive.clearPeer(remoteHash);
		log._v2Send.clearPeer(remoteHash);
		expect(log._v2Receive._cutoverPeerSessions.has(first.peerSession)).to.equal(
			false,
		);
		const successor = await openV2ReceiveLane(log, remoteKey, 4_243n);
		expect(successor.peerSession.hasPredecessor).to.equal(true);
		expect(successor.peerSession.phase).to.equal("open");
		expect(
			log._v2Receive._cutoverPeerSessions.has(successor.peerSession),
		).to.equal(false);
		return { db, log, remoteKey, remoteHash, range, first, successor };
	};

	// Keep the unchanged-Full repair that a session's first Full schedules from
	// dispatching real repair work or arming its delayed sweeps.
	const stubSuccessorRepair = (log: any, remoteHash: string) => {
		sinon
			.stub(log.syncronizer, "onMaybeMissingEntries")
			.callsFake(async (...args: unknown[]) => {
				const [{ entries, targets }] = args as [
					{
						entries: ReadonlyMap<string, unknown>;
						targets: string[];
					},
				];
				if (targets.includes(remoteHash))
					log.clearRepairFrontierHashes(remoteHash, [...entries.keys()]);
			});
		sinon.stub(log, "scheduleJoinAuthoritativeRepair");
		sinon
			.stub(log, "findEntryReplicatedLeaderBatch")
			.callsFake(async (...args: unknown[]) =>
				(args[0] as unknown[]).map(
					() => new Map([[remoteHash, { intersecting: true }]]),
				),
			);
	};

	it("edges while departing or opening wake nothing and run no hints", async () => {
		const { db, log } = await openDisconnectedLog(2);
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const range = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: remoteHash,
			offset: 0.1,
			timestamp: 1n,
		});
		await log.addReplicationRange([range], remoteKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const lane = await openV2ReceiveLane(log, remoteKey);
		stubSuccessorRepair(log, remoteHash);
		await db.log.onMessage(
			full(lane, [range.toReplicationRange()]),
			lane.context(1n),
		);
		expect(log._v2Receive._cutoverPeerSessions.has(lane.peerSession)).to.equal(
			true,
		);
		const { edge, wake, created, sendHint, receiveHint } = instrument(
			log,
			remoteKey,
		);
		const assertNothingRan = (phase: string) => {
			expect(wake.notCalled, phase + ": wake").to.equal(true);
			expect(created, phase + ": passes").to.have.length(0);
			expect(sendHint.notCalled, phase + ": send hint").to.equal(true);
			expect(receiveHint.notCalled, phase + ": receive hint").to.equal(true);
		};

		const departing = log._peerSessions.rotate(remoteHash, "departing");
		expect(departing.phase).to.equal("departing");
		edge();
		assertNothingRan("departing");

		const opening = log._peerSessions.rotate(remoteHash, "opening");
		expect(opening.phase).to.equal("opening");
		edge();
		assertNothingRan("opening");
	});

	it("an open first session whose Full has not committed is not woken, and both hints still run", async () => {
		const { log } = await openDisconnectedLog(2);
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const lane = await openV2ReceiveLane(log, remoteKey);
		expect(lane.peerSession.hasPredecessor).to.equal(false);
		expect(lane.peerSession.phase).to.equal("open");
		expect(lane.peerSession.isActive()).to.equal(true);
		expect(log._v2Receive._cutoverPeerSessions.has(lane.peerSession)).to.equal(
			false,
		);
		const { edge, wake, created, sendHint, receiveHint } = instrument(
			log,
			remoteKey,
		);

		edge();
		expect(wake.callCount, "wake at an unsettled first session").to.equal(0);
		expect(created).to.have.length(0);
		expect(log._entryInventoryRecovery.active.size).to.equal(0);
		expect(log._entryInventoryRecovery.pending.size).to.equal(0);
		expect(
			sendHint.calledOnceWithExactly(remoteHash, lane.peerSession),
		).to.equal(true);
		expect(
			receiveHint.calledOnceWithExactly({
				peerHash: remoteHash,
				peerSession: lane.peerSession,
				receiveEpoch: log._peerSessions.receiveEpoch(remoteHash),
			}),
		).to.equal(true);
	});

	it("a renewed session whose Full has not committed is not woken, even though its predecessor was settled", async () => {
		const { log, remoteKey, remoteHash, successor } =
			await openRenewedSession();
		const { edge, wake, created, sendHint, receiveHint } = instrument(
			log,
			remoteKey,
		);

		edge();
		expect(wake.callCount, "wake at an unsettled renewed session").to.equal(0);
		expect(created).to.have.length(0);
		expect(
			sendHint.calledOnceWithExactly(remoteHash, successor.peerSession),
		).to.equal(true);
		expect(
			receiveHint.calledOnceWithExactly({
				peerHash: remoteHash,
				peerSession: successor.peerSession,
				receiveEpoch: log._peerSessions.receiveEpoch(remoteHash),
			}),
		).to.equal(true);
	});

	it("a session is woken once after its Full commits, never by the commit, and a repeat edge is absorbed", async () => {
		const { db, log, remoteKey, remoteHash, range, successor } =
			await openRenewedSession();
		const { edge, wake, created } = instrument(log, remoteKey);

		await db.log.onMessage(
			full(successor, [range.toReplicationRange()]),
			successor.context(2n),
		);
		expect(wake.notCalled, "the commit itself wakes nothing").to.equal(true);
		expect(created).to.have.length(0);
		expect(log._v2Receive.hasCommittedFull(successor.peerSession)).to.equal(
			true,
		);
		expect(
			log._v2Receive._cutoverPeerSessions.has(successor.peerSession),
		).to.equal(true);
		expect(
			log._successorFullRepairSessions.has(successor.peerSession),
		).to.equal(true);

		edge();
		expect(
			wake.calledOnceWithExactly(remoteHash, successor.peerSession),
		).to.equal(true);
		expect(created).to.deep.equal([[remoteHash, successor.peerSession]]);
		const job = log._entryInventoryRecovery.active.get(remoteHash);
		expect(
			job?.session === successor.peerSession,
			"the active job serves the settled session",
		).to.equal(true);

		edge();
		expect(wake.callCount).to.equal(2);
		expect(created).to.have.length(1);
		expect(
			log._entryInventoryRecovery.active.get(remoteHash) === job,
			"the repeat edge keeps the running job",
		).to.equal(true);
		expect(job.controller.signal.aborted).to.equal(false);
	});

	it("an empty successor Full settles the session", async () => {
		const { db, log, remoteKey, remoteHash, successor } =
			await openRenewedSession();
		const { edge, wake, created } = instrument(log, remoteKey);

		await db.log.onMessage(full(successor, []), successor.context(2n));
		expect(
			log._successorFullRepairSessions.has(successor.peerSession),
		).to.equal(false);
		expect(log._v2Receive.hasCommittedFull(successor.peerSession)).to.equal(
			true,
		);
		expect(wake.notCalled).to.equal(true);

		edge();
		expect(
			wake.calledOnceWithExactly(remoteHash, successor.peerSession),
		).to.equal(true);
		expect(created).to.have.length(1);
	});

	it("a real settled session is woken once and runs both hints", async () => {
		session = await TestSession.connected(2);
		const [local, remote] = session.peers;
		const args = { replicate: 1, timeUntilRoleMaturity: 0 };
		const store = await local.open(new EventStore(), { args });
		await remote.open(store.clone(), { args });
		const log = store.log as any;
		const remoteKey = remote.identity.publicKey;
		const peerHash = remoteKey.hashcode();
		await waitForResolved(
			() => {
				expect(log._peerSessions.current(peerHash)?.phase).to.equal("open");
				expect(log._v2Receive._receiveStates.get(peerHash)?.phase).to.equal(
					"active",
				);
				expect(log._v2Send._sendStates.get(peerHash)?.established).to.equal(
					true,
				);
			},
			{ timeout: 5_000 },
		);
		await log._v2Send.drain();
		await waitForResolved(
			() => {
				expect(log._entryInventoryRecovery.active.size).to.equal(0);
				expect(log._entryInventoryRecovery.pending.size).to.equal(0);
			},
			{ timeout: 10_000 },
		);
		const current = log._peerSessions.current(peerHash);
		expect(log._v2Receive._cutoverPeerSessions.has(current)).to.equal(true);
		const { edge, wake, created, sendHint, receiveHint } = instrument(
			log,
			remoteKey,
		);

		edge();
		expect(wake.calledOnceWithExactly(peerHash, current)).to.equal(true);
		expect(created).to.deep.equal([[peerHash, current]]);
		expect(sendHint.calledOnceWithExactly(peerHash, current)).to.equal(true);
		expect(
			receiveHint.calledOnceWithExactly({
				peerHash,
				peerSession: current,
				receiveEpoch: log._peerSessions.receiveEpoch(peerHash),
			}),
		).to.equal(true);
	});
});
