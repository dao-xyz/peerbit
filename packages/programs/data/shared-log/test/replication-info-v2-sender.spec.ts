import { Ed25519PublicKey } from "@peerbit/crypto";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import {
	ReplicationInfoV2SendCoordinator,
	deriveReplicationInfoV2ReceiverBinding,
	type ReplicationInfoV2SendState,
} from "../src/replication-info-v2-send.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	FullReplicationInfoV2Message,
	RequestReplicationInfoV2Message,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

const key = (value: number) =>
	new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(value) });

const challenge = (value: number) => new Uint8Array(32).fill(value);

describe("receive admission replication-info V2 sender streams", () => {
	const self = key(1);
	const peerA = key(2);
	const peerB = key(3);
	const senderTransportSession = 0x20000000000001n;
	let closed: boolean;
	let openSessions: Set<object>;
	let rpcSend: sinon.SinonStub;
	let getSegments: sinon.SinonStub;
	let ownershipController: AbortController;
	let coordinator: ReplicationInfoV2SendCoordinator<"u32">;

	const makeRequest = (
		receiverChallenge: Uint8Array,
		intendedSender = self,
		senderSession = senderTransportSession,
	) =>
		new RequestReplicationInfoV2Message({
			receiverChallenge,
			intendedSender,
			senderSession,
		});

	const accept = (
		from: Ed25519PublicKey,
		peerSession: object,
		receiverChallenge: Uint8Array,
		requestTimestamp = 1n,
		request = makeRequest(receiverChallenge),
	) =>
		coordinator.acceptRequest(request, {
			from,
			peerSession,
			receiverTransportSession: BigInt(from.publicKey[0]),
			requestTimestamp,
		});

	beforeEach(() => {
		closed = false;
		openSessions = new Set();
		rpcSend = sinon.stub().resolves([]);
		getSegments = sinon.stub().resolves([]);
		ownershipController = new AbortController();
		coordinator = new ReplicationInfoV2SendCoordinator<"u32">({
			getRpc: () => ({ send: rpcSend }) as any,
			getSelfKey: () => self,
			getSenderTransportSession: () => senderTransportSession,
			getMyReplicationSegments: getSegments,
			validatePersistedReplicationRangeSnapshot: () => {},
			isClosed: () => closed,
			isPeerSessionOpen: (_peerHash, peerSession) =>
				openSessions.has(peerSession),
			captureReplicationOwnershipLifecycle: () => ownershipController,
			isReplicationOwnershipLifecycleActive: (controller) =>
				controller === ownershipController && !controller.signal.aborted,
		});
	});

	afterEach(() => {
		coordinator.clearForClose();
		sinon.restore();
	});

	it("rejects self, retargeted, wrong-session and non-open requests", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(self, peerSession, challenge(1))).to.be.false;
		expect(
			accept(
				peerA,
				peerSession,
				challenge(2),
				1n,
				makeRequest(challenge(2), peerB),
			),
		).to.be.false;
		expect(
			accept(
				peerA,
				peerSession,
				challenge(3),
				1n,
				makeRequest(challenge(3), self, senderTransportSession + 1n),
			),
		).to.be.false;
		openSessions.delete(peerSession);
		expect(accept(peerA, peerSession, challenge(4))).to.be.false;
		await coordinator.drain();
		expect(rpcSend.notCalled).to.be.true;
		expect(coordinator._sendStates.size).to.equal(0);
	});

	it("starts at sequence one with a receiver-bound authoritative snapshot", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const receiverChallenge = challenge(5);
		expect(accept(peerA, peerSession, receiverChallenge)).to.be.true;
		await coordinator.drain();

		expect(rpcSend.calledOnce).to.be.true;
		const [message, options] = rpcSend.firstCall.args;
		expect(message).to.be.instanceOf(FullReplicationInfoV2Message);
		expect(message.sequence).to.equal(1n);
		expect([...message.receiverChallenge]).to.deep.equal([
			...deriveReplicationInfoV2ReceiverBinding({
				receiverChallenge,
				receiver: peerA,
				receiverTransportSession: 2n,
				sender: self,
				senderTransportSession,
			}),
		]);
		expect([...message.receiverChallenge]).to.not.deep.equal([
			...receiverChallenge,
		]);
		expect(Buffer.from(message.receiverChallenge).toString("hex")).to.equal(
			"e5947998ab731e87a670da4b2d1a8e7b46a59df06e6f9c77f1910a9d63d49746",
		);
		expect(options.mode).to.be.instanceOf(AcknowledgeDelivery);
		expect(options.mode.to).to.have.length(1);
		expect(options.mode.to[0]).to.equal(peerA.hashcode());
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(state.established).to.be.true;
		expect(state.nextSequence).to.equal(2n);
	});

	it("retries an exact newer request without resetting its stream", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const receiverChallenge = challenge(6);
		expect(accept(peerA, peerSession, receiverChallenge, 10n)).to.be.true;
		await coordinator.drain();
		const epoch = (
			rpcSend.firstCall.args[0] as FullReplicationInfoV2Message
		).senderEpoch.slice();

		expect(accept(peerA, peerSession, receiverChallenge, 10n)).to.be.false;
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;

		expect(accept(peerA, peerSession, receiverChallenge, 11n)).to.be.true;
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		const retried = rpcSend.secondCall.args[0] as FullReplicationInfoV2Message;
		expect(retried.sequence).to.equal(2n);
		expect([...retried.senderEpoch]).to.deep.equal([...epoch]);
	});

	it("pins the first challenge and bounds a different-challenge flood", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const snapshot = pDefer<never[]>();
		getSegments.returns(snapshot.promise);
		expect(accept(peerA, peerSession, challenge(7), 20n)).to.be.true;
		await waitForResolved(() => expect(getSegments.calledOnce).to.be.true);
		const firstState = coordinator._sendStates.get(peerA.hashcode())!;

		for (let index = 0; index < 10_000; index++) {
			expect(
				accept(peerA, peerSession, challenge(8), 21n + BigInt(index)),
			).to.be.false;
		}
		expect(getSegments.calledOnce).to.be.true;
		expect(rpcSend.notCalled).to.be.true;
		expect(coordinator._sendStates.get(peerA.hashcode())).to.equal(firstState);
		expect(firstState.controller.signal.aborted).to.be.false;

		snapshot.resolve([]);
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
	});

	it("serializes capability rebinds behind a retiring snapshot read", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const firstSnapshot = pDefer<never[]>();
		getSegments.onFirstCall().returns(firstSnapshot.promise);
		expect(accept(peerA, peerSession, challenge(18), 30n)).to.be.true;
		await waitForResolved(() => expect(getSegments.calledOnce).to.be.true);

		coordinator.advancePeerCapability(peerA.hashcode());
		expect(coordinator._sendStates.size).to.equal(0);
		expect(coordinator._retiringWorkersByPeer.size).to.equal(1);
		let drainSettled = false;
		const drain = coordinator.drain().then(() => {
			drainSettled = true;
		});
		await Promise.resolve();
		expect(drainSettled).to.be.false;
		const abort = new AbortController();
		const abortableDrain = coordinator.drain(abort.signal);
		abort.abort();
		await abortableDrain;
		expect(drainSettled).to.be.false;
		for (let index = 0; index < 10_000; index++) {
			coordinator.advancePeerCapability(peerA.hashcode());
			expect(
				accept(peerA, peerSession, challenge(19), 31n + BigInt(index)),
			).to.be.false;
		}
		expect(getSegments.calledOnce).to.be.true;
		expect(rpcSend.notCalled).to.be.true;

		firstSnapshot.resolve([]);
		await drain;
		expect(drainSettled).to.be.true;
		expect(coordinator._retiringWorkersByPeer.size).to.equal(0);
		expect(coordinator._spentPeerSessions.has(peerSession)).to.be.false;
		expect(accept(peerA, peerSession, challenge(19), 10_031n)).to.be.true;
		await coordinator.drain();
		expect(getSegments.callCount).to.equal(2);
		expect(rpcSend.calledOnce).to.be.true;
	});

	it("serializes a reconnect behind the previous session's snapshot read", async () => {
		const oldPeerSession = {};
		const newPeerSession = {};
		openSessions.add(oldPeerSession);
		const firstSnapshot = pDefer<never[]>();
		getSegments.onFirstCall().returns(firstSnapshot.promise);
		expect(accept(peerA, oldPeerSession, challenge(22), 40n)).to.be.true;
		await waitForResolved(() => expect(getSegments.calledOnce).to.be.true);

		openSessions.delete(oldPeerSession);
		openSessions.add(newPeerSession);
		expect(accept(peerA, newPeerSession, challenge(23), 41n)).to.be.false;
		expect(coordinator._sendStates.size).to.equal(0);
		expect(coordinator._retiringWorkersByPeer.size).to.equal(1);
		expect(getSegments.calledOnce).to.be.true;

		firstSnapshot.resolve([]);
		await coordinator.drain();
		expect(coordinator._retiringWorkersByPeer.size).to.equal(0);
		expect(accept(peerA, newPeerSession, challenge(23), 42n)).to.be.true;
		await coordinator.drain();
		expect(getSegments.callCount).to.equal(2);
		expect(rpcSend.calledOnce).to.be.true;
	});

	it("keeps independent sequences and challenges for two destinations", async () => {
		const sessionA = {};
		const sessionB = {};
		openSessions.add(sessionA);
		openSessions.add(sessionB);
		expect(accept(peerA, sessionA, challenge(9))).to.be.true;
		expect(accept(peerB, sessionB, challenge(9))).to.be.true;
		await coordinator.drain();

		const messages = rpcSend.args.map(
			(args) => args[0] as FullReplicationInfoV2Message,
		);
		expect(messages).to.have.length(2);
		expect(messages.every((message) => message.sequence === 1n)).to.be.true;
		expect(messages[0]).to.not.equal(messages[1]);
		expect(
			new Set(
				messages.map((message) =>
					Buffer.from(message.receiverChallenge).toString("hex"),
				),
			).size,
		).to.equal(2);
	});

	it("bounds a blocked destination to one coalesced pending snapshot", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const firstSend = pDefer<void>();
		rpcSend.onFirstCall().returns(firstSend.promise);
		expect(accept(peerA, peerSession, challenge(11))).to.be.true;
		await waitForResolved(() => expect(rpcSend.calledOnce).to.be.true);

		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		for (let index = 0; index < 10_000; index++) {
			coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		}
		const state = coordinator._sendStates.get(
			peerA.hashcode(),
		) as ReplicationInfoV2SendState;
		expect(state.pending).to.deep.equal({ kind: "snapshot" });

		firstSend.resolve();
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.secondCall.args[0]).to.be.instanceOf(
			FullReplicationInfoV2Message,
		);
		expect(rpcSend.secondCall.args[0].sequence).to.equal(2n);
	});

	it("suspends only a failing destination and resumes with a safe Full", async () => {
		const sessionA = {};
		const sessionB = {};
		openSessions.add(sessionA);
		openSessions.add(sessionB);
		let failA = true;
		rpcSend.callsFake(async (_message, options) => {
			if (failA && options.mode.to[0] === peerA.hashcode()) {
				throw new Error("peer A delivery failed");
			}
			return [];
		});

		expect(accept(peerA, sessionA, challenge(12))).to.be.true;
		expect(accept(peerB, sessionB, challenge(13))).to.be.true;
		await coordinator.drain();
		const failedState = coordinator._sendStates.get(peerA.hashcode());
		expect(failedState?.suspended).to.be.true;
		expect(failedState?.nextSequence).to.equal(2n);
		expect(coordinator._sendStates.get(peerB.hashcode())?.established).to.be
			.true;
		coordinator.clearPeer(peerB.hashcode());
		rpcSend.resetHistory();
		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		expect(accept(peerA, sessionA, challenge(12), 1n)).to.be.false;
		await coordinator.drain();
		expect(rpcSend.notCalled).to.be.true;

		failA = false;
		expect(accept(peerA, sessionA, challenge(12), 2n)).to.be.true;
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
		const resumed = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		expect(resumed.sequence).to.equal(2n);
		expect(failedState?.suspended).to.be.false;
		expect(failedState?.established).to.be.true;
	});

	it("never wraps or recreates a stream after u64 exhaustion", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const receiverChallenge = challenge(17);
		expect(accept(peerA, peerSession, receiverChallenge)).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		state.nextSequence = (1n << 64n) - 1n;
		rpcSend.resetHistory();

		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
		expect(rpcSend.firstCall.args[0].sequence).to.equal((1n << 64n) - 1n);
		expect(coordinator._sendStates.has(peerA.hashcode())).to.be.false;

		rpcSend.resetHistory();
		expect(accept(peerA, peerSession, receiverChallenge, 2n)).to.be.false;
		await coordinator.drain();
		expect(rpcSend.notCalled).to.be.true;
	});

	it("fences normal sends on ownership abort and retains a terminal reset", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		let first = true;
		rpcSend.callsFake(async (_message, options) => {
			if (!first) {
				return [];
			}
			first = false;
			await new Promise<void>((_resolve, reject) => {
				options.signal.addEventListener(
					"abort",
					() => reject(new Error("ownership aborted")),
					{ once: true },
				);
			});
			return [];
		});

		expect(accept(peerA, peerSession, challenge(16))).to.be.true;
		await waitForResolved(() => expect(rpcSend.calledOnce).to.be.true);
		ownershipController.abort();
		await coordinator.drain();
		const retained = coordinator._sendStates.get(peerA.hashcode());
		expect(retained).to.exist;
		expect(retained!.established).to.be.false;

		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.callCount).to.equal(2);
		const terminal = rpcSend.secondCall.args[0] as FullReplicationInfoV2Message;
		expect(terminal.sequence).to.equal(2n);
		expect(terminal.segments).to.deep.equal([]);
	});

	it("uses the last safe u64 sequence for an idle terminal reset", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(24))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		state.nextSequence = (1n << 64n) - 1n;
		rpcSend.resetHistory();
		ownershipController.abort();

		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.calledOnce).to.be.true;
		const terminal = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		expect(terminal.sequence).to.equal((1n << 64n) - 1n);
		expect(terminal.segments).to.deep.equal([]);
	});

	it("uses the last safe u64 sequence after an ambiguous predecessor", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(25))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		state.nextSequence = (1n << 64n) - 2n;
		rpcSend.rejects(new Error("ambiguous predecessor"));
		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await coordinator.drain();
		expect(state.suspended).to.be.true;
		expect(state.nextSequence).to.equal((1n << 64n) - 1n);
		rpcSend.resetHistory();
		rpcSend.resolves([]);
		ownershipController.abort();

		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.calledOnce).to.be.true;
		const terminal = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		expect(terminal.sequence).to.equal((1n << 64n) - 1n);
		expect(terminal.segments).to.deep.equal([]);
	});

	it("rotates the sender epoch and aborts old queues across reopen", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(14))).to.be.true;
		await coordinator.drain();
		const oldState = coordinator._sendStates.get(peerA.hashcode())!;
		const oldEpoch = oldState.senderEpoch.slice();

		coordinator.resetForOpen();
		expect(oldState.controller.signal.aborted).to.be.true;
		expect(coordinator._sendStates.size).to.equal(0);
		expect([...coordinator._senderEpoch]).to.not.deep.equal([...oldEpoch]);
	});

	it("serializes incremental messages after the initial full", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(15))).to.be.true;
		await coordinator.drain();
		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await coordinator.drain();

		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.secondCall.args[0]).to.be.instanceOf(
			AddedReplicationInfoV2Message,
		);
		expect(rpcSend.secondCall.args[0].sequence).to.equal(2n);
	});
});

describe("receive admission replication-info V2 sender integration", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	it("requires current signed capabilities before honoring a request", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);
		const receiverTransportSession = 77n;
		const localTransportSession = BigInt(log.node.services.pubsub.session);
		const request = new RequestReplicationInfoV2Message({
			receiverChallenge: challenge(20),
			intendedSender: log.node.identity.publicKey,
			senderSession: localTransportSession,
		});
		const send = sinon.stub(log.rpc, "send").resolves([] as any);
		const activity = sinon.spy(log._liveness, "markReplicatorActivity");
		const context = (transportSession: bigint, timestamp: bigint) =>
			({
				from: remote,
				message: { header: { session: transportSession, timestamp } },
			}) as any;

		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
		);
		log._peerSyncCapabilitySessions.set(remoteHash, receiverTransportSession);
		log._peerSyncCapabilityTimestamps.set(remoteHash, 0n);
		await log.onMessage(request, context(receiverTransportSession, 1n));
		expect(send.notCalled).to.be.true;
		expect(activity.notCalled).to.be.true;
		expect(log._v2Send._sendStates.size).to.equal(0);

		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		);
		await log.onMessage(request, context(receiverTransportSession + 1n, 2n));
		expect(send.notCalled).to.be.true;
		expect(activity.notCalled).to.be.true;

		await log.onMessage(request, context(receiverTransportSession, 3n));
		await waitForResolved(() =>
			expect(
				send.calledWith(sinon.match.instanceOf(FullReplicationInfoV2Message)),
			).to.be.true,
		);
		expect(activity.calledOnceWith(remoteHash)).to.be.true;
		const state = log._v2Send._sendStates.get(remoteHash);
		expect(state.peerSession).to.equal(peerSession);
		expect(state.receiverTransportSession).to.equal(receiverTransportSession);

		await log._v2Send.drain();
		send.resetHistory();
		activity.resetHistory();
		await log.onMessage(request, context(receiverTransportSession, 3n));
		expect(send.notCalled).to.be.true;
		expect(activity.notCalled).to.be.true;
	});

	it("adds a directed V2 sidecar without changing the legacy primary send", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);
		const receiverTransportSession = 88n;
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		);
		log._peerSyncCapabilitySessions.set(remoteHash, receiverTransportSession);
		log._peerSyncCapabilityTimestamps.set(remoteHash, 0n);
		const send = sinon.stub(log.rpc, "send").resolves([] as any);
		const request = new RequestReplicationInfoV2Message({
			receiverChallenge: challenge(21),
			intendedSender: log.node.identity.publicKey,
			senderSession: BigInt(log.node.services.pubsub.session),
		});
		await log.onMessage(request, {
			from: remote,
			message: {
				header: { session: receiverTransportSession, timestamp: 1n },
			},
		} as any);
		await log._v2Send.drain();
		send.resetHistory();

		const legacy = new AddedReplicationSegmentMessage({ segments: [] });
		await log._announcements.sendReplicationAnnouncement(legacy);
		await log._v2Send.drain();
		expect(send.calledWith(legacy)).to.be.true;
		const v2 = send.args
			.map((args: any[]) => args[0])
			.find(
				(message: unknown) => message instanceof AddedReplicationInfoV2Message,
			) as AddedReplicationInfoV2Message | undefined;
		expect(v2).to.exist;
		expect(v2!.sequence).to.equal(2n);
	});

	it("keeps capabilities monotonic and fences the previous grant generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);
		const receiverTransportSession = 99n;
		const context = (timestamp: bigint) =>
			({
				from: remote,
				message: {
					header: { session: receiverTransportSession, timestamp },
				},
			}) as any;

		await log.onMessage(
			new SyncCapabilitiesMessage({
				capabilities:
					SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
			}),
			context(10n),
		);
		const request = (value: number) =>
			new RequestReplicationInfoV2Message({
				receiverChallenge: challenge(value),
				intendedSender: log.node.identity.publicKey,
				senderSession: BigInt(log.node.services.pubsub.session),
			});
		let blockFirst = true;
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (_message, options: any) => {
				if (!blockFirst) {
					return [] as any;
				}
				blockFirst = false;
				await new Promise<void>((_resolve, reject) => {
					options.signal.addEventListener(
						"abort",
						() => reject(new Error("capability generation advanced")),
						{ once: true },
					);
				});
				return [] as any;
			});

		await log.onMessage(request(30), context(11n));
		await waitForResolved(() => expect(send.calledOnce).to.be.true);
		await log.onMessage(
			new SyncCapabilitiesMessage({ capabilities: 0 }),
			context(12n),
		);
		expect(log._v2Send._sendStates.size).to.equal(0);
		expect(
			log._peerSyncCapabilities.get(remoteHash) &
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY);
		expect(log._peerSyncCapabilityTimestamps.get(remoteHash)).to.equal(12n);
		await waitForResolved(() =>
			expect(log._v2Send._retiringWorkersByPeer.size).to.equal(0),
		);

		await log.onMessage(request(30), context(11n));
		expect(send.calledOnce).to.be.true;
		await log.onMessage(request(31), context(12n));
		expect(send.calledOnce).to.be.true;
		await log.onMessage(request(31), context(13n));
		await waitForResolved(() => expect(send.callCount).to.equal(2));
		await log._v2Send.drain();
		expect(log._v2Send._sendStates.get(remoteHash)?.established).to.be.true;
	});

	it("keeps opening-barrier capabilities monotonic within a signed session", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		const receiverTransportSession = 101n;
		const context = (timestamp: bigint) =>
			({
				from: remote,
				message: {
					header: { session: receiverTransportSession, timestamp },
				},
			}) as any;
		peerSession.beginOpeningBarrier();
		log._peerSessions.blockReplicationInfo(remoteHash);

		try {
			await log.onMessage(
				new SyncCapabilitiesMessage({
					capabilities:
						SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
				}),
				context(10n),
			);
			await log.onMessage(
				new SyncCapabilitiesMessage({
					capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
				}),
				context(9n),
			);
			let staged = log._openingSyncCapabilitiesByPeer.get(remoteHash);
			expect(staged.capabilities).to.equal(
				SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
			);
			expect(staged.timestamp).to.equal(10n);

			await log.onMessage(
				new SyncCapabilitiesMessage({
					capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
				}),
				context(11n),
			);
			staged = log._openingSyncCapabilitiesByPeer.get(remoteHash);
			expect(staged.capabilities).to.equal(
				SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
			);
			expect(staged.transportSession).to.equal(receiverTransportSession);
			expect(staged.timestamp).to.equal(11n);
			expect(log._peerSyncCapabilities.has(remoteHash)).to.be.false;
		} finally {
			log._openingSyncCapabilitiesByPeer.delete(remoteHash);
			log._peerSessions.unblockReplicationInfo(remoteHash);
			peerSession.finishOpeningBarrier();
		}
	});
});
