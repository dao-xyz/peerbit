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
	type ReplicationInfoV2SendState,
	deriveReplicationInfoV2ReceiverBinding,
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
	const initialSenderTransportSession = 0x20000000000001n;
	let senderTransportSession: bigint;
	let closed: boolean;
	let openSessions: Set<object>;
	let closedReadinessGates: Set<object>;
	let rpcSend: sinon.SinonStub;
	let getSegments: sinon.SinonStub;
	let ownershipController: AbortController;
	let coordinator: ReplicationInfoV2SendCoordinator<"u32">;

	const createCoordinator = (options?: {
		sendRetryMs?: number;
		maxSendRetryMs?: number;
	}) =>
		new ReplicationInfoV2SendCoordinator<"u32">({
			getRpc: () => ({ send: rpcSend }) as any,
			getSelfKey: () => self,
			getSenderTransportSession: () => senderTransportSession,
			getMyReplicationSegments: getSegments,
			validatePersistedReplicationRangeSnapshot: () => {},
			isClosed: () => closed,
			isPeerSessionCurrent: (_peerHash, peerSession) =>
				openSessions.has(peerSession),
			isPeerSessionOpen: (_peerHash, peerSession) =>
				openSessions.has(peerSession) && !closedReadinessGates.has(peerSession),
			captureReplicationOwnershipLifecycle: () => ownershipController,
			isReplicationOwnershipLifecycleActive: (controller) =>
				controller === ownershipController && !controller.signal.aborted,
			sendRetryMs: options?.sendRetryMs,
			maxSendRetryMs: options?.maxSendRetryMs,
		});

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
			capabilityTimestamp: 1n,
			requestTimestamp,
		});

	beforeEach(() => {
		closed = false;
		senderTransportSession = initialSenderTransportSession;
		openSessions = new Set();
		closedReadinessGates = new Set();
		rpcSend = sinon.stub().resolves([]);
		getSegments = sinon.stub().resolves([]);
		ownershipController = new AbortController();
		coordinator = createCoordinator();
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

	it("rejects a request when captured ownership is already inactive", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		ownershipController.abort();

		expect(accept(peerA, peerSession, challenge(41))).to.be.false;
		await coordinator.drain();
		expect(getSegments.notCalled).to.be.true;
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
			expect(accept(peerA, peerSession, challenge(8), 21n + BigInt(index))).to
				.be.false;
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
			expect(accept(peerA, peerSession, challenge(19), 31n + BigInt(index))).to
				.be.false;
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

	it("fences an old state when the local sender transport session rotates", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const oldSnapshot = pDefer<never[]>();
		getSegments.onFirstCall().returns(oldSnapshot.promise);
		expect(accept(peerA, peerSession, challenge(42), 10n)).to.be.true;
		await waitForResolved(() => expect(getSegments.calledOnce).to.be.true);
		const oldState = coordinator._sendStates.get(peerA.hashcode())!;

		senderTransportSession += 1n;
		oldSnapshot.resolve([]);
		await coordinator.drain();
		expect(oldState.controller.signal.aborted).to.be.true;
		expect(coordinator._sendStates.size).to.equal(0);
		expect(rpcSend.notCalled).to.be.true;

		expect(accept(peerA, peerSession, challenge(43), 11n)).to.be.true;
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
		const replacement = rpcSend.firstCall
			.args[0] as FullReplicationInfoV2Message;
		expect(replacement.sequence).to.equal(1n);
		expect(coordinator._sendStates.get(peerA.hashcode())?.peerSession).to.equal(
			peerSession,
		);
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

	it("self-heals an ambiguous send with a Full at the next sequence", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		rpcSend.onFirstCall().rejects(new Error("ambiguous delivery"));
		rpcSend.onSecondCall().resolves([]);

		expect(accept(peerA, peerSession, challenge(26))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(state.suspended).to.be.true;
		expect(state.nextSequence).to.equal(2n);
		expect(state.retryAttempts).to.equal(1);
		expect(state.retryTimer).to.exist;
		expect((state.retryTimer as any).hasRef()).to.be.false;

		await clock.tickAsync(5);
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.firstCall.args[0]).to.be.instanceOf(
			FullReplicationInfoV2Message,
		);
		const recovered = rpcSend.secondCall
			.args[0] as FullReplicationInfoV2Message;
		expect(recovered).to.be.instanceOf(FullReplicationInfoV2Message);
		expect(recovered.sequence).to.equal(2n);
		expect(state.suspended).to.be.false;
		expect(state.retryAttempts).to.equal(0);
		expect(state.retryTimer).to.be.undefined;
	});

	it("parks sequence one when readiness closes during snapshot creation", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		const snapshot = pDefer<never[]>();
		getSegments.returns(snapshot.promise);

		expect(accept(peerA, peerSession, challenge(36))).to.be.true;
		await clock.tickAsync(0);
		expect(getSegments.calledOnce).to.be.true;
		closedReadinessGates.add(peerSession);
		snapshot.resolve([]);
		await coordinator.drain();

		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(rpcSend.notCalled).to.be.true;
		expect(state.nextSequence).to.equal(1n);
		expect(state.suspended).to.be.true;
		expect(state.pending).to.deep.equal({ kind: "snapshot" });
		expect(state.retryTimer).to.exist;

		closedReadinessGates.delete(peerSession);
		await clock.tickAsync(5);
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
		const resumed = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		expect(resumed.sequence).to.equal(1n);
		expect(state.established).to.be.true;
	});

	it("recovers when readiness closes before a successful send continuation", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		let closeGateOnSend = true;
		rpcSend.callsFake(async () => {
			if (closeGateOnSend) {
				closeGateOnSend = false;
				closedReadinessGates.add(peerSession);
			}
			return [];
		});

		expect(accept(peerA, peerSession, challenge(44))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(rpcSend.calledOnce).to.be.true;
		expect(rpcSend.firstCall.args[0].sequence).to.equal(1n);
		expect(state.nextSequence).to.equal(2n);
		expect(state.suspended).to.be.true;
		expect(state.pending).to.deep.equal({ kind: "snapshot" });

		closedReadinessGates.delete(peerSession);
		await clock.tickAsync(5);
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		const recovered = rpcSend.secondCall
			.args[0] as FullReplicationInfoV2Message;
		expect(recovered).to.be.instanceOf(FullReplicationInfoV2Message);
		expect(recovered.sequence).to.equal(2n);
		expect(state.established).to.be.true;
	});

	it("recovers a consumed sequence rejection after the readiness gate reopens", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(37))).to.be.true;
		await coordinator.drain();
		const initial = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		const rejectedSend = pDefer<void>();
		rpcSend.onSecondCall().returns(rejectedSend.promise);

		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await clock.tickAsync(0);
		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.secondCall.args[0].sequence).to.equal(2n);
		closedReadinessGates.add(peerSession);
		rejectedSend.reject(new Error("gate closed after sequence consumption"));
		await coordinator.drain();

		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(state.nextSequence).to.equal(3n);
		expect(state.inFlightSequence).to.be.undefined;
		expect(state.suspended).to.be.true;
		expect(state.pending).to.deep.equal({ kind: "snapshot" });

		closedReadinessGates.delete(peerSession);
		await clock.tickAsync(5);
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(3);
		const recovered = rpcSend.thirdCall.args[0] as FullReplicationInfoV2Message;
		expect(recovered).to.be.instanceOf(FullReplicationInfoV2Message);
		expect(recovered.sequence).to.equal(3n);
		expect([...recovered.senderEpoch]).to.deep.equal([...initial.senderEpoch]);
		expect([...recovered.receiverChallenge]).to.deep.equal([
			...initial.receiverChallenge,
		]);
	});

	it("spends a MAX_U64 stream before ownership teardown can retain it", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(38))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		state.nextSequence = (1n << 64n) - 1n;
		rpcSend.resetHistory();
		rpcSend.callsFake(
			async (_message, options) =>
				await new Promise<void>((_resolve, reject) => {
					options.signal.addEventListener(
						"abort",
						() => reject(new Error("ownership closed at MAX_U64")),
						{ once: true },
					);
				}),
		);

		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await waitForResolved(() => expect(rpcSend.calledOnce).to.be.true);
		expect(rpcSend.firstCall.args[0].sequence).to.equal((1n << 64n) - 1n);
		ownershipController.abort();
		await coordinator.drain();

		expect(state.inFlightSequence).to.be.undefined;
		expect(coordinator._sendStates.has(peerA.hashcode())).to.be.false;
		expect(coordinator._spentPeerSessions.has(peerSession)).to.be.true;
		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.calledOnce).to.be.true;
	});

	it("lets an actual PeerSession replacement start a new binding at sequence one", async () => {
		const oldPeerSession = {};
		const newPeerSession = {};
		openSessions.add(oldPeerSession);
		expect(accept(peerA, oldPeerSession, challenge(39))).to.be.true;
		await coordinator.drain();
		const oldState = coordinator._sendStates.get(peerA.hashcode())!;
		const oldBinding = (
			rpcSend.firstCall.args[0] as FullReplicationInfoV2Message
		).receiverChallenge.slice();

		closedReadinessGates.add(oldPeerSession);
		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		expect(oldState.suspended).to.be.true;
		expect(oldState.pending).to.deep.equal({ kind: "snapshot" });
		openSessions.delete(oldPeerSession);
		openSessions.add(newPeerSession);

		expect(accept(peerA, newPeerSession, challenge(40), 2n)).to.be.true;
		await coordinator.drain();
		expect(oldState.controller.signal.aborted).to.be.true;
		expect(rpcSend.callCount).to.equal(2);
		const replacement = rpcSend.secondCall
			.args[0] as FullReplicationInfoV2Message;
		expect(replacement.sequence).to.equal(1n);
		expect([...replacement.receiverChallenge]).to.not.deep.equal([
			...oldBinding,
		]);
		expect(coordinator._sendStates.get(peerA.hashcode())?.peerSession).to.equal(
			newPeerSession,
		);
	});

	it("coalesces every backoff mutation into the current authoritative Full", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		rpcSend.onFirstCall().rejects(new Error("ambiguous delivery"));
		rpcSend.onSecondCall().resolves([]);

		expect(accept(peerA, peerSession, challenge(27))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		for (let index = 0; index < 10_000; index++) {
			coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		}
		expect(state.pending).to.deep.equal({ kind: "snapshot" });
		expect(state.retryTimer).to.exist;

		await clock.tickAsync(5);
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.secondCall.args[0]).to.be.instanceOf(
			FullReplicationInfoV2Message,
		);
		expect(rpcSend.secondCall.args[0].sequence).to.equal(2n);
		expect(getSegments.callCount).to.equal(2);
	});

	it("lets a newer same-challenge request preempt backoff exactly once", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		const receiverChallenge = challenge(28);
		rpcSend.onFirstCall().rejects(new Error("ambiguous delivery"));
		rpcSend.onSecondCall().resolves([]);

		expect(accept(peerA, peerSession, receiverChallenge, 10n)).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		expect(state.retryTimer).to.exist;
		expect(accept(peerA, peerSession, receiverChallenge, 11n)).to.be.true;
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
		expect(rpcSend.secondCall.args[0].sequence).to.equal(2n);
		expect(state.retryTimer).to.be.undefined;

		await clock.tickAsync(1_000);
		await coordinator.drain();
		expect(rpcSend.callCount).to.equal(2);
	});

	it("fences retry timers on peer, reopen, session and ownership teardown", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		rpcSend.rejects(new Error("ambiguous delivery"));

		expect(accept(peerA, peerSession, challenge(29))).to.be.true;
		await coordinator.drain();
		const cleared = coordinator._sendStates.get(peerA.hashcode())!;
		coordinator.clearPeer(peerA.hashcode(), peerSession);
		expect(cleared.retryTimer).to.be.undefined;
		await clock.tickAsync(100);
		expect(rpcSend.callCount).to.equal(1);

		rpcSend.resetHistory();
		rpcSend.rejects(new Error("ambiguous delivery"));
		const reopeningSession = {};
		openSessions.add(reopeningSession);
		expect(accept(peerA, reopeningSession, challenge(30))).to.be.true;
		await coordinator.drain();
		const reopening = coordinator._sendStates.get(peerA.hashcode())!;
		coordinator.resetForOpen();
		expect(reopening.retryTimer).to.be.undefined;
		await clock.tickAsync(100);
		expect(rpcSend.callCount).to.equal(1);

		rpcSend.resetHistory();
		rpcSend.rejects(new Error("ambiguous delivery"));
		const rotatedSession = {};
		openSessions.add(rotatedSession);
		expect(accept(peerA, rotatedSession, challenge(31))).to.be.true;
		await coordinator.drain();
		openSessions.delete(rotatedSession);
		await clock.tickAsync(5);
		expect(rpcSend.callCount).to.equal(1);
		expect(coordinator._sendStates.has(peerA.hashcode())).to.be.false;

		rpcSend.resetHistory();
		rpcSend.rejects(new Error("ambiguous delivery"));
		const ownershipSession = {};
		openSessions.add(ownershipSession);
		expect(accept(peerA, ownershipSession, challenge(32))).to.be.true;
		await coordinator.drain();
		const retained = coordinator._sendStates.get(peerA.hashcode())!;
		ownershipController.abort();
		expect(retained.retryTimer).to.be.undefined;
		await clock.tickAsync(5);
		expect(rpcSend.callCount).to.equal(1);
		expect(retained.retryTimer).to.be.undefined;
		expect(coordinator._sendStates.get(peerA.hashcode())).to.equal(retained);
	});

	it("retires an ambiguous MAX_U64 attempt without scheduling a retry", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(33))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		state.nextSequence = (1n << 64n) - 1n;
		rpcSend.resetHistory();
		rpcSend.rejects(new Error("ambiguous final delivery"));

		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await coordinator.drain();
		expect(rpcSend.calledOnce).to.be.true;
		expect(rpcSend.firstCall.args[0].sequence).to.equal((1n << 64n) - 1n);
		expect(coordinator._sendStates.has(peerA.hashcode())).to.be.false;
		expect(coordinator._spentPeerSessions.has(peerSession)).to.be.true;
		await clock.tickAsync(100);
		expect(rpcSend.calledOnce).to.be.true;
	});

	it("backs off a failed destination without delaying another destination", async () => {
		const clock = sinon.useFakeTimers();
		coordinator.clearForClose();
		coordinator = createCoordinator({ sendRetryMs: 5, maxSendRetryMs: 20 });
		const sessionA = {};
		const sessionB = {};
		openSessions.add(sessionA);
		openSessions.add(sessionB);
		let failedA = false;
		rpcSend.callsFake(async (_message, options) => {
			if (!failedA && options.mode.to[0] === peerA.hashcode()) {
				failedA = true;
				throw new Error("peer A delivery failed");
			}
			return [];
		});

		expect(accept(peerA, sessionA, challenge(34))).to.be.true;
		expect(accept(peerB, sessionB, challenge(35))).to.be.true;
		await coordinator.drain();
		expect(coordinator._sendStates.get(peerA.hashcode())?.retryTimer).to.exist;
		expect(coordinator._sendStates.get(peerB.hashcode())?.established).to.be
			.true;
		coordinator.enqueue(new AddedReplicationSegmentMessage({ segments: [] }));
		await coordinator.drain();
		expect(
			rpcSend.args.some(
				(args) =>
					args[1].mode.to[0] === peerB.hashcode() &&
					args[0] instanceof AddedReplicationInfoV2Message,
			),
		).to.be.true;

		await clock.tickAsync(5);
		await coordinator.drain();
		expect(coordinator._sendStates.get(peerA.hashcode())?.established).to.be
			.true;
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

	it("consumes a new sequence for every terminal attempt, including sync throws", async () => {
		const peerSessionA = {};
		const peerSessionB = {};
		openSessions.add(peerSessionA);
		openSessions.add(peerSessionB);
		expect(accept(peerA, peerSessionA, challenge(45))).to.be.true;
		expect(accept(peerB, peerSessionB, challenge(46))).to.be.true;
		await coordinator.drain();
		const stateA = coordinator._sendStates.get(peerA.hashcode())!;
		const stateB = coordinator._sendStates.get(peerB.hashcode())!;
		ownershipController.abort();
		rpcSend.resetHistory();
		rpcSend.onFirstCall().throws(new Error("synchronous terminal failure"));
		rpcSend.onSecondCall().resolves([]);

		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.callCount).to.equal(2);
		const failed = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		const continued = rpcSend.secondCall
			.args[0] as FullReplicationInfoV2Message;
		expect(failed.sequence).to.equal(2n);
		expect(continued.sequence).to.equal(2n);
		expect(continued.segments).to.deep.equal([]);
		expect(stateA.nextSequence).to.equal(3n);
		expect(stateB.nextSequence).to.equal(3n);

		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.callCount).to.equal(4);
		const retriedA = rpcSend.thirdCall.args[0] as FullReplicationInfoV2Message;
		const retriedB = rpcSend.getCall(3).args[0] as FullReplicationInfoV2Message;
		expect(retriedA.sequence).to.equal(3n);
		expect(retriedB.sequence).to.equal(3n);
		expect(retriedA.segments).to.deep.equal([]);
		expect(retriedB.segments).to.deep.equal([]);
		expect(stateA.nextSequence).to.equal(4n);
		expect(stateB.nextSequence).to.equal(4n);
	});

	it("honors the terminal bound when the transport ignores abort", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		expect(accept(peerA, peerSession, challenge(47))).to.be.true;
		await coordinator.drain();
		const state = coordinator._sendStates.get(peerA.hashcode())!;
		ownershipController.abort();
		rpcSend.resetHistory();
		const transport = pDefer<unknown>();
		rpcSend.returns(transport.promise);
		const terminalController = new AbortController();

		const terminal = coordinator.sendTerminalReset(terminalController.signal);
		await waitForResolved(() => expect(rpcSend.calledOnce).to.be.true);
		let settled = false;
		void terminal.then(() => {
			settled = true;
		});
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(settled).to.be.false;

		terminalController.abort();
		await terminal;
		expect(settled).to.be.true;
		const reset = rpcSend.firstCall.args[0] as FullReplicationInfoV2Message;
		expect(reset.sequence).to.equal(2n);
		expect(reset.segments).to.deep.equal([]);
		expect(state.nextSequence).to.equal(3n);
		transport.resolve([]);
	});

	it("spends the last u64 terminal sequence and cannot retry or recreate it", async () => {
		const peerSession = {};
		openSessions.add(peerSession);
		const receiverChallenge = challenge(24);
		expect(accept(peerA, peerSession, receiverChallenge)).to.be.true;
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
		expect(state.nextSequence).to.equal(1n << 64n);
		expect(coordinator._spentPeerSessions.has(peerSession)).to.be.true;
		expect(coordinator._sendStates.has(peerA.hashcode())).to.be.false;

		rpcSend.resetHistory();
		await coordinator.sendTerminalReset(new AbortController().signal);
		expect(rpcSend.notCalled).to.be.true;
		ownershipController = new AbortController();
		expect(accept(peerA, peerSession, receiverChallenge, 2n)).to.be.false;
		await coordinator.drain();
		expect(rpcSend.notCalled).to.be.true;
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
		await waitForResolved(
			() =>
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

	it("inherits only transport-matched pre-opening capabilities and fences departure", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const remoteTransportSession = 78n;
		const capabilityTimestamp = 10n;
		const capability = new SyncCapabilitiesMessage({
			capabilities:
				SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		});
		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.resolves();
		await log.onMessage(capability, {
			from: remote,
			message: {
				header: {
					session: remoteTransportSession,
					timestamp: capabilityTimestamp,
				},
			},
		} as any);
		expect(log._peerSessions.current(remoteHash)).to.equal(null);
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			remoteTransportSession,
		);
		await waitForResolved(
			() =>
				expect(requestSubscribers.calledOnceWith(log.topic, remote)).to.be.true,
		);

		sinon.stub(log.rpc, "send").resolves([] as any);
		const event = {
			detail: {
				from: remote,
				topics: [log.topic],
				session: remoteTransportSession,
			},
		} as any;
		await log._onSubscription(event);
		const firstSession = log._peerSessions.current(remoteHash);
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			remoteTransportSession,
		);
		expect(log._peerSyncCapabilityTimestamps.get(remoteHash)).to.equal(
			capabilityTimestamp,
		);
		expect(
			log._v2Receive._receiveStates.get(remoteHash)?.senderTransportSession,
		).to.equal(remoteTransportSession);

		const successorTransportSession = 79n;
		await log.onMessage(capability, {
			from: remote,
			message: {
				header: {
					session: successorTransportSession,
					timestamp: capabilityTimestamp + 1n,
				},
			},
		} as any);
		await log._onSubscription({
			detail: {
				from: remote,
				topics: [log.topic],
				session: successorTransportSession,
			},
		} as any);
		const replacement = log._peerSessions.current(remoteHash);
		expect(replacement).not.to.equal(firstSession);
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			successorTransportSession,
		);
		expect(log._peerSyncCapabilityTimestamps.get(remoteHash)).to.equal(
			capabilityTimestamp + 1n,
		);
		expect(
			log._v2Receive._receiveStates.get(remoteHash)?.senderTransportSession,
		).to.equal(successorTransportSession);

		await log._onSubscription({
			detail: {
				from: remote,
				topics: [log.topic],
				session: successorTransportSession + 1n,
			},
		} as any);
		expect(log._peerSyncCapabilitySessions.has(remoteHash)).to.be.false;
		expect(log._peerSyncCapabilityTimestamps.has(remoteHash)).to.be.false;
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;

		const unboundTransportSession = successorTransportSession + 2n;
		await log.onMessage(capability, {
			from: remote,
			message: {
				header: {
					session: unboundTransportSession,
					timestamp: capabilityTimestamp + 2n,
				},
			},
		} as any);
		await log._onSubscription({
			detail: { from: remote, topics: [log.topic] },
		} as any);
		expect(log._peerSyncCapabilitySessions.has(remoteHash)).to.be.false;
		expect(log._peerSyncCapabilityTimestamps.has(remoteHash)).to.be.false;
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;

		await log._onUnsubscription(event);
		expect(log._peerSyncCapabilitySessions.has(remoteHash)).to.be.false;
		expect(log._peerSyncCapabilityTimestamps.has(remoteHash)).to.be.false;
	});

	it("inherits pre-opening capability on the first snapshot-fallback opening", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const remoteTransportSession = 91n;
		const capabilityTimestamp = 5n;
		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.resolves();
		await log.onMessage(
			new SyncCapabilitiesMessage({
				capabilities:
					SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
					SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
			}),
			{
				from: remote,
				message: {
					header: {
						session: remoteTransportSession,
						timestamp: capabilityTimestamp,
					},
				},
			} as any,
		);
		expect(log._peerSessions.current(remoteHash)).to.equal(null);
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			remoteTransportSession,
		);

		sinon.stub(log.rpc, "send").resolves([] as any);
		// A snapshot-fallback opening carries no signed transport session on the
		// subscription event. Its FIRST session (no predecessor) may inherit the
		// pre-opening capability; only successor openings must wipe it.
		await log._onSubscription({
			detail: { from: remote, topics: [log.topic] },
		} as any);
		const opened = log._peerSessions.current(remoteHash);
		expect(opened?.phase).to.equal("open");
		// The branch under test: fallback arm with hasPredecessor === false.
		expect(opened?.hasPredecessor).to.be.false;
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			remoteTransportSession,
		);
		expect(log._peerSyncCapabilityTimestamps.get(remoteHash)).to.equal(
			capabilityTimestamp,
		);
		// The inherited signed capability promotes straight into a receive state
		// bound to the pre-opening transport generation.
		expect(
			log._v2Receive._receiveStates.get(remoteHash)?.senderTransportSession,
		).to.equal(remoteTransportSession);
		requestSubscribers.restore();
	});

	it("coalesces subscriber snapshot requests for a session-less capability burst", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const capability = new SyncCapabilitiesMessage({
			capabilities:
				SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		});
		let releaseSnapshot!: () => void;
		const snapshot = new Promise<void>((resolve) => {
			releaseSnapshot = resolve;
		});
		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.returns(snapshot);
		const frame = (timestamp: bigint) =>
			log.onMessage(capability, {
				from: remote,
				message: { header: { session: 80n, timestamp } },
			} as any);

		try {
			// Every frame advances its sender-controlled timestamp, so each one
			// passes the observed gate — but the burst must coalesce into ONE
			// in-flight targeted snapshot request.
			for (let index = 0; index < 5; index++) {
				await frame(BigInt(10 + index));
			}
			await waitForResolved(() =>
				expect(requestSubscribers.callCount).to.equal(1),
			);
			expect(log._peerSessions.current(remoteHash)).to.equal(null);
			expect(requestSubscribers.firstCall.args).to.deep.equal([
				log.topic,
				remote,
			]);

			// Once the in-flight snapshot settles, a genuinely new session-less
			// capability may solicit a fresh snapshot again.
			releaseSnapshot();
			await waitForResolved(
				() =>
					expect(log._subscriberSnapshotRequestsByPeer.has(remoteHash)).to.be
						.false,
			);
			await frame(20n);
			await waitForResolved(() =>
				expect(requestSubscribers.callCount).to.equal(2),
			);
		} finally {
			releaseSnapshot();
			requestSubscribers.restore();
		}
	});

	it("does not open a replication session for an unsubscribed discovery candidate", async () => {
		session = await TestSession.connected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const remoteHash = session.peers[1].identity.publicKey.hashcode();

		expect((db.log as any)._peerSessions.current(remoteHash)).to.equal(null);
	});

	it("egresses only V2 mutations for a default open", async () => {
		// B12 default-mode pin: with no compatibility option, a mutation
		// announcement produces exactly ONE outbound frame — the directed V2
		// mutation — and no legacy primary of any class.
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
		const outbound = send.args.map((args: any[]) => args[0]);
		const legacyMessages = outbound.filter(
			(message: unknown) => message instanceof AddedReplicationSegmentMessage,
		);
		const v2Messages = outbound.filter(
			(message: unknown) => message instanceof AddedReplicationInfoV2Message,
		) as AddedReplicationInfoV2Message[];
		expect(outbound).to.have.length(1);
		expect(legacyMessages).to.have.length(0);
		expect(v2Messages).to.have.length(1);
		expect(v2Messages[0].sequence).to.equal(2n);
	});

	it("uses a newer capability to authorize one bounded challenge rebind", async () => {
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
		const firstSend = pDefer<void>();
		let blockFirst = true;
		const send = sinon.stub(log.rpc, "send").callsFake(async () => {
			if (!blockFirst) {
				return [] as any;
			}
			blockFirst = false;
			await firstSend.promise;
			return [] as any;
		});

		await log.onMessage(request(30), context(11n));
		await waitForResolved(() => expect(send.calledOnce).to.be.true);
		await log.onMessage(
			new SyncCapabilitiesMessage({ capabilities: 0 }),
			context(12n),
		);
		expect(log._v2Send._sendStates.size).to.equal(1);
		expect(
			log._peerSyncCapabilities.get(remoteHash) &
				SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
		).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY);
		expect(log._peerSyncCapabilityTimestamps.get(remoteHash)).to.equal(12n);
		expect(log._v2Send._retiringWorkersByPeer.size).to.equal(0);
		firstSend.resolve();
		await log._v2Send.drain();

		await log.onMessage(request(30), context(11n));
		expect(send.calledOnce).to.be.true;
		await log.onMessage(request(31), context(12n));
		expect(send.calledOnce).to.be.true;
		await log.onMessage(request(31), context(13n));
		await waitForResolved(() => expect(send.callCount).to.equal(2));
		await log.onMessage(request(30), context(13n));
		expect(send.callCount).to.equal(2);
		await log._v2Send.drain();
		expect(log._v2Send._sendStates.get(remoteHash)?.established).to.be.true;
		expect([
			...log._v2Send._sendStates.get(remoteHash)!.receiverRequestChallenge,
		]).to.deep.equal([...challenge(31)]);
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
