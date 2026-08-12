import { Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import {
	ReplicationIntent,
	ReplicationRangeMessageU32,
	ReplicationRangeMessageU64,
} from "../src/ranges.js";
import { deriveReplicationInfoV2ReceiverBinding } from "../src/replication-info-v2-binding.js";
import { ReplicationInfoV2ReceiveCoordinator } from "../src/replication-info-v2-receive.js";
import { ReplicationInfoV2SendCoordinator } from "../src/replication-info-v2-send.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	FullReplicationInfoV2Message,
	StoppedReplicationInfoV2Message,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

const key = (value: number) =>
	new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(value) });

const bytes = (value: number) => new Uint8Array(32).fill(value);
const senderCapabilities =
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND;

describe("receive admission replication-info V2 receiver state", () => {
	const self = key(1);
	const sender = key(2);
	const peerHash = sender.hashcode();
	const senderTransportSession = 0x20000000000001n;
	const receiverTransportSession = 0x20000000000002n;

	let closed: boolean;
	let currentSession: object;
	let currentReceiveEpoch: object;
	let currentSenderTransportSession: bigint;
	let peerStateReady: boolean;
	let sendRequest: sinon.SinonStub;
	let refreshLocalCapability: sinon.SinonStub;
	let coordinator: ReplicationInfoV2ReceiveCoordinator;

	const createCoordinator = (options?: {
		requestRetryMs?: number;
		maxRequestRetryMs?: number;
		requestMaxAttempts?: number;
	}) =>
		new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => self,
			getReceiverTransportSession: () => receiverTransportSession,
			isClosed: () => closed,
			isPeerSessionCurrent: (_peerHash, peerSession) =>
				peerSession === currentSession,
			isReceiveEpochCurrent: (_peerHash, receiveEpoch) =>
				receiveEpoch === currentReceiveEpoch,
			isPeerStateCurrent: (_peerHash, peerSession, receiveEpoch) =>
				peerStateReady &&
				peerSession === currentSession &&
				receiveEpoch === currentReceiveEpoch,
			isSenderTransportSessionCurrent: (_peerHash, transportSession) =>
				transportSession === currentSenderTransportSession,
			sendRequest,
			refreshLocalCapability,
			requestRetryMs: options?.requestRetryMs ?? 5,
			maxRequestRetryMs: options?.maxRequestRetryMs ?? 20,
			requestMaxAttempts: options?.requestMaxAttempts ?? 7,
		});

	const markLocalReady = (requestNotBeforeMs = Date.now()) =>
		coordinator.markLocalCapabilityReady({
			peerHash,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			receiverTransportSession,
			requestNotBeforeMs,
		});

	const observeSender = (
		capabilityTimestamp = 1n,
		transportSession = senderTransportSession,
	) =>
		coordinator.observeCapability({
			peerHash,
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			capabilities: senderCapabilities,
			senderTransportSession: transportSession,
			capabilityTimestamp,
		});

	const prepare = (
		message:
			| FullReplicationInfoV2Message
			| AddedReplicationInfoV2Message
			| StoppedReplicationInfoV2Message,
		transportTimestamp = 1n,
	) =>
		coordinator.prepare(message, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp,
		});

	beforeEach(() => {
		closed = false;
		currentSession = {};
		currentReceiveEpoch = {};
		currentSenderTransportSession = senderTransportSession;
		peerStateReady = true;
		sendRequest = sinon.stub().resolves();
		refreshLocalCapability = sinon.stub().callsFake(async () => ({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		}));
		coordinator = createCoordinator();
	});

	afterEach(() => {
		coordinator.clearForClose();
		sinon.restore();
	});

	it("waits for ACKed local readiness and retries the exact challenge", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({ requestRetryMs: 5 });

		expect(observeSender()).to.be.true;
		await clock.tickAsync(100);
		expect(sendRequest.notCalled).to.be.true;

		expect(markLocalReady(1_100)).to.be.true;
		await clock.tickAsync(1);
		expect(sendRequest.calledOnce).to.be.true;
		const first = sendRequest.firstCall.args[0];
		expect(first.intendedSender.equals(sender)).to.be.true;
		expect(first.senderSession).to.equal(senderTransportSession);

		const state = coordinator._receiveStates.get(peerHash)!;
		expect([...state.receiverBinding!]).to.deep.equal([
			...deriveReplicationInfoV2ReceiverBinding({
				receiverChallenge: state.receiverRequestChallenge,
				receiver: self,
				receiverTransportSession,
				sender,
				senderTransportSession,
			}),
		]);

		await clock.tickAsync(5);
		expect(sendRequest.callCount).to.equal(2);
		const retried = sendRequest.secondCall.args[0];
		expect([...retried.receiverChallenge]).to.deep.equal([
			...first.receiverChallenge,
		]);
	});

	it("promotes an ACKed advert immediately and holds the grant stable", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({ requestRetryMs: 5 });
		const lifecycle = new AbortController();
		expect(observeSender()).to.be.true;

		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await handle.firstAttempt;
		const advertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		// B12: there is no legacy barrier — the ACK promotes readiness at once.
		expect(advertisement.acknowledgedReady).to.exist;
		expect(advertisement.ready).to.be.true;
		const ready =
			coordinator._localCapabilityReadyBySession.get(currentSession);
		expect(ready).to.exist;

		// Once promoted the grant is stable: draining the timer queue neither
		// re-arms the advert worker nor rotates the ready record.
		await clock.tickAsync(1);
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect(advertisement.timer).to.be.undefined;
		expect(
			coordinator._localCapabilityReadyBySession.get(currentSession),
		).to.equal(ready);
		expect(advertisement.ready).to.be.true;
		expect(sendRequest.calledOnce).to.be.true;
	});

	it("promotes when a deferred ACK finally settles", async () => {
		const clock = sinon.useFakeTimers({ now: 1_100 });
		const pending = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.callsFake(() => pending.promise);
		coordinator = createCoordinator();
		const lifecycle = new AbortController();
		expect(observeSender()).to.be.true;

		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		expect(coordinator._localCapabilityReadyBySession.has(currentSession)).to.be
			.false;
		pending.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await handle.firstAttempt;
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)?.ready,
		).to.be.true;
		await clock.tickAsync(1);
		expect(sendRequest.calledOnce).to.be.true;
	});

	it("stops retrying once a retried ACK promotes readiness", async () => {
		const clock = sinon.useFakeTimers({ now: 1_200 });
		refreshLocalCapability.onFirstCall().rejects(new Error("advert failed"));
		refreshLocalCapability.onSecondCall().callsFake(async () => ({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		}));
		coordinator = createCoordinator({ requestRetryMs: 5 });
		const lifecycle = new AbortController();
		expect(observeSender()).to.be.true;

		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await handle.firstAttempt;
		const advertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		expect(advertisement.timer).to.exist;
		expect((advertisement.timer as any).hasRef()).to.be.false;

		await clock.tickAsync(5);
		expect(refreshLocalCapability.callCount).to.equal(2);
		expect(advertisement.acknowledgedReady).to.exist;
		// B12: the retried ACK promotes immediately — no barrier holds it back,
		// and the bounded capability worker stops retrying once ready.
		expect(advertisement.ready).to.be.true;
		expect(advertisement.timer).to.be.undefined;
		await clock.tickAsync(1);
		expect(sendRequest.calledOnce).to.be.true;
		// The capability advert worker never restarts after promotion (the
		// bounded REQUEST worker may still refresh a stale grant later; that
		// path is covered by the unpark tests).
		expect(refreshLocalCapability.callCount).to.equal(2);
		expect(advertisement.ready).to.be.true;
		expect(advertisement.timer).to.be.undefined;
	});

	it("parks one failed worker across a temporary receive gate closure", async () => {
		const clock = sinon.useFakeTimers({ now: 1_300 });
		refreshLocalCapability.onFirstCall().rejects(new Error("advert failed"));
		refreshLocalCapability.onSecondCall().callsFake(async () => ({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		}));
		coordinator = createCoordinator({ requestRetryMs: 5 });
		const lifecycle = new AbortController();
		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await handle.firstAttempt;
		const advertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;

		peerStateReady = false;
		await clock.tickAsync(10);
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash),
		).to.equal(advertisement);
		expect(advertisement.controller.signal.aborted).to.be.false;
		expect(advertisement.timer).to.exist;

		peerStateReady = true;
		await clock.tickAsync(5);
		expect(refreshLocalCapability.callCount).to.equal(2);
		expect(advertisement.ready).to.be.true;
		expect(advertisement.timer).to.be.undefined;
	});

	it("keeps independent bounded capability workers for two peers", async () => {
		const clock = sinon.useFakeTimers({ now: 2_000 });
		const secondSender = key(3);
		const sessionByPeer = new Map<string, object>();
		const epochByPeer = new Map<string, object>();
		const attemptsByPeer = new Map<string, number>();
		const lifecycle = new AbortController();
		for (const target of [sender, secondSender]) {
			sessionByPeer.set(target.hashcode(), {});
			epochByPeer.set(target.hashcode(), {});
		}
		coordinator = new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => self,
			getReceiverTransportSession: () => receiverTransportSession,
			isClosed: () => false,
			isPeerSessionCurrent: (hash, peerSession) =>
				sessionByPeer.get(hash) === peerSession,
			isReceiveEpochCurrent: (hash, receiveEpoch) =>
				epochByPeer.get(hash) === receiveEpoch,
			isPeerStateCurrent: (hash, peerSession, receiveEpoch) =>
				sessionByPeer.get(hash) === peerSession &&
				epochByPeer.get(hash) === receiveEpoch,
			isSenderTransportSessionCurrent: () => true,
			sendRequest: async () => {},
			refreshLocalCapability: async ({ peerHash }) => {
				const attempt = (attemptsByPeer.get(peerHash) ?? 0) + 1;
				attemptsByPeer.set(peerHash, attempt);
				if (attempt === 1) {
					throw new Error("first advert failed");
				}
				return {
					receiverTransportSession,
					requestNotBeforeMs: Date.now(),
				};
			},
			requestRetryMs: 5,
			maxRequestRetryMs: 20,
		});

		const handles = [sender, secondSender].map((target) =>
			coordinator.advertiseLocalCapability({
				target,
				peerSession: sessionByPeer.get(target.hashcode())!,
				receiveEpoch: epochByPeer.get(target.hashcode())!,
				signal: lifecycle.signal,
			}),
		);
		await Promise.all(handles.map((handle) => handle.firstAttempt));
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(2);
		expect(
			[...coordinator._localCapabilityAdvertisementsByPeer.values()].every(
				(state) => state.timer !== undefined,
			),
		).to.be.true;

		await clock.tickAsync(5);
		expect([...attemptsByPeer.values()]).to.deep.equal([2, 2]);
		// B12: each retried ACK promotes its own peer immediately; the workers
		// stay per-peer bounded and independent.
		expect(
			[...coordinator._localCapabilityAdvertisementsByPeer.values()].every(
				(state) =>
					state.acknowledgedReady !== undefined &&
					state.ready &&
					state.timer === undefined,
			),
		).to.be.true;
		expect(
			coordinator._localCapabilityReadyBySession.has(
				sessionByPeer.get(peerHash)!,
			),
		).to.be.true;
		expect(
			coordinator._localCapabilityReadyBySession.has(
				sessionByPeer.get(secondSender.hashcode())!,
			),
		).to.be.true;
	});

	it("fences stale completion and release after a session replacement", async () => {
		const oldAck = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.onFirstCall().callsFake(() => oldAck.promise);
		refreshLocalCapability.onSecondCall().resolves({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		const oldSession = currentSession;
		const oldEpoch = currentReceiveEpoch;
		const oldLifecycle = new AbortController();
		const oldHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: oldSession,
			receiveEpoch: oldEpoch,
			signal: oldLifecycle.signal,
		});

		currentSession = {};
		currentReceiveEpoch = {};
		const replacementSession = currentSession;
		const newLifecycle = new AbortController();
		const replacementHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: replacementSession,
			receiveEpoch: currentReceiveEpoch,
			signal: newLifecycle.signal,
		});
		await replacementHandle.firstAttempt;
		const replacement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		expect(replacement.peerSession).to.equal(replacementSession);
		expect(replacement.ready).to.be.true;

		oldAck.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await oldHandle.firstAttempt;
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash),
		).to.equal(replacement);
		expect(coordinator._localCapabilityReadyBySession.has(replacementSession))
			.to.be.true;
		expect(coordinator._localCapabilityReadyBySession.has(oldSession)).to.be
			.false;
	});

	it("fences an in-flight old-epoch completion after same-session recovery", async () => {
		const oldAck = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.onFirstCall().callsFake(() => oldAck.promise);
		refreshLocalCapability.onSecondCall().resolves({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		const peerSession = currentSession;
		const oldEpoch = currentReceiveEpoch;
		const lifecycle = new AbortController();
		const oldHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession,
			receiveEpoch: oldEpoch,
			signal: lifecycle.signal,
		});
		const oldAdvertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;

		currentReceiveEpoch = {};
		expect(
			coordinator.reAdvertiseLocalCapabilityForRecovery({
				peerHash,
				peerSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		const replacement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		expect(replacement).not.to.equal(oldAdvertisement);
		expect(replacement.context).to.equal(oldAdvertisement.context);
		await replacement.firstAttempt;
		expect(replacement.acknowledgedReady).to.exist;

		expect(replacement.ready).to.be.true;
		const replacementReady =
			coordinator._localCapabilityReadyBySession.get(peerSession);
		expect(replacementReady?.advertisement).to.equal(replacement);

		oldAck.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await oldHandle.firstAttempt;
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash),
		).to.equal(replacement);
		expect(
			coordinator._localCapabilityContextBySession.get(peerSession),
		).to.equal(replacement.context);
		expect(
			coordinator._localCapabilityReadyBySession.get(peerSession),
		).to.equal(replacementReady);
		expect(replacement.controller.signal.aborted).to.be.false;
		expect(replacement.ready).to.be.true;
	});

	it("cancels a capability generation across lifecycle abort and reopen", async () => {
		const pending = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.onFirstCall().callsFake(() => pending.promise);
		refreshLocalCapability.onSecondCall().resolves({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		const oldSession = currentSession;
		const oldLifecycle = new AbortController();
		const staleHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: oldSession,
			receiveEpoch: currentReceiveEpoch,
			signal: oldLifecycle.signal,
		});
		oldLifecycle.abort();
		pending.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await staleHandle.firstAttempt;
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(0);
		expect(coordinator._localCapabilityReadyBySession.has(oldSession)).to.be
			.false;

		currentSession = {};
		currentReceiveEpoch = {};
		const currentLifecycle = new AbortController();
		const currentHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: currentLifecycle.signal,
		});
		await currentHandle.firstAttempt;
		expect(coordinator._localCapabilityReadyBySession.has(currentSession)).to.be
			.true;
	});

	it("bounds repeated local capability starts to one retry slot", async () => {
		const clock = sinon.useFakeTimers({ now: 4_000 });
		refreshLocalCapability.rejects(new Error("advert failed"));
		coordinator = createCoordinator({
			requestRetryMs: 5,
			maxRequestRetryMs: 20,
		});
		const lifecycle = new AbortController();
		const first = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		for (let index = 0; index < 10_000; index++) {
			const coalesced = coordinator.advertiseLocalCapability({
				target: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				signal: lifecycle.signal,
			});
			expect(coalesced.firstAttempt).to.equal(first.firstAttempt);
		}
		await first.firstAttempt;
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(1);
		const state =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		expect(state.attempts).to.equal(1);
		expect(state.timer).to.exist;

		await clock.tickAsync(5 + 10 + 20 + 20);
		expect(refreshLocalCapability.callCount).to.equal(5);
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(1);
		expect(state.timer).to.exist;
	});

	it("fences a capability retry when the local transport session rotates", async () => {
		const clock = sinon.useFakeTimers({ now: 5_000 });
		let localTransportSession = receiverTransportSession;
		refreshLocalCapability.rejects(new Error("advert failed"));
		coordinator = new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => self,
			getReceiverTransportSession: () => localTransportSession,
			isClosed: () => closed,
			isPeerSessionCurrent: (_peerHash, peerSession) =>
				peerSession === currentSession,
			isReceiveEpochCurrent: (_peerHash, receiveEpoch) =>
				receiveEpoch === currentReceiveEpoch,
			isPeerStateCurrent: (_peerHash, peerSession, receiveEpoch) =>
				peerSession === currentSession && receiveEpoch === currentReceiveEpoch,
			isSenderTransportSessionCurrent: (_peerHash, transportSession) =>
				transportSession === currentSenderTransportSession,
			sendRequest,
			refreshLocalCapability,
			requestRetryMs: 5,
			maxRequestRetryMs: 20,
		});
		const lifecycle = new AbortController();
		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await handle.firstAttempt;
		const stale =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		expect(stale.timer).to.exist;

		localTransportSession++;
		await clock.tickAsync(5);
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect(stale.controller.signal.aborted).to.be.true;
		expect(stale.timer).to.be.undefined;
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(0);
		expect(coordinator._localCapabilityReadyBySession.has(currentSession)).to.be
			.false;
	});

	it("clears an advert when transport rotates before its ACK settles", async () => {
		let localTransportSession = receiverTransportSession;
		const pendingAck = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.callsFake(() => pendingAck.promise);
		coordinator = new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => self,
			getReceiverTransportSession: () => localTransportSession,
			isClosed: () => closed,
			isPeerSessionCurrent: (_peerHash, peerSession) =>
				peerSession === currentSession,
			isReceiveEpochCurrent: (_peerHash, receiveEpoch) =>
				receiveEpoch === currentReceiveEpoch,
			isPeerStateCurrent: (_peerHash, peerSession, receiveEpoch) =>
				peerSession === currentSession && receiveEpoch === currentReceiveEpoch,
			isSenderTransportSessionCurrent: (_peerHash, transportSession) =>
				transportSession === currentSenderTransportSession,
			sendRequest,
			refreshLocalCapability,
		});
		const lifecycle = new AbortController();
		const handle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		const stale =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;

		// The local transport rotates while the ACK is still in flight: the
		// settled ACK must not promote a grant bound to the rotated-away
		// transport, and the stale advert clears instead of retrying.
		localTransportSession++;
		pendingAck.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await handle.firstAttempt;
		expect(stale.acknowledgedReady).to.be.undefined;
		expect(stale.controller.signal.aborted).to.be.true;
		expect(coordinator._localCapabilityAdvertisementsByPeer.size).to.equal(0);
		expect(coordinator._localCapabilityReadyBySession.has(currentSession)).to.be
			.false;
		expect(coordinator._localCapabilityContextBySession.has(currentSession)).to
			.be.false;
	});

	it("recovers before remote capability and promotes on the recovery ACK", async () => {
		const clock = sinon.useFakeTimers({ now: 5_100 });
		refreshLocalCapability.onFirstCall().rejects(new Error("advert failed"));
		refreshLocalCapability.onSecondCall().callsFake(async () => ({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		}));
		const oldEpoch = currentReceiveEpoch;
		const lifecycle = new AbortController();
		const openingHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: oldEpoch,
			signal: lifecycle.signal,
		});
		await openingHandle.firstAttempt;
		const openingAdvertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;

		peerStateReady = false;
		currentReceiveEpoch = {};
		expect(
			coordinator.reAdvertiseLocalCapabilityForRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		const replacement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		await replacement.firstAttempt;
		expect(replacement).not.to.equal(openingAdvertisement);
		expect(replacement.context).to.equal(openingAdvertisement.context);
		expect(replacement.receiveEpoch).to.equal(currentReceiveEpoch);
		// The receive gate is closed: no ACK can land, so no promotion yet.
		expect(replacement.ready).to.be.false;
		expect(refreshLocalCapability.calledOnce).to.be.true;

		peerStateReady = true;
		await clock.tickAsync(5);
		expect(refreshLocalCapability.callCount).to.equal(2);
		expect(replacement.acknowledgedReady).to.exist;
		// B12: the recovery ACK promotes immediately once the gate reopens.
		expect(replacement.ready).to.be.true;
		expect(observeSender()).to.be.true;
		expect(coordinator._receiveStates.get(peerHash)?.receiverBinding).to.exist;
		await clock.tickAsync(1);
		expect(sendRequest.calledOnce).to.be.true;
	});

	it("keeps recovery readiness across remote sender-state replacement", async () => {
		refreshLocalCapability.onFirstCall().rejects(new Error("advert failed"));
		refreshLocalCapability.onSecondCall().resolves({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		const lifecycle = new AbortController();
		const openingHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await openingHandle.firstAttempt;

		currentReceiveEpoch = {};
		expect(
			coordinator.reAdvertiseLocalCapabilityForRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		const advertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		await advertisement.firstAttempt;
		expect(advertisement.ready).to.be.true;
		expect(observeSender()).to.be.true;
		const oldState = coordinator._receiveStates.get(peerHash)!;
		const oldBinding = oldState.receiverBinding!.slice();
		const ready =
			coordinator._localCapabilityReadyBySession.get(currentSession);

		currentSenderTransportSession++;
		expect(observeSender(2n, currentSenderTransportSession)).to.be.true;
		const replacementState = coordinator._receiveStates.get(peerHash)!;
		expect(oldState.controller.signal.aborted).to.be.true;
		expect(replacementState).not.to.equal(oldState);
		expect([...replacementState.receiverBinding!]).not.to.deep.equal([
			...oldBinding,
		]);
		expect(
			coordinator._localCapabilityReadyBySession.get(currentSession),
		).to.equal(ready);
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash),
		).to.equal(advertisement);
		expect(advertisement.controller.signal.aborted).to.be.false;
		expect(lifecycle.signal.aborted).to.be.false;
	});

	it("uses immediate refreshGrant after cutover without a parallel advert", async () => {
		const clock = sinon.useFakeTimers({ now: 5_200 });
		const lifecycle = new AbortController();
		const openingHandle = coordinator.advertiseLocalCapability({
			target: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			signal: lifecycle.signal,
		});
		await openingHandle.firstAttempt;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(8),
			sequence: 1n,
			segments: [],
		});
		const admission = prepare(full);
		expect(admission).to.exist;
		expect(coordinator.commit(admission!)).to.be.true;
		const advertisement =
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash)!;
		const refreshesBeforeRecovery = refreshLocalCapability.callCount;

		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		expect(
			coordinator.reAdvertiseLocalCapabilityForRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
		expect(
			coordinator._localCapabilityAdvertisementsByPeer.get(peerHash),
		).to.equal(advertisement);
		await clock.tickAsync(0);
		expect(refreshLocalCapability.callCount).to.equal(
			refreshesBeforeRecovery + 1,
		);
		await clock.tickAsync(1);
		expect(sendRequest.calledOnce).to.be.true;
	});

	it("orders Full, Added and recovery Full independently of transport time", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(3);

		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		const fullAdmission = prepare(full);
		expect(fullAdmission).to.exist;
		expect(coordinator.commit(fullAdmission!)).to.be.true;
		expect(coordinator._cutoverPeerSessions.has(currentSession)).to.be.true;

		const added = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [],
		});
		const addedAdmission = prepare(added);
		expect(addedAdmission).to.exist;
		expect(coordinator.commit(addedAdmission!)).to.be.true;

		expect(prepare(added)).to.be.undefined;
		const gap = new StoppedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 4n,
			segmentIds: [],
		});
		expect(prepare(gap)).to.be.undefined;
		expect(state.phase).to.equal("resync");
		expect(state.lastSequence).to.equal(2n);

		const repaired = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 5n,
			segments: [],
		});
		const repairedAdmission = prepare(repaired);
		expect(repairedAdmission).to.exist;
		expect(coordinator.commit(repairedAdmission!)).to.be.true;
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(5n);

		const wrongEpoch = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(4),
			sequence: 6n,
			segments: [],
		});
		expect(prepare(wrongEpoch)).to.be.undefined;
		expect(state.lastSequence).to.equal(5n);
	});

	it("fences a parked delta across same-session recovery", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(5);
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;

		const delta = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [],
		});
		const parked = prepare(delta)!;
		const rawChallenge = state.receiverRequestChallenge.slice();
		const binding = state.receiverBinding!.slice();
		currentReceiveEpoch = {};
		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;

		expect(coordinator.commit(parked)).to.be.false;
		expect(state.phase).to.equal("resync");
		expect([...state.receiverRequestChallenge]).to.deep.equal([
			...rawChallenge,
		]);
		expect([...state.receiverBinding!]).to.deep.equal([...binding]);
		expect(
			coordinator.prepare(delta, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 1n,
			}),
		).to.be.undefined;

		const repaired = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 3n,
			segments: [],
		});
		const admission = coordinator.prepare(repaired, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 1n,
		});
		expect(admission).to.exist;
		expect(coordinator.commit(admission!)).to.be.true;
	});

	it("makes exact capability replays side-effect free", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		expect(markLocalReady()).to.be.true;
		expect(observeSender(10n)).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(6),
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;
		const challenge = state.receiverRequestChallenge.slice();

		expect(observeSender(11n)).to.be.true;
		expect(coordinator._receiveStates.get(peerHash)).to.equal(state);
		expect(state.lastSequence).to.equal(1n);
		expect(state.phase).to.equal("active");
		expect([...state.receiverRequestChallenge]).to.deep.equal([...challenge]);
		sendRequest.resetHistory();
		await clock.tickAsync(100);
		expect(sendRequest.notCalled).to.be.true;
		expect(state.requestTimer).to.be.undefined;
		expect(observeSender(9n)).to.be.false;
	});

	it("revokes an old grant when a newer transport drops sender readiness", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender(10n)).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(11),
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;

		const newerTransport = senderTransportSession + 1n;
		currentSenderTransportSession = newerTransport;
		expect(
			coordinator.observeCapability({
				peerHash,
				target: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
				senderTransportSession: newerTransport,
				capabilityTimestamp: 20n,
			}),
		).to.be.false;
		expect(state.controller.signal.aborted).to.be.true;
		expect(coordinator._receiveStates.has(peerHash)).to.be.false;
		expect(coordinator._cutoverPeerSessions.has(currentSession)).to.be.false;
		expect(prepare(full)).to.be.undefined;
	});

	it("rejects every mismatched binding, session, epoch and sequence tuple", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(12);
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;

		const wrongBinding = new AddedReplicationInfoV2Message({
			receiverChallenge: bytes(99),
			senderEpoch,
			sequence: 2n,
			segments: [],
		});
		const wrongSenderEpoch = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(98),
			sequence: 2n,
			segments: [],
		});
		const zero = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 0n,
			segments: [],
		});
		expect(prepare(wrongBinding)).to.be.undefined;
		expect(prepare(wrongSenderEpoch)).to.be.undefined;
		expect(prepare(zero)).to.be.undefined;
		expect(prepare(full)).to.be.undefined;
		expect(
			coordinator.prepare(
				new AddedReplicationInfoV2Message({
					receiverChallenge: state.receiverBinding!.slice(),
					senderEpoch,
					sequence: 2n,
					segments: [],
				}),
				{
					from: sender,
					peerSession: {},
					receiveEpoch: currentReceiveEpoch,
					senderTransportSession,
					transportTimestamp: 1n,
				},
			),
		).to.be.undefined;
		expect(
			coordinator.prepare(full, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: {},
				senderTransportSession,
				transportTimestamp: 1n,
			}),
		).to.be.undefined;
		expect(
			coordinator.prepare(full, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession: senderTransportSession + 1n,
				transportTimestamp: 1n,
			}),
		).to.be.undefined;
	});

	it("defers one bounded resync when a successor overlaps an active reservation", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(13);
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;
		const next = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [],
		});
		const reserved = coordinator.reserve(next, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		for (let index = 0; index < 100; index++) {
			expect(
				coordinator.reserve(next, {
					from: sender,
					peerSession: currentSession,
					receiveEpoch: currentReceiveEpoch,
					senderTransportSession,
					transportTimestamp: 2n,
				}),
			).to.be.undefined;
		}
		expect(state.reservedAdmission).to.equal(reserved);

		const successor = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 3n,
			segments: [],
		});
		expect(
			coordinator.reserve(successor, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 3n,
			}),
		).to.be.undefined;
		expect(state.phase).to.equal("active");
		expect(state.reservedAdmission).to.equal(reserved);
		expect(reserved.resyncAfterRelease).to.be.true;
		expect(state.requestTimer).to.be.undefined;
		expect(coordinator.commit(reserved)).to.be.true;
		expect(state.lastSequence).to.equal(2n);
		expect(state.phase).to.equal("resync");
		expect(state.requestTimer).to.exist;
		expect(state.reservedAdmission).to.be.undefined;
	});

	it("recovers a successor that overlaps the first authoritative Full", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(14);
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		const reserved = coordinator.reserve(full, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 1n,
		})!;
		const successor = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [],
		});
		expect(
			coordinator.reserve(successor, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 2n,
			}),
		).to.be.undefined;
		expect(state.phase).to.equal("awaiting-full");
		expect(reserved.resyncAfterRelease).to.be.true;
		expect(coordinator.commit(reserved)).to.be.true;
		expect(state.lastSequence).to.equal(1n);
		expect(state.phase).to.equal("resync");
		expect(coordinator._cutoverPeerSessions.has(currentSession)).to.be.true;
		expect(state.requestTimer).to.exist;
	});

	it("pauses a replacement generation behind an old peer reservation", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const oldState = coordinator._receiveStates.get(peerHash)!;
		const oldEpoch = bytes(15);
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: oldState.receiverBinding!.slice(),
						senderEpoch: oldEpoch,
						sequence: 1n,
						segments: [],
					}),
					1n,
				)!,
			),
		).to.be.true;
		const oldAdmission = coordinator.reserve(
			new AddedReplicationInfoV2Message({
				receiverChallenge: oldState.receiverBinding!.slice(),
				senderEpoch: oldEpoch,
				sequence: 2n,
				segments: [],
			}),
			{
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 2n,
			},
		)!;

		sendRequest.resetHistory();
		const replacementTransportSession = senderTransportSession + 1n;
		currentSenderTransportSession = replacementTransportSession;
		expect(observeSender(2n, replacementTransportSession)).to.be.true;
		const replacement = coordinator._receiveStates.get(peerHash)!;
		expect(replacement).to.not.equal(oldState);
		expect(replacement.phase).to.equal("resync");
		expect(replacement.requestAttempts).to.equal(0);
		expect(replacement.requestTimer).to.be.undefined;
		await clock.tickAsync(100);
		expect(sendRequest.notCalled).to.be.true;
		expect(replacement.requestAttempts).to.equal(0);

		coordinator.release(oldAdmission);
		expect(replacement.requestTimer).to.exist;
		await clock.tickAsync(2);
		expect(sendRequest.calledOnce).to.be.true;
		const replacementFull = new FullReplicationInfoV2Message({
			receiverChallenge: replacement.receiverBinding!.slice(),
			senderEpoch: bytes(16),
			sequence: 1n,
			segments: [],
		});
		const replacementAdmission = coordinator.reserve(replacementFull, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession: replacementTransportSession,
			transportTimestamp: 3n,
		});
		expect(replacementAdmission).to.exist;
		expect(coordinator.commit(replacementAdmission!)).to.be.true;
		expect(replacement.phase).to.equal("active");
		expect(replacement.lastSequence).to.equal(1n);
	});

	it("backs off to a parked request state and never renews an active stream", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			maxRequestRetryMs: 4,
			requestMaxAttempts: 2,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(10);
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(sendRequest.callCount).to.equal(2);
		expect(state.requestParked).to.be.true;
		expect(state.requestTimer).to.be.undefined;
		await clock.tickAsync(1_000);
		expect(sendRequest.callCount).to.equal(2);

		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(14),
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;
		await clock.tickAsync(1_000);
		expect(sendRequest.callCount).to.equal(2);
	});

	it("resumes only an exact parked request without invalidating live work", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			maxRequestRetryMs: 4,
			requestMaxAttempts: 2,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(10);
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(state.requestParked).to.be.true;
		expect(sendRequest.callCount).to.equal(2);
		const parkedVersion = state.version;
		const parkedAttempts = state.requestAttempts;
		const parkedBinding = state.receiverBinding;

		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: {},
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
		peerStateReady = false;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
		peerStateReady = true;
		expect(state.version).to.equal(parkedVersion);
		expect(state.requestAttempts).to.equal(parkedAttempts);
		expect(state.receiverBinding).to.equal(parkedBinding);
		expect(state.requestParked).to.be.true;
		expect(state.capabilityRefreshRequired).to.be.false;
		expect(state.requestTimer).to.be.undefined;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: {},
			}),
		).to.be.false;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		expect(state.requestParked).to.be.false;
		// The local grant is still current, so the unpark must not rotate it.
		expect(state.capabilityRefreshRequired).to.be.false;
		expect(state.requestTimer).to.exist;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;

		await clock.tickAsync(1);
		expect(refreshLocalCapability.notCalled).to.be.true;
		expect(sendRequest.callCount).to.equal(3);
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(15),
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(full)!)).to.be.true;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
	});

	it("applies a Full bound to the pre-park challenge after an unpark", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			maxRequestRetryMs: 4,
			requestMaxAttempts: 2,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(10);
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(state.requestParked).to.be.true;
		expect(sendRequest.callCount).to.equal(2);
		expect(
			coordinator.isRequestParked({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		const challengeBefore = state.receiverRequestChallenge.slice();
		const bindingBefore = state.receiverBinding!.slice();

		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		expect(
			coordinator.isRequestParked({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
		await clock.tickAsync(1);
		// The unpark reuses the still-current grant: no refresh, same challenge.
		expect(refreshLocalCapability.notCalled).to.be.true;
		expect(sendRequest.callCount).to.equal(3);
		expect([...sendRequest.thirdCall.args[0].receiverChallenge]).to.deep.equal([
			...challengeBefore,
		]);
		expect([...state.receiverRequestChallenge]).to.deep.equal([
			...challengeBefore,
		]);

		// A Full that was already in flight answering the pre-park request
		// generation still applies after the unpark.
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: bindingBefore,
			senderEpoch: bytes(26),
			sequence: 1n,
			segments: [],
		});
		const admission = prepare(full);
		expect(admission).to.exist;
		expect(coordinator.commit(admission!)).to.be.true;
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
	});

	it("requires a capability refresh on unpark only when the grant is stale", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			maxRequestRetryMs: 4,
			requestMaxAttempts: 2,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(10);
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(state.requestParked).to.be.true;
		const challengeBefore = state.receiverRequestChallenge.slice();

		// Expire the local grant while parked: the next unpark must re-handshake.
		coordinator._localCapabilityReadyBySession.delete(currentSession);
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		expect(state.capabilityRefreshRequired).to.be.true;
		await clock.tickAsync(1);
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect([...state.receiverRequestChallenge]).to.not.deep.equal([
			...challengeBefore,
		]);
	});

	it("does not resume a parked request while an authoritative Full is applying", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			maxRequestRetryMs: 4,
			requestMaxAttempts: 2,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(10);
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(state.requestParked).to.be.true;
		expect(sendRequest.callCount).to.equal(2);

		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(16),
			sequence: 1n,
			segments: [],
		});
		const reserved = coordinator.reserve(full, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		expect(reserved).to.exist;
		expect(
			coordinator.resumeParkedRequest({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.false;
		expect(state.requestParked).to.be.true;
		expect(state.capabilityRefreshRequired).to.be.false;
		expect(state.requestTimer).to.be.undefined;
		expect(refreshLocalCapability.notCalled).to.be.true;
		expect(sendRequest.callCount).to.equal(2);

		expect(coordinator.commit(reserved)).to.be.true;
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
		expect(state.requestParked).to.be.false;
		expect(state.capabilityRefreshRequired).to.be.false;
		expect(state.requestTimer).to.be.undefined;
		expect(sendRequest.callCount).to.equal(2);
	});

	it("pauses an armed request timer while an authoritative Full is applying", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({ requestRetryMs: 2 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(state.requestTimer).to.exist;

		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(17),
			sequence: 1n,
			segments: [],
		});
		const first = coordinator.reserve(full, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		expect(state.requestTimer).to.be.undefined;
		await clock.tickAsync(100);
		expect(sendRequest.notCalled).to.be.true;

		coordinator.release(first);
		expect(state.requestTimer).to.exist;
		const committed = coordinator.reserve(full, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		expect(state.requestTimer).to.be.undefined;
		expect(coordinator.commit(committed)).to.be.true;
		await clock.tickAsync(100);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
		expect(sendRequest.notCalled).to.be.true;
	});

	it("does not let an in-flight capability refresh invalidate an applying Full", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		const pendingRefresh = pDefer<{
			receiverTransportSession: bigint;
			requestNotBeforeMs: number;
		}>();
		refreshLocalCapability.callsFake(() => pendingRefresh.promise);
		coordinator = createCoordinator({ requestRetryMs: 2 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		state.capabilityRefreshRequired = true;
		await clock.tickAsync(0);
		expect(refreshLocalCapability.calledOnce).to.be.true;
		expect(state.requestInFlight).to.exist;
		const version = state.version;
		const receiverChallenge = state.receiverRequestChallenge;
		const receiverBinding = state.receiverBinding;

		const full = new FullReplicationInfoV2Message({
			receiverChallenge: receiverBinding!.slice(),
			senderEpoch: bytes(18),
			sequence: 1n,
			segments: [],
		});
		const admission = coordinator.reserve(full, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		const request = state.requestInFlight!;
		pendingRefresh.resolve({
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		});
		await request;

		expect(state.version).to.equal(version);
		expect(state.receiverRequestChallenge).to.equal(receiverChallenge);
		expect(state.receiverBinding).to.equal(receiverBinding);
		expect(state.requestTimer).to.be.undefined;
		expect(sendRequest.notCalled).to.be.true;
		expect(coordinator.commit(admission)).to.be.true;
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
	});

	it("treats MAX_U64 as terminal for the current receiver grant", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const terminal = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(15),
			sequence: (1n << 64n) - 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(terminal)!)).to.be.true;
		sendRequest.resetHistory();
		currentReceiveEpoch = {};
		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		await clock.tickAsync(1_000);
		expect(sendRequest.notCalled).to.be.true;
		expect(state.requestTimer).to.be.undefined;
	});

	it("aborts an in-flight request without re-arming on clear", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		const pending = pDefer<void>();
		let requestSignal: AbortSignal | undefined;
		sendRequest = sinon.stub().callsFake((_request, _target, signal) => {
			requestSignal = signal;
			return pending.promise;
		});
		coordinator = createCoordinator({ requestRetryMs: 2 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		await clock.tickAsync(0);
		expect(sendRequest.calledOnce).to.be.true;
		coordinator.clearPeer(peerHash, currentSession);
		expect(requestSignal?.aborted).to.be.true;
		pending.resolve();
		await clock.tickAsync(1_000);
		expect(sendRequest.calledOnce).to.be.true;
		expect(coordinator._receiveStates.has(peerHash)).to.be.false;
	});

	it("re-handshakes after a B9 sender clears its same-session grant", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		const senderPeerSession = {};
		const ownershipController = new AbortController();
		let capabilityTimestamp = 1_000n;
		const delivered: FullReplicationInfoV2Message[] = [];
		let senderCoordinator: ReplicationInfoV2SendCoordinator<"u32">;

		const senderRpcSend = sinon.stub().callsFake(async (message) => {
			if (
				message instanceof FullReplicationInfoV2Message ||
				message instanceof AddedReplicationInfoV2Message
			) {
				if (message instanceof FullReplicationInfoV2Message) {
					delivered.push(message);
				}
				const admission = coordinator.prepare(message, {
					from: sender,
					peerSession: currentSession,
					receiveEpoch: currentReceiveEpoch,
					senderTransportSession,
					transportTimestamp: message.sequence,
				});
				if (admission) {
					expect(coordinator.commit(admission)).to.be.true;
				}
			}
			return [];
		});
		senderCoordinator = new ReplicationInfoV2SendCoordinator<"u32">({
			getRpc: () => ({ send: senderRpcSend }) as any,
			getSelfKey: () => sender,
			getSenderTransportSession: () => senderTransportSession,
			getMyReplicationSegments: async () => [],
			validatePersistedReplicationRangeSnapshot: () => {},
			isClosed: () => false,
			isPeerSessionOpen: (_hash, peerSession) =>
				peerSession === senderPeerSession,
			isPeerSessionCurrent: (_hash, peerSession) =>
				peerSession === senderPeerSession,
			captureReplicationOwnershipLifecycle: () => ownershipController,
			isReplicationOwnershipLifecycleActive: (controller) =>
				controller === ownershipController && !controller.signal.aborted,
		});

		sendRequest = sinon.stub().callsFake(async (request) => {
			expect(
				senderCoordinator.acceptRequest(request, {
					from: self,
					peerSession: senderPeerSession,
					receiverTransportSession,
					capabilityTimestamp,
					requestTimestamp: BigInt(Date.now()),
				}),
			).to.be.true;
		});
		refreshLocalCapability = sinon.stub().callsFake(async () => {
			capabilityTimestamp = BigInt(Date.now());
			// B9 treated every newer capability timestamp as a sender-generation
			// advance and cleared the per-peer stream immediately.
			senderCoordinator.advancePeerCapability(self.hashcode());
			return {
				receiverTransportSession,
				requestNotBeforeMs: Date.now(),
			};
		});
		coordinator = createCoordinator({ requestRetryMs: 2 });
		expect(markLocalReady(1_000)).to.be.true;
		expect(observeSender(capabilityTimestamp)).to.be.true;
		await clock.tickAsync(1);
		await senderCoordinator.drain();
		expect(coordinator._receiveStates.get(peerHash)?.phase).to.equal("active");
		const state = coordinator._receiveStates.get(peerHash)!;
		const oldBinding = state.receiverBinding!.slice();
		const oldFull = delivered[0];

		senderCoordinator.enqueue({ added: { segments: [] } });
		await senderCoordinator.drain();
		expect(state.lastSequence).to.equal(2n);
		senderCoordinator.advancePeerCapability(self.hashcode());
		currentReceiveEpoch = {};
		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		await clock.tickAsync(10);
		await senderCoordinator.drain();
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
		expect([...state.receiverBinding!]).not.to.deep.equal([...oldBinding]);
		expect(coordinator._cutoverPeerSessions.has(currentSession)).to.be.true;
		expect(
			coordinator.prepare(oldFull, {
				from: sender,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 1n,
			}),
		).to.be.undefined;
		senderCoordinator.clearForClose();
	});
});

describe("receive admission replication-info V2 receiver integration", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	it("completes an actual signed two-node capability and request handshake", async () => {
		session = await TestSession.connected(2);
		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: 1, timeUntilRoleMaturity: 0 },
			},
		);
		const receiver = db1.log as any;
		const senderLog = db2.log as any;
		const senderHash = session.peers[1].identity.publicKey.hashcode();
		const receiverHash = session.peers[0].identity.publicKey.hashcode();

		await waitForResolved(
			() => {
				const receiveState = receiver._v2Receive._receiveStates.get(senderHash);
				const sendState = senderLog._v2Send._sendStates.get(receiverHash);
				const capabilityTimestamp =
					senderLog._peerSyncCapabilityTimestamps.get(receiverHash);
				expect(receiveState?.phase).to.equal(
					"active",
					JSON.stringify({
						receive: receiveState && {
							phase: receiveState.phase,
							attempts: receiveState.requestAttempts,
							parked: receiveState.requestParked,
							senderTransportSession:
								receiveState.senderTransportSession?.toString(),
						},
						sender: sendState && {
							established: sendState.established,
							lastRequestTimestamp: sendState.lastRequestTimestamp?.toString(),
						},
						capabilityTimestamp: capabilityTimestamp?.toString(),
					}),
				);
				expect(receiveState?.lastSequence > 0n).to.be.true;
				expect(sendState?.established).to.be.true;
				expect(capabilityTimestamp).to.not.equal(undefined);
				expect(sendState.lastRequestTimestamp > capabilityTimestamp).to.be.true;
				expect(
					receiver._v2Receive._cutoverPeerSessions.has(
						receiver._peerSessions.current(senderHash),
					),
				).to.be.true;
			},
			{ timeout: 20_000 },
		);
		expect(
			receiver._replicationInfoRequestByPeer.get(senderHash)?.peerSession,
		).to.equal(receiver._peerSessions.current(senderHash));
	});

	it("recovers first capability-advert failures for both exact peer sessions", async () => {
		session = await TestSession.disconnected(2);
		const store1 = new EventStore();
		const store2 = new EventStore({ id: store1.id });
		const logs = [store1.log as any, store2.log as any];
		const adverts: sinon.SinonStub[] = [];
		for (const log of logs) {
			const original =
				log.advertiseReplicationInfoV2ReceiveCapability.bind(log);
			let first = true;
			adverts.push(
				sinon
					.stub(log, "advertiseReplicationInfoV2ReceiveCapability")
					.callsFake(async (...args: unknown[]) => {
						if (first) {
							first = false;
							throw new Error("first capability advert failed");
						}
						return original(...args);
					}),
			);
		}
		const db1 = await session.peers[0].open(store1, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const db2 = await session.peers[1].open(store2, {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.connect([[session.peers[0], session.peers[1]]]);
		const log1 = db1.log as any;
		const log2 = db2.log as any;
		const hash1 = session.peers[0].identity.publicKey.hashcode();
		const hash2 = session.peers[1].identity.publicKey.hashcode();

		await waitForResolved(
			() => {
				expect(log1._v2Receive._receiveStates.get(hash2)?.phase).to.equal(
					"active",
				);
				expect(log2._v2Receive._receiveStates.get(hash1)?.phase).to.equal(
					"active",
				);
				expect(adverts[0].callCount).to.be.greaterThanOrEqual(2);
				expect(adverts[1].callCount).to.be.greaterThanOrEqual(2);
				const advert1 =
					log1._v2Receive._localCapabilityAdvertisementsByPeer.get(hash2);
				const advert2 =
					log2._v2Receive._localCapabilityAdvertisementsByPeer.get(hash1);
				expect(advert1?.ready).to.be.true;
				expect(advert2?.ready).to.be.true;
				expect(advert1?.peerSession).to.equal(
					log1._peerSessions.current(hash2),
				);
				expect(advert2?.peerSession).to.equal(
					log2._peerSessions.current(hash1),
				);
			},
			{ timeout: 20_000 },
		);
	});

	it("restores a rejected V2 delta with a Full", async () => {
		session = await TestSession.connected(2);
		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: 1, timeUntilRoleMaturity: 0 },
			},
		);
		const receiver = db1.log as any;
		const senderLog = db2.log as any;
		const senderHash = session.peers[1].identity.publicKey.hashcode();
		const receiverHash = session.peers[0].identity.publicKey.hashcode();
		await waitForResolved(
			() => {
				expect(
					receiver._v2Receive._receiveStates.get(senderHash)?.phase,
				).to.equal("active");
				expect(senderLog._v2Send._sendStates.get(receiverHash)?.established).to
					.be.true;
			},
			{ timeout: 20_000 },
		);

		const range = new ReplicationRangeMessageU64({
			id: bytes(42),
			offset: 10n,
			factor: 10n,
			timestamp: 10n,
			mode: ReplicationIntent.NonStrict,
		});
		const indexedRange = range.toReplicationRangeIndexable(
			session.peers[1].identity.publicKey,
		);
		const getSegments = sinon
			.stub(senderLog, "getMyReplicationSegments")
			.resolves([indexedRange]);
		const originalSend = senderLog.rpc.send.bind(senderLog.rpc);
		let rejectedV2Delta = 0;
		let legacyEgress = 0;
		let recoveryFull: FullReplicationInfoV2Message | undefined;
		const send = sinon
			.stub(senderLog.rpc, "send")
			.callsFake(async (message: unknown, options: unknown) => {
				if (message instanceof AddedReplicationSegmentMessage) {
					// Default mode must never publish the legacy announcement class.
					legacyEgress++;
					return [];
				}
				if (
					message instanceof AddedReplicationInfoV2Message &&
					rejectedV2Delta === 0
				) {
					rejectedV2Delta++;
					throw new Error("ambiguous V2 delta delivery");
				}
				if (message instanceof FullReplicationInfoV2Message) {
					recoveryFull = message;
				}
				return originalSend(message, options);
			});
		(senderLog._v2Send as any).sendRetryMs = 25;
		(senderLog._v2Send as any).maxSendRetryMs = 50;
		try {
			await senderLog._announcements.sendReplicationAnnouncement({
				added: { segments: [range] },
			});
			await senderLog._v2Send.drain();
			const failedState = senderLog._v2Send._sendStates.get(receiverHash);
			expect(rejectedV2Delta).to.equal(1);
			expect(legacyEgress).to.equal(0);
			expect(failedState?.suspended).to.be.true;
			const beforeRecovery = await receiver.replicationIndex
				.iterate({ query: { hash: senderHash } })
				.all();
			expect(
				beforeRecovery.some(
					(result: any) => result.value.idString === indexedRange.idString,
				),
			).to.be.false;

			await waitForResolved(
				async () => {
					const ranges = await receiver.replicationIndex
						.iterate({ query: { hash: senderHash } })
						.all();
					expect(
						ranges.map((result: any) => result.value.idString),
					).to.deep.equal([indexedRange.idString]);
					expect(
						receiver._v2Receive._receiveStates.get(senderHash)?.phase,
					).to.equal("active");
				},
				{ timeout: 10_000 },
			);
			expect(recoveryFull).to.exist;
			expect(recoveryFull!.sequence).to.equal(failedState!.nextSequence - 1n);
			expect(getSegments.called).to.be.true;
		} finally {
			send.restore();
			getSegments.restore();
		}
	});

	it("rejects transport-generation downgrade replay on the signed host path", async () => {
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
		const receiveEpoch = log._peerSessions.receiveEpoch(remoteHash);
		const receiverTransportSession = BigInt(log.node.services.pubsub.session);
		const senderSessionA = 4_001n;
		const senderSessionB = 4_002n;
		const context = (transportSession: bigint, timestamp: bigint) =>
			({
				from: remote,
				message: { header: { session: transportSession, timestamp } },
			}) as any;

		expect(
			log._v2Receive.markLocalCapabilityReady({
				peerHash: remoteHash,
				peerSession,
				receiveEpoch,
				receiverTransportSession,
				requestNotBeforeMs: Date.now(),
			}),
		).to.be.true;
		await log.onMessage(
			new SyncCapabilitiesMessage({ capabilities: senderCapabilities }),
			context(senderSessionA, 10n),
		);
		const state = log._v2Receive._receiveStates.get(remoteHash)!;
		clearTimeout(state.requestTimer);
		state.requestTimer = undefined;
		const full = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch: bytes(16),
			sequence: 1n,
			segments: [],
		});
		await log.onMessage(full, context(senderSessionA, 11n));
		expect(log._v2Receive._cutoverPeerSessions.has(peerSession)).to.be.true;

		await log.onMessage(
			new SyncCapabilitiesMessage({
				capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			}),
			context(senderSessionB, 20n),
		);
		expect(state.controller.signal.aborted).to.be.true;
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;
		expect(log._v2Receive._cutoverPeerSessions.has(peerSession)).to.be.false;
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			senderSessionB,
		);

		await log.onMessage(
			new SyncCapabilitiesMessage({ capabilities: senderCapabilities }),
			context(senderSessionA, 10n),
		);
		expect(log._peerSyncCapabilitySessions.get(remoteHash)).to.equal(
			senderSessionB,
		);
		await log.onMessage(full, context(senderSessionA, 30n));
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;
	});

	it("commits first-Full cutover before fallible local bookkeeping", async () => {
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
		const receiveEpoch = log._peerSessions.receiveEpoch(remoteHash);
		const receiverTransportSession = BigInt(log.node.services.pubsub.session);
		const senderTransportSession = 4_101n;
		const send = sinon.stub(log.rpc, "send").resolves([] as any);
		log._peerSyncCapabilities.set(remoteHash, senderCapabilities);
		log._peerSyncCapabilitySessions.set(remoteHash, senderTransportSession);
		log._peerSyncCapabilityTimestamps.set(remoteHash, 1n);
		expect(
			log._v2Receive.markLocalCapabilityReady({
				peerHash: remoteHash,
				peerSession,
				receiveEpoch,
				receiverTransportSession,
				requestNotBeforeMs: 0,
			}),
		).to.be.true;
		expect(
			log._v2Receive.observeCapability({
				peerHash: remoteHash,
				target: remote,
				peerSession,
				receiveEpoch,
				capabilities: senderCapabilities,
				senderTransportSession,
				capabilityTimestamp: 1n,
			}),
		).to.be.true;
		const state = log._v2Receive._receiveStates.get(remoteHash)!;
		clearTimeout(state.requestTimer);
		state.requestTimer = undefined;
		const senderEpoch = bytes(18);
		const range = new ReplicationRangeMessageU64({
			id: bytes(19),
			offset: 10n,
			factor: 10n,
			timestamp: 10n,
			mode: ReplicationIntent.NonStrict,
		});
		const context = {
			from: remote,
			message: {
				header: { session: senderTransportSession, timestamp: 100n },
			},
		} as any;
		const bookkeepingFailure = sinon
			.stub(log.replicationChangeDebounceFn, "add")
			.throws(new Error("post-durable bookkeeping failed"));
		try {
			await log.onMessage(
				new FullReplicationInfoV2Message({
					receiverChallenge: state.receiverBinding!.slice(),
					senderEpoch,
					sequence: 1n,
					segments: [range],
				}),
				context,
			);
		} finally {
			bookkeepingFailure.restore();
		}

		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
		expect(state.lastSequence).to.equal(1n);
		expect(state.phase).to.equal("resync");
		expect(log._v2Receive._cutoverPeerSessions.has(peerSession)).to.be.true;
		await waitForResolved(() => expect(send.called).to.be.true);

		await log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 2n,
				segments: [range],
			}),
			context,
		);
		expect(state.lastSequence).to.equal(2n);
		expect(state.phase).to.equal("active");
	});

	it("rolls back stale Full and Stopped deletions after transport revocation", async () => {
		session = await TestSession.disconnected(2);
		for (const [caseIndex, kind] of ["full", "stopped"].entries()) {
			const db = await session.peers[0].open(new EventStore(), {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const log = db.log as any;
			const remote = session.peers[1].identity.publicKey;
			const remoteHash = remote.hashcode();
			const peerSession = log._peerSessions.rotate(remoteHash, "opening");
			log._peerSessions.unblockReplicationInfo(remoteHash);
			log._peerSessions.markOpen(remoteHash, peerSession);
			const receiveEpoch = log._peerSessions.receiveEpoch(remoteHash);
			const receiverTransportSession = BigInt(log.node.services.pubsub.session);
			const senderTransportSession = 4_200n + BigInt(caseIndex);
			log._peerSyncCapabilities.set(remoteHash, senderCapabilities);
			log._peerSyncCapabilitySessions.set(remoteHash, senderTransportSession);
			log._peerSyncCapabilityTimestamps.set(remoteHash, 1n);
			expect(
				log._v2Receive.markLocalCapabilityReady({
					peerHash: remoteHash,
					peerSession,
					receiveEpoch,
					receiverTransportSession,
					requestNotBeforeMs: 0,
				}),
			).to.be.true;
			expect(
				log._v2Receive.observeCapability({
					peerHash: remoteHash,
					target: remote,
					peerSession,
					receiveEpoch,
					capabilities: senderCapabilities,
					senderTransportSession,
					capabilityTimestamp: 1n,
				}),
			).to.be.true;
			const state = log._v2Receive._receiveStates.get(remoteHash)!;
			clearTimeout(state.requestTimer);
			state.requestTimer = undefined;
			const senderEpoch = bytes(25 + caseIndex);
			const range = new ReplicationRangeMessageU64({
				id: bytes(27 + caseIndex),
				offset: 10n,
				factor: 10n,
				timestamp: 10n,
				mode: ReplicationIntent.NonStrict,
			});
			const context = (transportSession: bigint, timestamp: bigint) =>
				({
					from: remote,
					message: { header: { session: transportSession, timestamp } },
				}) as any;
			await log.onMessage(
				new FullReplicationInfoV2Message({
					receiverChallenge: state.receiverBinding!.slice(),
					senderEpoch,
					sequence: 1n,
					segments: [range],
				}),
				context(senderTransportSession, 10n),
			);
			expect(
				await log.replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(1);

			const deleteCommitted = pDefer<void>();
			const releaseDelete = pDefer<void>();
			const originalDelete = log.replicationIndex.del.bind(
				log.replicationIndex,
			);
			const durableDelete = sinon
				.stub(log.replicationIndex, "del")
				.callsFake(async (...args: unknown[]) => {
					const result = await originalDelete(...args);
					deleteCommitted.resolve();
					await releaseDelete.promise;
					return result;
				});
			const nativeRestore = sinon.spy(log, "putNativeReplicationRange");
			const activity = sinon.spy(log._liveness, "markReplicatorActivity");
			let changes = 0;
			const onChange = () => {
				changes++;
			};
			log.events.addEventListener("replication:change", onChange);
			const staleMessage =
				kind === "full"
					? new FullReplicationInfoV2Message({
							receiverChallenge: state.receiverBinding!.slice(),
							senderEpoch,
							sequence: 2n,
							segments: [],
						})
					: new StoppedReplicationInfoV2Message({
							receiverChallenge: state.receiverBinding!.slice(),
							senderEpoch,
							sequence: 2n,
							segmentIds: [range.id],
						});
			try {
				const stale = log.onMessage(
					staleMessage,
					context(senderTransportSession, 20n),
				);
				await deleteCommitted.promise;
				await log.onMessage(
					new SyncCapabilitiesMessage({
						capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
					}),
					context(senderTransportSession + 100n, 30n),
				);
				activity.resetHistory();
				releaseDelete.resolve();
				await stale;
			} finally {
				releaseDelete.resolve();
				log.events.removeEventListener("replication:change", onChange);
				durableDelete.restore();
			}
			expect(state.controller.signal.aborted).to.be.true;
			expect(state.lastSequence).to.equal(1n);
			expect(
				await log.replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(1);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.true;
			expect(nativeRestore.called).to.be.true;
			expect(activity.notCalled).to.be.true;
			expect(changes).to.equal(0);
		}
	});

	it("applies sequence order and recovers a gap with Full", async () => {
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
		const receiveEpoch = log._peerSessions.receiveEpoch(remoteHash);
		const localTransportSession = BigInt(log.node.services.pubsub.session);
		const remoteTransportSession = 0x20000000000003n;
		const send = sinon.stub(log.rpc, "send").resolves([] as any);
		const activity = sinon.spy(log._liveness, "markReplicatorActivity");
		log._peerSyncCapabilities.set(remoteHash, senderCapabilities);
		log._peerSyncCapabilitySessions.set(remoteHash, remoteTransportSession);
		log._peerSyncCapabilityTimestamps.set(remoteHash, 1n);

		expect(
			log._v2Receive.markLocalCapabilityReady({
				peerHash: remoteHash,
				peerSession,
				receiveEpoch,
				receiverTransportSession: localTransportSession,
				requestNotBeforeMs: Date.now(),
			}),
		).to.be.true;
		expect(
			log._v2Receive.observeCapability({
				peerHash: remoteHash,
				target: remote,
				peerSession,
				receiveEpoch,
				capabilities: senderCapabilities,
				senderTransportSession: remoteTransportSession,
				capabilityTimestamp: 1n,
			}),
		).to.be.true;
		const state = log._v2Receive._receiveStates.get(remoteHash)!;
		clearTimeout(state.requestTimer);
		state.requestTimer = undefined;
		const localReady =
			log._v2Receive._localCapabilityReadyBySession.get(peerSession);
		localReady.requestNotBeforeMs = 0;
		const senderEpoch = bytes(7);
		const rangeA = new ReplicationRangeMessageU64({
			id: bytes(8),
			offset: 10n,
			factor: 10n,
			timestamp: 10n,
			mode: ReplicationIntent.NonStrict,
		});
		const rangeB = new ReplicationRangeMessageU64({
			id: bytes(9),
			offset: 30n,
			factor: 10n,
			timestamp: 11n,
			mode: ReplicationIntent.NonStrict,
		});
		const rangeC = new ReplicationRangeMessageU64({
			id: bytes(10),
			offset: 50n,
			factor: 10n,
			timestamp: 12n,
			mode: ReplicationIntent.NonStrict,
		});
		const context = (timestamp: bigint) =>
			({
				from: remote,
				message: {
					header: { session: remoteTransportSession, timestamp },
				},
			}) as any;

		await log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 1n,
				segments: [rangeA],
			}),
			context(100n),
		);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
		expect(activity.calledOnceWith(remoteHash)).to.be.true;

		activity.resetHistory();
		await log.onMessage(
			new AddedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 2n,
				segments: [rangeB],
			}),
			context(1n),
		);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(2);
		expect(activity.calledOnceWith(remoteHash)).to.be.true;

		activity.resetHistory();
		await log.onMessage(
			new StoppedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 3n,
				segmentIds: [bytes(99)],
			}),
			context(201n),
		);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(3n);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(2);

		await log.onMessage(
			new AddedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 4n,
				segments: [rangeC],
			}),
			context(202n),
		);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(4n);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(3);

		activity.resetHistory();
		await log.onMessage(
			new StoppedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 6n,
				segmentIds: [rangeA.id],
			}),
			context(0n),
		);
		expect(state.phase).to.equal("resync");
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(3);
		expect(activity.notCalled).to.be.true;
		await waitForResolved(() => expect(send.calledOnce).to.be.true);
		expect([...send.firstCall.args[0].receiverChallenge]).to.deep.equal([
			...state.receiverRequestChallenge,
		]);

		send.resetHistory();
		await log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 7n,
				segments: [rangeB],
			}),
			context(0n),
		);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(7n);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);

		activity.resetHistory();
		await log.onMessage(
			new AddedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 3n,
				segments: [rangeA],
			}),
			context(10_000n),
		);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
		expect(activity.notCalled).to.be.true;

		const parkedLane = pDefer<void>();
		let laneEntered = false;
		const blocker = log.withReplicationInfoApplyQueue(remoteHash, async () => {
			laneEntered = true;
			await parkedLane.promise;
		});
		await waitForResolved(() => expect(laneEntered).to.be.true);
		activity.resetHistory();
		const parkedDelta = log.onMessage(
			new AddedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 8n,
				segments: [rangeA],
			}),
			context(20_000n),
		);
		await Promise.resolve();
		const challengeBeforeRecovery = state.receiverRequestChallenge.slice();
		log.advanceReplicationInfoRecoveryEpoch(remoteHash);
		parkedLane.resolve();
		await blocker;
		await parkedDelta;

		expect(state.phase).to.equal("resync");
		expect(state.lastSequence).to.equal(7n);
		expect([...state.receiverRequestChallenge]).to.deep.equal([
			...challengeBeforeRecovery,
		]);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
		expect(activity.notCalled).to.be.true;

		await log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 9n,
				segments: [rangeB],
			}),
			context(30_000n),
		);
		expect(state.phase).to.equal("active");

		const removalDebounce = sinon.spy(log.replicationChangeDebounceFn, "add");
		const stoppedDeleteCommitted = pDefer<void>();
		const releaseStoppedDelete = pDefer<void>();
		const originalDelete = log.replicationIndex.del.bind(log.replicationIndex);
		const stoppedDelete = sinon
			.stub(log.replicationIndex, "del")
			.callsFake(async (...args: unknown[]) => {
				const result = await originalDelete(...args);
				stoppedDeleteCommitted.resolve();
				await releaseStoppedDelete.promise;
				return result;
			});
		let overlappingStopped: Promise<void> | undefined;
		try {
			overlappingStopped = log.onMessage(
				new StoppedReplicationInfoV2Message({
					receiverChallenge: state.receiverBinding!.slice(),
					senderEpoch,
					sequence: 10n,
					segmentIds: [rangeB.id],
				}),
				context(31_000n),
			);
			await stoppedDeleteCommitted.promise;
			const stoppedAdmission = state.reservedAdmission;
			expect(stoppedAdmission).to.exist;
			await log.onMessage(
				new AddedReplicationInfoV2Message({
					receiverChallenge: state.receiverBinding!.slice(),
					senderEpoch,
					sequence: 11n,
					segments: [rangeA],
				}),
				context(32_000n),
			);
			expect(stoppedAdmission.resyncAfterRelease).to.be.true;
			releaseStoppedDelete.resolve();
			await overlappingStopped;
		} finally {
			releaseStoppedDelete.resolve();
			await overlappingStopped?.catch(() => {});
			stoppedDelete.restore();
		}
		expect(state.lastSequence).to.equal(10n);
		expect(state.phase).to.equal("resync");
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(0);
		const removedChanges = removalDebounce
			.getCalls()
			.map((call) => call.args[0])
			.filter((change) => change.type === "removed");
		expect(removedChanges).to.have.length(1);
		expect(removedChanges[0].range.idString).to.equal(
			rangeB.toReplicationRangeIndexable(remote).idString,
		);
		removalDebounce.restore();

		await log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 12n,
				segments: [rangeB],
			}),
			context(33_000n),
		);
		expect(state.phase).to.equal("active");

		const putCommitted = pDefer<void>();
		const releasePut = pDefer<void>();
		const originalPut = log.replicationIndex.put.bind(log.replicationIndex);
		const put = sinon
			.stub(log.replicationIndex, "put")
			.callsFake(async (range: any) => {
				const result = await originalPut(range);
				if (
					range.idString === rangeA.toReplicationRangeIndexable(remote).idString
				) {
					putCommitted.resolve();
					await releasePut.promise;
				}
				return result;
			});
		const staleDuringDurablePut = log.onMessage(
			new AddedReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 13n,
				segments: [rangeA],
			}),
			context(40_000n),
		);
		await putCommitted.promise;
		await log.onMessage(
			new SyncCapabilitiesMessage({
				capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			}),
			{
				from: remote,
				message: {
					header: {
						session: remoteTransportSession + 1n,
						timestamp: 50_000n,
					},
				},
			} as any,
		);
		releasePut.resolve();
		await staleDuringDurablePut;
		put.restore();
		expect(state.controller.signal.aborted).to.be.true;
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
	});
});
