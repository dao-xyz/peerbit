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
	ReplicationRangeMessageU64,
} from "../src/ranges.js";
import { deriveReplicationInfoV2ReceiverBinding } from "../src/replication-info-v2-binding.js";
import { ReplicationInfoV2ReceiveCoordinator } from "../src/replication-info-v2-receive.js";
import { ReplicationInfoV2SendCoordinator } from "../src/replication-info-v2-send.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
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
	let sendRequest: sinon.SinonStub;
	let refreshLocalCapability: sinon.SinonStub;
	let coordinator: ReplicationInfoV2ReceiveCoordinator;

	const createCoordinator = (options?: {
		requestRetryMs?: number;
		maxRequestRetryMs?: number;
		requestMaxAttempts?: number;
		legacyFallbackDelayMs?: number;
	}) =>
		new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => self,
			getReceiverTransportSession: () => receiverTransportSession,
			isClosed: () => closed,
			isPeerStateCurrent: (_peerHash, peerSession, receiveEpoch) =>
				peerSession === currentSession && receiveEpoch === currentReceiveEpoch,
			isSenderTransportSessionCurrent: (_peerHash, transportSession) =>
				transportSession === currentSenderTransportSession,
			sendRequest,
			refreshLocalCapability,
			requestRetryMs: options?.requestRetryMs ?? 5,
			maxRequestRetryMs: options?.maxRequestRetryMs ?? 20,
			requestMaxAttempts: options?.requestMaxAttempts ?? 7,
			legacyFallbackDelayMs: options?.legacyFallbackDelayMs ?? 10,
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
		expect(coordinator.isLegacyCutover(currentSession)).to.be.true;

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

	it("fences a parked delta across same-session recovery without reopening legacy", () => {
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
		expect(coordinator.isLegacyCutover(currentSession)).to.be.true;
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
		expect(coordinator.isLegacyCutover(currentSession)).to.be.false;
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
		expect(coordinator.isLegacyCutover(currentSession)).to.be.true;
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

	it("fences legacy recovery evidence to the exact receive generation", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch: bytes(17),
						sequence: 1n,
						segments: [],
					}),
					10n,
				)!,
			),
		).to.be.true;
		const legacy = new AddedReplicationSegmentMessage({ segments: [] });
		expect(
			coordinator.noteLegacyAnnouncement({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession: senderTransportSession + 1n,
				transportTimestamp: 100n,
				message: legacy,
			}),
		).to.be.false;
		expect(
			coordinator.noteLegacyAnnouncement({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: {},
				senderTransportSession,
				transportTimestamp: 101n,
				message: legacy,
			}),
		).to.be.false;
		expect(state.phase).to.equal("active");
		expect(state.legacyFallbackTimer).to.be.undefined;
	});

	it("lets one fresh legacy observation restart parked recovery only once", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 1,
			requestMaxAttempts: 1,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch: bytes(18),
						sequence: 1n,
						segments: [],
					}),
					1n,
				)!,
			),
		).to.be.true;
		currentReceiveEpoch = {};
		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		await clock.tickAsync(1);
		expect(state.requestParked).to.be.true;

		const legacy = new AddedReplicationSegmentMessage({ segments: [] });
		const observeLegacy = (transportTimestamp: bigint) =>
			coordinator.noteLegacyAnnouncement({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp,
				message: legacy,
			});
		expect(observeLegacy(2n)).to.be.true;
		expect(state.requestParked).to.be.false;
		await clock.tickAsync(1);
		expect(state.requestParked).to.be.true;
		const refreshesAfterFreshEvidence = refreshLocalCapability.callCount;
		for (let replay = 0; replay < 100; replay++) {
			expect(observeLegacy(2n)).to.be.true;
		}
		expect(state.requestParked).to.be.true;
		expect(state.requestTimer).to.be.undefined;
		expect(refreshLocalCapability.callCount).to.equal(
			refreshesAfterFreshEvidence,
		);
		expect(observeLegacy(3n)).to.be.true;
		expect(state.requestParked).to.be.false;
		expect(state.requestTimer).to.exist;
	});

	it("uses strict transport time and payload evidence for legacy coverage", () => {
		expect(markLocalReady()).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(19);
		const rangeA = new ReplicationRangeMessageU64({
			id: bytes(30),
			offset: 1n,
			factor: 1n,
			timestamp: 1n,
			mode: ReplicationIntent.NonStrict,
		});
		const rangeB = new ReplicationRangeMessageU64({
			id: bytes(31),
			offset: 2n,
			factor: 1n,
			timestamp: 2n,
			mode: ReplicationIntent.NonStrict,
		});
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 1n,
						segments: [],
					}),
					1n,
				)!,
			),
		).to.be.true;
		expect(
			coordinator.commit(
				prepare(
					new AddedReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 2n,
						segments: [rangeA],
					}),
					10n,
				)!,
			),
		).to.be.true;
		expect(
			coordinator.commit(
				prepare(
					new AddedReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 3n,
						segments: [rangeB],
					}),
					20n,
				)!,
			),
		).to.be.true;
		const note = (
			message: AddedReplicationSegmentMessage,
			transportTimestamp: bigint,
		) =>
			coordinator.noteLegacyAnnouncement({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp,
				message,
			});
		expect(
			note(new AddedReplicationSegmentMessage({ segments: [rangeA] }), 10n),
		).to.be.true;
		expect(state.legacyFallbackTimer).to.be.undefined;

		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 4n,
						segments: [rangeA, rangeB],
					}),
					30n,
				)!,
			),
		).to.be.true;
		expect(
			note(new AddedReplicationSegmentMessage({ segments: [rangeB] }), 29n),
		).to.be.true;
		expect(state.legacyFallbackTimer).to.be.undefined;
		expect(
			note(new AddedReplicationSegmentMessage({ segments: [rangeA] }), 30n),
		).to.be.true;
		expect(state.legacyFallbackTimer).to.exist;

		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 5n,
						segments: [rangeA, rangeB],
					}),
					31n,
				)!,
			),
		).to.be.true;
		expect(state.legacyFallbackTimer).to.be.undefined;
	});

	it("keeps unmatched legacy fallback armed across unrelated V2 work", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(20);
		const initial = new FullReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 1n,
			segments: [],
		});
		expect(coordinator.commit(prepare(initial)!)).to.be.true;
		const rangeA = new ReplicationRangeMessageU64({
			id: bytes(21),
			offset: 1n,
			factor: 1n,
			timestamp: 1n,
			mode: ReplicationIntent.NonStrict,
		});
		const rangeB = new ReplicationRangeMessageU64({
			id: bytes(22),
			offset: 2n,
			factor: 1n,
			timestamp: 2n,
			mode: ReplicationIntent.NonStrict,
		});
		expect(
			coordinator.noteLegacyAnnouncement({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
				senderTransportSession,
				transportTimestamp: 2n,
				message: new AddedReplicationSegmentMessage({ segments: [rangeB] }),
			}),
		).to.be.true;
		const unrelated = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [rangeA],
		});
		expect(coordinator.commit(prepare(unrelated)!)).to.be.true;
		expect(state.legacyFallbackTimer).to.exist;
		await clock.tickAsync(10);
		expect(state.phase).to.equal("resync");
	});

	it("cancels legacy fallback only after a matching commit", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(23);
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 1n,
						segments: [],
					}),
				)!,
			),
		).to.be.true;
		const range = new ReplicationRangeMessageU64({
			id: bytes(24),
			offset: 3n,
			factor: 1n,
			timestamp: 3n,
			mode: ReplicationIntent.NonStrict,
		});
		const legacy = new AddedReplicationSegmentMessage({ segments: [range] });
		coordinator.noteLegacyAnnouncement({
			peerHash,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
			message: legacy,
		});
		const matching = new AddedReplicationInfoV2Message({
			receiverChallenge: state.receiverBinding!.slice(),
			senderEpoch,
			sequence: 2n,
			segments: [range],
		});
		const reserved = coordinator.reserve(matching, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		expect(state.legacyFallbackTimer).to.exist;
		coordinator.release(reserved);
		expect(state.legacyFallbackTimer).to.exist;
		const committed = coordinator.reserve(matching, {
			from: sender,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
		})!;
		expect(coordinator.commit(committed)).to.be.true;
		expect(state.legacyFallbackTimer).to.be.undefined;
		await clock.tickAsync(100);
		expect(state.phase).to.equal("active");
	});

	it("restarts parked recovery on fresh unmatched legacy evidence", async () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		coordinator = createCoordinator({
			requestRetryMs: 2,
			requestMaxAttempts: 1,
		});
		expect(markLocalReady(999)).to.be.true;
		expect(observeSender()).to.be.true;
		const state = coordinator._receiveStates.get(peerHash)!;
		const senderEpoch = bytes(25);
		expect(
			coordinator.commit(
				prepare(
					new FullReplicationInfoV2Message({
						receiverChallenge: state.receiverBinding!.slice(),
						senderEpoch,
						sequence: 1n,
						segments: [],
					}),
				)!,
			),
		).to.be.true;
		currentReceiveEpoch = {};
		expect(
			coordinator.advanceRecovery({
				peerHash,
				peerSession: currentSession,
				receiveEpoch: currentReceiveEpoch,
			}),
		).to.be.true;
		await clock.tickAsync(1);
		expect(state.requestParked).to.be.true;
		coordinator.noteLegacyAnnouncement({
			peerHash,
			peerSession: currentSession,
			receiveEpoch: currentReceiveEpoch,
			senderTransportSession,
			transportTimestamp: 2n,
			message: new AddedReplicationSegmentMessage({ segments: [] }),
		});
		expect(state.requestParked).to.be.false;
		expect(state.capabilityRefreshRequired).to.be.true;
		expect(state.requestTimer).to.exist;
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

		senderCoordinator.enqueue(
			new AddedReplicationSegmentMessage({ segments: [] }),
		);
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
		expect(coordinator.isLegacyCutover(currentSession)).to.be.true;
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
					receiver._v2Receive.isLegacyCutover(
						receiver._peerSessions.current(senderHash),
					),
				).to.be.true;
			},
			{ timeout: 20_000 },
		);
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
		expect(log._v2Receive.isLegacyCutover(peerSession)).to.be.true;

		await log.onMessage(
			new SyncCapabilitiesMessage({
				capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			}),
			context(senderSessionB, 20n),
		);
		expect(state.controller.signal.aborted).to.be.true;
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;
		expect(log._v2Receive.isLegacyCutover(peerSession)).to.be.false;
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
		expect(log._v2Receive.isLegacyCutover(peerSession)).to.be.true;
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

	it("applies sequence order, cuts over legacy and recovers a gap with Full", async () => {
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

		const releaseCutoverLane = pDefer<void>();
		let cutoverLaneEntered = false;
		const cutoverLaneBlocker = log.withReplicationInfoApplyQueue(
			remoteHash,
			async () => {
				cutoverLaneEntered = true;
				await releaseCutoverLane.promise;
			},
		);
		await waitForResolved(() => expect(cutoverLaneEntered).to.be.true);
		const initialFull = log.onMessage(
			new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverBinding!.slice(),
				senderEpoch,
				sequence: 1n,
				segments: [rangeA],
			}),
			context(100n),
		);
		await waitForResolved(() => expect(state.reservedAdmission).to.exist);
		const originalLegacyHandler =
			log.handleReplicationInfoAnnouncement.bind(log);
		const legacyQueued = pDefer<void>();
		const legacyHandler = sinon
			.stub(log, "handleReplicationInfoAnnouncement")
			.callsFake((...args: unknown[]) => {
				const result = originalLegacyHandler(...args);
				legacyQueued.resolve();
				return result;
			});
		const legacyAcrossCutover = log.onMessage(
			new AddedReplicationSegmentMessage({ segments: [rangeB] }),
			context(150n),
		);
		await legacyQueued.promise;
		legacyHandler.restore();
		releaseCutoverLane.resolve();
		await cutoverLaneBlocker;
		await Promise.all([initialFull, legacyAcrossCutover]);
		expect(state.phase).to.equal("active");
		expect(state.lastSequence).to.equal(1n);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(1);
		expect(activity.calledOnceWith(remoteHash)).to.be.true;
		expect(log._v2Receive.isLegacyCutover(peerSession)).to.be.true;
		expect(state.legacyFallbackTimer).to.exist;
		expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

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
		expect(state.legacyFallbackTimer).to.be.undefined;

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
			new AllReplicatingSegmentsMessage({ segments: [] }),
			context(1_000n),
		);
		expect(
			await log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(3);
		expect(activity.notCalled).to.be.true;
		expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

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
		expect(log._v2Receive.isLegacyCutover(peerSession)).to.be.true;
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
