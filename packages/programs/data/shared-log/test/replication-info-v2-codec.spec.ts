import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_RAW_EXCHANGE_HEADS,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import {
	ReplicationIntent,
	ReplicationRangeMessageU32,
} from "../src/ranges.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	ReplicationInfoV2AppliedMessage,
	RequestReplicationInfoMessage,
	RequestReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2Message,
	ResponseRoleMessage,
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
} from "../src/replication.js";
import { Observer } from "../src/role.js";
import { EventStore } from "./utils/stores/index.js";

const RECEIVER_CHALLENGE = Uint8Array.from({ length: 32 }, (_, index) => index);
const SENDER_EPOCH = Uint8Array.from(
	{ length: 32 },
	(_, index) => 0xff - index,
);
const SEQUENCE = 0x0102030405060708n;
const SENDER_SESSION = 0x1112131415161718n;
const REVISION = 0x2122232425262728n;
const INTENDED_SENDER = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => 0x80 + index),
});

const RECEIVER_CHALLENGE_HEX =
	"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const SENDER_EPOCH_HEX =
	"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0";
const SEQUENCE_HEX = "0807060504030201";
const SENDER_SESSION_HEX = "1817161514131211";
const REVISION_HEX = "2827262524232221";

const hex = (value: unknown): string =>
	Buffer.from(serialize(value)).toString("hex");

const createV2Messages = () =>
	[
		new FullReplicationInfoV2Message({
			receiverChallenge: RECEIVER_CHALLENGE,
			senderEpoch: SENDER_EPOCH,
			sequence: SEQUENCE,
			segments: [],
		}),
		new AddedReplicationInfoV2Message({
			receiverChallenge: RECEIVER_CHALLENGE,
			senderEpoch: SENDER_EPOCH,
			sequence: SEQUENCE,
			segments: [],
		}),
		new StoppedReplicationInfoV2Message({
			receiverChallenge: RECEIVER_CHALLENGE,
			senderEpoch: SENDER_EPOCH,
			sequence: SEQUENCE,
			segmentIds: [],
		}),
	] as const;

describe("receive admission replication-info V2 decode-only codec", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("keeps every legacy replication-info variant tag byte-identical", () => {
		const cases = [
			[new RequestReplicationInfoMessage(), "000100"],
			[new ResponseRoleMessage({ role: new Observer() }), "0001010101"],
			[new AllReplicatingSegmentsMessage({ segments: [] }), "00010200000000"],
			[new AddedReplicationSegmentMessage({ segments: [] }), "00010300000000"],
			[new StoppedReplicating({ segmentIds: [] }), "00010400000000"],
		] as const;

		for (const [message, expected] of cases) {
			const bytes = serialize(message);
			expect(Buffer.from(bytes).toString("hex")).to.equal(expected);
			const decoded = deserialize(bytes, TransportMessage);
			expect(decoded.constructor).to.equal(message.constructor);
			expect(Buffer.from(serialize(decoded)).toString("hex")).to.equal(
				expected,
			);
		}
	});

	it("pins the V2 tags, fixed-field order and little-endian u64", () => {
		const suffix = `${RECEIVER_CHALLENGE_HEX}${SENDER_EPOCH_HEX}${SEQUENCE_HEX}00000000`;
		const cases = [
			[createV2Messages()[0], FullReplicationInfoV2Message, `000106${suffix}`],
			[createV2Messages()[1], AddedReplicationInfoV2Message, `000107${suffix}`],
			[
				createV2Messages()[2],
				StoppedReplicationInfoV2Message,
				`000108${suffix}`,
			],
		] as const;

		for (const [message, MessageType, expected] of cases) {
			const bytes = serialize(message);
			expect(Buffer.from(bytes).toString("hex")).to.equal(expected);
			const decoded = deserialize(bytes, TransportMessage);
			expect(decoded).to.be.instanceOf(MessageType);
			expect(
				Buffer.from(
					(decoded as FullReplicationInfoV2Message).receiverChallenge,
				).toString("hex"),
			).to.equal(RECEIVER_CHALLENGE_HEX);
			expect(
				Buffer.from(
					(decoded as FullReplicationInfoV2Message).senderEpoch,
				).toString("hex"),
			).to.equal(SENDER_EPOCH_HEX);
			expect((decoded as FullReplicationInfoV2Message).sequence).to.equal(
				SEQUENCE,
			);
		}
	});

	it("pins the opt-in application confirmation variants", () => {
		const suffix = `${RECEIVER_CHALLENGE_HEX}${SENDER_EPOCH_HEX}${SEQUENCE_HEX}${REVISION_HEX}`;
		const cases = [
			[
				new RequestReplicationInfoV2AppliedMessage({
					receiverChallenge: RECEIVER_CHALLENGE,
					senderEpoch: SENDER_EPOCH,
					sequence: SEQUENCE,
					revision: REVISION,
				}),
				RequestReplicationInfoV2AppliedMessage,
				`00010a${suffix}`,
			],
			[
				new ReplicationInfoV2AppliedMessage({
					receiverChallenge: RECEIVER_CHALLENGE,
					senderEpoch: SENDER_EPOCH,
					sequence: SEQUENCE,
					revision: REVISION,
				}),
				ReplicationInfoV2AppliedMessage,
				`00010b${suffix}`,
			],
		] as const;
		for (const [message, type, expected] of cases) {
			expect(hex(message)).to.equal(expected);
			const decoded = deserialize(serialize(message), TransportMessage);
			expect(decoded).to.be.instanceOf(type);
			expect(hex(decoded)).to.equal(expected);
		}
	});

	it("pins the signed receiver request tag and sender binding", () => {
		const request = new RequestReplicationInfoV2Message({
			receiverChallenge: RECEIVER_CHALLENGE,
			intendedSender: INTENDED_SENDER,
			senderSession: SENDER_SESSION,
		});
		const intendedSenderHex = `00${Buffer.from(
			INTENDED_SENDER.publicKey,
		).toString("hex")}`;
		expect(hex(request)).to.equal(
			`000109${RECEIVER_CHALLENGE_HEX}${intendedSenderHex}${SENDER_SESSION_HEX}`,
		);

		const decoded = deserialize(
			serialize(request),
			TransportMessage,
		) as RequestReplicationInfoV2Message;
		expect(decoded).to.be.instanceOf(RequestReplicationInfoV2Message);
		expect(decoded.intendedSender.equals(INTENDED_SENDER)).to.be.true;
		expect(decoded.senderSession).to.equal(SENDER_SESSION);
		expect(() =>
			serialize(
				new RequestReplicationInfoV2Message({
					receiverChallenge: new Uint8Array(31),
					intendedSender: INTENDED_SENDER,
					senderSession: SENDER_SESSION,
				}),
			),
		).to.throw();
	});

	it("round-trips the unchanged replication payload schemas", () => {
		const range = new ReplicationRangeMessageU32({
			id: new Uint8Array([0xaa, 0xbb]),
			timestamp: 3n,
			offset: 4,
			factor: 5,
			mode: ReplicationIntent.NonStrict,
		});
		const full = deserialize(
			serialize(
				new FullReplicationInfoV2Message({
					receiverChallenge: RECEIVER_CHALLENGE,
					senderEpoch: SENDER_EPOCH,
					sequence: SEQUENCE,
					segments: [range],
				}),
			),
			TransportMessage,
		) as FullReplicationInfoV2Message;
		const added = deserialize(
			serialize(
				new AddedReplicationInfoV2Message({
					receiverChallenge: RECEIVER_CHALLENGE,
					senderEpoch: SENDER_EPOCH,
					sequence: SEQUENCE,
					segments: [range],
				}),
			),
			TransportMessage,
		) as AddedReplicationInfoV2Message;
		const stopped = deserialize(
			serialize(
				new StoppedReplicationInfoV2Message({
					receiverChallenge: RECEIVER_CHALLENGE,
					senderEpoch: SENDER_EPOCH,
					sequence: SEQUENCE,
					segmentIds: [new Uint8Array([0xcc, 0xdd])],
				}),
			),
			TransportMessage,
		) as StoppedReplicationInfoV2Message;

		for (const decoded of [full, added]) {
			expect(decoded.segments).to.have.length(1);
			expect(decoded.segments[0]).to.be.instanceOf(ReplicationRangeMessageU32);
			expect([...decoded.segments[0].id]).to.deep.equal([0xaa, 0xbb]);
		}
		expect(stopped.segmentIds.map((id) => [...id])).to.deep.equal([
			[0xcc, 0xdd],
		]);
	});

	it("rejects malformed fixed tokens and truncated frames", () => {
		const factories = [
			(receiverChallenge: Uint8Array, senderEpoch: Uint8Array) =>
				new FullReplicationInfoV2Message({
					receiverChallenge,
					senderEpoch,
					sequence: SEQUENCE,
					segments: [],
				}),
			(receiverChallenge: Uint8Array, senderEpoch: Uint8Array) =>
				new AddedReplicationInfoV2Message({
					receiverChallenge,
					senderEpoch,
					sequence: SEQUENCE,
					segments: [],
				}),
			(receiverChallenge: Uint8Array, senderEpoch: Uint8Array) =>
				new StoppedReplicationInfoV2Message({
					receiverChallenge,
					senderEpoch,
					sequence: SEQUENCE,
					segmentIds: [],
				}),
		];

		for (const factory of factories) {
			expect(() =>
				serialize(factory(new Uint8Array(31), SENDER_EPOCH)),
			).to.throw();
			expect(() =>
				serialize(factory(RECEIVER_CHALLENGE, new Uint8Array(33))),
			).to.throw();

			const bytes = serialize(factory(RECEIVER_CHALLENGE, SENDER_EPOCH));
			expect(() =>
				deserialize(bytes.subarray(0, bytes.length - 1), TransportMessage),
			).to.throw();
			expect(() =>
				deserialize(Uint8Array.from([...bytes, 0]), TransportMessage),
			).to.throw();
		}
	});

	it("pins the decode, send, apply and confirmation capability vocabulary", () => {
		expect(SYNC_CAPABILITY_RAW_EXCHANGE_HEADS).to.equal(1);
		expect(SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE).to.equal(2);
		expect(SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND).to.equal(4);
		expect(SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY).to.equal(8);
		expect(SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM).to.equal(16);
		expect(hex(new SyncCapabilitiesMessage())).to.equal("00000a01000000");
		expect(
			hex(
				new SyncCapabilitiesMessage({
					capabilities: SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
				}),
			),
		).to.equal("00000a02000000");
		expect(
			hex(
				new SyncCapabilitiesMessage({
					capabilities: 15,
				}),
			),
		).to.equal("00000a0f000000");
	});

	it("drops unsolicited V2 before state or mutation side effects", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const activity = sinon.spy(log._liveness, "markReplicatorActivity");
		const add = sinon.spy(log, "addReplicationRange");
		const remove = sinon.spy(log, "removeReplicationRanges");
		const send = sinon.spy(log.rpc, "send");

		try {
			for (const message of createV2Messages()) {
				await db.log.onMessage(message, {
					from: remote,
					message: { header: { timestamp: 1n } },
				} as any);
			}

			expect(activity.notCalled).to.be.true;
			expect(add.notCalled).to.be.true;
			expect(remove.notCalled).to.be.true;
			expect(send.notCalled).to.be.true;
			expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;
			expect(log._peerSessions.sessions.has(remoteHash)).to.be.false;
			expect(
				await db.log.replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			activity.restore();
			add.restore();
			remove.restore();
			send.restore();
		}
	});

	it("uses V2-only replication-info startup by default", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: true, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const sent: unknown[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				sent.push(message);
				return [] as any;
			});
		const requests = sinon
			.stub(log, "scheduleReplicationInfoRequests")
			.callsFake(() => {});
		const v2Recovery = sinon
			.stub(log, "scheduleReplicationInfoV2Recovery")
			.callsFake(() => {});
		const snapshot = sinon.spy(log, "getMyReplicationSegments");
		const advertise = sinon.spy(log._v2Receive, "advertiseLocalCapability");
		const markReady = sinon.spy(log._v2Receive, "recordLocalCapabilityReady");

		try {
			await log._onSubscription({
				detail: { from: remote, topics: [db.log.topic] },
			});
			const capabilityAdvertisement = advertise.returnValues[0];
			expect(capabilityAdvertisement).to.exist;
			await capabilityAdvertisement.firstAttempt;
			const remoteHash = remote.hashcode();
			const peerSession = log._peerSessions.current(remoteHash);
			// B12: no legacy barrier — the startup advert is promoted ready as
			// soon as its ACK lands.
			expect(
				log._v2Receive._localCapabilityAdvertisementsByPeer.get(remoteHash)
					?.ready,
			).to.be.true;
			expect(
				log._v2Receive._localCapabilityContextBySession.has(peerSession),
			).to.be.true;
			expect(markReady.calledOnce).to.be.true;
			const capability = sent.find(
				(message) => message instanceof SyncCapabilitiesMessage,
			) as SyncCapabilitiesMessage | undefined;
			expect(capability).to.exist;
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE);
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND);
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY);
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM);
			expect(
				capability!.capabilities &
					~(
						SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM |
						SYNC_CAPABILITY_RAW_EXCHANGE_HEADS
					),
			).to.equal(0);
			expect(
				sent.some(
					(message) => message instanceof AllReplicatingSegmentsMessage,
				),
			).to.be.false;
			expect(
				sent.some(
					(message) =>
						message instanceof RequestReplicationInfoMessage ||
						message instanceof ResponseRoleMessage,
				),
			).to.be.false;
			expect(
				sent.some(
					(message) =>
						message instanceof RequestReplicationInfoV2Message ||
						message instanceof FullReplicationInfoV2Message ||
						message instanceof AddedReplicationInfoV2Message ||
						message instanceof StoppedReplicationInfoV2Message,
				),
			).to.be.false;
			expect(snapshot.notCalled).to.be.true;
			expect(requests.notCalled).to.be.true;
			expect(v2Recovery.calledOnce).to.be.true;
		} finally {
			send.restore();
			requests.restore();
			v2Recovery.restore();
			snapshot.restore();
			advertise.restore();
			markReady.restore();
		}
	});

	it("drops every legacy replication-info control message in default mode", async () => {
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

		const synchronizer = sinon
			.stub(log.syncronizer, "onMessage")
			.resolves(false);
		const activity = sinon.spy(log._liveness, "markReplicatorActivity");
		const add = sinon.spy(log, "addReplicationRange");
		const remove = sinon.spy(log, "removeReplicationRanges");
		const send = sinon.spy(log.rpc, "send");
		const acquireLease = sinon.spy(log, "acquirePeerReceiveLease");
		const validateRanges = sinon.spy(
			log,
			"validateReplicationRangeAnnouncement",
		);
		const validateStopped = sinon.spy(
			log,
			"validateStoppedReplicationAnnouncement",
		);
		const messages = [
			new RequestReplicationInfoMessage(),
			new ResponseRoleMessage({ role: new Observer() }),
			new AllReplicatingSegmentsMessage({ segments: [] }),
			new AddedReplicationSegmentMessage({ segments: [] }),
			new StoppedReplicating({ segmentIds: [] }),
		];

		try {
			for (const [index, message] of messages.entries()) {
				await db.log.onMessage(message, {
					from: remote,
					message: {
						header: { session: 1n, timestamp: BigInt(index + 1) },
					},
				} as any);
			}

			expect(synchronizer.notCalled).to.be.true;
			expect(activity.notCalled).to.be.true;
			expect(add.notCalled).to.be.true;
			expect(remove.notCalled).to.be.true;
			expect(send.notCalled).to.be.true;
			expect(acquireLease.notCalled).to.be.true;
			expect(validateRanges.notCalled).to.be.true;
			expect(validateStopped.notCalled).to.be.true;
			expect(
				await db.log.replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			synchronizer.restore();
			activity.restore();
			add.restore();
			remove.restore();
			send.restore();
			acquireLease.restore();
			validateRanges.restore();
			validateStopped.restore();
		}
	});

	it("replaces one exact-session V2 recovery nudge across rapid reconnect", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
			},
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);
		// A park whose resume stays blocked (reserved admission, missing binding)
		// keeps polling at the base interval without escalating.
		const parked = sinon.stub(log._v2Receive, "isRequestParked").returns(true);
		const resume = sinon
			.stub(log._v2Receive, "resumeParkedRequest")
			.returns(false);
		const clock = sinon.useFakeTimers();

		try {
			log.scheduleReplicationInfoV2Recovery(
				remote,
				log._instanceLifecycle.membershipLifecycleController,
			);
			expect(log._replicationInfoRequestByPeer.has(remoteHash)).to.be.true;
			// The first observation of a park waits one base interval before the
			// first unpark attempt.
			expect(resume.notCalled).to.be.true;
			await clock.tickAsync(150);
			expect(resume.callCount).to.equal(3);
			for (const call of resume.getCalls()) {
				expect(call.args[0]).to.deep.equal({
					peerHash: remoteHash,
					peerSession,
					receiveEpoch: log._peerSessions.receiveEpoch(remoteHash),
				});
			}
			const exactSessionRecovery =
				log._replicationInfoRequestByPeer.get(remoteHash);
			log.cleanupPeerDisconnectTracking(remoteHash);
			expect(log._replicationInfoRequestByPeer.get(remoteHash)).to.equal(
				exactSessionRecovery,
			);

			const replacementSession = log._peerSessions.rotate(
				remoteHash,
				"opening",
			);
			log._peerSessions.unblockReplicationInfo(remoteHash);
			log._peerSessions.markOpen(remoteHash, replacementSession);
			log.scheduleReplicationInfoV2Recovery(
				remote,
				log._instanceLifecycle.membershipLifecycleController,
			);
			expect(resume.callCount).to.equal(3);
			expect(
				log._replicationInfoRequestByPeer.get(remoteHash).peerSession,
			).to.equal(replacementSession);
			await clock.tickAsync(50);
			expect(resume.callCount).to.equal(4);
			expect(resume.lastCall.args[0]).to.deep.equal({
				peerHash: remoteHash,
				peerSession: replacementSession,
				receiveEpoch: log._peerSessions.receiveEpoch(remoteHash),
			});

			log._peerSessions.rotate(remoteHash, "departing");
			await clock.tickAsync(50);
			expect(resume.callCount).to.equal(4);
			expect(log._replicationInfoRequestByPeer.has(remoteHash)).to.be.false;
		} finally {
			clock.restore();
			parked.restore();
			resume.restore();
		}
	});
});
