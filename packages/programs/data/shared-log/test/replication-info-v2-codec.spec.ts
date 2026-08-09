import { deserialize, serialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_RAW_EXCHANGE_HEADS,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
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
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

const RECEIVER_CHALLENGE = Uint8Array.from({ length: 32 }, (_, index) => index);
const SENDER_EPOCH = Uint8Array.from(
	{ length: 32 },
	(_, index) => 0xff - index,
);
const SEQUENCE = 0x0102030405060708n;

const RECEIVER_CHALLENGE_HEX =
	"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const SENDER_EPOCH_HEX =
	"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0";
const SEQUENCE_HEX = "0807060504030201";

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

	it("keeps every legacy replication-info payload byte-identical", () => {
		expect(hex(new AllReplicatingSegmentsMessage({ segments: [] }))).to.equal(
			"00010200000000",
		);
		expect(hex(new AddedReplicationSegmentMessage({ segments: [] }))).to.equal(
			"00010300000000",
		);
		expect(hex(new StoppedReplicating({ segmentIds: [] }))).to.equal(
			"00010400000000",
		);
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

	it("pins the decode-only capability vocabulary", () => {
		expect(SYNC_CAPABILITY_RAW_EXCHANGE_HEADS).to.equal(1);
		expect(SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE).to.equal(2);
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
					capabilities:
						SYNC_CAPABILITY_RAW_EXCHANGE_HEADS |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
				}),
			),
		).to.equal("00000a03000000");
	});

	it("drops unsolicited V2 before receive state or side effects", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const durablePoison = sinon.spy(log, "throwIfNativeDurableCommitFailed");
		const lease = sinon.spy(log, "acquirePeerReceiveLease");
		const ownership = sinon.spy(log, "captureReplicationOwnershipLifecycle");
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

			expect(durablePoison.notCalled).to.be.true;
			expect(lease.notCalled).to.be.true;
			expect(ownership.notCalled).to.be.true;
			expect(activity.notCalled).to.be.true;
			expect(add.notCalled).to.be.true;
			expect(remove.notCalled).to.be.true;
			expect(send.notCalled).to.be.true;
			expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;
			expect(log._peerSessions.sessions.has(remoteHash)).to.be.false;
			expect(
				await db.log.replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			durablePoison.restore();
			lease.restore();
			ownership.restore();
			activity.restore();
			add.restore();
			remove.restore();
			send.restore();
		}
	});

	it("advertises decode support while every replication send stays legacy", async () => {
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

		try {
			await log._onSubscription({
				detail: { from: remote, topics: [db.log.topic] },
			});
			const capability = sent.find(
				(message) => message instanceof SyncCapabilitiesMessage,
			) as SyncCapabilitiesMessage | undefined;
			expect(capability).to.exist;
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE);
			expect(
				capability!.capabilities &
					~(
						SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
						SYNC_CAPABILITY_RAW_EXCHANGE_HEADS
					),
			).to.equal(0);
			expect(
				sent.some(
					(message) => message instanceof AllReplicatingSegmentsMessage,
				),
			).to.be.true;
			expect(
				sent.some(
					(message) =>
						message instanceof FullReplicationInfoV2Message ||
						message instanceof AddedReplicationInfoV2Message ||
						message instanceof StoppedReplicationInfoV2Message,
				),
			).to.be.false;
		} finally {
			send.restore();
			requests.restore();
		}
	});

	it("advertises decode support before replication snapshot retrieval", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: true, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const sent: unknown[] = [];
		const snapshotFailure = new Error("replication snapshot failed");
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				sent.push(message);
				return [] as any;
			});
		const snapshot = sinon
			.stub(log, "getMyReplicationSegments")
			.rejects(snapshotFailure);

		try {
			await expect(
				log._onSubscription({
					detail: { from: remote, topics: [db.log.topic] },
				}),
			).to.be.rejectedWith(snapshotFailure.message);
			const capability = sent.find(
				(message) => message instanceof SyncCapabilitiesMessage,
			) as SyncCapabilitiesMessage | undefined;
			expect(capability).to.exist;
			expect(
				capability!.capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE);
		} finally {
			send.restore();
			snapshot.restore();
		}
	});
});
