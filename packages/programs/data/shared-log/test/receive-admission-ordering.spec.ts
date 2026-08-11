// Stage-4 B8 tripwire. The `latestReplicationInfoMessage` sender-timestamp
// watermark carries TWO roles: a fencing role (rejecting traffic across
// unsubscribe/eviction races) that the per-peer receive epoch now subsumes,
// and an intra-epoch ORDERING role that nothing else covers — within one
// (lifecycle, session, epoch, unblocked) regime the epoch token is constant
// across every message from the peer, so only the watermark can distinguish
// "older reset delivered after newer add". These pins convert that argument
// into an executable invariant: they fail if the watermark read sites are
// ever deleted on the "epoch subsumes it" theory. The sanctioned deletion
// path is sender-authoritative sequence numbers on the replication-info
// schema (stage 5); until then the watermark is KEEP-OLD.
import { randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { ReplicationIntent } from "../src/ranges.js";
import {
	AllReplicatingSegmentsMessage,
	StoppedReplicating,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission replication-info ordering watermark", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	const openLogWithSyntheticOwner = async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { compatibility: 9, replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const makeRange = (offset: number) =>
			new log.indexableDomain.constructorRange({
				id: randomBytes(32),
				offset: log.indexableDomain.numbers.denormalize(offset),
				width: log.indexableDomain.numbers.denormalize(0.2),
				publicKeyHash: ownerHash,
				mode: ReplicationIntent.NonStrict,
				timestamp: 1n,
			});
		const receive = (message: unknown, timestamp: bigint) =>
			db.log.onMessage(
				message as any,
				{
					from: ownerKey,
					message: { header: { timestamp } },
				} as any,
			);
		return {
			db,
			log,
			replicationIndex: db.log.replicationIndex as any,
			ownerHash,
			rangeA: makeRange(0.1),
			rangeB: makeRange(0.5),
			receive,
		};
	};

	it("drops an out-of-order stale replication-info snapshot arriving after a newer one", async () => {
		const { log, replicationIndex, ownerHash, rangeA, rangeB, receive } =
			await openLogWithSyntheticOwner();

		const newerTimestamp = 1_000n;
		await receive(
			new AllReplicatingSegmentsMessage({
				segments: [rangeA.toReplicationRange(), rangeB.toReplicationRange()],
			}),
			newerTimestamp,
		);
		expect(
			await replicationIndex.count({ query: { hash: ownerHash } }),
		).to.equal(2);
		expect(log.latestReplicationInfoMessage.get(ownerHash)).to.equal(
			newerTimestamp,
		);

		// A stale RESET (only the first segment) delivered out of order — the
		// unordered-pubsub / retransmit reorder the in-lane comment names. All
		// four gate terms (lifecycle, session, receive epoch, unblocked) pass
		// for it; only the ordering watermark can drop it.
		const apply = sinon.spy(log, "addReplicationRange");
		try {
			await receive(
				new AllReplicatingSegmentsMessage({
					segments: [rangeA.toReplicationRange()],
				}),
				newerTimestamp - 1n,
			);
			// The stale reset never reached the apply path: no segment erased, no
			// spurious removed-diff/rebalance source, watermark untouched.
			expect(apply.notCalled).to.be.true;
			expect(
				await replicationIndex.count({ query: { hash: ownerHash } }),
			).to.equal(2);
			expect(log.latestReplicationInfoMessage.get(ownerHash)).to.equal(
				newerTimestamp,
			);
		} finally {
			apply.restore();
		}
	});

	it("keeps dropping across the Stopped lane", async () => {
		const { log, replicationIndex, ownerHash, rangeA, rangeB, receive } =
			await openLogWithSyntheticOwner();

		const newerTimestamp = 1_000n;
		await receive(
			new AllReplicatingSegmentsMessage({
				segments: [rangeA.toReplicationRange(), rangeB.toReplicationRange()],
			}),
			newerTimestamp,
		);
		expect(
			await replicationIndex.count({ query: { hash: ownerHash } }),
		).to.equal(2);

		const remove = sinon.spy(log, "removeReplicationRanges");
		try {
			await receive(
				new StoppedReplicating({ segmentIds: [rangeA.id, rangeB.id] }),
				newerTimestamp - 1n,
			);
			expect(remove.notCalled).to.be.true;
			expect(
				await replicationIndex.count({ query: { hash: ownerHash } }),
			).to.equal(2);
			expect(log.latestReplicationInfoMessage.get(ownerHash)).to.equal(
				newerTimestamp,
			);
		} finally {
			remove.restore();
		}
	});
});
