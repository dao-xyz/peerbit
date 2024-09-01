import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	type ReplicationDomainTime,
	createReplicationDomainTime,
} from "../src/replication-domain-time.js";
import { scaleToU32 } from "../src/role.js";
import { EventStore } from "./utils/stores/event-store.js";

/**
 *
 * @param time nanoseconds
 * @returns
 */
const toEntry = (time: bigint | number) => {
	return {
		meta: { clock: { timestamp: { wallTime: BigInt(time) } } },
	} as Entry<any>;
};

describe("ReplicationDomainTime", function () {
	describe("fromTime", () => {
		it("milliseconds", () => {
			const origin = new Date();
			const domain = createReplicationDomainTime(origin);
			const step = 1000;
			const someTimeInTheFuture = origin.getTime() + step;
			expect(domain.fromTime(someTimeInTheFuture)).to.be.equal(step);
		});
	});

	describe("fromEntry", () => {
		it("milliseconds", () => {
			const origin = new Date();
			const domain = createReplicationDomainTime(origin, "milliseconds");
			const step = 1000;
			const someTimeInTheFuture = origin.getTime() + step;
			expect(
				domain.fromEntry(toEntry(someTimeInTheFuture * 1e6)),
			).to.be.closeTo(step, 1);
		});

		it("seconds", () => {
			const origin = new Date();
			const domain = createReplicationDomainTime(origin, "seconds");
			const step = 30;
			const someTimeInTheFuture = origin.getTime() + step * 1000;
			expect(
				domain.fromEntry(toEntry(someTimeInTheFuture * 1e6)),
			).to.be.closeTo(step, 1);
		});
	});

	describe("collect", () => {
		let session: TestSession;
		let db1: EventStore<string, ReplicationDomainTime>,
			db2: EventStore<string, ReplicationDomainTime>;

		beforeEach(async () => {
			session = await TestSession.connected(2, [
				{
					libp2p: {
						peerId: await deserialize(
							new Uint8Array([
								0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
								93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207,
								165, 88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235,
								154, 68, 205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13,
								62, 84, 62, 133, 97, 57, 150, 187, 247, 215,
							]),
							Ed25519Keypair,
						).toPeerId(),
					},
				},
				{
					libp2p: {
						peerId: await deserialize(
							new Uint8Array([
								0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158,
								45, 46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219,
								238, 163, 44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16,
								89, 240, 71, 145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185,
								206, 109, 168, 112, 65, 202, 154, 27, 63, 15,
							]),
							Ed25519Keypair,
						).toPeerId(),
					},
				},
			]);
		});

		afterEach(async () => {
			if (db1 && db1.closed === false) await db1.drop();
			if (db2 && db2.closed === false) await db2.drop();
			await session.stop();
		});

		const setup = async (domain: ReplicationDomainTime) => {
			db1 = await session.peers[0].open(
				new EventStore<string, ReplicationDomainTime>(),
				{
					args: {
						replicate: false,
						domain: domain,
					},
				},
			);

			db2 = await session.peers[1].open(db1.clone(), {
				args: {
					replicate: false,
					domain: domain,
				},
			});

			return { db1, db2 };
		};
		it("milliseconds", async () => {
			const origin = new Date();
			const domain = createReplicationDomainTime(origin);
			const { db1, db2 } = await setup(domain);
			const someTimeInTheFuture = origin.getTime() + 1000;
			const factor = domain.fromDuration(100);
			await db1.log.replicate({
				normalized: false,
				offset: domain.fromTime(someTimeInTheFuture),
				factor: factor,
				strict: true,
			});
			await waitForResolved(async () =>
				expect(
					scaleToU32(await db2.log.calculateTotalParticipation()),
				).to.be.closeTo(factor, 1),
			);

			const replicatorsDefined = await db2.log.getCover(
				{
					from: someTimeInTheFuture,
					to: someTimeInTheFuture + 100,
				},
				{ roleAge: 0 },
			);

			expect(replicatorsDefined).to.have.members([
				db1.node.identity.publicKey.hashcode(),
			]);

			const resplicatorsUndefined = await db2.log.getCover(
				{
					from: someTimeInTheFuture - 100,
					to: someTimeInTheFuture - 10,
				},
				{ roleAge: 0 },
			);

			expect(resplicatorsUndefined).to.have.length(0);
		});
	});
});

describe(`e2e`, function () {
	let session: TestSession;
	let db1: EventStore<string, ReplicationDomainTime>,
		db2: EventStore<string, ReplicationDomainTime>,
		db3: EventStore<string, ReplicationDomainTime>;

	const options = {
		args: {
			timeUntilRoleMaturity: 0,
			replicas: {
				min: 1,
				max: 10000,
			},
		},
	};
	before(async () => {
		session = await TestSession.connected(3, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);
	});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1 && db1.closed === false) await db1.drop();
		if (db2 && db2.closed === false) await db2.drop();
		if (db3 && db3.closed === false) await db3.drop();
	});

	it("select leaders for one or two peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const originTime = new Date();
		const domain = createReplicationDomainTime(originTime);

		const someTimeInTheFuture = originTime.getTime() + 1000;

		db1 = await session.peers[0].open(
			new EventStore<string, ReplicationDomainTime>(),
			{
				args: {
					...options.args,
					replicate: {
						normalized: false,
						offset: domain.fromTime(someTimeInTheFuture),
						factor: "right",
					},
					domain,
				},
			},
		);

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				...options.args,
				replicate: {
					normalized: false,
					offset: domain.fromTime(someTimeInTheFuture - 1000),
					factor: 999,
				},
				domain,
			},
		})) as EventStore<string, ReplicationDomainTime>;

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).to.equal(2),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(2),
		);

		let eps = 10;
		const isLeader1 = await db1.log.isLeader(
			toEntry(someTimeInTheFuture * 1e6 + eps),
			1,
		);
		expect(isLeader1).to.be.true;
		const isLeader2 = await db2.log.isLeader(
			toEntry(someTimeInTheFuture * 1e6 + eps),
			1,
		);
		expect(isLeader2).to.be.false;
	});
});
