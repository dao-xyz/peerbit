import { privateKeyFromRaw } from "@libp2p/crypto/keys";
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
						privateKey: privateKeyFromRaw(
							new Uint8Array([
								27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
								162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8,
								203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178,
								86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16,
								251, 206, 54, 49, 141, 91, 171,
							]),
						),
					},
				},
				{
					libp2p: {
						privateKey: privateKeyFromRaw(
							new Uint8Array([
								113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251, 158,
								121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184, 24,
								56, 27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117, 218,
								216, 120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63, 158,
								217, 111, 65, 105, 99, 83, 4, 113, 62, 15,
							]),
						),
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
					privateKey: await privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: await privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
							197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203,
							18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86,
							159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251,
							206, 54, 49, 141, 91, 171,
						]),
					),
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
		const isLeader1 = await db1.log.isLeader({
			entry: toEntry(someTimeInTheFuture * 1e6 + eps),
			replicas: 1,
		});
		expect(isLeader1).to.be.true;
		const isLeader2 = await db2.log.isLeader({
			entry: toEntry(someTimeInTheFuture * 1e6 + eps),
			replicas: 1,
		});
		expect(isLeader2).to.be.false;
	});
});
