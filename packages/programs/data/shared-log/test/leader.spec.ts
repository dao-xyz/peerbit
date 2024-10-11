import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { slowDownSend } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

/**
 * TOOD make these test part of ranges.test.ts
 */

const toEntry = (gid: string | number) => {
	return { meta: { gid: String(gid) } } as Entry<any>;
};

describe(`isLeader`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

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
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251, 158,
							121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184, 24, 56,
							27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117, 218, 216,
							120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63, 158, 217, 111,
							65, 105, 99, 83, 4, 113, 62, 15,
						]),
					),
				},
			},

			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							215, 31, 167, 188, 121, 226, 67, 218, 96, 8, 55, 233, 34, 68, 9,
							147, 11, 157, 187, 43, 39, 43, 25, 95, 184, 227, 137, 56, 4, 69,
							120, 214, 182, 163, 41, 82, 248, 210, 213, 22, 179, 112, 251, 219,
							52, 114, 102, 110, 6, 60, 216, 135, 218, 60, 196, 128, 251, 85,
							167, 121, 179, 136, 114, 83,
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

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: { ...options.args, replicate: { offset: 0, factor: 0.5 } },
		});
		const isLeaderAOneLeader = await db1.log.isLeader({
			entry: toEntry(123),
			replicas: 1,
		});
		expect(isLeaderAOneLeader);
		const isLeaderATwoLeader = await db1.log.isLeader({
			entry: toEntry(123),
			replicas: 2,
		});
		expect(isLeaderATwoLeader);

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, replicate: { offset: 0.5, factor: 0.5 } },
		})) as EventStore<string>;

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).to.equal(2),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(2),
		);

		// leader rotation is kind of random, so we do a sequence of tests
		for (let i = 0; i < 3; i++) {
			let slot = (0.1 + i) % 1;

			// One leader
			const isLeaderAOneLeader = await db1.log.isLeader({
				entry: toEntry(slot),
				replicas: 1,
			});
			const isLeaderBOneLeader = await db2.log.isLeader({
				entry: toEntry(slot),
				replicas: 1,
			});
			expect([isLeaderAOneLeader, isLeaderBOneLeader]).to.have.members([
				false,
				true,
			]);

			// Two leaders
			const isLeaderATwoLeaders = await db1.log.isLeader({
				entry: toEntry(slot),
				replicas: 2,
			});
			const isLeaderBTwoLeaders = await db2.log.isLeader({
				entry: toEntry(slot),
				replicas: 2,
			});

			expect([isLeaderATwoLeaders, isLeaderBTwoLeaders]).to.have.members([
				true,
				true,
			]);
		}
	});

	it("leader are selected from 1 replicating peer", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { ...options.args },
		});
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options,
		)) as EventStore<string>;

		await delay(5000); // some delay so that if peers are to replicate, they would have had time to notify each other

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader({
			entry: toEntry(slot),
			replicas: 2,
		});
		const isLeaderB = await db2.log.isLeader({
			entry: toEntry(slot),
			replicas: 2,
		});

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
	});

	it("leader are selected from 2 replicating peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { ...options.args, replicate: false },
		});

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, replicate: { factor: 0.5 } },
		})) as EventStore<string>;

		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: { ...options.args, replicate: { factor: 0.5 } },
		})) as EventStore<string>;

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(2),
		);

		await waitForResolved(async () =>
			expect((await db3.log.getReplicators()).size).to.equal(2),
		);

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader({
			entry: toEntry(slot),
			replicas: 3,
		});
		const isLeaderB = await db2.log.isLeader({
			entry: toEntry(slot),
			replicas: 3,
		});
		const isLeaderC = await db3.log.isLeader({
			entry: toEntry(slot),
			replicas: 3,
		});

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
		expect(isLeaderC);
	});

	it("select leaders for three peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					offset: 0,
					factor: 0.3333,
				},
			},
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				replicate: {
					offset: 0.333,
					factor: 0.3333,
				},
			},
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				replicate: {
					offset: 0.666,
					factor: 0.3333,
				},
			},
		})) as EventStore<string>;

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).to.equal(3),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(3),
		);
		await waitForResolved(async () =>
			expect((await db3.log.getReplicators()).size).to.equal(3),
		);

		let resolved = 0;
		for (let i = 0; i < 100; i++) {
			try {
				const slot = Math.random();
				const isLeaderAOneLeader = await db1.log.isLeader(
					{ entry: toEntry(slot), replicas: 1 },
					{
						roleAge: 0,
					},
				);
				const isLeaderBOneLeader = await db2.log.isLeader(
					{ entry: toEntry(slot), replicas: 1 },
					{
						roleAge: 0,
					},
				);
				const isLeaderCOneLeader = await db3.log.isLeader(
					{ entry: toEntry(slot), replicas: 1 },
					{
						roleAge: 0,
					},
				);
				expect([
					isLeaderAOneLeader,
					isLeaderBOneLeader,
					isLeaderCOneLeader,
				]).include.members([false, false, true]);

				// Two leaders
				const isLeaderATwoLeaders = await db1.log.isLeader(
					{ entry: toEntry(slot), replicas: 2 },
					{
						roleAge: 0,
					},
				);
				const isLeaderBTwoLeaders = await db2.log.isLeader(
					{ entry: toEntry(slot), replicas: 2 },
					{
						roleAge: 0,
					},
				);
				const isLeaderCTwoLeaders = await db3.log.isLeader(
					{ entry: toEntry(slot), replicas: 2 },
					{
						roleAge: 0,
					},
				);
				expect([
					isLeaderATwoLeaders,
					isLeaderBTwoLeaders,
					isLeaderCTwoLeaders,
				]).include.members([false, true, true]);

				// Three leders
				const isLeaderAThreeLeaders = await db1.log.isLeader(
					{ entry: toEntry(slot), replicas: 3 },
					{
						roleAge: 0,
					},
				);
				const isLeaderBThreeLeaders = await db2.log.isLeader(
					{ entry: toEntry(slot), replicas: 3 },
					{
						roleAge: 0,
					},
				);
				const isLeaderCThreeLeaders = await db3.log.isLeader(
					{ entry: toEntry(slot), replicas: 3 },
					{
						roleAge: 0,
					},
				);
				expect([
					isLeaderAThreeLeaders,
					isLeaderBThreeLeaders,
					isLeaderCThreeLeaders,
				]).include.members([true, true, true]);
				resolved += 1;
			} catch (error) {}
		}
		// since the distribution only in best scenarios distributes perfectly
		// we might have duplication, i.e. more than expected amount of leaders for a particular
		// slot
		expect(resolved).greaterThan(40);
	});

	it("evenly distributed", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options,
		)) as EventStore<string>;
		db3 = (await EventStore.open(
			db1.address!,
			session.peers[2],
			options,
		)) as EventStore<string>;

		let allowedError = 0.03;

		await waitForResolved(async () =>
			expect(
				Math.abs((await db1.log.getMyTotalParticipation()) - 0.33),
			).lessThan(allowedError),
		);
		await waitForResolved(async () =>
			expect(
				Math.abs((await db2.log.getMyTotalParticipation()) - 0.33),
			).lessThan(allowedError),
		);
		await waitForResolved(async () =>
			expect(
				Math.abs((await db3.log.getMyTotalParticipation()) - 0.33),
			).lessThan(allowedError),
		);

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).to.equal(3),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(3),
		);

		await waitForResolved(async () =>
			expect((await db3.log.getReplicators()).size).to.equal(3),
		);

		let a = 0,
			b = 0,
			c = 0;
		const count = 10000;

		for (let i = 0; i < count; i++) {
			a += (await db1.log.isLeader(
				{ entry: toEntry(String(i)), replicas: 2 },
				{ roleAge: 0 },
			))
				? 1
				: 0;
			b += (await db2.log.isLeader(
				{ entry: toEntry(String(i)), replicas: 2 },
				{ roleAge: 0 },
			))
				? 1
				: 0;
			c += (await db3.log.isLeader(
				{ entry: toEntry(String(i)), replicas: 2 },
				{ roleAge: 0 },
			))
				? 1
				: 0;
		}

		const from = count * 0.5;
		const to = count * 0.95;
		expect(a).greaterThan(from);
		expect(a).lessThan(to);
		expect(b).greaterThan(from);
		expect(b).lessThan(to);
		expect(c).greaterThan(from);
		expect(c).lessThan(to);
	});

	describe("union", () => {
		it("local first", async () => {
			const store = new EventStore<string>();
			db1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 0.5001, // numerical accuracy is bad so we need to use 0.5001 to make sure a single node can cover 0.5 of the content space
					},
					replicas: {
						min: 2,
					},
					timeUntilRoleMaturity: 10 * 1000,
				},
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							factor: 0.5001, // numerical accuracy is bad so we need to use 0.5001 to make sure a single node can cover 0.5 of the content space
						},
						replicas: {
							min: 2,
						},
						timeUntilRoleMaturity: 10 * 1000,
					},
				},
			);

			await waitForResolved(async () => {
				expect((await db1.log.getReplicators()).size).equal(2);
			});

			await waitForResolved(async () => {
				expect((await db2.log.getReplicators()).size).equal(2);
			});

			// expect either db1 to replicate more than 50% or db2 to replicate more than 50%
			// for these
			expect(await db1.log.getCover(undefined, { roleAge: 0 })).to.deep.equal([
				session.peers[0].identity.publicKey.hashcode(),
			]);

			expect(await db2.log.getCover(undefined, { roleAge: 0 })).to.deep.equal([
				session.peers[1].identity.publicKey.hashcode(),
			]);
		});

		it("will consider in flight", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store.clone(), {
				args: {
					replicate: {
						factor: 0.5001, // numerical accuracy is bad so we need to use 0.5001 to make sure a single node can cover 0.5 of the content space
					},
					replicas: {
						min: 2,
					},
					timeUntilRoleMaturity: 1e4,
				},
			});

			const abortController = new AbortController();
			await db1.add("hello!");
			slowDownSend(db1.log, ExchangeHeadsMessage, 1e5, abortController.signal);

			db2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: {
						factor: 0.5001, // numerical accuracy is bad so we need to use 0.5001 to make sure a single node can cover 0.5 of the content space
					},
					replicas: {
						min: 2,
					},
					timeUntilRoleMaturity: 1e4,
				},
			});

			await waitForResolved(async () => {
				expect((await db1.log.getReplicators()).size).equal(2);
			});

			await waitForResolved(async () => {
				expect((await db2.log.getReplicators()).size).equal(2);
			});

			await waitForResolved(
				() =>
					expect(
						db2.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode()),
					).to.be.true,
			);

			// expect either db1 to replicate more than 50% or db2 to replicate more than 50%
			// for these
			expect(await db2.log.getCover(undefined, { roleAge: 0 })).to.have.members(
				[
					session.peers[0].identity.publicKey.hashcode(),
					session.peers[1].identity.publicKey.hashcode(),
				],
			);

			abortController.abort("Start sending now");
			await waitForResolved(() => {
				expect(
					db2.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode()),
				).to.be.false;
			});

			// no more inflight
			expect(await db2.log.getCover(undefined, { roleAge: 0 })).to.deep.equal([
				session.peers[1].identity.publicKey.hashcode(),
			]);
		});

		it("sets replicators groups correctly", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1,
					},
					replicate: {
						offset: 0,
						factor: 0.34,
					},
				},
			});
			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							offset: 0.333,
							factor: 0.34,
						},
					},
				},
			);

			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							offset: 0.666,
							factor: 0.34,
						},
					},
				},
			);

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db3.log.getReplicators()).size).to.equal(3),
			);

			for (let i = 1; i <= 3; i++) {
				db1.log.replicas.min = { getValue: () => i };

				// min replicas 3 only need to query 1 (every one have all the data)
				// min replicas 2 only need to query 2
				// min replicas 1 only need to query 3 (data could end up at any of the 3 nodes)
				expect(
					await db1.log.getCover(undefined, { roleAge: 0 }),
				).to.have.length(3 - i + 1);
			}
		});

		describe("eager", () => {
			it("eager, me not-mature, all included", async () => {
				const store = new EventStore<string>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							factor: 0.34,
						},
					},
				});

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								factor: 0.34,
							},
						},
					},
				);

				db3 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								factor: 0.34,
							},
						},
					},
				);

				await waitForResolved(async () =>
					expect((await db1.log.getReplicators()).size).to.equal(3),
				);

				await waitForResolved(async () =>
					expect((await db2.log.getReplicators()).size).to.equal(3),
				);

				await waitForResolved(async () =>
					expect((await db3.log.getReplicators()).size).to.equal(3),
				);

				for (let i = 3; i <= 3; i++) {
					db3.log.replicas.min = { getValue: () => i };

					// Should always include all nodes since no is mature
					expect(
						await db3.log.getCover(undefined, {
							roleAge: 0xffffffff,
							eager: true,
						}),
					).to.have.length(3);
				}
			});
		});

		it("all non-mature, only me included", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1,
					},
					replicate: {
						factor: 0.34,
					},
				},
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							factor: 0.34,
						},
					},
				},
			);

			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							factor: 0.34,
						},
					},
				},
			);

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db3.log.getReplicators()).size).to.equal(3),
			);

			for (let i = 3; i <= 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(
					await db3.log.getCover(undefined, { roleAge: 0xffffffff }),
				).to.have.length(1);
			}
		});

		describe("maturity", () => {
			it("one mature, all included", async () => {
				const store = new EventStore<string>();

				const MATURE_TIME = 2000;
				db1 = await session.peers[0].open(store, {
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							offset: 0,
							factor: 0.34,
						},
					},
				});

				await delay(MATURE_TIME);

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0.334,
								factor: 0.34,
							},
						},
					},
				);

				db3 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0.666,
								factor: 0.34,
							},
						},
					},
				);

				await waitForResolved(async () =>
					expect((await db1.log.getReplicators()).size).to.equal(3),
				);

				await waitForResolved(async () =>
					expect((await db2.log.getReplicators()).size).to.equal(3),
				);

				await waitForResolved(async () =>
					expect((await db3.log.getReplicators()).size).to.equal(3),
				);

				// TODO not sure if db2 results should be included here
				// db2 is not mature from db3 perspective (?). Might be (?)
				// this test is kind of pointless anyway since we got the range.test.ts that tests all the cases

				for (let i = 1; i < 3; i++) {
					db3.log.replicas.min = { getValue: () => i };
					let list = await db3.log.getCover(undefined, {
						roleAge: MATURE_TIME,
					});
					expect(list).to.have.length(2); // TODO unmature nodes should not be queried
					expect(list).to.have.members([
						session.peers[0].identity.publicKey.hashcode(),
						session.peers[2].identity.publicKey.hashcode(),
					]);
				}

				await delay(MATURE_TIME);

				for (let i = 1; i <= 3; i++) {
					db3.log.replicas.min = { getValue: () => i };

					// all is matured now
					expect(
						await db3.log.getCover(undefined, { roleAge: MATURE_TIME }),
					).to.have.length(3 - i + 1); // since I am replicating with factor 1 and is mature
				}
			});
		});
	});

	describe("balance", () => {
		it("small fractions means little replication", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.05,
					}, // cover 5%
				},
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.48,
						}, // cover 48%  appx 10 times more)
					},
				},
			);

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(2),
			);

			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(2),
			);

			let a = 0,
				b = 0;
			const count = 10000;

			// expect db1 and db2 segments to not overlap for this test asserts to work out well
			for (const segmentsA of await db1.log.getMyReplicationSegments()) {
				for (const segmentsB of await db2.log.getMyReplicationSegments()) {
					expect(segmentsA.overlaps(segmentsB)).to.be.false;
				}
			}

			for (let i = 0; i < count; i++) {
				a += (await db1.log.isLeader(
					{ entry: toEntry(String(i)), replicas: 1 },
					{ roleAge: 0 },
				))
					? 1
					: 0;
				b += (await db2.log.isLeader(
					{ entry: toEntry(String(i)), replicas: 1 },
					{ roleAge: 0 },
				))
					? 1
					: 0;
			}

			expect(a + b).equal(count);

			/*  
			
			expect(a / count).greaterThan(0.04);
			expect(a / count).lessThan(0.06);
			expect(b / count).greaterThan(0.94);
			expect(b / count).lessThan(0.96); 
			
			*/
			// TODO since the new indexing solution for replicaiton ranges, gaps are not treated with a scaling factor that is proportionate to the replication segment width
			// this means that small segments that occupy a space between gaps might need to replicate larger amounts of data than wanted
			expect(a / count).greaterThan(0.04);
			expect(a / count).lessThan(0.3);
			expect(b / count).greaterThan(0.7);
			expect(b / count).lessThan(0.96);
		});
	});

	it("leader always defined", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					...options.args,

					factor: 0.3333,
				},
			},
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				...options.args,
				replicate: {
					factor: 0.3333,
				},
			},
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				...options.args,
				replicate: {
					factor: 0.3333,
				},
			},
		})) as EventStore<string>;

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).to.equal(3),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators()).size).to.equal(3),
		);

		await waitForResolved(async () =>
			expect((await db3.log.getReplicators()).size).to.equal(3),
		);

		for (let i = 0; i < 100; i++) {
			const leaders: Set<string | undefined> = new Set([
				...(
					await db1.log.findLeaders(
						{ entry: toEntry(String(i)), replicas: 3 },
						{ roleAge: 0 },
					)
				).keys(),
			]);
			expect(leaders.has(undefined)).to.be.false;
			expect(leaders.size).equal(3);
		}
	});

	describe("get replicators sorted", () => {
		it("can handle peers leaving and joining", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), options);
			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options,
			)) as EventStore<string>;

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(2),
			);

			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(2),
			);

			db3 = (await EventStore.open(
				db1.address!,
				session.peers[2],
				options,
			)) as EventStore<string>;

			await waitForResolved(async () =>
				expect((await db3.log.getReplicators()).size).to.equal(3),
			);

			await db2.close();

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(2),
			);

			await waitForResolved(async () =>
				expect([...(await db1.log.getReplicators())]).to.have.members([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode(),
				]),
			);

			await waitForResolved(async () =>
				expect([...(await db3.log.getReplicators())]).to.have.members([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode(),
				]),
			);

			expect(db2.log["_replicationRangeIndex"]).equal(undefined);

			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options,
			)) as EventStore<string>;

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(3),
			);

			await waitForResolved(async () =>
				expect((await db3.log.getReplicators()).size).to.equal(3),
			);

			expect([...(await db1.log.getReplicators())]).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode(),
			]);
			expect([...(await db2.log.getReplicators())]).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode(),
			]);
			expect([...(await db3.log.getReplicators())]).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode(),
			]);
		});
	});
});
