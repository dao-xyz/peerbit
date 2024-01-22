import { EventStore } from "./utils/stores/event-store";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { Ed25519Keypair, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Replicator } from "../role.js";
import { deserialize } from "@dao-xyz/borsh";

describe(`leaders`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const options = {
		args: {
			replicas: {
				min: 1,
				max: 10000
			}
		}
	};
	beforeAll(async () => {
		session = await TestSession.connected(3, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242
						]),
						Ed25519Keypair
					).toPeerId()
				}
			}
		]);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		if (db3) await db3.drop();
	});

	it("select leaders for one or two peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		});
		const isLeaderAOneLeader = await db1.log.isLeader(123, 1);
		expect(isLeaderAOneLeader);
		const isLeaderATwoLeader = await db1.log.isLeader(123, 2);
		expect(isLeaderATwoLeader);

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).toHaveLength(2)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(2)
		);

		// leader rotation is kind of random, so we do a sequence of tests
		for (let slot = 0; slot < 3; slot++) {
			// One leader
			const isLeaderAOneLeader = await db1.log.isLeader(slot, 1);
			const isLeaderBOneLeader = await db2.log.isLeader(slot, 1);
			expect([isLeaderAOneLeader, isLeaderBOneLeader]).toContainAllValues([
				false,
				true
			]);

			// Two leaders
			const isLeaderATwoLeaders = await db1.log.isLeader(slot, 2);
			const isLeaderBTwoLeaders = await db2.log.isLeader(slot, 2);

			expect([isLeaderATwoLeaders, isLeaderBTwoLeaders]).toContainAllValues([
				true,
				true
			]);
		}
	});

	it("leader are selected from 1 replicating peer", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { role: "observer", ...options.args }
		});
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options
		)) as EventStore<string>;

		await delay(5000); // some delay so that if peers are to replicate, they would have had time to notify each other

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader(slot, 2);
		const isLeaderB = await db2.log.isLeader(slot, 2);

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
	});

	it("leader are selected from 2 replicating peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { role: "observer", ...options.args }
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(2)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).toHaveLength(2)
		);

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader(slot, 3);
		const isLeaderB = await db2.log.isLeader(slot, 3);
		const isLeaderC = await db3.log.isLeader(slot, 3);

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
		expect(isLeaderC);
	});

	it("select leaders for three peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
		);

		let resolved = 0;
		for (let i = 0; i < 100; i++) {
			try {
				const slot = Math.random();
				const isLeaderAOneLeader = await db1.log.isLeader(slot, 1, {
					roleAge: 0
				});
				const isLeaderBOneLeader = await db2.log.isLeader(slot, 1, {
					roleAge: 0
				});
				const isLeaderCOneLeader = await db3.log.isLeader(slot, 1, {
					roleAge: 0
				});
				expect([
					isLeaderAOneLeader,
					isLeaderBOneLeader,
					isLeaderCOneLeader
				]).toContainValues([false, false, true]);

				// Two leaders
				const isLeaderATwoLeaders = await db1.log.isLeader(slot, 2, {
					roleAge: 0
				});
				const isLeaderBTwoLeaders = await db2.log.isLeader(slot, 2, {
					roleAge: 0
				});
				const isLeaderCTwoLeaders = await db3.log.isLeader(slot, 2, {
					roleAge: 0
				});
				expect([
					isLeaderATwoLeaders,
					isLeaderBTwoLeaders,
					isLeaderCTwoLeaders
				]).toContainValues([false, true, true]);

				// Three leders
				const isLeaderAThreeLeaders = await db1.log.isLeader(slot, 3, {
					roleAge: 0
				});
				const isLeaderBThreeLeaders = await db2.log.isLeader(slot, 3, {
					roleAge: 0
				});
				const isLeaderCThreeLeaders = await db3.log.isLeader(slot, 3, {
					roleAge: 0
				});
				expect([
					isLeaderAThreeLeaders,
					isLeaderBThreeLeaders,
					isLeaderCThreeLeaders
				]).toContainValues([true, true, true]);
				resolved += 1;
			} catch (error) {}
		}
		// since the distribution only in best scenarios distributes perfectly
		// we might have duplication, i.e. more than expected amount of leaders for a particular
		// slot
		expect(resolved).toBeGreaterThan(40);
	});

	it("evenly distributed", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options
		)) as EventStore<string>;
		db3 = (await EventStore.open(
			db1.address!,
			session.peers[2],
			options
		)) as EventStore<string>;

		await waitForResolved(() =>
			expect(Math.abs((db1.log.role as Replicator).factor - 0.33)).toBeLessThan(
				0.02
			)
		);
		await waitForResolved(() =>
			expect(Math.abs((db2.log.role as Replicator).factor - 0.33)).toBeLessThan(
				0.02
			)
		);
		await waitForResolved(() =>
			expect(Math.abs((db3.log.role as Replicator).factor - 0.33)).toBeLessThan(
				0.02
			)
		);

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
		);

		let a = 0,
			b = 0,
			c = 0;
		const count = 10000;
		for (let i = 0; i < count; i++) {
			a += (await db1.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
			b += (await db2.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
			c += (await db3.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
		}

		const from = count * 0.5;
		const to = count * 0.95;
		expect(a).toBeGreaterThan(from);
		expect(a).toBeLessThan(to);
		expect(b).toBeGreaterThan(from);
		expect(b).toBeLessThan(to);
		expect(c).toBeGreaterThan(from);
		expect(c).toBeLessThan(to);
	});

	describe("balance", () => {
		it("small fractions means little replication", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					role: { type: "replicator", factor: 0.05 } // cover 5%
				}
			});
			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						role: { type: "replicator", factor: 0.5 } // cover 50%  10x)
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(2)
			);

			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(2)
			);

			let a = 0,
				b = 0;
			const count = 10000;
			for (let i = 0; i < count; i++) {
				a += (await db1.log.isLeader(String(i), 1, { roleAge: 0 })) ? 1 : 0;
				b += (await db2.log.isLeader(String(i), 1, { roleAge: 0 })) ? 1 : 0;
			}

			expect(a + b).toEqual(count);
			expect(a / count).toBeGreaterThan(0.08);
			expect(a / count).toBeLessThan(0.12);
			expect(b / count).toBeGreaterThan(0.88);
			expect(b / count).toBeLessThan(0.92);
		});
	});

	it("leader always defined", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: {
					...options.args,
					type: "replicator",
					factor: 0.3333
				}
			}
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				...options.args,
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				...options.args,
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
		);

		for (let i = 0; i < 100; i++) {
			const leaders: Set<string | undefined> = new Set(
				await db1.log.findLeaders(String(i), 3, { roleAge: 0 })
			);
			expect(leaders.has(undefined)).toBeFalse();
			expect(leaders.size).toEqual(3);
		}
	});

	describe("get replicators sorted", () => {
		const checkSorted = (values: { role: { offset: number } }[]) => {
			const sorted = [...values].sort((a, b) => a.role.offset - b.role.offset);
			expect(sorted).toEqual(values);
		};
		it("can handle peers leaving and joining", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), options);
			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(2)
			);

			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(2)
			);

			db3 = (await EventStore.open(
				db1.address!,
				session.peers[2],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
			);

			await db2.close();

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(2)
			);

			await waitForResolved(() =>
				expect(
					db1.log
						.getReplicatorsSorted()
						?.toArray()
						?.map((x) => x.publicKey.hashcode())
				).toContainAllValues([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
				])
			);

			await waitForResolved(() =>
				expect(
					db3.log
						.getReplicatorsSorted()
						?.toArray()
						?.map((x) => x.publicKey.hashcode())
				).toContainAllValues([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
				])
			);

			expect(db2.log.getReplicatorsSorted()).toBeUndefined();

			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
			);

			expect(
				db1.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db2.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db3.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);

			checkSorted(db1.log.getReplicatorsSorted()!.toArray());
			checkSorted(db2.log.getReplicatorsSorted()!.toArray());
			checkSorted(db3.log.getReplicatorsSorted()!.toArray());
		});
	});
});
