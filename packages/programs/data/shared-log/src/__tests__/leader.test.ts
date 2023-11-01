import { EventStore } from "./utils/stores/event-store";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { DirectSub } from "@peerbit/pubsub";
import { DirectBlock } from "@peerbit/blocks";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Observer } from "../role.js";

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
		session = await TestSession.connected(3, {
			libp2p: {
				services: {
					blocks: (c) => new DirectBlock(c),
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false
						})
				}
			}
		});
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

		db1 = await session.peers[0].open(new EventStore<string>(), options);
		const isLeaderAOneLeader = await db1.log.isLeader(123, 1);
		expect(isLeaderAOneLeader);
		const isLeaderATwoLeader = await db1.log.isLeader(123, 2);
		expect(isLeaderATwoLeader);

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
			args: { role: new Observer(), ...options.args }
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
			args: { role: new Observer(), ...options.args }
		});
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
			expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
		);

		// One leader
		const slot = 0;

		const isLeaderAOneLeader = await db1.log.isLeader(slot, 1);
		const isLeaderBOneLeader = await db2.log.isLeader(slot, 1);
		const isLeaderCOneLeader = await db3.log.isLeader(slot, 1);
		expect([
			isLeaderAOneLeader,
			isLeaderBOneLeader,
			isLeaderCOneLeader
		]).toContainValues([false, false, true]);

		// Two leaders
		const isLeaderATwoLeaders = await db1.log.isLeader(slot, 2);
		const isLeaderBTwoLeaders = await db2.log.isLeader(slot, 2);
		const isLeaderCTwoLeaders = await db3.log.isLeader(slot, 2);
		expect([
			isLeaderATwoLeaders,
			isLeaderBTwoLeaders,
			isLeaderCTwoLeaders
		]).toContainValues([false, true, true]);

		// Three leders
		const isLeaderAThreeLeaders = await db1.log.isLeader(slot, 3);
		const isLeaderBThreeLeaders = await db2.log.isLeader(slot, 3);
		const isLeaderCThreeLeaders = await db3.log.isLeader(slot, 3);
		expect([
			isLeaderAThreeLeaders,
			isLeaderBThreeLeaders,
			isLeaderCThreeLeaders
		]).toContainValues([true, true, true]);
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
			a += (await db1.log.isLeader(String(i), 2)) ? 1 : 0;
			b += (await db2.log.isLeader(String(i), 2)) ? 1 : 0;
			c += (await db3.log.isLeader(String(i), 2)) ? 1 : 0;
		}

		const from = count * 0.6;
		const to = count * 0.8;
		expect(a > from).toBeTrue();
		expect(a < to).toBeTrue();
		expect(b > from).toBeTrue();
		expect(b < to).toBeTrue();
		expect(c > from).toBeTrue();
		expect(c < to).toBeTrue();
	});

	it("leader always defined", async () => {
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
				await db1.log.findLeaders(String(i), 3)
			);
			expect(leaders.has(undefined)).toBeFalse();
			expect(leaders.size).toEqual(3);
		}
	});

	describe("get replicators sorted", () => {
		const checkSorted = (strings: { hash: string }[]) => {
			const sorted = [...strings].sort((a, b) => a.hash.localeCompare(b.hash));
			expect(sorted).toEqual(strings);
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
					db1.log.getReplicatorsSorted()?.map((x) => x.hash)
				).toContainAllValues([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
				])
			);

			await waitForResolved(() =>
				expect(
					db3.log.getReplicatorsSorted()?.map((x) => x.hash)
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
				db1.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db2.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db3.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);

			checkSorted(db1.log.getReplicatorsSorted()!);
			checkSorted(db2.log.getReplicatorsSorted()!);
			checkSorted(db3.log.getReplicatorsSorted()!);
		});
	});
});
