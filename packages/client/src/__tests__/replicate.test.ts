import assert from "assert";
import mapSeries from "p-each-series";
import { Entry } from "@dao-xyz/peerbit-log";
import { waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer.js";
import { EventStore, Operation } from "./utils/stores/event-store";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";

describe(`Replication`, function () {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;
	let fetchEvents: number;
	let fetchHashes: Set<string>;
	let fromMultihash: any;
	beforeAll(() => {
		fromMultihash = Entry.fromMultihash;

		// TODO monkeypatching might lead to sideeffects in other tests!
		Entry.fromMultihash = (s, h, o) => {
			fetchHashes.add(h);
			fetchEvents += 1;
			return fromMultihash(s, h, o);
		};
	});
	afterAll(() => {
		Entry.fromMultihash = fromMultihash;
	});

	beforeEach(async () => {
		fetchEvents = 0;
		fetchHashes = new Set();
		session = await LSession.connected(2);
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({ libp2p: session.peers[1] });
		db1 = await client1.open(new EventStore<string>());
	});

	afterEach(async () => {
		if (db1) await db1.drop();

		if (db2) await db2.drop();

		if (client1) await client1.stop();

		if (client2) await client2.stop();
		await session.stop();
	});

	it("replicates database of 1 entry", async () => {
		let updated = 0;
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{
				log: {
					onChange: async () => {
						updated += 1;
					},
				},
			}
		);
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address.toString());

		const value = "hello";
		await db1.add(value);

		try {
			await waitFor(() => updated === 1);
		} catch (error) {
			const q = 123;
		}
		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(1);

		const db1Entries: Entry<Operation<string>>[] = (
			await db1.iterator({ limit: -1 })
		).collect();
		expect(db1Entries.length).toEqual(1);
		expect(
			await client1.findLeaders(
				db1.address,
				db1Entries[0].gid,
				client1._minReplicas
			)
		).toContainAllValues(
			[client1.idKeyHash, client2.idKeyHash].map((p) => p.toString())
		);
		expect(db1Entries[0].payload.getValue().value).toEqual(value);

		const db2Entries: Entry<Operation<string>>[] = (
			await db2.iterator({ limit: -1 })
		).collect();
		expect(db2Entries.length).toEqual(1);
		expect(
			await client2.findLeaders(
				db1.address,
				db2Entries[0].gid,
				client1._minReplicas
			)
		).toContainValues(
			[client1.idKeyHash, client2.idKeyHash].map((p) => p.toString())
		);
		expect(db2Entries[0].payload.getValue().value).toEqual(value);
	});

	it("replicates database of 100 entries", async () => {
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!
		);

		const entryCount = 100;
		const entryArr: number[] = [];

		for (let i = 0; i < entryCount; i++) {
			//	entryArr.push(i);
			await db1.add("hello" + i);
		}

		/* 	const add = (i: number) => db1.add("hello" + i);
			await mapSeries(entryArr, add); */

		// Once db2 has finished replication, make sure it has all elements
		// and process to the asserts below
		try {
			await waitFor(() => db2.log.length === entryCount);
		} catch (error) {
			console.error(
				"Did not recieve all entries, missing: " +
					(db2.log.length - entryCount),
				"Fetch events: " +
					fetchEvents +
					", fetch hashes size: " +
					fetchHashes.size
			);
			const entries = (await db2.iterator({ limit: -1 })).collect();
			console.error(
				"Entries: (" +
					entries.length +
					"), " +
					entries.map((x) => x.payload.getValue().value).join(", ")
			);
			throw error;
		}

		const entries = (await db2.iterator({ limit: -1 })).collect();
		expect(entries.length).toEqual(entryCount);
		for (let i = 0; i < entryCount; i++) {
			try {
				expect(entries[i].payload.getValue().value).toEqual("hello" + i);
			} catch (error) {
				console.error(
					"Entries out of order: " +
						entries.map((x) => x.payload.getValue().value).join(", ")
				);
				throw error;
			}
		}
	});

	it("emits correct replication info", async () => {
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());

		client1.replicationReorganization = async (_changed: any) => {
			return true; // do a noop becaus in this test we want to make sure that writes are only treated once
			// and we don't want extra replication events
		};

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!
		);

		const entryCount = 99;

		// Trigger replication
		let adds: number[] = [];
		for (let i = 0; i < entryCount; i++) {
			adds.push(i);
			await db1.add("hello " + i, { nexts: [] });
			// TODO when nexts is omitted, entrise will dependon each other,
			// When entries arrive in db2 unecessary fetches occur because there is already a sync in progress?
		}

		//await mapSeries(adds, (i) => db1.add("hello " + i));

		// All entries should be in the database
		await waitFor(() => db2.log.length === entryCount);

		// All entries should be in the database
		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
			entryCount
		);

		// progress events should increase monotonically
		expect(fetchEvents).toEqual(fetchHashes.size);
		expect(fetchEvents).toEqual(0); // becausel all entries were sent
	});

	it("emits correct replication info on fresh replication", async () => {
		const entryCount = 15;

		// Trigger replication
		const adds: number[] = [];
		for (let i = 0; i < entryCount; i++) {
			adds.push(i);
		}

		const add = async (i: number) => {
			process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ");
			await db1.add("hello " + i);
		};

		await mapSeries(adds, add);

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!
		);

		// All entries should be in the database
		await waitFor(() => db2.log.length === entryCount);

		// progress events should (increase monotonically)
		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
			entryCount
		);
		expect(fetchEvents).toEqual(fetchHashes.size);
		expect(fetchEvents).toEqual(entryCount - 3); // - 3 because we also send some references for faster syncing (see exchange-heads.ts)
	});

	it("emits correct replication info in two-way replication", async () => {
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());

		const entryCount = 15;

		// Trigger replication
		const adds: number[] = [];
		for (let i = 0; i < entryCount; i++) {
			adds.push(i);
		}

		const add = async (i: number) => {
			process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ");
			await Promise.all([db1.add("hello-1-" + i), db2.add("hello-2-" + i)]);
		};

		// Open second instance again
		client1.replicationReorganization = async (_changed: any) => {
			return true; // do a noop becaus in this test we want to make sure that writes are only treated once
			// and we don't want extra replication events
		};

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!
		);

		expect(db1.address).toBeDefined();
		expect(db2.address).toBeDefined();
		expect(db1.address!.toString()).toEqual(db2.address!.toString());

		await mapSeries(adds, add);

		// All entries should be in the database
		await waitForAsync(
			async () =>
				(await db2.iterator({ limit: -1 })).collect().length === entryCount * 2,
			{ delayInterval: 200, timeout: 20000 }
		);

		// Database values should match

		try {
			await waitFor(() => db1.log.values.length === db2.log.values.length);
		} catch (error) {
			throw new Error(
				`${db1.log.values.length}  +" --- " + ${db2.log.values.length}`
			);
		}

		const values1 = (await db1.iterator({ limit: -1 })).collect();
		const values2 = (await db2.iterator({ limit: -1 })).collect();
		expect(values1.length).toEqual(values2.length);
		for (let i = 0; i < values1.length; i++) {
			assert(values1[i].equals(values2[i]));
		}
		// All entries should be in the database
		expect(values1.length).toEqual(entryCount * 2);
		expect(values2.length).toEqual(entryCount * 2);
	});
});
