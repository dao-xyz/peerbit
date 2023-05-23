import assert from "assert";
import mapSeries from "p-each-series";
import { Entry, Log } from "@dao-xyz/peerbit-log";
import {
	delay,
	waitFor,
	waitForAsync,
	waitForResolved,
} from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer.js";
import { EventStore, Operation } from "./utils/stores/event-store";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";

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
		await client1.waitForPeer(client2, db1);
		await client2.waitForPeer(client1, db1);

		await delay(3000);
		const value = "hello";

		await db1.add(value);

		await waitFor(() => updated === 1);

		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(1);

		const db1Entries: Entry<Operation<string>>[] = (
			await db1.iterator({ limit: -1 })
		).collect();
		expect(db1Entries.length).toEqual(1);
		expect(
			await client1.findLeaders(
				db1.log,
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
				db1.log,
				db2Entries[0].gid,
				client1._minReplicas
			)
		).toContainValues(
			[client1.idKeyHash, client2.idKeyHash].map((p) => p.toString())
		);
		expect(db2Entries[0].payload.getValue().value).toEqual(value);
	});

	it("replicates database of 100 entries", async () => {
		await client2.waitForPeer(client1, db1);

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
		await client2.waitForPeer(client1, db1);

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
		await client2.waitForPeer(client1, db1);

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

	it("default params are sufficient for dialing", async () => {
		@variant("some_store")
		class Store extends Program {
			@field({ type: Log })
			log: Log<string>;
			constructor() {
				super();
				this.log = new Log();
			}

			async setup(): Promise<void> {
				return this.log.setup();
			}
		}
		const peer = await Peerbit.create();
		const peer2 = await Peerbit.create();
		await peer.dial(peer2.libp2p.getMultiaddrs());

		const store = await peer.open(new Store());
		const store2 = await peer2.open<Store>(store.address);

		await store.log.append("hello");
		await waitForResolved(() => expect(store2.log.length).toEqual(1));

		await peer.stop();
		await peer2.stop();
	});
});
