import assert from "assert";
import mapSeries from "p-each-series";
import { Entry } from "@peerbit/log";
import { waitFor, waitForAsync, waitForResolved } from "@peerbit/time";
import { EventStore, Operation } from "./utils/stores/event-store";
import { LSession } from "@peerbit/test-utils";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";

describe(`Replication`, function () {
	let session: LSession;
	let db1: EventStore<string>, db2: EventStore<string>;
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

		db1 = await new EventStore<string>().open(session.peers[0]);
	});

	afterEach(async () => {
		if (db1) await db1.drop();

		if (db2) await db2.drop();

		await session.stop();
	});

	it("replicates database of 1 entry", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

		const value = "hello";

		await db1.add(value);

		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));

		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(1);

		const db1Entries: Entry<Operation<string>>[] = (
			await db1.iterator({ limit: -1 })
		).collect();
		expect(db1Entries.length).toEqual(1);
		expect(await db1.log.findLeaders(db1Entries[0].gid)).toContainAllValues(
			[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
				getPublicKeyFromPeerId(p).hashcode()
			)
		);
		expect(db1Entries[0].payload.getValue().value).toEqual(value);

		const db2Entries: Entry<Operation<string>>[] = (
			await db2.iterator({ limit: -1 })
		).collect();
		expect(db2Entries.length).toEqual(1);
		expect(await db2.log.findLeaders(db2Entries[0].gid)).toContainValues(
			[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
				getPublicKeyFromPeerId(p).hashcode()
			)
		);
		expect(db2Entries[0].payload.getValue().value).toEqual(value);
	});

	it("replicates database of 100 entries", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

		const entryCount = 100;

		for (let i = 0; i < entryCount; i++) {
			//	entryArr.push(i);
			await db1.add("hello" + i);
		}

		/* 	const add = (i: number) => db1.add("hello" + i);
			await mapSeries(entryArr, add); */

		// Once db2 has finished replication, make sure it has all elements
		// and process to the asserts below
		try {
			await waitFor(() => db2.log.log.length === entryCount);
		} catch (error) {
			console.error(
				"Did not recieve all entries, missing: " +
					(db2.log.log.length - entryCount),
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
		db1.log.replicationReorganization = async () => {
			return true; // do a noop becaus in this test we want to make sure that writes are only treated once
			// and we don't want extra replication events
		};

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

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
		await waitFor(() => db2.log.log.length === entryCount);

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

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		// All entries should be in the database
		await waitFor(() => db2.log.log.length === entryCount);

		// progress events should (increase monotonically)
		expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
			entryCount
		);
		expect(fetchEvents).toEqual(fetchHashes.size);
		expect(fetchEvents).toEqual(entryCount - 3); // - 3 because we also send some references for faster syncing (see exchange-heads.ts)
	});

	it("emits correct replication info in two-way replication", async () => {
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
		db1.log.replicationReorganization = async () => {
			return true; // do a noop becaus in this test we want to make sure that writes are only treated once
			// and we don't want extra replication events
		};

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

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
			await waitFor(
				() => db1.log.log.values.length === db2.log.log.values.length
			);
		} catch (error) {
			throw new Error(
				`${db1.log.log.values.length}  +" --- " + ${db2.log.log.values.length}`
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
