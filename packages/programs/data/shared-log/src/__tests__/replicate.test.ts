import assert from "assert";
import mapSeries from "p-each-series";
import { Entry } from "@peerbit/log";
import { delay, waitFor, waitForAsync, waitForResolved } from "@peerbit/time";
import { EventStore, Operation } from "./utils/stores/event-store";
import { LSession } from "@peerbit/test-utils";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { AbsolutMinReplicas, maxMinReplicas } from "../replication";
import { Observer } from "../role";

describe(`exchange`, function () {
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

		db1 = await session.peers[0].open(new EventStore<string>());
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
		expect(
			await db1.log.findLeaders(
				db1Entries[0].gid,
				maxMinReplicas(db1.log, db1Entries)
			)
		).toContainAllValues(
			[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
				getPublicKeyFromPeerId(p).hashcode()
			)
		);
		expect(db1Entries[0].payload.getValue().value).toEqual(value);

		const db2Entries: Entry<Operation<string>>[] = (
			await db2.iterator({ limit: -1 })
		).collect();
		expect(db2Entries.length).toEqual(1);
		expect(
			await db2.log.findLeaders(
				db2Entries[0].gid,
				maxMinReplicas(db2.log, db2Entries)
			)
		).toContainValues(
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
			await db1.add("hello " + i, { meta: { next: [] } });
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
		expect(fetchEvents).toEqual(entryCount - 1); // - 1 because we also send some references for faster syncing (see exchange-heads.ts)
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

describe("replication degree", () => {
	let session: LSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const init = async (min: number, max?: number) => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min,
					max,
				},
				role: new Observer(),
			},
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max,
					},
				},
			}
		))!;

		db3 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicas: {
						min,
						max,
					},
				},
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
		await db3.waitFor(session.peers[0].peerId);
	};
	beforeEach(async () => {
		session = await LSession.connected(3);
	});

	afterEach(async () => {
		if (db1) await db1.drop();

		if (db2) await db2.drop();

		if (db3) await db3.drop();

		await session.stop();
	});

	it("can override min on program level", async () => {
		let minReplicas = 2;
		await init(minReplicas);

		const value = "hello";

		const e1 = await db1.add(value, {
			replicas: new AbsolutMinReplicas(1), // will be overriden by 'minReplicas' above
			meta: { next: [] },
		});

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));
	});

	it("can override min on program level", async () => {
		let minReplicas = 1;
		let maxReplicas = 1;

		await init(minReplicas, maxReplicas);

		const value = "hello";

		const e1 = await db1.add(value, {
			replicas: new AbsolutMinReplicas(100), // will be overriden by 'maxReplicas' above
			meta: { next: [] },
		});

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() =>
			expect(db2.log.log.length).not.toEqual(db3.log.log.length)
		);
		await delay(3000); // wait if so more replcation will eventually occur
		await waitForResolved(() =>
			expect(db2.log.log.length).not.toEqual(db3.log.log.length)
		);
	});

	it("control per commmit", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, {
			replicas: new AbsolutMinReplicas(1),
			meta: { next: [] },
		});
		const e2 = await db1.add(value, {
			replicas: new AbsolutMinReplicas(3),
			meta: { next: [] },
		});

		// expect e1 to be replated at db1 and/or 1 other peer (when you write you always store locally)
		// expect e2 to be replicated everywhere

		await waitForResolved(() => expect(db1.log.log.length).toEqual(2));
		await waitForResolved(() =>
			expect(db2.log.log.length).toBeGreaterThanOrEqual(1)
		);
		await waitForResolved(() =>
			expect(db3.log.log.length).toBeGreaterThanOrEqual(1)
		);
		expect(db2.log.log.length).not.toEqual(db3.log.log.length);
	});

	it("min replicas with be maximum value for gid", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, { replicas: new AbsolutMinReplicas(3) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));

		// e2 only sets minReplicas to 1 which means only db2 or db3 needs to hold it
		const e2 = await db1.add(value, {
			replicas: new AbsolutMinReplicas(1),
			meta: { next: [e1.entry] },
		});

		await waitForResolved(() => expect(db1.log.log.length).toEqual(2));
		await waitForResolved(() =>
			expect(db2.log.log.length).not.toEqual(db3.log.log.length)
		);
		let min = Math.min(db2.log.log.length, db3.log.log.length);
		expect(min).toEqual(0); // because e2 dictates that only one of db2 and db3 needs to hold the e2 -> e1 log chain
	});

	it("will not delete unless replicated", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, { replicas: new AbsolutMinReplicas(1) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() =>
			expect(db2.log.log.length).not.toEqual(db3.log.log.length)
		);

		let dbWithEntry = db2.log.log.length === 1 ? db2 : db3;
		expect(dbWithEntry.log.log.length).toEqual(1);
		await expect(
			() => dbWithEntry.log.safelyDelete([e1.entry], { timeout: 3000 })[0]
		).rejects.toThrowError("Timeout");
		expect(dbWithEntry.log.log.length).toEqual(1); // No deletions
	});

	it("will reject early if leaders does not have entry", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, { replicas: new AbsolutMinReplicas(2) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));
		await db3.log.log.deleteRecursively(await db3.log.log.getHeads());
		await waitForResolved(() => expect(db3.log.log.length).toEqual(0));

		expect(db2.log.log.length).toEqual(1);
		const fn = () => db2.log.safelyDelete([e1.entry], { timeout: 3000 })[0];
		await expect(fn).rejects.toThrowError(
			"Insufficient replicators to safelyDelete: " + e1.entry.hash
		);
		expect(db2.log.log.length).toEqual(1);
	});
});
