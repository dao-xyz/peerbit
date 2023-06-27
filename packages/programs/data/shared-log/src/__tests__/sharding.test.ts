import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForAsync, waitForResolved } from "@peerbit/time";
import { PermissionedEventStore } from "./utils/stores/test-store";
import { AbsolutMinReplicas } from "../exchange-heads";
import { Replicator } from "../role";

describe(`sharding`, () => {
	let session: LSession;
	let db1: PermissionedEventStore,
		db2: PermissionedEventStore,
		db3: PermissionedEventStore;

	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	afterEach(async () => {
		await db1?.drop();
		await db2?.drop();
		await db3?.drop();
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
	});

	afterAll(async () => {
		await session.stop();
	});

	const sampleSize = 200;

	const checkReplicas = async (
		dbs: PermissionedEventStore[],
		minReplicas: number,
		entryCount: number,
		noGarbage: boolean
	) => {
		const map = new Map<string, number>();
		for (const db of dbs) {
			for (const value of await db.store.log.log.values.toArray()) {
				expect(await db.store.log.log.storage.has(value.hash)).toBeTrue();
				map.set(value.hash, (map.get(value.hash) || 0) + 1);
			}
		}
		for (const [k, v] of map) {
			if (noGarbage) {
				expect(v).toEqual(minReplicas);
			} else {
				expect(v).toBeGreaterThanOrEqual(minReplicas);
				expect(v).toBeLessThanOrEqual(dbs.length);
			}
		}
		expect(map.size).toEqual(entryCount);
	};

	it("can distribute evenly among peers", async () => {
		const store = new PermissionedEventStore({
			trusted: session.peers.map((x) => x.peerId),
		});

		db1 = await session.peers[0].open(store, {
			args: {
				role: new Replicator(),
				trim: { to: 0, from: 1, type: "length" as const },
			},
		});
		db2 = await PermissionedEventStore.open(db1.address!, session.peers[1]);
		db3 = await PermissionedEventStore.open(db1.address!, session.peers[2]);

		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		const entryCount = sampleSize;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.store.add(i.toString(), { nexts: [] });
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.store.log.log.values.length > entryCount * 0.5 &&
				db1.store.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		expect(
			db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		).toBeTrue();
		expect(
			db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			db1.store.log.minReplicas.value,
			entryCount,
			false
		);
	});

	// TODO add tests for late joining and leaving peers

	it("distributes to joining peers", async () => {
		db1 = await session.peers[0].open(
			new PermissionedEventStore({
				trusted: session.peers.map((x) => x.peerId),
			})
		);
		db2 = await PermissionedEventStore.open(db1.address!, session.peers[1]);
		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 2);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.log.log.values.length === entryCount);
		await waitFor(() => db2.store.log.log.values.length === entryCount);

		db3 = await PermissionedEventStore.open(db1.address!, session.peers[2]);
		// client 3 will subscribe and start to recive heads before recieving subscription info about other peers

		await waitFor(() => db1.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		expect(
			db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		expect(
			db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			db1.store.log.minReplicas.value,
			entryCount,
			false
		);
	});

	it("distributes to leaving peers", async () => {
		db1 = await session.peers[0].open(
			new PermissionedEventStore({
				trusted: session.peers.map((x) => x.peerId),
			})
		);

		db2 = await PermissionedEventStore.open(db1.address!, session.peers[1]);
		db3 = await PermissionedEventStore.open(db1.address!, session.peers[2]);

		const entryCount = sampleSize;

		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(() => db1.store.log.log.values.length === entryCount);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		await waitForResolved(
			() =>
				checkReplicas(
					[db1, db2, db3],
					db1.store.log.minReplicas.value,
					entryCount,
					false
				),
			{ delayInterval: 500, timeout: 20000 }
		);

		await db3.close();

		await waitFor(() => db1.store.log.log.values.length === entryCount);
		await waitFor(() => db2.store.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			db1.store.log.minReplicas.value,
			entryCount,
			false
		);
	});

	it("handles peer joining and leaving multiple times", async () => {
		db1 = await session.peers[0].open(
			new PermissionedEventStore({
				trusted: session.peers.map((x) => x.peerId),
			})
		);

		db2 = await PermissionedEventStore.open(db1.address!, session.peers[1]);
		db3 = await PermissionedEventStore.open(db1.address!, session.peers[2]);

		const entryCount = sampleSize;

		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(() => db1.store.log.log.values.length === entryCount);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		await db3.close();
		await session.peers[2].open(db3);
		await db3.close();
		await session.peers[2].open(db3);
		await db3.close();
		await session.peers[2].open(db3);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);
		await db3.close();
		await waitFor(() => db1.store.log.log.values.length === entryCount);
		await waitFor(() => db2.store.log.log.values.length === entryCount);

		await waitForResolved(() =>
			checkReplicas(
				[db1, db2],
				db1.store.log.minReplicas.value,
				entryCount,
				false
			)
		);
	});

	it("trims responsibly", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		db1 = await session.peers[0].open(
			new PermissionedEventStore({
				trusted: session.peers.map((x) => x.peerId),
			}),
			{
				args: { trim: { to: 0, from: 1, type: "length" as const } },
			}
		);
		db2 = await session.peers[1].open<PermissionedEventStore>(db1.address, {
			args: { trim: { to: 0, from: 1, type: "length" as const } },
		});
		db3 = await session.peers[2].open<PermissionedEventStore>(db1.address, {
			args: { trim: { to: 0, from: 1, type: "length" as const } },
		});
		try {
			await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		} catch (error) {
			const q = 123;
		}

		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.store.log.log.values.length > entryCount * 0.5 &&
				db1.store.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db1.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		await checkReplicas(
			[db1, db2, db3],
			db1.store.log.minReplicas.value,
			entryCount,
			true
		);

		await db3.close();
		await waitFor(() => db2.store.log.log.values.length === entryCount);
		await waitFor(() => db1.store.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			db1.store.log.minReplicas.value,
			entryCount,
			true
		);
	});

	it("trimming can resume once more peers join", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		const entryCount = sampleSize;

		const client1WantedDbSize = Math.round(0.95 * entryCount);
		db1 = await session.peers[0].open(
			new PermissionedEventStore({
				trusted: session.peers.map((x) => x.peerId),
			}),
			{
				args: { trim: { to: client1WantedDbSize, type: "length" as const } },
			}
		);
		db2 = await PermissionedEventStore.open(db1.address!, session.peers[1]);

		await waitFor(() => db1.store.log.getReplicatorsSorted()?.length === 2);

		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 2);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.log.log.values.length === entryCount);
		await waitFor(() => db2.store.log.log.values.length === entryCount);

		db3 = await PermissionedEventStore.open(db1.address!, session.peers[2]);

		await waitFor(() => db1.store.log.getReplicatorsSorted()?.length === 3);

		await waitFor(() => db2.store.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.store.log.getReplicatorsSorted()?.length === 3);

		await waitFor(
			() => db1.store.log.log.values.length === client1WantedDbSize
		);

		await waitFor(
			() =>
				db2.store.log.log.values.length > entryCount * 0.5 &&
				db2.store.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.store.log.log.values.length > entryCount * 0.5 &&
				db3.store.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db1.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});

		await checkReplicas(
			[db1, db2, db3],
			db1.store.log.minReplicas.value,
			entryCount,
			false
		);
		await db3.close();
		await waitFor(() => db2.store.log.log.values.length === entryCount);
		await waitFor(() => db1.store.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			db1.store.log.minReplicas.value,
			entryCount,
			true
		);
	});

	it("sets replicators groups correctly", async () => {
		const store = new PermissionedEventStore({
			trusted: session.peers.map((x) => x.peerId),
		});

		db1 = await session.peers[0].open(store, {
			args: { minReplicas: 1 },
		});

		const replicatorsFn = store.store.log.replicators.bind(store.store.log);

		db1.store.log.getReplicatorsSorted = () => ["a", "b", "c", "d", "e"];
		expect(replicatorsFn()).toEqual([["a"], ["b"], ["c"], ["d"], ["e"]]);
		db1.store.log.minReplicas = new AbsolutMinReplicas(2);
		expect(replicatorsFn()).toEqual([["a", "d"], ["b", "e"], ["c"]]);
		db1.store.log.minReplicas = new AbsolutMinReplicas(3);
		expect(replicatorsFn()).toEqual([
			["a", "c", "e"],
			["b", "d"],
		]);
		db1.store.log.minReplicas = new AbsolutMinReplicas(5);
		expect(replicatorsFn()).toEqual([["a", "b", "c", "d", "e"]]);
	});

	// TODO test untrusted filtering
});
