import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForAsync, waitForResolved } from "@peerbit/time";
import { AbsolutMinReplicas, maxMinReplicas } from "../replication.js";
import { Replicator } from "../role";

describe(`sharding`, () => {
	let session: LSession;
	let db1: EventStore<Uint8Array>,
		db2: EventStore<Uint8Array>,
		db3: EventStore<Uint8Array>;

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

	const sampleSize = 200; // must be < 255

	const checkReplicas = async (
		dbs: EventStore<Uint8Array>[],
		minReplicas: number,
		entryCount: number,
		noGarbage: boolean
	) => {
		const map = new Map<string, number>();
		for (const db of dbs) {
			for (const value of await db.log.log.values.toArray()) {
				expect(await db.log.log.storage.has(value.hash)).toBeTrue();
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
		const store = new EventStore<Uint8Array>();

		db1 = await session.peers[0].open(store, {
			args: {
				role: new Replicator(),
				trim: { to: 0, from: 1, type: "length" as const },
			},
		});
		db2 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[2]
		);

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		const entryCount = sampleSize;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(new Uint8Array([i]), { meta: { next: [] } });
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.log.log.values.length > entryCount * 0.5 &&
				db1.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db2), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3), {
			timeout: 20000,
			delayInterval: 500,
		});

		expect(
			db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		).toBeTrue();
		expect(
			db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			false
		);
	});

	// TODO add tests for late joining and leaving peers

	it("distributes to joining peers", async () => {
		db1 = await session.peers[0].open(new EventStore<Uint8Array>());
		db2 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[1]
		);
		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 2);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.log.log.values.length === entryCount);
		await waitFor(() => db2.log.log.values.length === entryCount);

		db3 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[2]
		);
		// client 3 will subscribe and start to recive heads before recieving subscription info about other peers

		await waitFor(() => db1.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db2), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3), {
			timeout: 20000,
			delayInterval: 500,
		});

		expect(
			db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		expect(
			db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			false
		);
	});

	it("distributes to leaving peers", async () => {
		db1 = await session.peers[0].open(new EventStore<Uint8Array>());

		db2 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[2]
		);

		const entryCount = sampleSize;

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);

		await waitFor(() => db1.log.log.values.length === entryCount);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		await waitForResolved(
			async () =>
				checkReplicas(
					[db1, db2, db3],
					maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
					entryCount,
					false
				),
			{ delayInterval: 500, timeout: 20000 }
		);

		await db3.close();

		await waitFor(() => db1.log.log.values.length === entryCount);
		await waitFor(() => db2.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			false
		);
	});

	it("handles peer joining and leaving multiple times", async () => {
		db1 = await session.peers[0].open(new EventStore<Uint8Array>());

		db2 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[2]
		);

		const entryCount = sampleSize;

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);

		await waitFor(() => db1.log.log.values.length === entryCount);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		await db3.close();
		await session.peers[2].open(db3);
		await db3.close();
		await session.peers[2].open(db3);
		await db3.close();
		await session.peers[2].open(db3);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);
		await db3.close();
		await waitFor(() => db1.log.log.values.length === entryCount);
		await waitFor(() => db2.log.log.values.length === entryCount);

		await waitForResolved(async () =>
			checkReplicas(
				[db1, db2],
				maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
				entryCount,
				false
			)
		);
	});

	it("trims responsibly", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		db1 = await session.peers[0].open(new EventStore<Uint8Array>(), {
			args: { trim: { to: 0, from: 1, type: "length" as const } },
		});
		db2 = await session.peers[1].open<EventStore<Uint8Array>>(db1.address, {
			args: { trim: { to: 0, from: 1, type: "length" as const } },
		});
		db3 = await session.peers[2].open<EventStore<Uint8Array>>(db1.address, {
			args: { trim: { to: 0, from: 1, type: "length" as const } },
		});

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);

		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.log.log.values.length > entryCount * 0.5 &&
				db1.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db1), {
			timeout: 20000,
			delayInterval: 500,
		});

		await waitForAsync(() => checkConverged(db2), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3), {
			timeout: 20000,
			delayInterval: 500,
		});

		await checkReplicas(
			[db1, db2, db3],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			true
		);

		await db3.close();
		await waitFor(() => db2.log.log.values.length === entryCount);
		await waitFor(() => db1.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			true
		);
	});

	it("trimming can resume once more peers join", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		const entryCount = sampleSize;

		const client1WantedDbSize = Math.round(0.95 * entryCount);
		db1 = await session.peers[0].open(new EventStore<Uint8Array>(), {
			args: { trim: { to: client1WantedDbSize, type: "length" as const } },
		});
		db2 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[1]
		);

		await waitFor(() => db1.log.getReplicatorsSorted()?.length === 2);

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 2);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.add(new Uint8Array([i]), { meta: { next: [] } }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.log.log.values.length === entryCount);
		await waitFor(() => db2.log.log.values.length === entryCount);

		db3 = await EventStore.open<EventStore<Uint8Array>>(
			db1.address!,
			session.peers[2]
		);

		await waitFor(() => db1.log.getReplicatorsSorted()?.length === 3);

		await waitFor(() => db2.log.getReplicatorsSorted()?.length === 3);
		await waitFor(() => db3.log.getReplicatorsSorted()?.length === 3);

		await waitFor(() => db1.log.log.values.length === client1WantedDbSize);

		await waitFor(
			() =>
				db2.log.log.values.length > entryCount * 0.5 &&
				db2.log.log.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.log.log.values.length > entryCount * 0.5 &&
				db3.log.log.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.log.log.values.length;
			await delay(2500); // arb delay
			return a === db.log.log.values.length;
		};

		await waitForAsync(() => checkConverged(db1), {
			timeout: 20000,
			delayInterval: 500,
		});

		await waitForAsync(() => checkConverged(db2), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3), {
			timeout: 20000,
			delayInterval: 500,
		});

		await checkReplicas(
			[db1, db2, db3],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			false
		);
		await db3.close();
		await waitFor(() => db2.log.log.values.length === entryCount);
		await waitFor(() => db1.log.log.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			maxMinReplicas(db1.log, [...(await db1.log.log.values.toArray())]),
			entryCount,
			true
		);
	});

	it("sets replicators groups correctly", async () => {
		const store = new EventStore<Uint8Array>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1,
				},
			},
		});

		const getDiscoveryGroupsFn = () => {
			const r = store.log.getDiscoveryGroups();
			return r.map((x) => x.map((y) => y.hash));
		};

		db1.log.getReplicatorsSorted = () =>
			["a", "b", "c", "d", "e"].map((x) => {
				return { hash: x, timestamp: +new Date() };
			});
		expect(getDiscoveryGroupsFn()).toEqual([["a"], ["b"], ["c"], ["d"], ["e"]]);
		db1.log.replicas.min = new AbsolutMinReplicas(2);
		expect(getDiscoveryGroupsFn()).toEqual([["a", "d"], ["b", "e"], ["c"]]);
		db1.log.replicas.min = new AbsolutMinReplicas(3);
		expect(getDiscoveryGroupsFn()).toEqual([
			["a", "c", "e"],
			["b", "d"],
		]);
		db1.log.replicas.min = new AbsolutMinReplicas(5);
		expect(getDiscoveryGroupsFn()).toEqual([["a", "b", "c", "d", "e"]]);
	});

	// TODO test untrusted filtering
});
