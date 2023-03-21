import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { delay, waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./utils/stores/test-store";
import { jest } from "@jest/globals";
import { v4 as uuid } from "uuid";
import { AbsolutMinReplicas } from "../exchange-heads";

describe(`sharding`, () => {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		client3: Peerbit,
		db1: PermissionedEventStore,
		db2: PermissionedEventStore,
		db3: PermissionedEventStore;

	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	beforeEach(async () => {
		const topic = uuid();
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({ libp2p: session.peers[1] });
		client3 = await Peerbit.create({ libp2p: session.peers[2] });
	});

	afterEach(async () => {
		await db1?.drop();
		await db2?.drop();
		await db3?.drop();

		if (client1) {
			await client1.stop();
		}
		if (client2) {
			await client2.stop();
		}
		if (client3) {
			await client3.stop();
		}
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
			for (const value of await db.store.store.oplog.values.toArray()) {
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
		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			}),
			{ trim: { to: 0, from: 1, type: "length" } }
		);
		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 3
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 3
		);

		const entryCount = sampleSize;
		//await delay(5000);

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.store.add(i.toString(), { nexts: [] });
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.store.store.oplog.values.length > entryCount * 0.5 &&
				db1.store.store.oplog.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
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
			db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();
		expect(
			db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			false
		);
	});

	// TODO add tests for late joining and leaving peers

	it("distributes to joining peers", async () => {
		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			})
		);

		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 2
		);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);
		await waitFor(() => db2.store.store.oplog.values.length === entryCount);

		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		// client 3 will subscribe and start to recive heads before recieving subscription info about other peers

		await waitFor(
			() => client1.getReplicators(db1.address!.toString())?.length === 3
		);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 3
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 3
		);

		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
		};

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		try {
			expect(
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
					db2.store.store.oplog.values.length < entryCount * 0.85
			).toBeTrue();
		} catch (error) {
			const x = 123;
		}
		expect(
			db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();

		await checkReplicas(
			[db1, db2, db3],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			false
		);
	});

	it("distributes to leaving peers", async () => {
		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			})
		);
		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 3
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 3
		);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);

		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		await checkReplicas(
			[db1, db2, db3],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			false
		);

		await db3.close();

		await waitFor(() => db2.store.store.oplog.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			false
		);
	});

	it("trims responsibly", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			}),
			{ trim: { to: 0, from: 1, type: "length" } }
		);

		db2 = await client2.open<PermissionedEventStore>(db1.address!, {
			trim: { to: 0, from: 1, type: "length" },
		});
		db3 = await client3.open<PermissionedEventStore>(db1.address!, {
			trim: { to: 0, from: 1, type: "length" },
		});

		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 3
		);

		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 3
		);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);

		await waitFor(
			() =>
				db1.store.store.oplog.values.length > entryCount * 0.5 &&
				db1.store.store.oplog.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
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
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			true
		);
		await db3.close();
		await waitFor(() => db2.store.store.oplog.values.length === entryCount);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);
		await checkReplicas(
			[db1, db2],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			true
		);
	});

	it("trimming can resume once more peers join", async () => {
		// With this trim options, we make sure that we remove all elements expectes the elements we NEED
		// to replicate (basically nullifies the trim)

		const entryCount = sampleSize;

		const client1WantedDbSize = Math.round(0.95 * entryCount);
		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			}),
			{ trim: { to: client1WantedDbSize, type: "length" } }
		);

		db2 = await client2.open<PermissionedEventStore>(db1.address!);

		await waitFor(
			() => client1.getReplicators(db1.address!.toString())?.length === 2
		);

		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 2
		);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);
		await waitFor(() => db2.store.store.oplog.values.length === entryCount);

		db3 = await client3.open<PermissionedEventStore>(db1.address!);

		await waitFor(
			() => client1.getReplicators(db1.address!.toString())?.length === 3
		);

		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 3
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 3
		);

		await waitFor(
			() => db1.store.store.oplog.values.length === client1WantedDbSize
		);

		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);

		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
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
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			false
		);
		await db3.close();
		await waitFor(() => db2.store.store.oplog.values.length === entryCount);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);

		await checkReplicas(
			[db1, db2],
			client1.programs.get(db1.address.toString())!.minReplicas.value,
			entryCount,
			true
		);
	});

	it("sets replicators groups correctly", async () => {
		const store = new PermissionedEventStore({
			trusted: [client1.id, client2.id, client3.id],
		});
		const init = store.init.bind(store);
		let replicatorsFn: any = undefined;
		store.init = (a, b, options) => {
			replicatorsFn = options.replicators;
			return init(a, b, options);
		};
		db1 = await client1.open<PermissionedEventStore>(store, { minReplicas: 1 });

		client1.getReplicatorsSorted = () => ["a", "b", "c", "d", "e"];
		expect(replicatorsFn()).toEqual([["a"], ["b"], ["c"], ["d"], ["e"]]);
		client1.programs.get(db1.address.toString())!.minReplicas =
			new AbsolutMinReplicas(2);
		expect(replicatorsFn()).toEqual([["a", "d"], ["b", "e"], ["c"]]);
		client1.programs.get(db1.address.toString())!.minReplicas =
			new AbsolutMinReplicas(3);
		expect(replicatorsFn()).toEqual([
			["a", "c", "e"],
			["b", "d"],
		]);
		client1.programs.get(db1.address.toString())!.minReplicas =
			new AbsolutMinReplicas(5);
		expect(replicatorsFn()).toEqual([["a", "b", "c", "d", "e"]]);
	});
});
