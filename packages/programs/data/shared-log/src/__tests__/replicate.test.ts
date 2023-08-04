import assert from "assert";
import mapSeries from "p-each-series";
import { Entry } from "@peerbit/log";
import { delay, waitFor, waitForAsync, waitForResolved } from "@peerbit/time";
import { EventStore, Operation } from "./utils/stores/event-store";
import { LSession } from "@peerbit/test-utils";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { AbsoluteReplicas, maxReplicas } from "../replication";
import { Observer, Replicator } from "../role";
import { ExchangeHeadsMessage } from "../exchange-heads";

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

	it("references all gids on exchange", async () => {
		const { entry: entryA } = await db1.add("a", { meta: { next: [] } });
		const { entry: entryB } = await db1.add("b", { meta: { next: [] } });
		const { entry: entryAB } = await db1.add("ab", {
			meta: { next: [entryA, entryB] },
		});

		expect(entryA.meta.gid).not.toEqual(entryB.gid);
		expect(
			entryAB.meta.gid === entryA.meta.gid ||
				entryAB.meta.gid === entryB.meta.gid
		).toBeTrue();

		let entryWithNotSameGid =
			entryAB.meta.gid === entryA.meta.gid ? entryB : entryA;

		const sendFn = db1.log.rpc.send.bind(db1.log.rpc);

		db1.log.rpc.send = async (msg, options) => {
			if (msg instanceof ExchangeHeadsMessage) {
				expect(msg.heads.map((x) => x.entry.hash)).toEqual([entryAB.hash]);
				expect(
					msg.heads.map((x) => x.references.map((y) => y.hash)).flat()
				).toEqual([entryWithNotSameGid.hash]);
			}
			return sendFn(msg, options);
		};

		let cacheLookups: Entry<any>[][] = [];
		let db1GetShallowFn = db1.log["_gidParentCache"].get.bind(
			db1.log["_gidParentCache"]
		);
		db1.log["_gidParentCache"].get = (k) => {
			const result = db1GetShallowFn(k);
			cacheLookups.push(result!);
			return result;
		};

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await waitForResolved(() => expect(db2.log.log.length).toEqual(3));

		expect(cacheLookups).toHaveLength(1);
		expect(
			cacheLookups.map((x) => x.map((y) => y.hash)).flat()
		).toContainAllValues([entryWithNotSameGid.hash, entryAB.hash]);

		await db1.close();
		expect(db1.log["_gidParentCache"].size).toEqual(0);
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
				maxReplicas(db1.log, db1Entries)
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
				maxReplicas(db2.log, db2Entries)
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
				"Did not receive all entries, missing: " +
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

describe("canReplicate", () => {
	let session: LSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const init = async (
		canReplicate: (publicKey: PublicSignKey) => Promise<boolean> | boolean
	) => {
		let min = 100;
		let max = undefined;
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min,
					max,
				},
				canReplicate,
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
					canReplicate,
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
					canReplicate,
				},
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
		await db3.waitFor(session.peers[0].peerId);
	};
	beforeEach(async () => {
		session = await LSession.connected(3);
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
	});

	afterEach(async () => {
		if (db1) await db1.drop();

		if (db2) await db2.drop();

		if (db3) await db3.drop();

		await session.stop();
	});

	it("can filter unwanted replicators", async () => {
		await init((key) => !key.equals(session.peers[0].identity.publicKey));
		const expectedReplicators = [
			session.peers[1].identity.publicKey.hashcode(),
			session.peers[2].identity.publicKey.hashcode(),
		];

		await waitForResolved(() =>
			expect(
				db1.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues(expectedReplicators)
		);
		await waitForResolved(() =>
			expect(
				db2.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues(expectedReplicators)
		);
		await waitForResolved(() =>
			expect(
				db3.log.getReplicatorsSorted()?.map((x) => x.hash)
			).toContainAllValues(expectedReplicators)
		);

		await db2.add("hello");

		const groups1 = db2.log.getDiscoveryGroups();
		expect(groups1).toHaveLength(1);
		expect(groups1[0].map((x) => x.hash)).toContainAllValues(
			expectedReplicators
		);

		const groups2 = db2.log.getDiscoveryGroups();
		expect(groups2).toHaveLength(1);
		expect(groups2[0].map((x) => x.hash)).toContainAllValues(
			expectedReplicators
		);

		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));
		await delay(1000); // Add some delay so that all replication events most likely have occured
		expect(db1.log.log.length).toEqual(0); // because not trusted for replication job
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
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
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
			replicas: new AbsoluteReplicas(1), // will be overriden by 'minReplicas' above
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
			replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
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
			replicas: new AbsoluteReplicas(1),
			meta: { next: [] },
		});
		const e2 = await db1.add(value, {
			replicas: new AbsoluteReplicas(3),
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

		const e1 = await db1.add(value, { replicas: new AbsoluteReplicas(3) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));

		// e2 only sets minReplicas to 1 which means only db2 or db3 needs to hold it
		const e2 = await db1.add(value, {
			replicas: new AbsoluteReplicas(1),
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

		const e1 = await db1.add(value, { replicas: new AbsoluteReplicas(1) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() =>
			expect(db2.log.log.length).not.toEqual(db3.log.log.length)
		);

		let dbWithEntry = db2.log.log.length === 1 ? db2 : db3;
		expect(dbWithEntry.log.log.length).toEqual(1);
		await expect(
			() => dbWithEntry.log.pruneSafely([e1.entry], { timeout: 3000 })[0]
		).rejects.toThrowError("Timeout");
		expect(dbWithEntry.log.log.length).toEqual(1); // No deletions
	});

	it("keep degree while updating role", async () => {
		let min = 1;
		let max = 1;

		// peer 1 observer
		// peer 2 observer

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
					role: new Observer(),
				},
			}
		))!;

		let db2ReorgCounter = 0;
		let db2ReplicationReorganizationFn = db2.log.replicationReorganization.bind(
			db2.log
		);
		db2.log.replicationReorganization = () => {
			db2ReorgCounter += 1;
			return db2ReplicationReorganizationFn();
		};
		await db1.add("hello");

		// peer 1 observer
		// peer 2 replicator (will get entry)

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await db2.log.updateRole(new Replicator());
		expect(db2ReorgCounter).toEqual(0); // since was oberver, which means I should not have any meaningful data
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));

		// peer 1 removed
		// peer 2 replicator (has entry)
		await db1.drop();

		// peer 1 observer
		// peer 2 replicator (has entry)
		await db1.open({
			replicas: {
				min,
				max,
			},
			role: new Observer(),
		});

		// peer 1 observer
		// peer 2 observer
		expect(db2.log.log.length).toEqual(1);
		await delay(2000);
		expect(db2ReorgCounter).toEqual(0); // no reorgs as db1 is dropping and  opening as Observer

		await db2.log.updateRole(new Observer());
		expect(db2ReorgCounter).toEqual(1);
		expect(db2.log.role instanceof Observer).toBeTrue();

		// peer 1 replicator (will get entry)
		// peer 2 observer (will safely delete the entry)
		await db1.log.updateRole(new Replicator());
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(0));
		expect(db2ReorgCounter).toEqual(2);
	});

	it("time out when pending IHave are never resolve", async () => {
		let min = 1;
		let max = 1;

		// peer 1 observer
		// peer 2 observer

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min,
					max,
				},
				role: new Observer(),
			},
		});

		let respondToIHaveTimeout = 3000;
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max,
					},
					role: new Replicator(),
					respondToIHaveTimeout,
				},
			}
		);

		const onMessageFn = db2.log._onMessage.bind(db2.log);
		db2.log.rpc["_responseHandler"] = async (msg, cxt) => {
			if (msg instanceof ExchangeHeadsMessage) {
				return; // prevent replication
			}
			return onMessageFn(msg, cxt);
		};
		const { entry } = await db1.add("hello");
		const expectPromise = expect(
			() => db1.log.pruneSafely([entry], { timeout: 3000 })[0]
		).rejects.toThrowError("Timeout");
		await waitForResolved(() =>
			expect(db2.log["_pendingIHave"].size).toEqual(1)
		);
		await delay(respondToIHaveTimeout);
		expect(db2.log["_pendingIHave"].size).toEqual(0);
		await expectPromise;
	});

	/*  TODO feat
	it("will reject early if leaders does not have entry", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, { replicas: new AbsoluteReplicas(2) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));
		await db3.log.log.deleteRecursively(await db3.log.log.getHeads());
		await waitForResolved(() => expect(db3.log.log.length).toEqual(0));

		expect(db2.log.log.length).toEqual(1);
		const fn = () => db2.log.safelyDelete([e1.entry], { timeout: 3000 })[0];
		await expect(fn).rejects.toThrowError(
			"Insufficient replicators to safely delete: " + e1.entry.hash
		);
		expect(db2.log.log.length).toEqual(1);
	}); */
});
