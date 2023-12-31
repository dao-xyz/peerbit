import mapSeries from "p-each-series";
import { Entry } from "@peerbit/log";
import { delay, waitForResolved } from "@peerbit/time";
import { EventStore, Operation } from "./utils/stores/event-store";
import { TestSession } from "@peerbit/test-utils";
import {
	Ed25519Keypair,
	PublicSignKey,
	getPublicKeyFromPeerId
} from "@peerbit/crypto";
import { AbsoluteReplicas, decodeReplicas, maxReplicas } from "../replication";
import { Observer, Replicator } from "../role";
import { ExchangeHeadsMessage } from "../exchange-heads";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { jest } from "@jest/globals";

describe(`exchange`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;
	let fetchEvents: number;
	let fetchHashes: Set<string>;
	let fromMultihash: any;
	beforeAll(() => {
		jest.retryTimes(5);

		fromMultihash = Entry.fromMultihash;

		// TODO monkeypatching might lead to sideeffects in other tests!
		Entry.fromMultihash = (s, h, o) => {
			fetchHashes.add(h);
			fetchEvents += 1;
			return fromMultihash(s, h, o);
		};
	});
	afterAll(() => {
		jest.retryTimes(1);

		Entry.fromMultihash = fromMultihash;
	});

	beforeEach(async () => {
		fetchEvents = 0;
		fetchHashes = new Set();
		session = await TestSession.connected(2);

		db1 = await session.peers[0].open(new EventStore<string>());
	});

	afterEach(async () => {
		if (db1) await db1.drop();

		if (db2) await db2.drop();

		await session.stop();
	});

	it("verifies remote signatures by default", async () => {
		const entry = await db1.add("a", { meta: { next: [] } });
		await session.peers[0]["libp2p"].hangUp(session.peers[1].peerId);
		db2 = await session.peers[1].open(new EventStore<string>());

		const clonedEntry = deserialize(serialize(entry.entry), Entry);

		let verified = false;
		const verifyFn = clonedEntry.verifySignatures.bind(clonedEntry);

		clonedEntry.verifySignatures = () => {
			verified = true;
			return verifyFn();
		};
		await db2.log.log.join([clonedEntry]);
		expect(verified).toBeTrue();
	});

	it("does not verify owned signatures by default", async () => {
		const entry = await db1.add("a", { meta: { next: [] } });
		await session.peers[0]["libp2p"].hangUp(session.peers[1].peerId);
		db2 = await session.peers[1].open(new EventStore<string>());

		const clonedEntry = deserialize(serialize(entry.entry), Entry);

		let verified = false;
		const verifyFn = clonedEntry.verifySignatures.bind(clonedEntry);
		clonedEntry.createdLocally = true;
		clonedEntry.verifySignatures = () => {
			verified = true;
			return verifyFn();
		};
		await db2.log.log.join([clonedEntry]);
		expect(verified).toBeFalse();
	});

	it("references all gids on exchange", async () => {
		const { entry: entryA } = await db1.add("a", { meta: { next: [] } });
		const { entry: entryB } = await db1.add("b", { meta: { next: [] } });
		const { entry: entryAB } = await db1.add("ab", {
			meta: { next: [entryA, entryB] }
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

		await waitForResolved(async () =>
			expect(
				await db1.log.findLeaders(
					db1Entries[0].gid,
					maxReplicas(db1.log, db1Entries)
					// 0
				)
			).toContainAllValues(
				[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
					getPublicKeyFromPeerId(p).hashcode()
				)
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
				// 0
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
			await waitForResolved(() =>
				expect(db2.log.log.length).toEqual(entryCount)
			);
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

	describe("info", () => {
		it("insertion", async () => {
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
			await waitForResolved(() =>
				expect(db2.log.log.length).toEqual(entryCount)
			);

			// All entries should be in the database
			expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
				entryCount
			);

			// progress events should increase monotonically
			expect(fetchEvents).toEqual(fetchHashes.size);
			expect(fetchEvents).toEqual(0); // becausel all entries were sent
		});
		it("open after insertion", async () => {
			const entryCount = 15;

			// Trigger replication
			const adds: number[] = [];
			for (let i = 0; i < entryCount; i++) {
				adds.push(i);
			}

			const add = async (i: number) => {
				await db1.add("hello " + i);
			};

			await mapSeries(adds, add);

			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1]
			))!;

			// All entries should be in the database
			await waitForResolved(() =>
				expect(db2.log.log.length).toEqual(entryCount)
			);

			// progress events should (increase monotonically)
			expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
				entryCount
			);
			expect(fetchEvents).toEqual(fetchHashes.size);
			expect(fetchEvents).toEqual(entryCount - 1); // - 1 because we also send some references for faster syncing (see exchange-heads.ts)
		});

		it("two-way replication", async () => {
			const entryCount = 15;

			// Trigger replication
			const adds: number[] = [];
			for (let i = 0; i < entryCount; i++) {
				adds.push(i);
			}

			const add = async (i: number) => {
				await Promise.all([db1.add("hello-1-" + i), db2.add("hello-2-" + i)]);
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
			await waitForResolved(async () =>
				expect((await db2.iterator({ limit: -1 })).collect().length).toEqual(
					entryCount * 2
				)
			);

			// Database values should match

			await waitForResolved(() =>
				expect(db1.log.log.values.length).toEqual(db2.log.log.values.length)
			);

			const values1 = (await db1.iterator({ limit: -1 })).collect();
			const values2 = (await db2.iterator({ limit: -1 })).collect();
			expect(values1.length).toEqual(values2.length);
			for (let i = 0; i < values1.length; i++) {
				expect(values1[i].equals(values2[i])).toBeTrue();
			}
			// All entries should be in the database
			expect(values1.length).toEqual(entryCount * 2);
			expect(values2.length).toEqual(entryCount * 2);
		});
	});
});

describe("canReplicate", () => {
	let session: TestSession;
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
					max
				},
				canReplicate
			}
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max
					},
					canReplicate
				}
			}
		))!;

		db3 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicas: {
						min,
						max
					},
					canReplicate
				}
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
		await db3.waitFor(session.peers[0].peerId);
	};
	beforeEach(async () => {
		session = await TestSession.connected(3);
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
		// allow all replicaotors except node 0
		await init((key) => !key.equals(session.peers[0].identity.publicKey));

		const expectedReplicators = [
			session.peers[1].identity.publicKey.hashcode(),
			session.peers[2].identity.publicKey.hashcode()
		];

		await waitForResolved(() =>
			expect(
				db1.log
					.getReplicatorsSorted()!
					.toArray()
					.map((x) => x.publicKey.hashcode())
			).toContainAllValues(expectedReplicators)
		);
		await waitForResolved(() =>
			expect(
				db2.log
					.getReplicatorsSorted()!
					.toArray()
					.map((x) => x.publicKey.hashcode())
			).toContainAllValues(expectedReplicators)
		);
		await waitForResolved(() =>
			expect(
				db3.log
					.getReplicatorsSorted()!
					.toArray()
					.map((x) => x.publicKey.hashcode())
			).toContainAllValues(expectedReplicators)
		);

		await db2.add("hello");

		const groups1 = db2.log.getReplicatorUnion(0);
		expect(groups1).toHaveLength(1); // min replicas = 2 (because only 2 peers in network), which means we only need to query 1 peer to find all docs

		const groups2 = db2.log.getReplicatorUnion(0);
		expect(groups2).toHaveLength(1); // min replicas = 2 (because only 2 peers in network), which means we only need to query 1 peer to find all docs

		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db3.log.log.length).toEqual(1));
		await delay(1000); // Add some delay so that all replication events most likely have occured
		expect(db1.log.log.length).toEqual(0); // because not trusted for replication job
	});
});

describe("replication degree", () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const init = async (min: number, max?: number) => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min,
					max
				},
				role: "observer"
			}
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max
					}
				}
			}
		))!;

		db3 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicas: {
						min,
						max
					}
				}
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
		await db2.waitFor(session.peers[2].peerId);
		await db3.waitFor(session.peers[0].peerId);
	};
	beforeEach(async () => {
		session = await TestSession.connected(3, [
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
			},

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
			}
		]);
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
		let minReplicas = 1;
		let maxReplicas = 1;

		await init(minReplicas, maxReplicas);

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
				meta: { next: [] }
			});
		}

		await waitForResolved(() => {
			expect(db1.log.log.length).toEqual(entryCount);
			let total = db2.log.log.length + db3.log.log.length;
			expect(total).toBeGreaterThanOrEqual(entryCount);
			expect(total).toBeLessThan(entryCount * 2);
			expect(db2.log.log.length).toBeGreaterThan(entryCount * 0.2);
			expect(db3.log.log.length).toBeGreaterThan(entryCount * 0.2);
		});
	});

	it("will prune once reaching max replicas", async () => {
		await session.stop();
		session = await TestSession.disconnected(3);

		let minReplicas = 1;
		let maxReplicas = 1;

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas
				},
				role: "observer"
			}
		});
		db2 = (await session.peers[1].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas
				}
			}
		}))!;

		db3 = (await session.peers[2].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas
				}
			}
		}))!;

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
				meta: { next: [] }
			});
		}

		await session.peers[1].dial(session.peers[0].getMultiaddrs());
		await waitForResolved(() => expect(db2.log.log.length).toEqual(entryCount));

		await db2.close();

		session.peers[2].dial(session.peers[0].getMultiaddrs());
		await waitForResolved(() => expect(db3.log.log.length).toEqual(entryCount));

		// reopen db2 again and make sure either db3 or db2 drops the entry (not both need to replicate)
		await delay(2000);
		db2 = await session.peers[1].open(db2, {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas
				}
			}
		});

		await waitForResolved(() => {
			expect(db1.log.log.length).toEqual(entryCount);
			let total = db2.log.log.length + db3.log.log.length;
			expect(total).toBeGreaterThanOrEqual(entryCount);
			expect(total).toBeLessThan(entryCount * 2);
			expect(db2.log.log.length).toBeGreaterThan(entryCount * 0.2);
			expect(db3.log.log.length).toBeGreaterThan(entryCount * 0.2);
		});
	});

	it("control per commmit", async () => {
		await init(1);

		const value = "hello";

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add(value, {
				replicas: new AbsoluteReplicas(1),
				meta: { next: [] }
			});
			await db1.add(value, {
				replicas: new AbsoluteReplicas(3),
				meta: { next: [] }
			});
		}

		// expect e1 to be replicated at db1 and/or 1 other peer (when you write you always store locally)
		// expect e2 to be replicated everywhere
		const check = async (log: EventStore<string>) => {
			let replicated3Times = 0;
			let other = 0;
			for (const entry of await log.log.log.toArray()) {
				if (decodeReplicas(entry).getValue(db2.log) === 3) {
					replicated3Times += 1;
				} else {
					other += 1;
				}
			}
			expect(replicated3Times).toEqual(entryCount);
			expect(other).toBeGreaterThan(0);
		};
		await waitForResolved(() => check(db2));
		await waitForResolved(() => check(db3));
	});

	it("min replicas with be maximum value for gid", async () => {
		await init(1);

		// followwing entries set minReplicas to 1 which means only db2 or db3 needs to hold it
		const entryCount = 100;
		for (let i = 0; i < entryCount / 2; i++) {
			const e1 = await db1.add(String(i), {
				replicas: new AbsoluteReplicas(3),
				meta: { next: [] }
			});
			await db1.add(String(i), {
				replicas: new AbsoluteReplicas(1), // will be overriden by 'maxReplicas' above
				meta: { next: [e1.entry] }
			});
		}

		await waitForResolved(() => {
			expect(db1.log.log.length).toEqual(entryCount);
			let total = db2.log.log.length + db3.log.log.length;
			expect(total).toBeGreaterThanOrEqual(entryCount);
			expect(total).toBeLessThan(entryCount * 2);
			expect(db2.log.log.length).toBeGreaterThan(entryCount * 0.2);
			expect(db3.log.log.length).toBeGreaterThan(entryCount * 0.2);
		});
	});

	it("observer will not delete unless replicated", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: 10
				},
				role: "observer"
			}
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 10
					},
					role: {
						type: "replicator",
						factor: 1
					}
				}
			}
		))!;

		const e1 = await db1.add("hello");

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await delay(3000);
		await expect(
			async () => (await db1.log.prune([e1.entry], { timeout: 3000 }))[0]
		).rejects.toThrowError("Timeout for checked pruning");
		expect(db1.log.log.length).toEqual(1); // No deletions
	});

	it("replicator will not delete unless replicated", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: 10
				},
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 10
					},
					role: {
						type: "replicator",
						factor: 1
					}
				}
			}
		))!;

		const e1 = await db1.add("hello");
		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));
		await expect(() =>
			db1.log.prune([e1.entry], { timeout: 3000 })
		).rejects.toThrowError("Failed to delete, is leader");
		expect(db1.log.log.length).toEqual(1); // No deletions */
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
					max
				},
				role: "observer"
			}
		});

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max
					},
					role: "observer"
				}
			}
		))!;

		let db2ReorgCounter = 0;
		let db2ReplicationReorganizationFn = db2.log.distribute.bind(db2.log);
		db2.log.distribute = () => {
			db2ReorgCounter += 1;
			return db2ReplicationReorganizationFn();
		};
		await db1.add("hello");

		// peer 1 observer
		// peer 2 replicator (will get entry)

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		expect(db2ReorgCounter).toEqual(0);
		await db2.log.updateRole({
			type: "replicator",
			factor: 1
		});
		expect(db2ReorgCounter).toEqual(1);
		await waitForResolved(() => expect(db2.log.log.length).toEqual(1));

		// peer 1 removed
		// peer 2 replicator (has entry)
		await db1.drop();

		// peer 1 observer
		// peer 2 replicator (has entry)
		await session.peers[0].open(db1, {
			args: {
				replicas: {
					min,
					max
				},
				role: "observer"
			}
		});

		// peer 1 observer
		// peer 2 observer
		expect(db2.log.log.length).toEqual(1);
		await delay(2000);
		expect(db2ReorgCounter).toEqual(1);

		await db2.log.updateRole("observer");
		expect(db2ReorgCounter).toEqual(2);
		expect(db2.log.role instanceof Observer).toBeTrue();

		// peer 1 replicator (will get entry)
		// peer 2 observer (will safely delete the entry)
		await db1.log.updateRole({
			type: "replicator",
			factor: 1
		});

		await waitForResolved(() => expect(db1.log.log.length).toEqual(1));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(0));
		// expect(db2ReorgCounter).toEqual(3); TODO
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
					max
				},
				role: {
					type: "replicator",
					factor: 0
				}
			}
		});

		let respondToIHaveTimeout = 3000;
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min,
						max
					},
					role: {
						type: "replicator",
						factor: 1
					},
					respondToIHaveTimeout
				}
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
		const expectPromise = expect(() =>
			db1.log.prune([entry], { timeout: 3000 })
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
