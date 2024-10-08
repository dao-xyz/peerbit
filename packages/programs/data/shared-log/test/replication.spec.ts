import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	Ed25519Keypair,
	type PublicSignKey,
	getPublicKeyFromPeerId,
	randomBytes,
	toBase64,
} from "@peerbit/crypto";
import { SearchRequest } from "@peerbit/indexer-interface";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import mapSeries from "p-each-series";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import type { ReplicationOptions } from "../src/index.js";
import {
	AbsoluteReplicas,
	type ReplicationRangeIndexable,
	decodeReplicas,
	maxReplicas,
} from "../src/replication.js";
import { collectMessages, getReceivedHeads } from "./utils.js";
import { EventStore, type Operation } from "./utils/stores/event-store.js";

describe(`replication`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;
	let fetchEvents: number;
	let fetchHashes: Set<string>;
	let fromMultihash: any;
	before(() => {
		fromMultihash = Entry.fromMultihash;
		// TODO monkeypatching might lead to sideeffects in other tests!
		Entry.fromMultihash = (s, h, o) => {
			fetchHashes.add(h);
			fetchEvents += 1;
			return fromMultihash(s, h, o);
		};
	});
	after(() => {
		Entry.fromMultihash = fromMultihash;
	});

	beforeEach(async () => {
		fetchEvents = 0;
		fetchHashes = new Set();
		session = await TestSession.connected(2, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});
	});

	afterEach(async () => {
		if (db1 && db1.closed === false) {
			await db1.drop();
		}
		if (db2 && db2.closed === false) {
			await db2.drop();
		}

		await session.stop();
	});

	it("verifies remote signatures by default", async () => {
		const entry = await db1.add("a", { meta: { next: [] } });
		await (session.peers[0] as any)["libp2p"].hangUp(session.peers[1].peerId);
		db2 = await session.peers[1].open(new EventStore<string>());

		const clonedEntry = deserialize(serialize(entry.entry), Entry);

		let verified = false;
		const verifyFn = clonedEntry.verifySignatures.bind(clonedEntry);

		clonedEntry.verifySignatures = () => {
			verified = true;
			return verifyFn();
		};
		await db2.log.log.join([clonedEntry]);
		expect(verified).to.be.true;
	});

	it("does not verify owned signatures by default", async () => {
		const entry = await db1.add("a", { meta: { next: [] } });
		await (session.peers[0] as any)["libp2p"].hangUp(session.peers[1].peerId);
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
		expect(verified).to.be.false;
	});

	it("logs are unique", async () => {
		const entryCount = 33;
		const entryArr: number[] = [];

		const db1 = await session.peers[0].open(new EventStore<string>());
		const db3 = await session.peers[0].open(new EventStore<string>());

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		))!;

		const db4 = (await EventStore.open<EventStore<string>>(
			db3.address!,
			session.peers[1],
		))!;

		await waitForResolved(async () =>
			expect((await db2.iterator({ limit: -1 })).collect()).to.have.length(
				entryCount,
			),
		);

		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).equal(result2.length);
		for (let i = 0; i < result1.length; i++) {
			expect(result1[i].equals(result2[i])).to.be.true;
		}

		expect(db3.log.log.length).equal(0);
		expect(db4.log.log.length).equal(0);
	});

	describe("references", () => {
		it("joins by references", async () => {
			db1.log.replicas = { min: new AbsoluteReplicas(1) };
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1,
						},
						waitForReplicatorTimeout: 2000,
					},
				},
			))!;
			await db1.log.replicate({ factor: 0.5 });
			await db2.log.replicate({ factor: 0.5 });

			const getParticipationPerPer = (ranges: ReplicationRangeIndexable[]) => {
				let map = new Map<string, number>();
				for (const range of ranges) {
					map.set(
						range.hash,
						(map.get(range.hash) || 0) + range.widthNormalized,
					);
				}
				return map;
			};

			await waitForResolved(async () =>
				expect(
					[
						...getParticipationPerPer(
							(
								await db1.log.replicationIndex.query(
									new SearchRequest({ fetch: 0xffffffff }),
								)
							).results.map((x) => x.value),
						).values(),
					].map((x) => Math.round(x * 200)),
				).to.deep.equal([100, 100]),
			);

			await waitForResolved(async () =>
				expect(
					[
						...getParticipationPerPer(
							(
								await db2.log.replicationIndex.query(
									new SearchRequest({ fetch: 0xffffffff }),
								)
							).results.map((x) => x.value),
						).values(),
					].map((x) => Math.round(x * 200)),
				).to.deep.equal([100, 100]),
			);

			const { entry: entryA } = await db1.add("a", {
				meta: { next: [], gidSeed: new Uint8Array([1]) },
			});
			const { entry: entryB } = await db1.add("b", {
				meta: { next: [], gidSeed: new Uint8Array([0]) },
			});
			await db1.add("ab", {
				meta: { next: [entryA, entryB] },
			});

			await waitForResolved(() => {
				expect(Math.max(db1.log.log.length, db2.log.log.length)).equal(3); // one is now responsible for everything
				expect(Math.min(db1.log.log.length, db2.log.log.length)).equal(0); // one has to do nothing
			});
		});
		it("references all gids on exchange", async () => {
			const { entry: entryA } = await db1.add("a", { meta: { next: [] } });
			const { entry: entryB } = await db1.add("b", { meta: { next: [] } });
			const { entry: entryAB } = await db1.add("ab", {
				meta: { next: [entryA, entryB] },
			});

			expect(entryA.meta.gid).not.equal(entryB.meta.gid);
			expect(
				entryAB.meta.gid === entryA.meta.gid ||
					entryAB.meta.gid === entryB.meta.gid,
			).to.be.true;

			let entryWithNotSameGid =
				entryAB.meta.gid === entryA.meta.gid
					? entryB.meta.gid
					: entryA.meta.gid;

			const sendFn = db1.log.rpc.send.bind(db1.log.rpc);

			db1.log.rpc.send = async (msg, options) => {
				if (msg instanceof ExchangeHeadsMessage) {
					expect(msg.heads.map((x) => x.entry.hash)).to.deep.equal([
						entryAB.hash,
					]);
					expect(
						msg.heads.map((x) => x.gidRefrences.map((y) => y)).flat(),
					).to.deep.equal([entryWithNotSameGid]);
				}
				return sendFn(msg, options);
			};

			let cacheLookups: Entry<any>[][] = [];
			let db1GetShallowFn = db1.log["_gidParentCache"].get.bind(
				db1.log["_gidParentCache"],
			);
			db1.log["_gidParentCache"].get = (k) => {
				const result = db1GetShallowFn(k);
				cacheLookups.push(result!);
				return result;
			};

			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							factor: 1,
						},
					},
				},
			))!;

			await waitForResolved(() => expect(db2.log.log.length).equal(3));

			expect(cacheLookups).to.have.length(1);
			expect(
				cacheLookups.map((x) => x.map((y) => y.meta.gid)).flat(),
			).to.have.members([entryWithNotSameGid, entryAB.meta.gid]);

			await db1.close();
			expect(db1.log["_gidParentCache"].size).equal(0);
		});
	});

	describe("replication", () => {
		it("replicates database of 1 entry", async () => {
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
			))!;

			await db1.waitFor(session.peers[1].peerId);
			await db2.waitFor(session.peers[0].peerId);

			const value = "hello";

			await db1.add(value);
			await waitForResolved(() => expect(db2.log.log.length).equal(1));

			expect((await db2.iterator({ limit: -1 })).collect().length).equal(1);

			const db1Entries: Entry<Operation<string>>[] = (
				await db1.iterator({ limit: -1 })
			).collect();
			expect(db1Entries.length).equal(1);

			await waitForResolved(async () =>
				expect(
					await db1.log.findLeaders(
						db1Entries[0],
						maxReplicas(db1.log, db1Entries),
						// 0
					),
				).to.have.members(
					[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
						getPublicKeyFromPeerId(p).hashcode(),
					),
				),
			);

			expect(db1Entries[0].payload.getValue().value).equal(value);
			const db2Entries: Entry<Operation<string>>[] = (
				await db2.iterator({ limit: -1 })
			).collect();
			expect(db2Entries.length).equal(1);
			expect(
				await db2.log.findLeaders(
					db2Entries[0],
					maxReplicas(db2.log, db2Entries),
					// 0
				),
			).include.members(
				[session.peers[0].peerId, session.peers[1].peerId].map((p) =>
					getPublicKeyFromPeerId(p).hashcode(),
				),
			);
			expect(db2Entries[0].payload.getValue().value).equal(value);
		});

		it("replicates database of 1000 entries", async () => {
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
			))!;

			await db1.waitFor(session.peers[1].peerId);
			await db2.waitFor(session.peers[0].peerId);

			const entryCount = 1000;
			for (let i = 0; i < entryCount; i++) {
				//	entryArr.push(i);
				await db1.add("hello" + i);
			}

			await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

			const entries = (await db2.iterator({ limit: -1 })).collect();
			expect(entries.length).equal(entryCount);
			for (let i = 0; i < entryCount; i++) {
				try {
					expect(entries[i].payload.getValue().value).equal("hello" + i);
				} catch (error) {
					console.error(
						"Entries out of order: " +
							entries.map((x) => x.payload.getValue().value).join(", "),
					);
					throw error;
				}
			}
		});

		it("replicates database of large entries", async () => {
			let count = 10;
			for (let i = 0; i < count; i++) {
				const value = toBase64(randomBytes(4e6));
				await db1.add(value, { meta: { next: [] } }); // force unique heads
			}
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							factor: 1,
						},
					},
				},
			))!;

			await waitForResolved(() => expect(db2.log.log.length).equal(count));
		});
	});
});

describe("redundancy", () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	let fetchEvents: number;
	let fetchHashes: Set<string>;
	let fromMultihash: any;

	before(async () => {
		session = await TestSession.connected(3);
		fromMultihash = Entry.fromMultihash;
		// TODO monkeypatching might lead to sideeffects in other tests!
		Entry.fromMultihash = (s, h, o) => {
			fetchHashes.add(h);
			fetchEvents += 1;
			return fromMultihash(s, h, o);
		};
	});
	after(async () => {
		await session.stop();
	});

	beforeEach(() => {
		fetchEvents = 0;
		fetchHashes = new Set();
	});
	afterEach(async () => {
		if (db1 && db1.closed === false) await db1.drop();
		if (db2 && db2.closed === false) await db2.drop();
		if (db3 && db3.closed === false) await db3.drop();
	});

	it("only sends entries once, 2 peers dynamic", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		await db1.log.replicate();
		let count = 10;
		for (let i = 0; i < count; i++) {
			await db1.add("hello " + i, { meta: { next: [] } });
		}
		const message1 = collectMessages(db1.log);

		const interval = setInterval(() => {
			db1.log.distribute();
		}, 100);
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: true,
				},
			},
		))!;

		const message2 = collectMessages(db2.log);
		await delay(3000);
		clearInterval(interval);

		const dataMessages2 = getReceivedHeads(message2);
		await waitForResolved(() => expect(dataMessages2).to.have.length(count));

		const dataMessages1 = getReceivedHeads(message1);
		expect(dataMessages1).to.be.empty; // no data is sent back
	});

	it("only sends entries once, 2 peers fixed", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		db1.log.replicate({ factor: 1 });
		let count = 1000;
		for (let i = 0; i < count; i++) {
			await db1.add("hello " + i, { meta: { next: [] } });
		}
		const message1 = collectMessages(db1.log);

		const interval = setInterval(() => {
			db1.log.distribute();
		}, 100);
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		const message2 = collectMessages(db2.log);
		await delay(3000);
		clearInterval(interval);

		const dataMessages2 = getReceivedHeads(message2);
		await waitForResolved(() => expect(dataMessages2).to.have.length(count));

		const dataMessages1 = getReceivedHeads(message1);
		expect(dataMessages1).to.be.empty; // no data is sent back
	});

	it("only sends entries once, 2 peers fixed, write after open", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: { factor: 1 },
			},
		});
		let count = 1;
		const message1 = collectMessages(db1.log);

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		const message2 = collectMessages(db2.log);

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators())?.size).equal(2),
		);

		await waitForResolved(async () =>
			expect((await db2.log.getReplicators())?.size).equal(2),
		);

		await db1.add("hello", { meta: { next: [] } });
		await db1.log.distribute(); // manually call distribute to make sure no more messages are sent

		await waitForResolved(() => expect(db2.log.log.length).equal(1));

		const dataMessages2 = getReceivedHeads(message2);
		await waitForResolved(() => expect(dataMessages2).to.have.length(count));

		const dataMessages1 = getReceivedHeads(message1);
		expect(dataMessages1).to.be.empty; // no data is sent back
	});

	it("only sends entries once, 3 peers", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});
		const message1 = collectMessages(db1.log);

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);
		const message2 = collectMessages(db2.log);

		let count = 10; // TODO make higher count work in Github CI

		for (let i = 0; i < count; i++) {
			await db1.add("hello " + i, { meta: { next: [] } });
		}
		await waitForResolved(() => expect(db2.log.log.length).equal(count));

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);
		const message3 = collectMessages(db3.log);

		await waitForResolved(() => expect(db3.log.log.length).equal(count));

		const heads = getReceivedHeads(message3);
		expect(heads).to.have.length(count);

		expect(getReceivedHeads(message1)).to.be.empty;
		expect(getReceivedHeads(message2)).to.have.length(count);

		await waitForResolved(() => expect(db3.log.log.length).equal(count));

		// gc check,.
		await waitForResolved(() => {
			expect(db3.log["syncInFlightQueue"].size).equal(0);
			expect(db3.log["syncInFlightQueueInverted"].size).equal(0);
		});
	});

	it("no fetches needed when replicating live ", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
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
		await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

		// All entries should be in the database
		expect((await db2.iterator({ limit: -1 })).collect().length).equal(
			entryCount,
		);

		// progress events should increase monotonically
		expect(fetchEvents).equal(fetchHashes.size);
		expect(fetchEvents).equal(0); // becausel all entries were sent
	});
	it("fetches only once after open", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

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
			session.peers[1],
		))!;

		// All entries should be in the database
		await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

		// progress events should (increase monotonically)
		expect((await db2.iterator({ limit: -1 })).collect().length).equal(
			entryCount,
		);
		expect(fetchEvents).equal(fetchHashes.size);
		expect(fetchEvents).equal(entryCount - 1); // - 1 because we also send some references for faster syncing (see exchange-heads.ts)
	});
});

describe(`start/stop`, function () {
	let session: TestSession;
	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("replicate on connect", async () => {
		const entryCount = 1000;
		const entryArr: number[] = [];
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		await waitForResolved(async () =>
			expect(db2.log.log.length).equal(entryCount),
		);
		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).equal(result2.length);
		for (let i = 0; i < result1.length; i++) {
			expect(result1[i].equals(result2[i])).to.be.true;
		}
	});

	it("can restart replicate", async () => {
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		await db1.add("hello");

		let db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		await waitForResolved(() => expect(db2.log.log.length).equal(1));

		await db2.close();
		await db1.add("world");
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		))!;
		await waitForResolved(() => expect(db2.log.log.length).equal(2));
	});
});

describe("canReplicate", () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const init = async (
		canReplicate: (publicKey: PublicSignKey) => Promise<boolean> | boolean,
		replicate: ReplicationOptions = { factor: 1 },
	) => {
		let min = 100;
		let max = undefined;
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min,
					max,
				},
				replicate,
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
					replicate,
					canReplicate,
				},
			},
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
					replicate,
					canReplicate,
				},
			},
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
			session.peers[2].identity.publicKey.hashcode(),
		];

		await Promise.all(
			[db1, db2, db3].map((db) =>
				waitForResolved(async () =>
					expect([...(await db.log.getReplicators())]).to.have.members(
						expectedReplicators,
					),
				),
			),
		);

		const unionFromPeer0 = await db1.log.getCover(undefined, { roleAge: 0 });
		let selfIndex = unionFromPeer0.findIndex(
			(x) => x === db1.node.identity.publicKey.hashcode(),
		);

		// should always include self in the cover set, also include one of the remotes since their replication factor is 1
		expect([
			db2.node.identity.publicKey.hashcode(),
			db3.node.identity.publicKey.hashcode(),
		]).include(unionFromPeer0[selfIndex === 0 ? 1 : 0]);
		expect(unionFromPeer0).to.have.length(2);

		// the other ones should only have to cover themselves
		await Promise.all(
			[db2, db3].map((log) =>
				waitForResolved(async () =>
					expect(
						await log.log.getCover(undefined, { roleAge: 0 }),
					).to.have.members([log.node.identity.publicKey.hashcode()]),
				),
			),
		);

		await db2.add("hello");

		await waitForResolved(() => {
			expect(db2.log.log.length).equal(1);
			expect(db3.log.log.length).equal(1);
		});
		await delay(1000); // Add some delay so that all replication events most likely have occured
		expect(db1.log.log.length).equal(0); // because not trusted for replication job
	});

	/* TODO feat(?)
	
	it("replicate even if not allowed if factor is 1 ", async () => {
		await init(() => false, { factor: 1 });

		const mySegments = await db1.log.getMyReplicationSegments();
		expect(mySegments).to.have.length(1);
		expect(mySegments[0].widthNormalized).to.equal(1);
	}); */

	it("does not replicate if not allowed and dynamic ", async () => {
		await init(() => false, true);

		const mySegments = await db1.log.getMyReplicationSegments();
		expect(mySegments).to.have.length(0);
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
					max,
				},
				replicate: false,
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
					replicate: {
						factor: 0.5,
						offset: 0,
					},
				},
			},
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
					replicate: {
						factor: 0.5,
						offset: 0.5,
					},
				},
			},
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
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},

			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
	});

	afterEach(async () => {
		if (db1 && db1.closed === false) await db1.drop();

		if (db2 && db1.closed === false) await db2.drop();

		if (db3 && db1.closed === false) await db3.drop();

		await session.stop();
	});

	it("will prune once reaching max replicas", async () => {
		await session.stop();
		session = await TestSession.disconnected(3);

		let minReplicas = 2;
		let maxReplicas = 2;

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
			},
		});
		db2 = (await session.peers[1].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
			},
		}))!;

		db3 = (await session.peers[2].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
			},
		}))!;

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
				meta: { next: [] },
			});
		}

		await session.peers[1].dial(session.peers[0].getMultiaddrs());
		await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

		await db2.close();

		session.peers[2].dial(session.peers[0].getMultiaddrs());

		await waitForResolved(() => expect(db3.log.log.length).equal(entryCount));

		// reopen db2 again and make sure either db3 or db2 drops the entry (not both need to replicate)
		await delay(2000);
		db2 = await session.peers[1].open(db2, {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
			},
		});

		await waitForResolved(() => {
			expect(db1.log.log.length).to.be.lessThan(entryCount);
		});
	});

	it("control per commmit", async () => {
		await init(1);

		const value = "hello";

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add(value, {
				replicas: new AbsoluteReplicas(1),
				meta: { next: [] },
			});
			await db1.add(value, {
				replicas: new AbsoluteReplicas(3),
				meta: { next: [] },
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
			expect(replicated3Times).equal(entryCount);
			expect(other).greaterThan(0);
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
				meta: { next: [] },
			});
			await db1.add(String(i), {
				replicas: new AbsoluteReplicas(1), // will be overriden by 'maxReplicas' above
				meta: { next: [e1.entry] },
			});
		}

		await waitForResolved(
			() => {
				expect(db1.log.log.length).equal(0);
				let total = db2.log.log.length + db3.log.log.length;
				expect(total).greaterThanOrEqual(entryCount);
				expect(total).lessThan(entryCount * 2);
				expect(db2.log.log.length).greaterThan(entryCount * 0.2);
				expect(db3.log.log.length).greaterThan(entryCount * 0.2);
			},
			{ timeout: 20 * 1000 },
		);
	});

	it("observer will not delete unless replicated", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: 10,
				},
				replicate: false,
			},
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 10,
					},
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		const e1 = await db1.add("hello");

		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await expect(
			Promise.all(db1.log.prune([e1.entry], { timeout: 3000 })),
		).rejectedWith("Timeout for checked pruning");
		expect(db1.log.log.length).equal(1); // No deletions
	});

	it("replicator will not delete unless replicated", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: 10,
				},
				replicate: {
					factor: 1,
				},
			},
		});
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 10,
					},
					replicate: {
						factor: 1,
					},
				},
			},
		))!;

		const e1 = await db1.add("hello");
		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await expect(
			Promise.all(db1.log.prune([e1.entry], { timeout: 3000 })),
		).rejectedWith("Failed to delete, is leader");
		expect(db1.log.log.length).equal(1); // No deletions */
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
				replicate: false,
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
			},
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

		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		expect(db2ReorgCounter).equal(0);
		await db2.log.replicate({
			factor: 1,
		});
		expect(db2ReorgCounter).equal(1);
		await waitForResolved(() => expect(db2.log.log.length).equal(1));

		// peer 1 removed
		// peer 2 replicator (has entry)
		await db1.drop();

		// peer 1 observer
		// peer 2 replicator (has entry)
		await session.peers[0].open(db1, {
			args: {
				replicas: {
					min,
					max,
				},
				replicate: false,
			},
		});

		// peer 1 observer
		// peer 2 observer
		expect(db2.log.log.length).equal(1);
		await delay(2000);
		// expect(db2ReorgCounter).equal(1); TODO limit distributions and test this

		await db2.log.replicate(false);

		// 	expect(db2ReorgCounter).equal(2); TODO limit distributions and test this
		expect(await db2.log.isReplicating()).to.be.false;

		// peer 1 replicator (will get entry)
		// peer 2 observer (will safely delete the entry)
		await db1.log.replicate({
			factor: 1,
		});

		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		await waitForResolved(() => expect(db2.log.log.length).equal(0));
		// expect(db2ReorgCounter).equal(3); TODO
	});
	it("can override min on program level", async () => {
		let minReplicas = 1;
		let maxReplicas = 1;

		await init(minReplicas, maxReplicas);

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
				meta: { next: [] },
			});
		}
		await waitForResolved(
			() => {
				expect(db1.log.log.length).equal(0); // because db1 is not replicating at all, but just pruning once it knows entries are replicated elsewhere
				let total = db2.log.log.length + db3.log.log.length;
				expect(total).greaterThanOrEqual(entryCount);
				expect(total).lessThan(entryCount * 2);
				expect(db2.log.log.length).greaterThan(entryCount * 0.2);
				expect(db3.log.log.length).greaterThan(entryCount * 0.2);
			},
			{ timeout: 3e4 },
		);
	});
	it("time out when pending IHave are never resolved", async () => {
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
				replicate: false,
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
					replicate: {
						factor: 1,
					},
					respondToIHaveTimeout,
				},
			},
		);

		// TODO this test is flaky because background prune calls are intefering with assertions
		// Todo make sure no background prunes are done (?)

		const onMessageFn = db2.log._onMessage.bind(db2.log);
		db2.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
			if (msg instanceof ExchangeHeadsMessage) {
				return; // prevent replication
			}
			return onMessageFn(msg, cxt);
		};
		const { entry } = await db1.add("hello");
		const expectPromise = expect(
			Promise.all(
				db1.log.prune([entry], { timeout: db1.log.timeUntilRoleMaturity }),
			),
		).rejectedWith("Timeout");
		await waitForResolved(() => expect(db2.log["_pendingIHave"].size).equal(1));
		await delay(respondToIHaveTimeout + 1000);
		await waitForResolved(() => expect(db2.log["_pendingIHave"].size).equal(0)); // shoulld clear up
		await expectPromise;
	});

	it("does not get blocked by slow sends", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = await session.peers[1].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		await waitForResolved(async () =>
			expect((await db1.log.getReplicators()).size).equal(2),
		);

		let db1Delay = 0;
		const db1Send = db1.log.rpc.send.bind(db1.log.rpc);
		db1.log.rpc.send = async (message, options) => {
			const controller = new AbortController();
			db1.log.rpc.events.addEventListener("close", () => {
				controller.abort(new AbortError());
			});
			db1.log.rpc.events.addEventListener("drop", () => {
				controller.abort(new AbortError());
			});
			try {
				await delay(db1Delay, { signal: controller.signal });
			} catch (error) {
				return;
			}
			return db1Send(message, options);
		};

		db1Delay = 1e4;

		db1.add("hello");

		await delay(1000); // make sure we have gotten "stuck" into the rpc.send unction

		let t0 = +new Date();
		db3 = await session.peers[2].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db1Delay = 0;

		await waitForResolved(() => expect(db3.log.log.length).equal(1));
		let t1 = +new Date();
		expect(t1 - t0).lessThan(2000);
	});

	it("restarting node will receive entries", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = await session.peers[1].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});
		await db1.add("hello");
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await db2.drop();
		await session.peers[1].stop();
		await session.peers[1].start();
		db2 = await session.peers[1].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
	});

	it("can handle many large messages", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		// append more than 30 mb
		const count = 5;
		for (let i = 0; i < count; i++) {
			await db1.add(toBase64(randomBytes(6e6)), { meta: { next: [] } });
		}
		db2 = await session.peers[1].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});
		await waitForResolved(() => expect(db2.log.log.length).equal(count));
	});
	/*  TODO feat
	it("will reject early if leaders does not have entry", async () => {
		await init(1);

		const value = "hello";

		const e1 = await db1.add(value, { replicas: new AbsoluteReplicas(2) });

		// Assume all peers gets it
		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await waitForResolved(() => expect(db3.log.log.length).equal(1));
		await db3.log.log.deleteRecursively(await db3.log.log.getHeads());
		await waitForResolved(() => expect(db3.log.log.length).equal(0));

		expect(db2.log.log.length).equal(1);
		const fn = () => db2.log.safelyDelete([e1.entry], { timeout: 3000 })[0];
		await expect(fn).rejectedWith(
			"Insufficient replicators to safely delete: " + e1.entry.hash
		);
		expect(db2.log.log.length).equal(1);
	}); */
});

describe("sync", () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	before(async () => {
		session = await TestSession.connected(2);
	});
	after(async () => {
		await session.stop();
	});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
	});

	it("manually synced entries will not get pruned", async () => {
		db1 = await session.peers[0].open<EventStore<string>>(new EventStore(), {
			args: {
				/* sync: () => true, */
				replicas: {
					min: 1,
				},
				replicate: {
					factor: 1,
				},
			},
		})!;

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					/* 	sync: () => true, */
					replicas: {
						min: 1,
					},
					replicate: {
						factor: 1,
					},
				},
			},
		))!;
		await db1.add("data");
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await db2.log.replicate(false);
		await delay(3000);
		await waitForResolved(() => expect(db2.log.log.length).equal(0));
	});
});
