import { deserialize, serialize } from "@dao-xyz/borsh";
import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import {
	BlockRequest,
	BlockResponse,
	type BlockMessage as InnerBlocksMessage,
} from "@peerbit/blocks";
import {
	type PublicSignKey,
	getPublicKeyFromPeerId,
	randomBytes,
	toBase64,
} from "@peerbit/crypto";
import { Entry, EntryType } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import {
	/* AbortError, */
	AbortError,
	delay,
	waitForResolved,
} from "@peerbit/time";
import { expect } from "chai";
import mapSeries from "p-each-series";
import sinon from "sinon";
import { BlocksMessage } from "../src/blocks.js";
import { ExchangeHeadsMessage, RequestIPrune } from "../src/exchange-heads.js";
import type { ReplicationOptions } from "../src/index.js";
import type { ReplicationRangeIndexable } from "../src/ranges.js";
import {
	AbsoluteReplicas,
	AddedReplicationSegmentMessage,
	decodeReplicas,
	/* decodeReplicas, */
	maxReplicas,
} from "../src/replication.js";
import {
	checkBounded,
	collectMessages,
	collectMessagesFn,
	getReceivedHeads,
	waitForConverged,
} from "./utils.js";
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
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
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
							(await db1.log.replicationIndex.iterate().all()).map(
								(x) => x.value,
							),
						).values(),
					].map((x) => Math.round(x * 200)),
				).to.deep.equal([100, 100]),
			);

			await waitForResolved(async () =>
				expect(
					[
						...getParticipationPerPer(
							(await db2.log.replicationIndex.iterate().all()).map(
								(x) => x.value,
							),
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

		/* TODO feature not implemented yet, when is this expected?
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
		*/

		it("fetches next blocks once", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicas: {
						min: 0,
					},
					replicate: false,
					timeUntilRoleMaturity: 1000,
				},
			});

			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 0,
						},

						replicate: {
							factor: 1,
						},
						timeUntilRoleMaturity: 1000,
					},
				},
			))!;

			// followwing entries set minReplicas to 1 which means only db2 or db3 needs to hold it
			const e1 = await db1.add("0", {
				replicas: new AbsoluteReplicas(3),
				meta: { next: [] },
			});
			await db1.add("1", {
				replicas: new AbsoluteReplicas(1), // will be overriden by 'maxReplicas' above
				meta: { next: [e1.entry] },
			});

			const onMessageFn1 = db1.log._onMessage.bind(db1.log);

			let receivedMessageDb1: InnerBlocksMessage[] = [];
			db1.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
				if (msg instanceof BlocksMessage) {
					receivedMessageDb1.push(msg.message);
				}
				return onMessageFn1(msg, cxt);
			};

			let receivedMessageDb2: InnerBlocksMessage[] = [];
			const onMessageFn2 = db2.log._onMessage.bind(db2.log);
			db2.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
				if (msg instanceof BlocksMessage) {
					receivedMessageDb2.push(msg.message);
				}
				return onMessageFn2(msg, cxt);
			};

			await waitForResolved(() => {
				expect(db1.log.log.length).equal(0);
				expect(db2.log.log.length).greaterThanOrEqual(1);
			});

			expect(receivedMessageDb1).to.have.length(1);
			expect(receivedMessageDb1[0]).to.be.instanceOf(BlockRequest);
			expect(receivedMessageDb2).to.have.length(1);
			expect(receivedMessageDb2[0]).to.be.instanceOf(BlockResponse);
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
				expect([
					...(
						await db1.log.findLeaders(
							{
								entry: db1Entries[0],
								replicas: maxReplicas(db1.log, db1Entries),
							},
							// 0
						)
					).keys(),
				]).to.have.members(
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
			expect([
				...(
					await db2.log.findLeaders(
						{
							entry: db2Entries[0],
							replicas: maxReplicas(db2.log, db2Entries),
						},
						// 0
					)
				).keys(),
			]).include.members(
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
				{
					args: {
						replicate: {
							factor: 1,
						},
					},
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);
			await db2.waitFor(session.peers[0].peerId);

			const entryCount = 1e3;
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

		it("replicates 1 entry with cut next", async () => {
			const first = await db1.add("old");
			const second = await db1.add("new", {
				meta: { type: EntryType.CUT, next: [first.entry] },
			});
			expect(
				(await db1.iterator({ limit: -1 })).collect().map((x) => x.hash),
			).to.deep.equal([second.entry.hash]);
			expect(db1.log.log.length).equal(1);

			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
			))!;

			await waitForResolved(async () => {
				expect(
					(await db2.iterator({ limit: -1 })).collect().map((x) => x.hash),
				).to.deep.equal([second.entry.hash]);
			});
		});

		it("it does not fetch missing entries from remotes when exchanging heads to remote", async () => {
			const first = await db1.add("a", { meta: { next: [] } });
			const second = await db1.add("b", { meta: { next: [] } });
			await db1.log.log.entryIndex.delete(second.entry.hash);

			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
			))!;

			let remoteFetchOptions: any[] = [];
			const db1LogGet = db1.log.log.get.bind(db1.log.log);

			db1.log.log.get = async (hash, options) => {
				if (hash === second.entry.hash) {
					remoteFetchOptions.push(options?.remote);
					return undefined;
				}
				return db1LogGet(hash, options);
			};

			await waitForResolved(async () => {
				expect(
					(await db2.iterator({ limit: -1 })).collect().map((x) => x.hash),
				).to.deep.equal([first.entry.hash]);
			});
			await waitForResolved(() => expect(remoteFetchOptions).to.have.length(1));
			expect(remoteFetchOptions[0]).to.be.undefined;
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
		let count = 100;
		for (let i = 0; i < count; i++) {
			await db1.add("hello " + i, { meta: { next: [] } });
		}
		const message1 = collectMessages(db1.log);

		let db2 = db1.clone();

		// start to collect messages before opening the second db so we don't miss any
		const { messages: message2, fn } = collectMessagesFn(db2.log);
		db2 = await session.peers[1].open(db2, {
			args: {
				replicate: true,
				onMessage: fn,
			},
		});

		const check = () => {
			const dataMessages2 = getReceivedHeads(message2);
			expect(dataMessages2).to.have.length(count);

			const dataMessages1 = getReceivedHeads(message1);
			expect(dataMessages1).to.be.empty; // no data is sent back
		};
		try {
			await waitForResolved(() => {
				check();
			});
			await delay(3000);
			check();
		} catch (error) {
			console.error(error);
			throw new Error(
				"Did not resolve all heads. Log length: " + db2.log.log.length,
			);
		}
	});

	it("only sends entries once, 2 peers fixed", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		db1.log.replicate({ factor: 1 });
		let count = 1000;
		for (let i = 0; i < count; i++) {
			await db1.add("hello " + i, { meta: { next: [] } });
		}
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
		await delay(3000);

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

		const entryCount = 10; // todo when larger (N) this test usually times out at N - 1 or 2, unless a delay is put beforehand

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
		expect(db2.log.log.length).equal(entryCount);

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

	const init = async (props: {
		min: number;
		max?: number;
		beforeOther?: () => Promise<any> | void;
	}) => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: props,
				replicate: false,
				timeUntilRoleMaturity: 1000,
			},
		});

		await props.beforeOther?.();
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: props,

					replicate: {
						factor: 0.5,
						offset: 0,
					},
					timeUntilRoleMaturity: 1000,
				},
			},
		))!;

		db3 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicas: props,

					replicate: {
						factor: 0.5,
						offset: 0.5,
					},
					timeUntilRoleMaturity: 1000,
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
					privateKey: await privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
							197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203,
							18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86,
							159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251,
							206, 54, 49, 141, 91, 171,
						]),
					),
				},
			},

			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
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

	it("will not prune below replication degree", async () => {
		let replicas = 2;
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: false,
				replicas: {
					min: replicas,
				},
			},
		});

		let db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
					replicas: {
						min: replicas,
					},
				},
			},
		))!;

		await db1.add("hello");

		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		await delay(3e3);
		expect(db1.log.log.length).equal(1);
	});

	it("prune on insert many", async () => {
		await init({ min: 1 });
		let count = 100;
		for (let i = 0; i < count; i++) {
			await db1.add("hello", {
				meta: { next: [] },
			});
		}
		/* await delay(2e4) */

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() =>
			expect(db2.log.log.length + db3.log.log.length).equal(count),
		);
	});

	it("will prune on before join", async () => {
		await init({ min: 1, beforeOther: () => db1.add("hello") });
		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() =>
			expect(db2.log.log.length + db3.log.log.length).equal(1),
		);
	});
	it("will prune on put 300 before join", async () => {
		let count = 100;
		await init({
			min: 1,
			beforeOther: async () => {
				for (let i = 0; i < count; i++) {
					await db1.add("hello", {
						meta: { next: [] },
					});
				}
			},
		});
		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() =>
			expect(db2.log.log.length + db3.log.log.length).equal(count),
		);
	});

	it("will prune on put 300 after join", async () => {
		await init({ min: 1 });

		let count = 300;
		for (let i = 0; i < count; i++) {
			await db1.add("hello", {
				meta: { next: [] },
			});
		}

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() =>
			expect(db2.log.log.length + db3.log.log.length).equal(count),
		);
	});

	it("will prune when join with partial coverage", async () => {
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: false,
				replicas: {
					min: 1,
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
						offset: 0,
						factor: 1,
						normalized: false,
					},

					replicas: {
						min: 1,
					},
				},
			},
		))!;

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
	});

	it("will prune when join with complete coverage", async () => {
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: false,
				replicas: {
					min: 1,
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
						offset: 0,
						factor: 1,
						normalized: true,
					},

					replicas: {
						min: 1,
					},
				},
			},
		))!;

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
	});

	it("will prune on insert after join 2 peers", async () => {
		const db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					offset: 0,
					factor: 0,
					normalized: false,
				},
				replicas: {
					min: 1,
				},
			},
		});

		let db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						offset: 0,
						factor: 1,
						normalized: false,
					},

					replicas: {
						min: 1,
					},
				},
			},
		))!;

		await db1.add("hello");

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
	});

	it("will prune once reaching max replicas", async () => {
		await session.stop();

		session = await TestSession.disconnected(3, [
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							48, 245, 17, 66, 32, 106, 72, 98, 203, 253, 86, 138, 133, 155,
							243, 214, 8, 11, 14, 230, 18, 126, 173, 3, 62, 252, 92, 46, 214,
							0, 226, 184, 104, 58, 22, 118, 214, 182, 125, 233, 106, 94, 13,
							16, 6, 164, 236, 215, 159, 135, 117, 8, 240, 168, 169, 96, 38, 86,
							213, 250, 103, 183, 38, 205,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251, 158,
							121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184, 24, 56,
							27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117, 218, 216,
							120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63, 158, 217, 111,
							65, 105, 99, 83, 4, 113, 62, 15,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
							197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203,
							18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86,
							159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251,
							206, 54, 49, 141, 91, 171,
						]),
					),
				},
			},
		]);

		let minReplicas = 2;
		let maxReplicas = 2;

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
				replicate: {
					offset: 0,
					factor: 0.333,
				},
			},
		});
		db2 = (await session.peers[1].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
				replicate: {
					offset: 0.333,
					factor: 0.666,
				},
			},
		}))!;

		db3 = (await session.peers[2].open(db1.clone(), {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
				replicate: {
					offset: 0.666,
					factor: 0.333,
				},
			},
		}))!;

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(3), // will be overriden by 'maxReplicas' above
				meta: { next: [] },
			});
		}

		// TODO why is this needed?
		await waitForResolved(() =>
			session.peers[1].dial(session.peers[0].getMultiaddrs()),
		);

		await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

		await db2.close();

		session.peers[2].dial(session.peers[0].getMultiaddrs());

		await waitForResolved(() =>
			expect(db3.log.log.length).to.be.greaterThan(0),
		);
		await waitForConverged(() => db3.log.log.length);

		// reopen db2 again and make sure either db3 or db2 drops the entry (not both need to replicate)
		await delay(2000);
		db2 = await session.peers[1].open(db2, {
			args: {
				replicas: {
					min: minReplicas,
					max: maxReplicas,
				},
				replicate: {
					offset: 0.333,
					factor: 0.666,
				},
			},
		});

		// await db1.log["pruneDebouncedFn"]();
		//await db1.log.waitForPruned()

		try {
			await waitForResolved(() => {
				expect(db1.log.log.length).to.be.lessThan(entryCount);
			});
		} catch (error) {
			const prunable = await db1.log.getPrunable();
			console.log(prunable.length);
			const ranges1 = await db1.log.replicationIndex.iterate().all();
			const ranges2 = await db2.log.replicationIndex.iterate().all();
			const ranges3 = await db3.log.replicationIndex.iterate().all();
			console.log(ranges1, ranges2, ranges3);
			throw error;
		}
	});

	describe("commit options", () => {
		it("control per commmit put before join", async () => {
			const entryCount = 100;

			await init({
				min: 1,
				beforeOther: async () => {
					const value = "hello";
					for (let i = 0; i < entryCount; i++) {
						await db1.add(value, {
							replicas: new AbsoluteReplicas(3),
							meta: { next: [] },
						});
					}
				},
			});

			const check = async (log: EventStore<string>) => {
				let replicated3Times = 0;
				for (const entry of await log.log.log.toArray()) {
					if (decodeReplicas(entry).getValue(db2.log) === 3) {
						replicated3Times += 1;
					}
				}
				expect(replicated3Times).equal(entryCount);
			};

			await waitForResolved(() => check(db2));
			await waitForResolved(() => check(db3));
		});

		it("control per commmit", async () => {
			const entryCount = 100;

			await init({
				min: 1,
			});

			const value = "hello";
			for (let i = 0; i < entryCount; i++) {
				await db1.add(value, {
					replicas: new AbsoluteReplicas(3),
					meta: { next: [] },
				});
			}

			const check = async (log: EventStore<string>) => {
				let replicated3Times = 0;
				for (const entry of await log.log.log.toArray()) {
					if (decodeReplicas(entry).getValue(db2.log) === 3) {
						replicated3Times += 1;
					}
				}
				expect(replicated3Times).equal(entryCount);
			};

			await waitForResolved(() => check(db2));
			await waitForResolved(() => check(db3));
		});

		it("mixed control per commmit", async () => {
			await init({ min: 1 });

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

		it("will index replication underflow degree", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicas: {
						min: 4,
					},
					replicate: false,
					timeUntilRoleMaturity: 1000,
				},
			});

			db2 = await session.peers[1].open<EventStore<string>>(db1.address, {
				args: {
					replicas: {
						min: 4,
					},
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 1000,
				},
			});

			await db1.add("hello", {
				replicas: new AbsoluteReplicas(4),
			});

			await waitForResolved(() => expect(db1.log.log.length).equal(1));
			await waitForResolved(() => expect(db2.log.log.length).equal(1));

			const indexedDb1 = await db1.log.entryCoordinatesIndex.iterate().all();
			const indexedDb2 = await db2.log.entryCoordinatesIndex.iterate().all();

			expect(
				indexedDb1.filter((x) => x.value.assignedToRangeBoundary),
			).to.have.length(4);
			expect(
				indexedDb2.filter((x) => x.value.assignedToRangeBoundary),
			).to.have.length(4);
		});
	});

	it("min replicas with be maximum value for gid", async () => {
		await init({ min: 1 });

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

		await waitForResolved(() => {
			expect(db1.log.log.length).equal(0);
			let total = db2.log.log.length + db3.log.log.length;
			expect(total).greaterThanOrEqual(entryCount);
			expect(db2.log.log.length).greaterThan(entryCount * 0.2);
			expect(db3.log.log.length).greaterThan(entryCount * 0.2);
		});
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
		expect(db1.log.log.length).equal(1); // No deletions
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
					replicate: {
						factor: 0,
					},
					replicas: {
						min,
						max,
					},
				},
			},
		))!;

		let db2ReorgCounter = 0;
		let db2ReplicationReorganizationFn = db2.log.onReplicationChange.bind(
			db2.log,
		);
		db2.log.onReplicationChange = (args) => {
			db2ReorgCounter += 1;
			return db2ReplicationReorganizationFn(args);
		};
		await db1.add("hello");

		// peer 1 observer
		// peer 2 replicator (will get entry)

		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		expect(db2ReorgCounter).equal(0);
		await db2.log.replicate({
			factor: 1,
		});
		await waitForResolved(() => expect(db2ReorgCounter).equal(1));
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

		await init({ min: minReplicas, max: maxReplicas });

		const entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			await db1.add("hello", {
				replicas: new AbsoluteReplicas(5), // will be overriden by 'maxReplicas' above
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
		db1Delay = 0;
		db3 = await session.peers[2].open<EventStore<any>>(db1.address, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

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

	describe("update", () => {
		it("shift", async () => {
			const u32Div2 = 2147483647;
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: u32Div2,
						normalized: false,
					},
					replicas: {
						min: 1,
					},
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0,
							factor: u32Div2,
							normalized: false,
						},
						replicas: {
							min: 1,
						},
					},
				},
			))!;

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}
			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.above(entryCount / 3),
			);

			await db2.log.replicate(
				{ factor: u32Div2, offset: u32Div2, normalized: false },
				{ reset: true },
			);

			await waitForResolved(() =>
				expect(db1.log.log.length).to.closeTo(entryCount / 2, 20),
			);
			await waitForResolved(() =>
				expect(db2.log.log.length).to.closeTo(entryCount / 2, 20),
			);
			await waitForResolved(() =>
				expect(db1.log.log.length + db2.log.log.length).to.equal(entryCount),
			);
		});

		it("to same range", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 1,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0,
							factor: 1,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;
			await db1.add("hello", { meta: { next: [] } });
			await waitForResolved(() => expect(db1.log.log.length).equal(1));
			await waitForResolved(() => expect(db2.log.log.length).equal(1));

			const findLeaders1 = sinon.spy(db1.log, "findLeaders");
			const findLeaders2 = sinon.spy(db2.log, "findLeaders");
			const onMessage1 = sinon.spy(db1.log, "_onMessage");

			const range = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();
			await db2.log.replicate(range);

			expect(findLeaders1.callCount).equal(0); // no changes
			await waitForResolved(() => expect(onMessage1.callCount).equal(1)); // one message
			expect(findLeaders2.callCount).equal(0); // no changes emitted
		});

		it("to smaller but already replicated", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 1,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0,
							factor: 1,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}
			await waitForResolved(() => expect(db1.log.log.length).equal(entryCount));
			await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));

			const findLeaders1 = sinon.spy(db1.log, "findLeaders");
			const findLeaders2 = sinon.spy(db2.log, "findLeaders");
			const onMessage1 = sinon.spy(db1.log, "_onMessage");

			const range = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			let newFactor = 0.5;
			await db2.log.replicate({ factor: newFactor, offset: 0, id: range.id });
			const expectedAmountOfEntriesToPrune = entryCount * newFactor;

			await waitForResolved(async () => {
				expect(db2.log.log.length).to.be.closeTo(
					entryCount - expectedAmountOfEntriesToPrune,
					30,
				);

				expect(onMessage1.callCount).equal(2); // two messages (the updated range) and request for pruning
				expect(findLeaders1.callCount).to.be.lessThan(entryCount * 3); // some upper bound, TODO make more strict
				expect(findLeaders2.callCount).to.be.lessThan(entryCount * 3); // some upper bound, TODO make more strict
				/* 
				TODO stricter boundes like below
				expect(findLeaders1.callCount).to.closeTo(prunedEntries * 2, 30); // redistribute + prune about 50% of the entries
				expect(findLeaders2.callCount).to.closeTo(prunedEntries * 2, 30); // redistribute + handle prune requests 
				*/
			});

			// we do below separetly because this will interefere with the callCounts above
			await waitForResolved(async () =>
				expect(await db2.log.getPrunable()).to.length(0),
			);

			// eslint-disable-next-line no-useless-catch
			try {
				expect(onMessage1.getCall(0).args[0]).instanceOf(
					AddedReplicationSegmentMessage,
				);
				expect(onMessage1.getCall(1).args[0]).instanceOf(RequestIPrune);
			} catch (error) {
				// eslint-disable-next-line no-useless-catch
				try {
					expect(onMessage1.getCall(1).args[0]).instanceOf(
						AddedReplicationSegmentMessage,
					);
					expect(onMessage1.getCall(0).args[0]).instanceOf(RequestIPrune);
				} catch (error) {
					throw error;
				}
			}
			/* const entryRefs1 = await db1.log.entryCoordinatesIndex.iterate().all();
			const entryRefs2 = await db2.log.entryCoordinatesIndex.iterate().all();

			expect(
				entryRefs1.filter((x) => x.value.replicators === 2),
			).to.have.length(db2.log.log.length);
			expect(
				entryRefs1.filter((x) => x.value.replicators === 1),
			).to.have.length(entryCount - db2.log.log.length);
			expect(
				entryRefs2.filter((x) => x.value.replicators === 2),
			).to.have.length(db2.log.log.length); */
		});

		it("to smaller will need transfer", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}

			await waitForResolved(() =>
				expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 30),
			);
			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 30),
			);

			/* 
			// TODO assert findLeaders call count strict
			const findLeaders1 = sinon.spy(db1.log, "findLeaders");
			const findLeaders2 = sinon.spy(db2.log, "findLeaders"); 
			*/

			const prune2 = sinon.spy(db2.log, "prune");

			const range = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await db2.log.replicate({ factor: 0.001, offset: 0.99, id: range.id });

			/* const entriesThatWillBeChecked = entryCount / 2;
			const entriesThatWillBePruned = entryCount / 4; // the change is that the range [0.5, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
			await waitForResolved(() =>
				expect(findLeaders2.callCount).to.closeTo(
					entriesThatWillBeChecked + entriesThatWillBePruned,
					30,
				),
			); TODO assert findLeaders call count strictly */

			await waitForResolved(() => {
				expect(prune2.callCount).to.eq(1);
				expect([...prune2.getCall(0).args[0].values()].length).to.be.closeTo(
					entryCount / 4,
					15,
				); // a quarter of the entries should be pruned becuse the range [0, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
			});

			// TODO assert some kind of findLeaders callCount ?
		});

		it("to smaller then to larger", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}
			await waitForResolved(() =>
				expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 30),
			);
			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 30),
			);

			const range = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await waitForConverged(() => db2.log.log.length);

			let startSize = db2.log.log.length;
			await db2.log.replicate({ factor: 0.25, offset: 0.5, id: range.id });

			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.lessThan(startSize),
			);
			await delay(1000);

			await db2.log.replicate({ factor: 0.5, offset: 0.5, id: range.id });
			await waitForResolved(() => expect(db2.log.log.length).to.eq(startSize));
		});

		it("replace range with another node write before join", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			let db3 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			await waitForConverged(() => db1.log.log.length);
			await waitForConverged(() => db2.log.log.length);
			await waitForConverged(() => db3.log.log.length);

			const db2Length = db2.log.log.length;
			const db3Length = db3.log.log.length;

			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.greaterThan(0),
			);

			await waitForResolved(() =>
				expect(db3.log.log.length).to.be.greaterThan(0),
			);

			const range2 = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await db2.log.replicate({ id: range2.id, offset: 0.1, factor: 0.1 });

			const range3 = (
				await db3.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await db3.log.replicate({ id: range3.id, offset: 0.1, factor: 0.1 });

			await waitForConverged(() => db2.log.log.length);
			await waitForConverged(() => db3.log.log.length);
			expect(db2.log.log.length).to.be.lessThan(db2Length);
			expect(db3.log.log.length).to.be.lessThan(db3Length);

			// reset to original

			await db2.log.replicate({ id: range2.id, offset: 0.5, factor: 0.5 });

			await db3.log.replicate({ id: range3.id, offset: 0.5, factor: 0.5 });

			await waitForResolved(() => expect(db2.log.log.length).to.eq(db2Length));
			await waitForResolved(() => expect(db3.log.log.length).to.eq(db3Length));
		});

		it("replace range with another node write after join", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
					replicas: {
						min: 1,
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			let db3 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.eq(3),
			);

			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}

			await waitForConverged(() => db1.log.log.length);
			await waitForConverged(() => db2.log.log.length);
			await waitForConverged(() => db3.log.log.length);

			const db2Length = db2.log.log.length;
			const db3Length = db3.log.log.length;

			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.greaterThan(0),
			);

			await waitForResolved(() =>
				expect(db3.log.log.length).to.be.greaterThan(0),
			);

			const range2 = (
				await db2.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await db2.log.replicate({ id: range2.id, offset: 0.1, factor: 0.1 });

			const range3 = (
				await db3.log.getMyReplicationSegments()
			)[0].toReplicationRange();

			await db3.log.replicate({ id: range3.id, offset: 0.1, factor: 0.1 });

			await waitForConverged(() => db2.log.log.length);
			await waitForConverged(() => db3.log.log.length);
			expect(db2.log.log.length).to.be.lessThan(db2Length);
			expect(db3.log.log.length).to.be.lessThan(db3Length);

			// reset to original

			await db2.log.replicate({ id: range2.id, offset: 0.5, factor: 0.5 });

			await db3.log.replicate({ id: range3.id, offset: 0.5, factor: 0.5 });

			await waitForResolved(() => expect(db2.log.log.length).to.eq(db2Length));
			await waitForResolved(() => expect(db3.log.log.length).to.eq(db3Length));
		});
		it("distribute", async () => {
			const u32Div3 = Math.round(0xffffffff / 3);
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0xffffffff,
						normalized: false,
					},
					replicas: {
						min: 1,
					},
				},
			});

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0,
							factor: 0xffffffff,
							normalized: false,
						},
						replicas: {
							min: 1,
						},
					},
				},
			))!;

			let db3 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicate: {
							offset: 0,
							factor: 0xffffffff,
							normalized: false,
						},
						replicas: {
							min: 1,
						},
					},
				},
			))!;

			let entryCount = 300;

			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}

			await waitForResolved(() => expect(db1.log.log.length).equal(entryCount));
			await waitForResolved(() => expect(db2.log.log.length).equal(entryCount));
			await waitForResolved(() => expect(db3.log.log.length).equal(entryCount));

			db1.log.replicate(
				{ factor: u32Div3, offset: 0, normalized: false },
				{ reset: true },
			);
			db2.log.replicate(
				{ factor: u32Div3, offset: u32Div3, normalized: false },
				{ reset: true },
			);
			db3.log.replicate(
				{ factor: u32Div3, offset: u32Div3 * 2, normalized: false },
				{ reset: true },
			);

			await waitForResolved(() =>
				expect(db1.log.log.length).to.closeTo(entryCount / 3, 30),
			);
			await waitForResolved(() =>
				expect(db2.log.log.length).to.closeTo(entryCount / 3, 30),
			);
			await waitForResolved(() =>
				expect(db3.log.log.length).to.closeTo(entryCount / 3, 30),
			);
			await waitForResolved(() =>
				expect(
					db1.log.log.length + db2.log.log.length + db3.log.log.length,
				).to.equal(entryCount),
			);
			for (const db of [db1, db2, db3]) {
				expect(await db.log.getPrunable()).to.have.length(0);
			}
		});

		it("close", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						factor: 0.333,
						offset: 0.333,
					},
				},
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							factor: 0.333,
							offset: 0,
						},
					},
				},
			);
			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicate: {
							factor: 0.333,
							offset: 0.666,
						},
					},
				},
			);

			const sampleSize = 1e3;
			const entryCount = sampleSize;

			await waitForResolved(async () =>
				expect(await db1.log.replicationIndex?.getSize()).equal(3),
			);
			await waitForResolved(async () =>
				expect(await db2.log.replicationIndex?.getSize()).equal(3),
			);
			await waitForResolved(async () =>
				expect(await db3.log.replicationIndex?.getSize()).equal(3),
			);

			const promises: Promise<any>[] = [];
			for (let i = 0; i < entryCount; i++) {
				promises.push(
					db1.add(toBase64(new Uint8Array([i])), {
						meta: { next: [] },
					}),
				);
			}

			await Promise.all(promises);

			await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

			const distribute = sinon.spy(db1.log.onReplicationChange);
			db1.log.onReplicationChange = distribute;
			await db3.close();
			await checkBounded(entryCount, 1, 1, db1, db2);
		});

		it("a smaller replicator join leave joins", async () => {
			const db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					replicate: {
						factor: 1, // this  replicator will get all entries
					},
					replicas: {
						min: 2, // we set min replicas to 2 to ensure second node should have all entries no matter what
					},
					timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
				},
			});
			let entryCount = 100;
			for (let i = 0; i < entryCount; i++) {
				await db1.add("hello" + i, { meta: { next: [] } });
			}

			let db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							offset: 0.1,
							factor: 0.1, // some small range
						},
						replicas: {
							min: 2,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			await waitForResolved(() =>
				expect(db1.log.log.length).to.be.equal(entryCount),
			);
			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.equal(entryCount),
			);

			await db2.close();
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: {
							factor: 0.2, // some small range
							offset: 0.2, // but on another place
						},
						replicas: {
							min: 2,
						},
						timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
					},
				},
			))!;

			await waitForResolved(() =>
				expect(db1.log.log.length).to.be.equal(entryCount),
			);

			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.equal(entryCount),
			);
		});
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
