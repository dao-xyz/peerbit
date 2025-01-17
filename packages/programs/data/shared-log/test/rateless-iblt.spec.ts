import { Cache } from "@peerbit/cache";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src";
import type { TransportMessage } from "../src/message";
import {
	MoreSymbols,
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";
import { EventStore } from "./utils/stores";

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
	coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
};

describe("rateless-iblt-syncronizer", () => {
	let session: TestSession;
	let db1: EventStore<string, ReplicationDomainHash<"u64">>,
		db2: EventStore<string, ReplicationDomainHash<"u64">>;

	beforeEach(async () => {});

	afterEach(async () => {
		await session.stop();
	});

	const collectMessages = async (
		log: EventStore<string, ReplicationDomainHash<"u64">>,
	) => {
		const onMessageDb = sinon.spy(log.log, "onMessage");
		log.log.onMessage = onMessageDb;
		return {
			get calls(): TransportMessage[] {
				return onMessageDb.getCalls().map((x) => x.args[0]);
			},
		};
	};

	const countMessages = (messages: TransportMessage[], type: any) => {
		return messages.filter((x) => x instanceof type).length;
	};

	const setupLogs = async (
		syncedCount: number,
		unsyncedCount: number,
		oneSided = false,
	) => {
		session = await TestSession.disconnected(2);
		db1 = await session.peers[0].open(
			new EventStore<string, ReplicationDomainHash<"u64">>(),
			{
				args: {
					replicate: {
						factor: 1,
					},
					setup,
				},
			},
		);

		db2 = await session.peers[1].open(db1.clone(), {
			args: {
				replicate: {
					factor: 1,
				},
				setup,
			},
		});

		for (let i = 0; i < syncedCount; i++) {
			const out = await db1.add("test", { meta: { next: [] } });
			await db2.log.join([out.entry]);
		}

		for (let i = 0; i < unsyncedCount; i++) {
			await db1.add("test", { meta: { next: [] } });
			if (!oneSided) {
				await db2.add("test", { meta: { next: [] } });
			}
		}
		expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount);
		expect(db2.log.log.length).to.equal(
			syncedCount + (oneSided ? 0 : unsyncedCount),
		);
	};

	it("already synced", async () => {
		const syncedCount = 1000;
		await setupLogs(syncedCount, 0);

		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await db1.node.dial(db2.node.getMultiaddrs());

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("all missing will skip iblt syncing", async () => {
		const syncedCount = 0;
		const unsyncedCount = 1000;
		let oneSided = true;

		await setupLogs(syncedCount, unsyncedCount, oneSided); // only db1 will have entries
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);
		await db1.node.dial(db2.node.getMultiaddrs());
		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(unsyncedCount),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(unsyncedCount),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.be.equal(0);
		expect(countMessages(db1Messages.calls, RequestAll)).to.be.equal(1);
		expect(countMessages(db1Messages.calls, StartSync)).to.be.equal(0);

		expect(countMessages(db2Messages.calls, MoreSymbols)).to.be.equal(0);
		expect(countMessages(db2Messages.calls, RequestAll)).to.be.equal(0);
		expect(countMessages(db2Messages.calls, StartSync)).to.be.equal(1);
	});

	it("one missing", async () => {
		const syncedCount = 1000;
		const unsyncedCount = 1;
		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);
		await db1.node.dial(db2.node.getMultiaddrs());

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0); // becase StartSync will emit a few symbols that will be enough
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0); // becase StartSync will emit a few symbols that will be enough
	});

	it("many missing", async () => {
		const syncedCount = 3e3;
		const unsyncedCount = 3e3;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await db1.node.dial(db2.node.getMultiaddrs());

		await waitForResolved(
			() =>
				expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
			{
				timeout: 2e4,
			},
		);
		await waitForResolved(
			() =>
				expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
			{
				timeout: 2e4,
			},
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.be.greaterThan(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.be.greaterThan(0);
	});

	/* it("builds", async () => {
		const { indices } = await createFromValues(
			"u64",
			[{ publicKey: a, offset: 0, length: 1 }],
			[0.5],
		);
		const rangeEncoders = new RangeToEncoders(
			a,
			indices.rangeIndex,
			indices.entryIndex,
		);
		await rangeEncoders.build();
		expect(rangeEncoders.encoders.size).to.equal(1);
	});
	
	it("generates determenistically", async () => {
		const { indices, ranges } = await createFromValues(
			"u64",
			[{ publicKey: a, offset: 0, length: 1 }],
			[0.5],
		);
		const rangeEncoders = new RangeToEncoders(
			a,
			indices.rangeIndex,
			indices.entryIndex,
		);
		await rangeEncoders.build();
		expect(rangeEncoders.encoders.size).to.equal(1);
	
		const generator = rangeEncoders.createSymbolGenerator(ranges[0]);
	
		let symbol1 = generator.next();
		expect(typeof symbol1.count).to.equal("bigint");
		expect(typeof symbol1.hash).to.equal("bigint");
		expect(typeof symbol1.symbol).to.equal("bigint");
		expect(symbol1.hash).to.not.equal(0n);
	
		const generator2 = rangeEncoders.createSymbolGenerator(ranges[0]);
	
		let symbol2 = generator2.next();
		expect(symbol1).to.deep.equal(symbol2);
	});
	
	describe("diff", () => {
		it("no difference", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[0.5],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[0.5],
			);
	
			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});
	
			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.have.length(0);
		});
	
		it("remote is missing entry", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[0.5],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[],
			);
	
			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});
	
			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.deep.eq([BigInt(local.entry[0].coordinate)]);
		});
	
		it("local is missing entry", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[0.5],
			);
	
			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});
	
			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.deep.eq([]);
		});
	}); */
});

/* 
describe("sync", () => {
	let indicesArr: SQLiteIndices[];

	let createRangeEncoder = async <R extends "u32" | "u64">(
		resolution: R,
		publicKey: PublicSignKey,
		rects: { publicKey: PublicSignKey; length: number; offset: number }[],
		entries: number[],
	) => {
		const { indices, entry, ranges } = await createFromValues(
			resolution,
			rects,
			entries,
		);
		const rangeEncoders = new RangeToEncoders(
			publicKey,
			indices.rangeIndex,
			indices.entryIndex,
		);
		await rangeEncoders.build();
		return { indices, rangeEncoders, entry, ranges };
	};

	let createFromValues = async <R extends "u32" | "u64">(
		resolution: R,
		rects: { publicKey: PublicSignKey; length: number; offset: number }[],
		entries: number[],
	) => {
		const { rangeClass, entryClass } = resolveClasses(resolution);
		const denormalizerFN = denormalizer(resolution);
		let ranges: ReplicationRangeIndexable<R>[] = rects.map(
			(x) =>
				new rangeClass({
					publicKey: x.publicKey,
					// @ts-ignore
					length: denormalizerFN(x.length),
					// @ts-ignore
					offset: denormalizerFN(x.offset),
					timestamp: 0n,
				}) as unknown as ReplicationRangeIndexable<R>,
		) as ReplicationRangeIndexable<R>[];
		let entry: EntryReplicated<R>[] = entries.map(
			(x) =>
				// @ts-ignore
				new entryClass({
					// @ts-ignore
					coordinate: denormalizerFN(x) as NumberFromType<R>,
					assignedToRangeBoundary: false,
					hash: String("a"),
					meta: new Meta({
						clock: new LamportClock({ id: randomBytes(32) }),
						gid: String(x),
						next: [],
						type: 0,
						data: undefined,
					}),
				}) as EntryReplicated<R>,
		);

		return {
			indices: await create(ranges, entry, resolution),
			ranges,
			entry,
		};
	};

	let create = async <R extends "u32" | "u64">(
		rects: ReplicationRangeIndexable<R>[],
		entries: EntryReplicated<R>[],
		resolution: R,
	) => {
		let indices = await createIndices();
		await indices.start();

		const rangeClass =
			resolution === "u32"
				? ReplicationRangeIndexableU32
				: ReplicationRangeIndexableU64;
		const indexRects = await indices.init({ schema: rangeClass as any });
		for (const rect of rects) {
			await indexRects.put(rect);
		}

		const entryClass =
			resolution === "u32" ? EntryReplicatedU32 : EntryReplicatedU64;
		const indexEntries = await indices.init({ schema: entryClass as any });
		for (const entry of entries) {
			await indexEntries.put(entry);
		}

		indicesArr.push(indices);
		return {
			rangeIndex: indexRects,
			entryIndex: indexEntries,
		} as {
			rangeIndex: Index<ReplicationRangeIndexable<R>>;
			entryIndex: Index<EntryReplicated<R>>;
		};
	};
	let a: Ed25519PublicKey;
	let b: Ed25519PublicKey;

	beforeEach(async () => {
		indicesArr = [];
		a = (await Ed25519Keypair.create()).publicKey;
		b = (await Ed25519Keypair.create()).publicKey;
	});

	afterEach(async () => {
		await Promise.all(indicesArr.map((x) => x.stop()));
	});

	it("builds", async () => {
		const { indices } = await createFromValues(
			"u64",
			[{ publicKey: a, offset: 0, length: 1 }],
			[0.5],
		);
		const rangeEncoders = new RangeToEncoders(
			a,
			indices.rangeIndex,
			indices.entryIndex,
		);
		await rangeEncoders.build();
		expect(rangeEncoders.encoders.size).to.equal(1);
	});

	it("generates determenistically", async () => {
		const { indices, ranges } = await createFromValues(
			"u64",
			[{ publicKey: a, offset: 0, length: 1 }],
			[0.5],
		);
		const rangeEncoders = new RangeToEncoders(
			a,
			indices.rangeIndex,
			indices.entryIndex,
		);
		await rangeEncoders.build();
		expect(rangeEncoders.encoders.size).to.equal(1);

		const generator = rangeEncoders.createSymbolGenerator(ranges[0]);

		let symbol1 = generator.next();
		expect(typeof symbol1.count).to.equal("bigint");
		expect(typeof symbol1.hash).to.equal("bigint");
		expect(typeof symbol1.symbol).to.equal("bigint");
		expect(symbol1.hash).to.not.equal(0n);

		const generator2 = rangeEncoders.createSymbolGenerator(ranges[0]);

		let symbol2 = generator2.next();
		expect(symbol1).to.deep.equal(symbol2);
	});

	describe("diff", () => {
		it("no difference", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[0.5],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[0.5],
			);

			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});

			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.have.length(0);
		});

		it("remote is missing entry", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[0.5],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[],
			);

			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});

			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.deep.eq([BigInt(local.entry[0].coordinate)]);
		});

		it("local is missing entry", async () => {
			const local = await createRangeEncoder(
				"u64",
				a,
				[{ publicKey: a, offset: 0, length: 1 }],
				[],
			);
			const remote = await createRangeEncoder(
				"u64",
				b,
				[{ publicKey: b, offset: 0, length: 1 }],
				[0.5],
			);

			const receiver = await getMissingValuesInRemote({
				myEncoder: local.rangeEncoders,
				remoteRange: remote.ranges[0],
			});

			const bobGenerator = remote.rangeEncoders.createSymbolGenerator(
				remote.ranges[0],
			);
			const next = bobGenerator.next();
			const out = receiver.process(next);
			expect(out.done).to.equal(true);
			expect(out.missing).to.deep.eq([]);
		});
	});
});
 */
