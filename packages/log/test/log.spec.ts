import { deserialize, serialize } from "@dao-xyz/borsh";
import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { HashmapIndices } from "@peerbit/indexer-simple";
import {
	CONVERGENCE_MESSAGE_PRIORITY,
	FOREGROUND_READ_MESSAGE_PRIORITY,
} from "@peerbit/stream-interface";
import assert from "assert";
import { expect } from "chai";
import sinon from "sinon";
import { Timestamp } from "../src/clock.js";
import { Log } from "../src/log.js";
import { signKey, signKey2, signKey3 } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("properties", function () {
	let store: BlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("constructor", () => {
		it("creates an empty log with default params", async () => {
			const log = new Log();
			await log.open(store, signKey, undefined);
			assert.notStrictEqual(log.entryIndex, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.toArray(), null);
			assert.deepStrictEqual(await log.toArray(), []);
			assert.deepStrictEqual(await log.getHeads().all(), []);
			assert.deepStrictEqual(await log.getTailHashes(), []);
		});

		it("initializes runtime lifecycle state after a borsh round trip", async () => {
			const log = deserialize(
				serialize(new Log<Uint8Array>()),
				Log,
			) as Log<Uint8Array>;
			let changes = 0;
			const onChange = async () => {
				changes++;
			};

			expect(log.closed).to.equal(true);
			await log.open(store, signKey, { onChange });
			expect(log.closed).to.equal(false);
			await log.append(new Uint8Array([1]));
			expect(changes).to.equal(1);
			await log.close();
			expect(log.closed).to.equal(true);

			await log.open(store, signKey, { onChange });
			await log.append(new Uint8Array([2]));
			expect(changes).to.equal(2);
			await log.drop();
			expect(log.closed).to.equal(true);
		});

		it("closes and drops unopened borsh round trips", async () => {
			const roundTrip = () =>
				deserialize(serialize(new Log<Uint8Array>()), Log) as Log<Uint8Array>;
			const closing = roundTrip();
			const dropping = roundTrip();

			expect(closing.closed).to.equal(true);
			await closing.close();
			expect(closing.closed).to.equal(true);
			expect(dropping.closed).to.equal(true);
			await dropping.drop();
			expect(dropping.closed).to.equal(true);
		});

		it("rejects reopen without corrupting the live generation", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey, undefined);
			const first = await log.append(new Uint8Array([1]));
			await expect(log.open(store, signKey, undefined)).rejectedWith(
				"Already open",
			);
			expect(log.closed).to.equal(false);
			expect(log.length).to.equal(1);
			expect((await log.get(first.entry.hash))?.hash).to.equal(
				first.entry.hash,
			);
			await log.append(new Uint8Array([2]));
			expect(log.length).to.equal(2);
			await log.close();
			expect(log.closed).to.equal(true);
		});

		it("lets only the first concurrent open initialize the log", async () => {
			const log = new Log<Uint8Array>();
			const indexer = new HashmapIndices();
			const originalStart = indexer.start.bind(indexer);
			let markStarted!: () => void;
			const started = new Promise<void>((resolve) => {
				markStarted = resolve;
			});
			let releaseStart!: () => void;
			const startGate = new Promise<void>((resolve) => {
				releaseStart = resolve;
			});
			const start = sinon.stub(indexer, "start").callsFake(async () => {
				markStarted();
				await startGate;
				await originalStart();
			});

			const first = log.open(store, signKey, { indexer });
			const second = log.open(store, signKey, { indexer });
			try {
				await expect(second).rejectedWith("Already open");
				await started;
				expect(start.calledOnce).to.equal(true);
				releaseStart();
				await first;
				expect(log.closed).to.equal(false);
				await log.append(new Uint8Array([3]));
				expect(log.length).to.equal(1);
				await log.close();
				expect(log.closed).to.equal(true);
			} finally {
				releaseStart();
				await first.catch(() => undefined);
				await log.close().catch(() => undefined);
				start.restore();
			}
		});

		it("stops a started indexer when later open initialization fails", async () => {
			const log = new Log<Uint8Array>();
			const failedIndexer = new HashmapIndices();
			const start = sinon.spy(failedIndexer, "start");
			const stop = sinon.spy(failedIndexer, "stop");
			const failure = new Error("scope initialization failed");
			const scope = sinon.stub(failedIndexer, "scope").rejects(failure);

			expect(
				await log.open(store, signKey, { indexer: failedIndexer }).then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(failure);
			expect(start.calledOnce).to.equal(true);
			expect(stop.calledOnce).to.equal(true);
			expect(log.closed).to.equal(true);

			await log.open(store, signKey, { indexer: new HashmapIndices() });
			await log.append(Uint8Array.of(4));
			expect(log.length).to.equal(1);
			await log.close();
			scope.restore();
		});

		it("treats a post-init open failure as already closed", async () => {
			const log = new Log<Uint8Array>();
			const failedIndexer = new HashmapIndices();
			const heads = await failedIndexer.scope("heads");
			const originalInit = heads.init.bind(heads);
			const failure = new Error("persisted failed after init");
			const init = sinon.stub(heads, "init").callsFake(async (properties) => {
				const index = await originalInit(properties as any);
				sinon.stub(index, "persisted").rejects(failure);
				return index;
			});
			const stop = sinon.spy(failedIndexer, "stop");

			expect(
				await log.open(store, signKey, { indexer: failedIndexer }).then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(failure);
			expect(stop.calledOnce).to.equal(true);
			await log.close();
			expect(stop.calledOnce).to.equal(true);
			expect((log as any)._lifecycleState).to.equal("closed");

			await log.open(store, signKey, { indexer: new HashmapIndices() });
			await log.close();
			init.restore();
		});

		it("lets concurrent close own a post-init open failure", async () => {
			const log = new Log<Uint8Array>();
			const failedIndexer = new HashmapIndices();
			const heads = await failedIndexer.scope("heads");
			const originalInit = heads.init.bind(heads);
			const failure = new Error("persisted failed after init");
			let markPersisted!: () => void;
			const persisted = new Promise<void>((resolve) => {
				markPersisted = resolve;
			});
			let releasePersisted!: () => void;
			const persistedGate = new Promise<void>((resolve) => {
				releasePersisted = resolve;
			});
			const init = sinon.stub(heads, "init").callsFake(async (properties) => {
				const index = await originalInit(properties as any);
				sinon.stub(index, "persisted").callsFake(async () => {
					markPersisted();
					await persistedGate;
					throw failure;
				});
				return index;
			});
			const stop = sinon.spy(failedIndexer, "stop");
			const opening = log.open(store, signKey, { indexer: failedIndexer });
			try {
				await persisted;
				const closing = log.close();
				releasePersisted();
				const [openResult, closeResult] = await Promise.allSettled([
					opening,
					closing,
				]);
				expect(openResult.status).to.equal("rejected");
				if (openResult.status === "rejected") {
					expect(openResult.reason).to.equal(failure);
				}
				expect(closeResult.status).to.equal("fulfilled");
				expect(stop.calledOnce).to.equal(true);
				expect((log as any)._lifecycleState).to.equal("closed");

				await log.open(store, signKey, { indexer: new HashmapIndices() });
				await log.close();
			} finally {
				releasePersisted();
				await opening.catch(() => undefined);
				await log.close().catch(() => undefined);
				init.restore();
			}
		});
		it("sets an id", async () => {
			const log = new Log({ id: new Uint8Array(1) });
			await log.open(store, signKey);
			expect(log.id).to.deep.equal(new Uint8Array(1));
		});

		it("generates if id is not passed as an argument", async () => {
			const log = new Log();
			await log.open(store, signKey);
			expect(log.id).to.be.instanceOf(Uint8Array);
		});
	});

	describe("toString", () => {
		let log: Log<string>;
		const expectedData =
			'"five"\n└─"four"\n  └─"three"\n    └─"two"\n      └─"one"';

		beforeEach(async () => {
			log = new Log<string>();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("two", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("three", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("four", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("five", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("returns a nicely formatted string", async () => {
			expect(
				await log.toString((p) => Buffer.from(p.data).toString()),
			).to.deep.equal(expectedData);
		});
	});

	describe("get", () => {
		let log: Log<any>;

		beforeEach(async () => {
			log = new Log<Uint8Array>();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", {
				meta: {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				},
			});
		});

		it("returns an Entry", async () => {
			const entry = await log.get((await log.toArray())[0].hash)!;
			expect(entry?.hash).to.equal(
				"zb2rhYpDDgijHQyZRYovg3mKpgLDCBb89uFGFrRbYoiVCKGiX",
			);
		});

		it("returns undefined when Entry is not in the log", async () => {
			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			);
			assert.deepStrictEqual(entry, undefined);
		});

		it("rechecks storage after a previous miss", async () => {
			const sourceStore = new AnyBlockStore();
			const targetStore = new AnyBlockStore();
			await Promise.all([sourceStore.start(), targetStore.start()]);

			const sourceLog = new Log<string>();
			const targetLog = new Log<string>();
			try {
				await sourceLog.open(sourceStore, signKey, {
					encoding: JSON_ENCODING,
				});
				await targetLog.open(targetStore, signKey, {
					encoding: JSON_ENCODING,
				});

				const { entry } = await sourceLog.append("late");
				const encodedEntry = await sourceStore.get(entry.hash);
				expect(encodedEntry).to.exist;

				expect(await targetLog.get(entry.hash)).to.equal(undefined);
				expect(await targetStore.put(encodedEntry!)).to.equal(entry.hash);

				const resolved = await targetLog.get(entry.hash);
				expect(await resolved?.getPayloadValue()).to.equal("late");
			} finally {
				await Promise.all([
					sourceLog.close(),
					targetLog.close(),
					sourceStore.stop(),
					targetStore.stop(),
				]);
			}
		});

		it("does not fetch from remotes by if missing block by default", async () => {
			const storeGetFn = store.get.bind(store);
			let remoteFetchOptions: any = undefined;
			let fetched = false;
			store.get = async (hash, options) => {
				remoteFetchOptions = options;
				fetched = true;
				return storeGetFn(hash, options);
			};
			log.entryIndex.has = () => Promise.resolve(true);

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			);
			assert.deepStrictEqual(entry, undefined);
			expect(fetched).to.be.true;
			expect(!remoteFetchOptions.remote).to.be.true;
		});

		it("fetches remotes with timeout", async () => {
			const storeGetFn = store.get.bind(store);
			let timeout: number | undefined;
			let fetched = false;
			store.get = async (hash, options) => {
				timeout =
					typeof options?.remote === "object"
						? options.remote.timeout
						: undefined;
				fetched = true;
				return storeGetFn(hash, options);
			};
			log.entryIndex.has = () => Promise.resolve(true);

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
				{
					remote: {
						timeout: 123,
					},
				},
			);
			assert.deepStrictEqual(entry, undefined);
			expect(fetched).to.be.true;
			expect(timeout).to.eq(123);
		});

		it("fetches remote entries with metadata priority by default", async () => {
			const storeGetFn = store.get.bind(store);
			let priority: number | undefined;
			store.get = async (hash, options) => {
				priority =
					typeof options?.remote === "object"
						? options.remote.priority
						: undefined;
				return storeGetFn(hash, options);
			};

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
				{ remote: true },
			);
			assert.deepStrictEqual(entry, undefined);
			expect(priority).to.eq(FOREGROUND_READ_MESSAGE_PRIORITY);
		});

		it("preserves explicit remote entry priority", async () => {
			const storeGetFn = store.get.bind(store);
			let priority: number | undefined;
			store.get = async (hash, options) => {
				priority =
					typeof options?.remote === "object"
						? options.remote.priority
						: undefined;
				return storeGetFn(hash, options);
			};

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
				{ remote: { timeout: 123, priority: CONVERGENCE_MESSAGE_PRIORITY } },
			);
			assert.deepStrictEqual(entry, undefined);
			expect(priority).to.eq(CONVERGENCE_MESSAGE_PRIORITY);
		});

		it("injects remote.from when resolveRemotePeers is configured", async () => {
			const fromPeers = ["peer-a", "peer-b", "peer-c"];
			const storeGetFn = store.get.bind(store);
			let observedFrom: string[] | undefined;
			let observedPriority: number | undefined;
			store.get = async (hash, options) => {
				observedFrom =
					typeof options?.remote === "object" ? options.remote.from : undefined;
				observedPriority =
					typeof options?.remote === "object"
						? options.remote.priority
						: undefined;
				return storeGetFn(hash, options);
			};

			const logWithResolver = new Log<Uint8Array>();
			await logWithResolver.open(store, signKey, {
				encoding: JSON_ENCODING,
				indexer: new HashmapIndices(),
				resolveRemotePeers: () => fromPeers,
			});

			const entry = await logWithResolver.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
				{ remote: true },
			);
			assert.deepStrictEqual(entry, undefined);
			expect(observedFrom).to.deep.equal(fromPeers);
			expect(observedPriority).to.eq(FOREGROUND_READ_MESSAGE_PRIORITY);
		});

		it("gets ordered entries, duplicates, and misses with one block batch read", async () => {
			const localStore = new AnyBlockStore();
			await localStore.start();
			const source = new Log<string>();
			const reader = new Log<string>();
			await source.open(localStore, signKey, { encoding: JSON_ENCODING });
			await reader.open(localStore, signKey2, { encoding: JSON_ENCODING });

			let getManyStub: sinon.SinonStub | undefined;
			let getSpy: sinon.SinonSpy | undefined;
			try {
				const { entry: first } = await source.append("first", {
					meta: { next: [] },
				});
				const { entry: second } = await source.append("second", {
					meta: { next: [] },
				});
				const missing = "zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
				const stored = new Map([
					[first.hash, await localStore.get(first.hash)],
					[second.hash, await localStore.get(second.hash)],
				]);
				getManyStub = sinon
					.stub(localStore, "getMany")
					.callsFake(async (hashes) => hashes.map((hash) => stored.get(hash)));
				getSpy = sinon.spy(localStore, "get");

				const entries = await reader.getMany([
					first.hash,
					missing,
					second.hash,
					first.hash,
				]);

				expect(entries.map((entry) => entry?.hash)).to.deep.equal([
					first.hash,
					undefined,
					second.hash,
					first.hash,
				]);
				expect(await entries[0]!.getPayloadValue()).to.equal("first");
				expect(await entries[2]!.getPayloadValue()).to.equal("second");
				expect(getManyStub.callCount).to.equal(1);
				expect(getManyStub.firstCall.args[0]).to.deep.equal([
					first.hash,
					missing,
					second.hash,
					first.hash,
				]);
				expect(getSpy.callCount).to.equal(0);
			} finally {
				getManyStub?.restore();
				getSpy?.restore();
				await Promise.all([source.close(), reader.close()]);
				await localStore.stop();
			}
		});

		it("preserves remote options when getMany falls back to per-entry reads", async () => {
			const localStore = new AnyBlockStore();
			await localStore.start();
			const reader = new Log<string>();
			await reader.open(localStore, signKey, { encoding: JSON_ENCODING });
			const getManySpy = sinon.spy(localStore, "getMany");
			const observedOptions: any[] = [];
			const getStub = sinon
				.stub(localStore, "get")
				.callsFake(async (_hash, options) => {
					observedOptions.push(options);
					return undefined;
				});

			try {
				const entries = await reader.getMany(
					[
						"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
						"zb2rhYpDDgijHQyZRYovg3mKpgLDCBb89uFGFrRbYoiVCKGiX",
					],
					{ remote: { timeout: 123 } },
				);

				expect(entries).to.deep.equal([undefined, undefined]);
				expect(getManySpy.callCount).to.equal(0);
				expect(getStub.callCount).to.equal(2);
				expect(
					observedOptions.map((options) => options.remote.timeout),
				).to.deep.equal([123, 123]);
				expect(
					observedOptions.map((options) => options.remote.priority),
				).to.deep.equal([
					FOREGROUND_READ_MESSAGE_PRIORITY,
					FOREGROUND_READ_MESSAGE_PRIORITY,
				]);
			} finally {
				getManySpy.restore();
				getStub.restore();
				await reader.close();
				await localStore.stop();
			}
		});
	});

	describe("setIdentity", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("changes identity", async () => {
			expect((await log.toArray())[0].meta.clock.id).to.deep.equal(
				signKey.publicKey.bytes,
			);
			log.setIdentity(signKey2);
			await log.append("two", { meta: { gidSeed: Buffer.from("a") } });
			assert.deepStrictEqual(
				(await log.toArray())[1].meta.clock.id,
				signKey2.publicKey.bytes,
			);
			log.setIdentity(signKey3);
			await log.append("three", { meta: { gidSeed: Buffer.from("a") } });
			assert.deepStrictEqual(
				(await log.toArray())[2].meta.clock.id,
				signKey3.publicKey.bytes,
			);
		});
	});

	describe("has", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("returns true if it has an Entry", async () => {
			assert(await log.has((await log.toArray())[0].hash));
		});

		it("returns true if it has an Entry, hash lookup", async () => {
			assert(await log.has((await log.toArray())[0].hash));
		});

		it("returns false if it doesn't have the Entry", async () => {
			expect(
				await log.has("zb2rhbnwihVVVVEVVPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"),
			).equal(false);
		});
	});

	describe("values", () => {
		it("returns all entries in the log", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			expect((await log.toArray()) instanceof Array).equal(true);
			expect(log.length).equal(0);
			await log.append(new Uint8Array([1]));
			await log.append(new Uint8Array([2]));
			await log.append(new Uint8Array([3]));
			expect((await log.toArray()) instanceof Array).equal(true);
			expect(log.length).equal(3);
			expect((await log.toArray())[0].payload.getValue()).to.deep.equal(
				new Uint8Array([1]),
			);
			expect((await log.toArray())[1].payload.getValue()).to.deep.equal(
				new Uint8Array([2]),
			);
			expect((await log.toArray())[2].payload.getValue()).to.deep.equal(
				new Uint8Array([3]),
			);
		});
	});

	describe("size", () => {
		it("returns the sum of payloads", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			await log.append(new Uint8Array([1]));
			await log.append(new Uint8Array([2, 3]));
			await log.append(new Uint8Array([3, 4, 5]));
			const arr = await log.toArray();
			const size = arr.reduce(
				(acc, entry) => acc + entry.payload.byteLength,
				0,
			);
			expect(log.length).equal(3);
			expect(6n).equal(BigInt(size));
		});
	});

	describe("indexer", () => {
		it("unique", async () => {
			// TODO what is the purpose of this test?
			// if indices.scope is called we assert that scope needs to be created outside the open

			let indices = new HashmapIndices();

			const log1 = new Log();
			await log1.open(store, signKey, { indexer: await indices.scope("x") });

			const log2 = new Log();
			await log2.open(store, signKey, { indexer: await indices.scope("y") });
			await log1.append(new Uint8Array([0]));

			expect(await log1.toArray()).to.have.length(1);
			expect(await log2.toArray()).to.have.length(0);
		});
	});
});
