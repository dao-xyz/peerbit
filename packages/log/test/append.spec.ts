import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { EntryType } from "../src/entry-type.js";
import { EntryV0 } from "../src/entry-v0.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

describe("append", function () {
	let store: BlockStore;
	let signKey: Ed25519Keypair;

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			return !!(await store.get(hash, { remote: { timeout: 3000 } }));
		} catch (error) {
			return false;
		}
	};

	before(async () => {
		store = new AnyBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("append one", () => {
		let log: Log<Uint8Array>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey);
			await log.append(new Uint8Array([1]));
		});

		it("added the correct amount of items", () => {
			expect(log.length).equal(1);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.payload.getValue()).to.deep.equal(new Uint8Array([1]));
			});
		});

		it("added the correct amount of next pointers", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.meta.next.length).equal(0);
			});
		});

		it("has the correct heads", async () => {
			for (const head of await log.getHeads().all()) {
				expect(head.hash).to.deep.equal((await log.toArray())[0].hash);
			}
		});

		it("updated the clocks correctly", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
				expect(entry.meta.clock.timestamp.logical).equal(0);
			});
		});
	});

	describe("reset", () => {
		it("append", async () => {
			const log = new Log();
			await log.open(store, signKey);
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append(new Uint8Array([2]));
			expect(await blockExists(e1.hash)).to.be.true;
			expect(await blockExists(e2.hash)).to.be.true;
			const { entry: e3 } = await log.append(new Uint8Array([3]), {
				meta: { type: EntryType.CUT },
			});
			expect((await log.entryIndex.getHasNext(e1.hash).all()).length).equal(0);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await blockExists(e3.hash)).to.be.true;
		});

		it("can resolve the full entry from deleted", async () => {
			const log = new Log();

			let resolved: any = undefined;
			await log.open(store, signKey, {
				onChange: async (change) => {
					if (change.removed.length > 0) {
						resolved = await (
							await log.get(change.removed[0].hash)
						)?.getPayloadValue();
					}
				},
			});
			await log.append(new Uint8Array([1]));
			await log.append(new Uint8Array([2]), { meta: { type: EntryType.CUT } });
			expect(resolved).to.deep.eq(new Uint8Array([1]));
		});
	});

	it("buffers head index writes by default for ephemeral indexers", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, { indexer: new HashmapIndices() });
		const putSpy = sinon.spy(log.entryIndex.properties.index, "put");
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});

		expect((await log.get(entry.hash))?.hash).equal(entry.hash);
		expect(putSpy.callCount).equal(0);
		expect(
			(await log.getHeads().all()).map((head) => head.hash),
		).to.have.members([entry.hash]);
		expect(putSpy.callCount).equal(1);
		await log.close();
		expect(putSpy.callCount).equal(1);
		putSpy.restore();
	});

	it("writes head index eagerly when strict durability is requested on ephemeral indexers", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			appendDurability: "strict",
		});
		const putSpy = sinon.spy(log.entryIndex.properties.index, "put");
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});

		expect((await log.get(entry.hash))?.hash).equal(entry.hash);
		expect(
			(await log.getHeads().all()).map((head) => head.hash),
		).to.have.members([entry.hash]);
		expect(putSpy.callCount).equal(1);
		await log.close();
		expect(putSpy.callCount).equal(1);
		putSpy.restore();
	});

	it("rolls back only its native index generation and preserves same-CID pending facts", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			appendDurability: "strict",
		});
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});
		const index = log.entryIndex.properties.index;
		const concurrentShallow = entry.toShallow(false);
		const rejectedShallow = entry.toShallow(true);

		await log.entryIndex.putNativeCommittedAppendFacts({
			hash: entry.hash,
			unique: false,
			externalNextHashes: [],
			shallowEntry: concurrentShallow,
			isHead: false,
		});
		const transaction =
			log.entryIndex.beginNativeCommittedAppendFactsTransaction();
		log.entryIndex.putNativeCommittedAppendFacts(
			{
				hash: entry.hash,
				unique: true,
				externalNextHashes: [],
				shallowEntry: rejectedShallow,
				isHead: true,
			},
			transaction,
		);
		// A later same-CID publication gets its own generation and must survive
		// compensation of the rejected transaction.
		await log.entryIndex.putNativeCommittedAppendFacts({
			hash: entry.hash,
			unique: false,
			externalNextHashes: [],
			shallowEntry: concurrentShallow,
			isHead: false,
		});

		const failure = new Error("native index generation failed");
		const putStub = sinon.stub(index, "put").rejects(failure);
		try {
			const rejected = await log.entryIndex
				.flushNativeCommittedAppendFacts(transaction)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(rejected).equal(failure);
			await log.entryIndex.rollbackNativeCommittedAppendFacts(transaction);
			expect(log.length).equal(1);
		} finally {
			putStub.restore();
		}

		const laterPutSpy = sinon.spy(index, "put");
		try {
			await new Promise((resolve) => setTimeout(resolve, 400));
			expect(laterPutSpy.callCount).equal(1);
			expect((await log.entryIndex.getShallow(entry.hash))?.value.head).equal(
				false,
			);
			await log.entryIndex.flushPendingWrites();
			expect(laterPutSpy.callCount).equal(1);
			expect(log.length).equal(1);
		} finally {
			laterPutSpy.restore();
			await log.close();
		}
	});

	it("restores an earlier same-CID pending fact after an applied native index write rejects", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			appendDurability: "strict",
		});
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});
		const index = log.entryIndex.properties.index;
		const previousShallow = entry.toShallow(false);
		const rejectedShallow = entry.toShallow(true);

		await log.entryIndex.putNativeCommittedAppendFacts({
			hash: entry.hash,
			unique: false,
			externalNextHashes: [],
			shallowEntry: previousShallow,
			isHead: false,
		});
		const transaction =
			log.entryIndex.beginNativeCommittedAppendFactsTransaction();
		log.entryIndex.putNativeCommittedAppendFacts(
			{
				hash: entry.hash,
				unique: true,
				externalNextHashes: [],
				shallowEntry: rejectedShallow,
				isHead: true,
			},
			transaction,
		);

		const failure = new Error("applied native index generation failed");
		const originalPut = index.put.bind(index);
		const putStub = sinon.stub(index, "put").callsFake(async (value) => {
			await originalPut(value);
			throw failure;
		});
		try {
			const rejected = await log.entryIndex
				.flushNativeCommittedAppendFacts(transaction)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(rejected).equal(failure);
			await log.entryIndex.rollbackNativeCommittedAppendFacts(transaction);
			expect(log.length).equal(1);
			expect((await log.entryIndex.getShallow(entry.hash))?.value.head).equal(
				false,
			);
		} finally {
			putStub.restore();
		}

		const restoredPutSpy = sinon.spy(index, "put");
		try {
			await new Promise((resolve) => setTimeout(resolve, 400));
			expect(restoredPutSpy.callCount).equal(1);
			expect((await log.entryIndex.getShallow(entry.hash))?.value.head).equal(
				false,
			);
			expect(log.length).equal(1);
		} finally {
			restoredPutSpy.restore();
			await log.close();
		}
	});

	it("appendMany appends a local chain with one coalesced change", async () => {
		const log = new Log<Uint8Array>();
		const changes: any[] = [];
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
			onChange: (change) => {
				changes.push(change);
			},
		});
		const root = (await log.append(new Uint8Array([0]), { meta: { next: [] } }))
			.entry;
		changes.length = 0;

		const iterateSpy = sinon.spy(log.entryIndex.properties.index, "iterate");
		const putSpy = sinon.spy(log.entryIndex.properties.index, "put");
		const putBatchSpy = sinon.spy(log.entryIndex.properties.index, "putBatch");
		const blockPutSpy = sinon.spy(store, "put");
		const blockPutManySpy = sinon.spy(store, "putMany");
		const blockPutKnownManySpy =
			"putKnownMany" in store &&
			typeof (store as { putKnownMany?: unknown }).putKnownMany === "function"
				? sinon.spy(
						store as unknown as {
							putKnownMany: (blocks: [string, Uint8Array][]) => any;
						},
						"putKnownMany",
					)
				: undefined;
		const blockPutKnownManyColumnsSpy =
			"putKnownManyColumns" in store &&
			typeof (store as { putKnownManyColumns?: unknown })
				.putKnownManyColumns === "function"
				? sinon.spy(
						store as unknown as {
							putKnownManyColumns: (cids: string[], bytes: Uint8Array[]) => any;
						},
						"putKnownManyColumns",
					)
				: undefined;
		const shallowSpy = sinon.spy(EntryV0.prototype, "toShallow");
		const nativeAppendChainSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"putAppendChain",
		);
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainAndPut",
		);
		const nativeCommitSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainCommit",
		);
		const preparedBlockSpy = sinon.spy(Entry, "takePreparedBlock");
		const preparedShallowSpy = sinon.spy(Entry, "takePreparedShallowEntry");
		const preparedNativeSpy = sinon.spy(Entry, "takePreparedNativeLogEntry");

		try {
			const result = await log.appendMany([
				new Uint8Array([1]),
				new Uint8Array([2]),
				new Uint8Array([3]),
			]);

			expect(result.entries).to.have.length(3);
			expect(result.entries[0].meta.next).to.deep.equal([root.hash]);
			expect(result.entries[1].meta.next).to.deep.equal([
				result.entries[0].hash,
			]);
			expect(result.entries[2].meta.next).to.deep.equal([
				result.entries[1].hash,
			]);
			for (const entry of result.entries) {
				expect((await log.get(entry.hash))?.hash).equal(entry.hash);
			}
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([result.entries[2].hash]);
			expect(changes).to.have.length(1);
			expect(changes[0].added.map((added: any) => added.head)).to.deep.equal([
				false,
				false,
				true,
			]);
			expect(iterateSpy.callCount).equal(0);
			expect(putSpy.callCount).equal(0);
			expect(putBatchSpy.callCount).equal(1);
			expect(putBatchSpy.firstCall.args[0]).to.have.length(
				result.entries.length,
			);
			expect(blockPutSpy.callCount).equal(0);
			expect(blockPutManySpy.callCount).equal(0);
			expect(
				(blockPutKnownManySpy?.callCount ?? 0) +
					(blockPutKnownManyColumnsSpy?.callCount ?? 0),
			).equal(1);
			if (blockPutKnownManyColumnsSpy?.called) {
				expect(blockPutKnownManyColumnsSpy.firstCall.args[0]).to.have.length(
					result.entries.length,
				);
				expect(blockPutKnownManyColumnsSpy.firstCall.args[1]).to.have.length(
					result.entries.length,
				);
			} else {
				expect(blockPutKnownManySpy?.firstCall.args[0]).to.have.length(
					result.entries.length,
				);
			}
			expect(nativeCommitSpy.callCount).equal(1);
			expect(nativeCommitSpy.firstCall.args[0].payloadDatas).to.have.length(
				result.entries.length,
			);
			expect(shallowSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(1);
			expect(nativeAppendChainSpy.callCount).equal(0);
			expect(preparedBlockSpy.callCount).equal(0);
			expect(preparedShallowSpy.callCount).equal(0);
			expect(preparedNativeSpy.callCount).equal(0);
		} finally {
			iterateSpy.restore();
			putSpy.restore();
			putBatchSpy.restore();
			blockPutSpy.restore();
			blockPutManySpy.restore();
			blockPutKnownManySpy?.restore();
			blockPutKnownManyColumnsSpy?.restore();
			shallowSpy.restore();
			nativeAppendChainSpy.restore();
			nativePrepareAndPutSpy.restore();
			nativeCommitSpy.restore();
			preparedBlockSpy.restore();
			preparedShallowSpy.restore();
			preparedNativeSpy.restore();
			await log.close();
		}
	});

	it("appendMany commits blocks and graph in one native call when storage is native", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const root = (await log.append(new Uint8Array([0]), { meta: { next: [] } }))
			.entry;

		const blockPutManySpy = sinon.spy(nativeStore, "putMany");
		const indexPutSpy = sinon.spy(log.entryIndex.properties.index, "put");
		const indexPutBatchSpy = sinon.spy(
			log.entryIndex.properties.index,
			"putBatch",
		);
		const preparedBlockFromBytesSpy = sinon.spy(
			Entry,
			"preparedBlockFromBytes",
		);
		const nativeCommitSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainCommit",
		);
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainAndPut",
		);
		const nativeAppendChainSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"putAppendChain",
		);

		try {
			const result = await log.appendMany([
				new Uint8Array([1]),
				new Uint8Array([2]),
				new Uint8Array([3]),
			]);

			expect(result.entries).to.have.length(3);
			expect(result.entries[0].meta.next).to.deep.equal([root.hash]);
			expect(await result.entries[0].getPayloadValue()).to.deep.equal(
				new Uint8Array([1]),
			);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([result.entries[2].hash]);
			expect(nativeCommitSpy.callCount).equal(1);
			expect(nativeCommitSpy.firstCall.args[0].payloadDatas).to.have.length(
				result.entries.length,
			);
			expect(blockPutManySpy.callCount).equal(0);
			expect(preparedBlockFromBytesSpy.callCount).equal(0);
			expect(indexPutBatchSpy.callCount).equal(0);
			expect(indexPutSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(0);
			expect(nativeAppendChainSpy.callCount).equal(0);
			for (const entry of result.entries) {
				expect(await nativeStore.has(entry.hash)).to.equal(true);
				expect((await log.get(entry.hash))?.hash).equal(entry.hash);
			}
			expect(await log.toArray()).to.have.length(4);
			expect(indexPutSpy.callCount).equal(0);
			expect(indexPutBatchSpy.callCount).equal(1);
		} finally {
			blockPutManySpy.restore();
			indexPutSpy.restore();
			indexPutBatchSpy.restore();
			preparedBlockFromBytesSpy.restore();
			nativeCommitSpy.restore();
			nativePrepareAndPutSpy.restore();
			nativeAppendChainSpy.restore();
			await log.close();
			await nativeStore.stop();
		}
	});

	it("append commits one block and graph in one native call when storage is native", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const blockPutSpy = sinon.spy(nativeStore, "put");
		const blockPutManySpy = sinon.spy(nativeStore, "putMany");
		const preparedBlockFromBytesSpy = sinon.spy(
			Entry,
			"preparedBlockFromBytes",
		);
		const nativeCommitSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainCommit",
		);
		const nativeEntryCommitSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainEntryCommit",
		);
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainAndPut",
		);

		try {
			const { entry } = await log.append(new Uint8Array([1]), {
				meta: { next: [] },
			});

			expect(nativeEntryCommitSpy.callCount).equal(1);
			expect(nativeEntryCommitSpy.firstCall.args[0].payloadData).to.deep.equal(
				new Uint8Array([1]),
			);
			expect(nativeCommitSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(0);
			expect(blockPutSpy.callCount).equal(0);
			expect(blockPutManySpy.callCount).equal(0);
			expect(preparedBlockFromBytesSpy.callCount).equal(0);
			expect(await entry.getPayloadValue()).to.deep.equal(new Uint8Array([1]));
			expect(await nativeStore.has(entry.hash)).to.equal(true);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([entry.hash]);
			expect((await log.get(entry.hash))?.hash).equal(entry.hash);
		} finally {
			blockPutSpy.restore();
			blockPutManySpy.restore();
			preparedBlockFromBytesSpy.restore();
			nativeCommitSpy.restore();
			nativeEntryCommitSpy.restore();
			nativePrepareAndPutSpy.restore();
			await log.close();
			await nativeStore.stop();
		}
	});

	it("holds native commit-only publication behind its durable barrier", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		let releaseBarrier!: () => void;
		const barrierGate = new Promise<void>((resolve) => {
			releaseBarrier = resolve;
		});
		let markBarrierStarted!: () => void;
		const barrierStarted = new Promise<void>((resolve) => {
			markBarrierStarted = resolve;
		});
		const barrierSpy = sinon.spy(async () => {
			markBarrierStarted();
			await barrierGate;
		});
		(
			nativeStore as typeof nativeStore & {
				waitForDurableWrites?: () => Promise<void>;
			}
		).waitForDurableWrites = barrierSpy;
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		try {
			let settled = false;
			const pending = Promise.resolve(
				(log as any).appendLocallyPreparedCommitOnly(
					new Uint8Array([1]),
					{ meta: { next: [] } },
					{
						skipMissingNextJoin: true,
						resolveTrimmedEntries: false,
						includeMaterializationBytes: false,
						includeAppendFactsBytes: true,
					},
				),
			);
			void pending.then(
				() => {
					settled = true;
				},
				() => {
					settled = true;
				},
			);
			await barrierStarted;
			await Promise.resolve();
			expect(settled).equal(false);
			expect(log.length).equal(0);

			releaseBarrier();
			const result = await pending;
			expect(result).to.not.equal(undefined);
			expect(log.length).equal(1);
			expect(barrierSpy.callCount).equal(1);
		} finally {
			releaseBarrier();
			delete (
				nativeStore as typeof nativeStore & {
					waitForDurableWrites?: () => Promise<void>;
				}
			).waitForDurableWrites;
			await log.close();
			await nativeStore.stop();
		}
	});

	it("uses single native committed append bookkeeping for prepared append", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const singleNativeAppendSpy = sinon.spy(
			log.entryIndex as any,
			"putNativeCommittedAppend",
		);
		const appendBatchSpy = sinon.spy(log.entryIndex, "putAppendBatch");

		try {
			const { entry, removed } = await (log as any).appendLocallyPrepared(
				new Uint8Array([1]),
				{
					meta: { next: [] },
				},
			);

			expect(removed).to.be.empty;
			expect(singleNativeAppendSpy.callCount).equal(1);
			expect(appendBatchSpy.callCount).equal(0);
			expect(await nativeStore.has(entry.hash)).to.equal(true);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([entry.hash]);
			expect((await log.get(entry.hash))?.hash).equal(entry.hash);
		} finally {
			appendBatchSpy.restore();
			singleNativeAppendSpy.restore();
			await log.close();
			await nativeStore.stop();
		}
	});

	it("uses single native committed append bookkeeping after js block commit", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const singleNativeAppendSpy = sinon.spy(
			log.entryIndex as any,
			"putNativeCommittedAppend",
		);
		const appendBatchSpy = sinon.spy(log.entryIndex, "putAppendBatch");
		const blockPutManySpy = sinon.spy(store, "putMany");
		const blockPutKnownSpy = sinon.spy(store as any, "putKnown");
		const blockPutKnownManySpy = sinon.spy(store as any, "putKnownMany");
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainEntryAndPut",
		);

		try {
			const { entry, removed } = await (log as any).appendLocallyPrepared(
				new Uint8Array([1]),
				{
					meta: { next: [] },
				},
			);

			expect(removed).to.be.empty;
			expect(nativePrepareAndPutSpy.callCount).equal(1);
			expect(blockPutManySpy.callCount).equal(0);
			expect(blockPutKnownSpy.callCount).equal(1);
			expect(blockPutKnownManySpy.callCount).equal(0);
			expect(singleNativeAppendSpy.callCount).equal(1);
			expect(appendBatchSpy.callCount).equal(0);
			expect(await blockExists(entry.hash)).to.be.true;
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([entry.hash]);
			expect((await log.get(entry.hash))?.hash).equal(entry.hash);
		} finally {
			nativePrepareAndPutSpy.restore();
			blockPutKnownManySpy.restore();
			blockPutKnownSpy.restore();
			blockPutManySpy.restore();
			appendBatchSpy.restore();
			singleNativeAppendSpy.restore();
			await log.close();
		}
	});

	it("uses commit-only prepared append facts before entry materialization", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const commitOnlySpy = sinon.spy(
			log.entryIndex as any,
			"putNativeCommittedAppendFacts",
		);
		const appendBatchSpy = sinon.spy(log.entryIndex, "putAppendBatch");
		const trimSpy = sinon.spy(log, "trim");
		const initSpy = sinon.spy(EntryV0.prototype, "init");
		const createCommitOnlySpy = sinon.spy(
			EntryV0,
			"createPlainAppendChainCommitOnly",
		);

		try {
			const resultMaybe = (log as any).appendLocallyPreparedCommitOnly(
				new Uint8Array([1]),
				{ meta: { next: [] } },
				{ skipMissingNextJoin: true, includeMaterializationBytes: false },
			);
			expect((resultMaybe as { then?: unknown }).then).equal(undefined);
			const result = await resultMaybe;

			expect(result).to.exist;
			expect(result.appendFacts.metaBytes).equal(undefined);
			expect(result.appendFacts.hashDigestBytes).equal(undefined);
			expect(
				(createCommitOnlySpy.returnValues[0] as { then?: unknown } | undefined)
					?.then,
			).equal(undefined);
			expect(commitOnlySpy.callCount).equal(1);
			expect(commitOnlySpy.returnValues[0]).equal(undefined);
			expect(appendBatchSpy.callCount).equal(0);
			expect(trimSpy.callCount).equal(0);
			expect(initSpy.callCount).equal(0);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([result.appendFacts.hash]);
			expect(await blockExists(result.appendFacts.hash)).to.be.true;

			const entry = result.entry;
			expect(initSpy.callCount).greaterThan(0);
			expect(entry.hash).equal(result.appendFacts.hash);
			expect(entry.meta.gid).equal(result.appendFacts.gid);
		} finally {
			initSpy.restore();
			trimSpy.restore();
			appendBatchSpy.restore();
			commitOnlySpy.restore();
			createCommitOnlySpy.restore();
			await log.close();
		}
	});

	it("keeps uncontended native append-facts bookkeeping synchronous and single-pass", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry: template } = await log.append(new Uint8Array([41]), {
			meta: { next: [] },
		});
		const entryIndex = log.entryIndex as any;
		const makeShallow = (hash: string) => {
			const shallow = template.toShallow(true);
			shallow.hash = hash;
			shallow.meta.next = [];
			return shallow;
		};
		const singleSpy = sinon.spy(
			log.entryIndex,
			"putNativeCommittedAppendFacts",
		);
		const batchSpy = sinon.spy(
			log.entryIndex,
			"putNativeCommittedAppendFactsBatch",
		);
		try {
			const generationBefore = entryIndex.nextPendingIndexWriteGeneration;
			const lengthBefore = log.length;
			const single = log.entryIndex.putNativeCommittedAppendFacts({
				hash: "uncontended-single",
				unique: true,
				externalNextHashes: [],
				shallowEntry: makeShallow("uncontended-single"),
			});
			expect((single as { then?: unknown } | undefined)?.then).to.equal(
				undefined,
			);
			expect(singleSpy.callCount).to.equal(1);
			expect(log.length).to.equal(lengthBefore + 1);
			expect(entryIndex.nextPendingIndexWriteGeneration).to.equal(
				generationBefore + 1,
			);

			const batchGenerationBefore = entryIndex.nextPendingIndexWriteGeneration;
			const batchLengthBefore = log.length;
			const batch = log.entryIndex.putNativeCommittedAppendFactsBatch([
				{
					hash: "uncontended-batch-a",
					unique: true,
					externalNextHashes: [],
					shallowEntry: makeShallow("uncontended-batch-a"),
				},
				{
					hash: "uncontended-batch-b",
					unique: true,
					externalNextHashes: [],
					shallowEntry: makeShallow("uncontended-batch-b"),
				},
			]);
			expect((batch as { then?: unknown } | undefined)?.then).to.equal(
				undefined,
			);
			expect(batchSpy.callCount).to.equal(1);
			expect(log.length).to.equal(batchLengthBefore + 2);
			expect(entryIndex.nextPendingIndexWriteGeneration).to.equal(
				batchGenerationBefore + 2,
			);
		} finally {
			batchSpy.restore();
			singleSpy.restore();
			await log.close();
		}
	});

	it("defers same-hash native append-facts bookkeeping until the held lease releases", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry: template } = await log.append(new Uint8Array([42]), {
			meta: { next: [] },
		});
		const hash = "contended-native-facts";
		const shallowEntry = template.toShallow(true);
		shallowEntry.hash = hash;
		shallowEntry.meta.next = [];
		const entryIndex = log.entryIndex as any;
		const owner = await log.entryIndex.acquireHashMutationLocks([hash]);
		const lengthBefore = log.length;
		const generationBefore = entryIndex.nextPendingIndexWriteGeneration;
		try {
			const pending = log.entryIndex.putNativeCommittedAppendFacts({
				hash,
				unique: true,
				externalNextHashes: [],
				shallowEntry,
			});
			expect(pending).to.be.instanceOf(Promise);
			expect(log.length).to.equal(lengthBefore);
			expect(entryIndex.nextPendingIndexWriteGeneration).to.equal(
				generationBefore,
			);
			log.entryIndex.releaseHashMutationLocks(owner);
			await pending;
			expect(log.length).to.equal(lengthBefore + 1);
			expect(entryIndex.nextPendingIndexWriteGeneration).to.equal(
				generationBefore + 1,
			);
		} finally {
			try {
				log.entryIndex.releaseHashMutationLocks(owner);
			} catch {
				// The successful path already released the caller-owned test lease.
			}
			await log.close();
		}
	});

	it("releases native hash leases after synchronous throws and async rejection", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry: template } = await log.append(new Uint8Array([43]), {
			meta: { next: [] },
		});
		const makeShallow = (hash: string) => {
			const shallow = template.toShallow(true);
			shallow.hash = hash;
			shallow.meta.next = [];
			return shallow;
		};
		try {
			const syncHash = "sync-throw-native-facts";
			expect(() =>
				log.entryIndex.putNativeCommittedAppendFactsBatch([
					{
						hash: syncHash,
						unique: true,
						externalNextHashes: [],
					},
				]),
			).to.throw("Missing shallow entry");
			const afterSyncThrow = log.entryIndex.putNativeCommittedAppendFacts({
				hash: syncHash,
				unique: true,
				externalNextHashes: [],
				shallowEntry: makeShallow(syncHash),
			});
			expect((afterSyncThrow as { then?: unknown } | undefined)?.then).to.equal(
				undefined,
			);

			const asyncHash = "async-reject-native-facts";
			const failure = new Error("injected native facts lookup failure");
			const has = sinon.stub(log.entryIndex, "has").rejects(failure);
			const rejected = log.entryIndex.putNativeCommittedAppendFacts({
				hash: asyncHash,
				unique: false,
				externalNextHashes: [],
				shallowEntry: makeShallow(asyncHash),
			});
			expect(rejected).to.be.instanceOf(Promise);
			expect(
				await Promise.resolve(rejected).then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(failure);
			has.restore();
			const afterAsyncReject = log.entryIndex.putNativeCommittedAppendFacts({
				hash: asyncHash,
				unique: true,
				externalNextHashes: [],
				shallowEntry: makeShallow(asyncHash),
			});
			expect(
				(afterAsyncReject as { then?: unknown } | undefined)?.then,
			).to.equal(undefined);
		} finally {
			sinon.restore();
			await log.close();
		}
	});

	it("does not release a caller-supplied native hash-lock owner", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry: template } = await log.append(new Uint8Array([44]), {
			meta: { next: [] },
		});
		const hash = "caller-owned-native-facts";
		const makeShallow = (value = hash) => {
			const shallow = template.toShallow(true);
			shallow.hash = value;
			shallow.meta.next = [];
			return shallow;
		};
		const owner = await log.entryIndex.acquireHashMutationLocks([hash]);
		try {
			const lengthBeforeRejectedOwners = log.length;
			expect(() =>
				log.entryIndex.putNativeCommittedAppendFacts(
					{
						hash: "uncovered-single",
						unique: true,
						externalNextHashes: [],
						shallowEntry: makeShallow("uncovered-single"),
					},
					undefined,
					owner,
				),
			).to.throw("does not cover uncovered-single");
			expect(() =>
				log.entryIndex.putNativeCommittedAppendFactsBatch(
					[
						{
							hash: "uncovered-batch",
							unique: true,
							externalNextHashes: [],
							shallowEntry: makeShallow("uncovered-batch"),
						},
					],
					undefined,
					owner,
				),
			).to.throw("does not cover uncovered-batch");
			expect(log.length).to.equal(lengthBeforeRejectedOwners);
			const owned = log.entryIndex.putNativeCommittedAppendFacts(
				{
					hash,
					unique: true,
					externalNextHashes: [],
					shallowEntry: makeShallow(),
				},
				undefined,
				owner,
			);
			expect((owned as { then?: unknown } | undefined)?.then).to.equal(
				undefined,
			);
			const waiting = log.entryIndex.putNativeCommittedAppendFacts({
				hash,
				unique: false,
				externalNextHashes: [],
				shallowEntry: makeShallow(),
			});
			expect(waiting).to.be.instanceOf(Promise);
			log.entryIndex.releaseHashMutationLocks(owner);
			await waiting;
		} finally {
			try {
				log.entryIndex.releaseHashMutationLocks(owner);
			} catch {
				// The test releases its owner before awaiting the contended mutation.
			}
			await log.close();
		}
	});

	it("uses native graph trim facts for commit-only prepared append without native block store", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
			trim: { type: "length", to: 1 },
		});

		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainEntryAndPut",
		);
		const trimSpy = sinon.spy(log, "trim");
		const blockRmManySpy = sinon.spy(store, "rmMany");

		try {
			const first = await (log as any).appendLocallyPreparedCommitOnly(
				new Uint8Array([1]),
				{ meta: { next: [] } },
				{
					skipMissingNextJoin: true,
					resolveTrimmedEntries: false,
					includeMaterializationBytes: false,
					includeAppendFactsBytes: true,
				},
			);
			const second = await (log as any).appendLocallyPreparedCommitOnly(
				new Uint8Array([2]),
				{},
				{
					skipMissingNextJoin: true,
					resolveTrimmedEntries: false,
					includeMaterializationBytes: false,
					includeAppendFactsBytes: true,
				},
			);

			expect(first.removed).to.be.empty;
			expect(second.removed.map((entry: any) => entry.hash)).to.deep.equal([
				first.appendFacts.hash,
			]);
			expect(nativePrepareAndPutSpy.callCount).equal(2);
			expect(nativePrepareAndPutSpy.secondCall.args[0].trimLengthTo).equal(1);
			expect(trimSpy.callCount).equal(0);
			expect(blockRmManySpy.callCount).equal(1);
			expect(await blockExists(first.appendFacts.hash)).to.be.false;
			expect(await blockExists(second.appendFacts.hash)).to.be.true;
			expect(log.length).equal(1);
		} finally {
			blockRmManySpy.restore();
			trimSpy.restore();
			nativePrepareAndPutSpy.restore();
			await log.close();
		}
	});

	it("mirrors trim deletion only after native confirms hot deletion", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
		});
		const nativeDeleteMirror = sinon.stub().resolves();
		(
			store as BlockStore & {
				rmManyAfterNativeDelete?: (hashes: string[]) => Promise<void>;
			}
		).rmManyAfterNativeDelete = nativeDeleteMirror;

		try {
			const first = await log.append(new Uint8Array([1]), {
				meta: { next: [] },
			});
			await log.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
				[first.entry.hash],
				{
					skipNextHeadUpdates: true,
					deleteBlocks: false,
				},
			);
			expect(nativeDeleteMirror.callCount).equal(0);

			const second = await log.append(new Uint8Array([2]), {
				meta: { next: [] },
			});
			await log.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
				[second.entry.hash],
				{
					skipNextHeadUpdates: true,
					deleteBlocks: false,
					nativeBlocksDeleted: true,
				},
			);
			expect(
				nativeDeleteMirror.calledOnceWithExactly([second.entry.hash]),
			).equal(true);
		} finally {
			delete (
				store as BlockStore & {
					rmManyAfterNativeDelete?: (hashes: string[]) => Promise<void>;
				}
			).rmManyAfterNativeDelete;
			await log.close();
		}
	});

	it("keeps known no-next native append on the direct path with length trim", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
			trim: { type: "length", to: 1 },
		});

		const getNextsForAppendSpy = sinon.spy(log as any, "getNextsForAppend");
		const trimSpy = sinon.spy(log, "trim");
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const nativePrepareAndPutSpy = sinon.spy(
			nativeGraph,
			"prepareEntryV0PlainEntryAndPut",
		);
		const prepare = (input: any) =>
			nativeGraph.prepareEntryV0PlainEntryAndPut!({
				...input,
				next: [],
				includeMaterializationBytes: false,
			});

		try {
			const first = await (
				log as any
			).appendLocallyPreparedNativeNoNextCommitOnly(
				new Uint8Array([1]),
				{ meta: { next: [] } },
				{ resolveTrimmedEntries: false },
				prepare,
			);
			const second = await (
				log as any
			).appendLocallyPreparedNativeNoNextCommitOnly(
				new Uint8Array([2]),
				{ meta: { next: [] } },
				{ resolveTrimmedEntries: false },
				prepare,
			);

			expect(first.removed).to.be.empty;
			expect(second.removed.map((entry: any) => entry.hash)).to.deep.equal([
				first.appendFacts.hash,
			]);
			expect(first.entry.hash).equal(first.appendFacts.hash);
			expect(getNextsForAppendSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(2);
			expect(nativePrepareAndPutSpy.secondCall.args[0].trimLengthTo).equal(1);
			expect(trimSpy.callCount).equal(0);
			expect(await blockExists(second.appendFacts.hash)).to.be.true;
			expect(log.length).equal(1);
		} finally {
			nativePrepareAndPutSpy.restore();
			trimSpy.restore();
			getNextsForAppendSpy.restore();
			await log.close();
		}
	});

	it("waits for an admitted native prepare and compensates terminal admission", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
		});
		const cid = await store.put(Uint8Array.of(99));
		let markPrepared!: () => void;
		const prepared = new Promise<void>((resolve) => {
			markPrepared = resolve;
		});
		let releasePrepare!: () => void;
		const prepareGate = new Promise<void>((resolve) => {
			releasePrepare = resolve;
		});
		const pending = Promise.resolve(
			(log as any).appendLocallyPreparedNativeNoNextCommitOnly(
				Uint8Array.of(1),
				{ meta: { next: [] } },
				{
					resolveTrimmedEntries: false,
					skipMissingNextJoin: true,
					retainMaterializationBytes: false,
					deferNativeTransactionAcknowledgement: true,
				},
				async () => {
					markPrepared();
					await prepareGate;
					return {
						cid,
						byteLength: 1,
						nativeCommitOwnershipToken: {},
					};
				},
			),
		);
		try {
			await prepared;
			let closeSettled = false;
			const closing = log.close().finally(() => {
				closeSettled = true;
			});
			await Promise.resolve();
			await Promise.resolve();
			expect(closeSettled).to.equal(false);
			releasePrepare();
			const [appendResult, closeResult] = await Promise.allSettled([
				pending,
				closing,
			]);
			expect(appendResult.status).to.equal("rejected");
			expect(closeResult.status).to.equal("fulfilled");
			expect(await store.get(cid)).to.equal(undefined);
			expect((log as any)._nativeCommittedAppendFinalizers?.size ?? 0).to.equal(
				0,
			);
		} finally {
			releasePrepare();
			await pending.catch(() => undefined);
			await log.close().catch(() => undefined);
		}
	});

	for (const method of ["append", "appendMany"] as const) {
		it(`waits for an admitted public ${method} before closing`, async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey, {
				indexer: new HashmapIndices(),
				nativeGraph: true,
			});
			const originalCreateNativePlainAppendChain = (
				log as any
			).createNativePlainAppendChain.bind(log);
			let markPrepared!: () => void;
			const prepared = new Promise<void>((resolve) => {
				markPrepared = resolve;
			});
			let releasePrepared!: () => void;
			const preparedGate = new Promise<void>((resolve) => {
				releasePrepared = resolve;
			});
			const createNativePlainAppendChain = sinon
				.stub(log as any, "createNativePlainAppendChain")
				.callsFake(async (...args: any[]) => {
					const chain = await originalCreateNativePlainAppendChain(...args);
					expect(chain).to.not.equal(undefined);
					expect(chain.nativeBlocksCommitted).to.equal(false);
					markPrepared();
					await preparedGate;
					return chain;
				});
			const pending =
				method === "append"
					? log.append(Uint8Array.of(7))
					: log.appendMany([Uint8Array.of(7), Uint8Array.of(8)]);
			try {
				await prepared;
				let closeSettled = false;
				const closing = log.close().finally(() => {
					closeSettled = true;
				});
				await Promise.resolve();
				await Promise.resolve();
				expect(closeSettled).to.equal(false);

				releasePrepared();
				const [appendResult, closeResult] = await Promise.allSettled([
					pending,
					closing,
				]);
				expect(appendResult.status).to.equal("fulfilled");
				expect(closeResult.status).to.equal("fulfilled");
				if (appendResult.status === "fulfilled") {
					const hashes =
						"entry" in appendResult.value
							? [appendResult.value.entry.hash]
							: appendResult.value.entries.map((entry) => entry.hash);
					for (const hash of hashes) {
						expect(await blockExists(hash)).to.equal(true);
					}
				}
				expect((log.entryIndex as any).pendingIndexWrites.size).to.equal(0);
			} finally {
				releasePrepared();
				await pending.catch(() => undefined);
				await log.close().catch(() => undefined);
				createNativePlainAppendChain.restore();
			}
		});
	}

	it("lets an onChange observer catch terminal reentrancy and retry later", async () => {
		const log = new Log<Uint8Array>();
		let closeError: unknown;
		let dropError: unknown;
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
			onChange: async () => {
				closeError = await log.close().then(
					() => undefined,
					(error: unknown) => error,
				);
				dropError = await log.drop().then(
					() => undefined,
					(error: unknown) => error,
				);
			},
		});

		const result = await Promise.race([
			log.append(Uint8Array.of(10)),
			new Promise<never>((_, reject) =>
				setTimeout(() => reject(new Error("onChange close deadlocked")), 1000),
			),
		]);
		expect(result.entry.hash).to.be.a("string");
		expect(String(closeError)).to.contain("mutation callback");
		expect(String(dropError)).to.contain("mutation callback");
		expect(log.closed).to.equal(false);
		await log.drop();
		expect(log.closed).to.equal(true);
		expect(await blockExists(result.entry.hash)).to.equal(false);
	});

	it("rejects post-commit on uncaught onChange terminal reentrancy", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
			onChange: async () => log.close(),
		});

		const result = await Promise.race([
			log.append(Uint8Array.of(12)).then(
				() => ({ status: "fulfilled" as const }),
				(error: unknown) => ({ status: "rejected" as const, error }),
			),
			new Promise<never>((_, reject) =>
				setTimeout(() => reject(new Error("onChange close deadlocked")), 1000),
			),
		]);
		expect(result.status).to.equal("rejected");
		if (result.status === "rejected") {
			expect(String(result.error)).to.contain("mutation callback");
		}
		expect(log.length).to.equal(1);
		expect(log.closed).to.equal(false);
		await log.close();
		expect(log.closed).to.equal(true);
	});

	it("rejects without deadlocking when canAppend closes the log", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			canAppend: async () => {
				await log.close();
				return true;
			},
		});

		const result = await Promise.race([
			log.append(Uint8Array.of(11)).then(
				() => ({ status: "fulfilled" as const }),
				(error: unknown) => ({ status: "rejected" as const, error }),
			),
			new Promise<never>((_, reject) =>
				setTimeout(() => reject(new Error("canAppend close deadlocked")), 1000),
			),
		]);
		expect(result.status).to.equal("rejected");
		if (result.status === "rejected") {
			expect(String(result.error)).to.contain("mutation callback");
		}
		expect(log.length).to.equal(0);
		expect(log.closed).to.equal(false);
		await log.close();
		expect(log.closed).to.equal(true);
	});

	it("rejects terminal reentrancy from canTrim without deadlocking", async () => {
		const log = new Log<Uint8Array>();
		let closeError: unknown;
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			trim: {
				type: "length",
				to: 1,
				filter: {
					canTrim: async () => {
						closeError = await log.close().then(
							() => undefined,
							(error: unknown) => error,
						);
						return true;
					},
				},
			},
		});
		await log.append(Uint8Array.of(15));

		await Promise.race([
			log.append(Uint8Array.of(16)),
			new Promise<never>((_, reject) =>
				setTimeout(() => reject(new Error("canTrim close deadlocked")), 1000),
			),
		]);
		expect(String(closeError)).to.contain("mutation callback");
		expect(log.length).to.equal(1);
		expect(log.closed).to.equal(false);
		await log.close();
		expect(log.closed).to.equal(true);
	});

	it("finishes CUT deletion before terminal admission is released", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, { indexer: new HashmapIndices() });
		const { entry: first } = await log.append(Uint8Array.of(13));
		let markChange!: () => void;
		const changeStarted = new Promise<void>((resolve) => {
			markChange = resolve;
		});
		let releaseChange!: () => void;
		const changeGate = new Promise<void>((resolve) => {
			releaseChange = resolve;
		});
		const cutting = log.append(Uint8Array.of(14), {
			meta: { type: EntryType.CUT },
			onChange: async () => {
				markChange();
				await changeGate;
			},
		});
		try {
			await changeStarted;
			const closeError = await log.close().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(String(closeError)).to.contain("mutation callback");
			expect(await blockExists(first.hash)).to.equal(true);

			releaseChange();
			await cutting;
			expect(await blockExists(first.hash)).to.equal(false);
			await log.close();
			expect(log.closed).to.equal(true);
		} finally {
			releaseChange();
			await cutting.catch(() => undefined);
			await log.close().catch(() => undefined);
		}
	});

	it("keeps batched native append results materializable after length trim", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
			trim: { type: "length", to: 1 },
		});

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const prepare = (inputs: any[]) =>
			inputs.map((input) =>
				nativeGraph.prepareEntryV0PlainEntryAndPut!({
					...input,
					next: [],
					includeMaterializationBytes: false,
				}),
			);

		try {
			const first = await (
				log as any
			).appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch(
				[new Uint8Array([1])],
				{ meta: { next: [] } },
				{
					payloadDatas: [new Uint8Array([1])],
					resolveTrimmedEntries: false,
				},
				prepare,
			);
			const second = await (
				log as any
			).appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch(
				[new Uint8Array([2])],
				{ meta: { next: [] } },
				{
					payloadDatas: [new Uint8Array([2])],
					resolveTrimmedEntries: false,
				},
				prepare,
			);

			expect(await blockExists(first.appendFacts[0].hash)).to.be.false;
			expect(first.entries[0].hash).equal(first.appendFacts[0].hash);
			expect(second.entries[0].hash).equal(second.appendFacts[0].hash);
		} finally {
			await log.close();
		}
	});

	it("uses storage-only native block-store commits for commit-only prepared append", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const commitOnlySpy = sinon.spy(
			log.entryIndex as any,
			"putNativeCommittedAppendFacts",
		);
		const fullEntryAppendSpy = sinon.spy(
			log.entryIndex as any,
			"putNativeCommittedAppend",
		);
		const appendBatchSpy = sinon.spy(log.entryIndex, "putAppendBatch");
		const blockPutSpy = sinon.spy(nativeStore, "put");
		const blockPutManySpy = sinon.spy(nativeStore, "putMany");
		const blockPutKnownSpy = sinon.spy(nativeStore, "putKnown");
		const blockPutKnownManySpy = sinon.spy(nativeStore, "putKnownMany");
		const initSpy = sinon.spy(EntryV0.prototype, "init");
		const nativeCommitSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainEntryCommit",
		);
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainEntryAndPut",
		);

		try {
			const result = await (log as any).appendLocallyPreparedCommitOnly(
				new Uint8Array([1]),
				{ meta: { next: [] } },
				{ skipMissingNextJoin: true, includeMaterializationBytes: false },
			);

			expect(result).to.exist;
			expect(result.appendFacts.metaBytes).equal(undefined);
			expect(result.appendFacts.hashDigestBytes).equal(undefined);
			expect(nativeCommitSpy.callCount).equal(1);
			expect(nativePrepareAndPutSpy.callCount).equal(0);
			expect(blockPutSpy.callCount).equal(0);
			expect(blockPutManySpy.callCount).equal(0);
			expect(blockPutKnownSpy.callCount).equal(0);
			expect(blockPutKnownManySpy.callCount).equal(0);
			expect(commitOnlySpy.callCount).equal(1);
			expect(fullEntryAppendSpy.callCount).equal(0);
			expect(appendBatchSpy.callCount).equal(0);
			expect(initSpy.callCount).equal(0);
			expect(await nativeStore.has(result.appendFacts.hash)).equal(true);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([result.appendFacts.hash]);

			const entry = result.entry;
			expect(initSpy.callCount).greaterThan(0);
			expect(entry.hash).equal(result.appendFacts.hash);
			expect(await entry.getPayloadValue()).to.deep.equal(new Uint8Array([1]));
		} finally {
			nativePrepareAndPutSpy.restore();
			nativeCommitSpy.restore();
			initSpy.restore();
			blockPutKnownManySpy.restore();
			blockPutKnownSpy.restore();
			blockPutManySpy.restore();
			blockPutSpy.restore();
			appendBatchSpy.restore();
			fullEntryAppendSpy.restore();
			commitOnlySpy.restore();
			await log.close();
			await nativeStore.stop();
		}
	});

	it("uses facts-only native block-store commits before entry materialization", async () => {
		const { createNativeLogBlockStore } = await import("@peerbit/log-rust");
		const nativeStore = await createNativeLogBlockStore();
		await nativeStore.start();
		const log = new Log<Uint8Array>();
		await log.open(nativeStore, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const initSpy = sinon.spy(EntryV0.prototype, "init");

		try {
			const result = await (log as any).appendLocallyPreparedCommitOnly(
				new Uint8Array([1]),
				{ meta: { data: new Uint8Array([9]), next: [] } },
				{
					skipMissingNextJoin: true,
					includeMaterializationBytes: false,
					includeAppendFactsBytes: true,
				},
			);

			expect(result).to.exist;
			expect(result.appendFacts.metaBytes).to.be.instanceOf(Uint8Array);
			expect(result.appendFacts.hashDigestBytes).to.be.instanceOf(Uint8Array);
			expect(initSpy.callCount).equal(0);
			expect(await nativeStore.has(result.appendFacts.hash)).equal(true);

			const entry = result.entry;
			expect(initSpy.callCount).greaterThan(0);
			expect(entry.hash).equal(result.appendFacts.hash);
			expect(await entry.getPayloadValue()).to.deep.equal(new Uint8Array([1]));
		} finally {
			initSpy.restore();
			await log.close();
			await nativeStore.stop();
		}
	});

	it("rolls back native graph when prepared appendMany block write fails", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const root = (await log.append(new Uint8Array([0]), { meta: { next: [] } }))
			.entry;
		const graph = log.entryIndex.properties.nativeGraph!.graph;
		const blockPutManyStub = sinon
			.stub(store, "putMany")
			.rejects(new Error("boom"));
		const blockPutKnownManyStub =
			"putKnownMany" in store &&
			typeof (store as { putKnownMany?: unknown }).putKnownMany === "function"
				? sinon
						.stub(
							store as unknown as {
								putKnownMany: (blocks: [string, Uint8Array][]) => any;
							},
							"putKnownMany",
						)
						.rejects(new Error("boom"))
				: undefined;
		const blockPutKnownManyColumnsStub =
			"putKnownManyColumns" in store &&
			typeof (store as { putKnownManyColumns?: unknown })
				.putKnownManyColumns === "function"
				? sinon
						.stub(
							store as unknown as {
								putKnownManyColumns: (
									cids: string[],
									bytes: Uint8Array[],
								) => any;
							},
							"putKnownManyColumns",
						)
						.rejects(new Error("boom"))
				: undefined;

		try {
			await expect(
				log.appendMany([new Uint8Array([1]), new Uint8Array([2])]),
			).rejectedWith("boom");
			expect(
				blockPutManyStub.callCount +
					(blockPutKnownManyStub?.callCount ?? 0) +
					(blockPutKnownManyColumnsStub?.callCount ?? 0),
			).equal(1);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([root.hash]);
			expect(graph.length).equal(1);
		} finally {
			blockPutKnownManyColumnsStub?.restore();
			blockPutKnownManyStub?.restore();
			blockPutManyStub.restore();
			await log.close();
		}
	});

	describe("append 100 items to a log", () => {
		const amount = 100;

		let log: Log<Uint8Array>;

		before(async () => {
			// Do sign function really need to returnr publcikey
			log = new Log();
			await log.open(store, signKey);
			let prev: any = undefined;
			for (let i = 0; i < amount; i++) {
				prev = (
					await log.append(new TextEncoder().encode("hello" + i), {
						meta: {
							next: prev ? [prev] : undefined,
						},
					})
				).entry;

				// Make sure the log has the right heads after each append
				const values = await log.toArray();
				const heads = await log.getHeads().all();
				expect(heads.length).equal(1);
				expect(heads[0].hash).equal(values[values.length - 1].hash);
			}
		});

		it("added the correct amount of items", () => {
			expect(log.length).equal(amount);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry, index) => {
				expect(entry.payload.getValue()).to.deep.equal(
					new TextEncoder().encode("hello" + index),
				);
			});
		});

		it("updated the clocks correctly", async () => {
			for (const [index, entry] of (await log.toArray()).entries()) {
				if (index > 0) {
					expect(
						entry.meta.clock.timestamp.compare(
							(await log.toArray())[index - 1].meta.clock.timestamp,
						),
					).greaterThan(0);
				}
				expect(entry.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
			}
		});

		/*    it('added the correct amount of refs pointers', async () => {
	   log.values.forEach((entry, index) => {
		 expect(entry.refs.length).equal(index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
	   })
	 }) */
	});
});
