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
		const shallowSpy = sinon.spy(EntryV0.prototype, "toShallow");
		const nativeAppendChainSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"putAppendChain",
		);
		const nativePrepareAndPutSpy = sinon.spy(
			log.entryIndex.properties.nativeGraph!.graph,
			"prepareEntryV0PlainChainAndPut",
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
			expect(blockPutManySpy.callCount).equal(1);
			expect(blockPutManySpy.firstCall.args[0]).to.have.length(
				result.entries.length,
			);
			expect(shallowSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(1);
			expect(
				nativePrepareAndPutSpy.firstCall.args[0].payloadDatas,
			).to.have.length(result.entries.length);
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
			shallowSpy.restore();
			nativeAppendChainSpy.restore();
			nativePrepareAndPutSpy.restore();
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
			const { entry, removed } = await log.appendLocallyPrepared(
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
			const { entry, removed } = await log.appendLocallyPrepared(
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
			const first = await (log as any).appendLocallyPreparedNativeNoNextCommitOnly(
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

		try {
			await expect(
				log.appendMany([new Uint8Array([1]), new Uint8Array([2])]),
			).rejectedWith("boom");
			expect(blockPutManyStub.callCount).equal(1);
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([root.hash]);
			expect(graph.length).equal(1);
		} finally {
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
