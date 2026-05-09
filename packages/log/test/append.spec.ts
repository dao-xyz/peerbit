import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { Entry } from "../src/entry.js";
import { EntryV0 } from "../src/entry-v0.js";
import { EntryType } from "../src/entry-type.js";
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
		expect((await log.getHeads().all()).map((head) => head.hash)).to.have.members([
			entry.hash,
		]);
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
		expect((await log.getHeads().all()).map((head) => head.hash)).to.have.members([
			entry.hash,
		]);
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
		const root = (
			await log.append(new Uint8Array([0]), { meta: { next: [] } })
		).entry;
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
			expect(putSpy.callCount).equal(1);
			expect(putBatchSpy.callCount).equal(1);
			expect(putBatchSpy.firstCall.args[0]).to.have.length(result.entries.length);
			expect(blockPutSpy.callCount).equal(0);
			expect(blockPutManySpy.callCount).equal(1);
			expect(blockPutManySpy.firstCall.args[0]).to.have.length(
				result.entries.length,
			);
			expect(shallowSpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(1);
			expect(nativePrepareAndPutSpy.firstCall.args[0].payloadDatas).to.have.length(
				result.entries.length,
			);
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
		const root = (
			await log.append(new Uint8Array([0]), { meta: { next: [] } })
		).entry;

		const blockPutManySpy = sinon.spy(nativeStore, "putMany");
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
			expect(
				(await log.getHeads().all()).map((head) => head.hash),
			).to.deep.equal([result.entries[2].hash]);
			expect(nativeCommitSpy.callCount).equal(1);
			expect(nativeCommitSpy.firstCall.args[0].payloadDatas).to.have.length(
				result.entries.length,
			);
			expect(blockPutManySpy.callCount).equal(0);
			expect(nativePrepareAndPutSpy.callCount).equal(0);
			expect(nativeAppendChainSpy.callCount).equal(0);
			for (const entry of result.entries) {
				expect(await nativeStore.has(entry.hash)).to.equal(true);
				expect((await log.get(entry.hash))?.hash).equal(entry.hash);
			}
		} finally {
			blockPutManySpy.restore();
			nativeCommitSpy.restore();
			nativePrepareAndPutSpy.restore();
			nativeAppendChainSpy.restore();
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
		const root = (
			await log.append(new Uint8Array([0]), { meta: { next: [] } })
		).entry;
		const graph = log.entryIndex.properties.nativeGraph!.graph;
		const blockPutManyStub = sinon
			.stub(store, "putMany")
			.rejects(new Error("boom"));

		try {
			await expect(
				log.appendMany([new Uint8Array([1]), new Uint8Array([2])]),
			).rejectedWith("boom");
			expect(blockPutManyStub.callCount).equal(1);
			expect((await log.getHeads().all()).map((head) => head.hash)).to.deep.equal(
				[root.hash],
			);
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
