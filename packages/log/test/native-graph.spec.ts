import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { EntryType } from "../src/entry-type.js";
import { Log } from "../src/log.js";

const absoluteReplicaData = (value: number) =>
	new Uint8Array([
		0,
		value & 0xff,
		(value >>> 8) & 0xff,
		(value >>> 16) & 0xff,
		(value >>> 24) & 0xff,
	]);

describe("native graph", () => {
	let store: AnyBlockStore;
	let signKey: Ed25519Keypair;

	before(async () => {
		store = new AnyBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	it("serves heads while preserving buffered index flush behavior", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const putSpy = sinon.spy(log.entryIndex.properties.index, "put");
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});

		expect(putSpy.callCount).equal(0);
		expect((await log.getHeads().all()).map((head) => head.hash)).to.deep.equal(
			[entry.hash],
		);
		expect(putSpy.callCount).equal(1);

		putSpy.restore();
		await log.close();
	});

	it("plans auto-next append from the native graph before flushing buffered heads", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const putSpy = sinon.spy(log.entryIndex.properties.index, "put");

		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		expect(putSpy.callCount).equal(0);

		let putsBeforeEntryStorage: number | undefined;
		await log.append(new Uint8Array([2]), {
			canAppend: () => {
				putsBeforeEntryStorage = putSpy.callCount;
				return true;
			},
		});

		expect(putsBeforeEntryStorage).equal(0);
		expect(putSpy.callCount).greaterThan(0);

		putSpy.restore();
		await log.close();
	});

	it("rebuilds the native graph from the persistent entry index on open", async () => {
		const indexer = new HashmapIndices();
		const first = new Log<Uint8Array>();
		await first.open(store, signKey, {
			appendDurability: "strict",
			indexer,
			nativeGraph: true,
		});
		await first.append(new Uint8Array([1]), { meta: { next: [] } });
		const { entry: head } = await first.append(new Uint8Array([2]));
		await first.close();

		const reopened = new Log<Uint8Array>();
		await reopened.open(store, signKey, { indexer, nativeGraph: true });
		expect(
			(await reopened.getHeads().all()).map((entry) => entry.hash),
		).to.deep.equal([head.hash]);
		await reopened.close();
	});

	it("resolves full native graph heads with one block batch read", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const entryCount = 12;
		for (let i = 0; i < entryCount; i++) {
			await log.append(new Uint8Array([i]), { meta: { next: [] } });
		}

		const getManySpy = sinon.spy(store, "getMany");
		try {
			const heads = await log.getHeads(true).all();
			expect(heads).to.have.length(entryCount);
			expect(getManySpy.callCount).equal(1);
			expect(getManySpy.firstCall.args[0]).to.have.length(2);
		} finally {
			getManySpy.restore();
			await log.close();
		}
	});

	it("serves shaped native graph heads without index reads", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [], data: new Uint8Array([7, 8]) },
		});

		const indexGetSpy = sinon.spy(log.entryIndex.properties.index, "get");
		const indexIterateSpy = sinon.spy(
			log.entryIndex.properties.index,
			"iterate",
		);
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const headsSpy = sinon.spy(nativeGraph, "heads");
		const headDataEntriesSpy = sinon.spy(nativeGraph, "headDataEntries");
		try {
			const hashes = await log.entryIndex
				.getHeads(undefined, { type: "shape", shape: { hash: true } })
				.all();
			const data = await log.entryIndex
				.getHeads(undefined, {
					type: "shape",
					shape: { hash: true, meta: { data: true } },
				})
				.all();

			expect(hashes).to.deep.equal([{ hash: entry.hash }]);
			expect(data).to.have.length(1);
			expect(data[0]!.hash).equal(entry.hash);
			expect([...data[0]!.meta.data!]).to.deep.equal([7, 8]);
			expect(headsSpy.callCount).equal(1);
			expect(headDataEntriesSpy.callCount).equal(1);
			expect(indexGetSpy.callCount).equal(0);
			expect(indexIterateSpy.callCount).equal(0);
		} finally {
			headDataEntriesSpy.restore();
			headsSpy.restore();
			indexIterateSpy.restore();
			indexGetSpy.restore();
			await log.close();
		}
	});

	it("computes max head data u32 in the native graph", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const first = await log.append(new Uint8Array([1]), {
			meta: { next: [], data: absoluteReplicaData(2) },
		});
		const second = await log.append(new Uint8Array([2]), {
			meta: { next: [], data: absoluteReplicaData(5) },
		});

		const indexIterateSpy = sinon.spy(
			log.entryIndex.properties.index,
			"iterate",
		);
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const maxHeadDataU32Spy = sinon.spy(nativeGraph, "maxHeadDataU32");
		const maxHeadDataU32BatchSpy = sinon.spy(nativeGraph, "maxHeadDataU32Batch");
		try {
			expect(await log.entryIndex.getMaxHeadDataU32()).equal(5);
			expect(
				await log.entryIndex.getMaxHeadDataU32Batch([
					first.entry.meta.gid,
					second.entry.meta.gid,
					"missing",
				]),
			).to.deep.equal([2, 5, undefined]);
			expect(maxHeadDataU32Spy.callCount).equal(1);
			expect(maxHeadDataU32BatchSpy.callCount).equal(1);
			expect(indexIterateSpy.callCount).equal(0);
		} finally {
			maxHeadDataU32BatchSpy.restore();
			maxHeadDataU32Spy.restore();
			indexIterateSpy.restore();
			await log.close();
		}
	});

	it("checks head existence in the native graph", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});

		const indexIterateSpy = sinon.spy(
			log.entryIndex.properties.index,
			"iterate",
		);
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const hasHeadSpy = sinon.spy(nativeGraph, "hasHead");
		const hasAnyHeadSpy = sinon.spy(nativeGraph, "hasAnyHead");
		const hasAnyHeadBatchSpy = sinon.spy(nativeGraph, "hasAnyHeadBatch");
		try {
			expect(await log.entryIndex.hasHead()).equal(true);
			expect(await log.entryIndex.hasHead("missing")).equal(false);
			expect(
				await log.entryIndex.hasAnyHead(["missing", entry.meta.gid]),
			).equal(true);
			expect(await log.entryIndex.hasAnyHead(["missing"])).equal(false);
			expect(
				await log.entryIndex.hasAnyHeadBatch([
					["missing", entry.meta.gid],
					["missing"],
					[],
				]),
			).to.deep.equal([true, false, false]);
			expect(hasHeadSpy.callCount).equal(2);
			expect(hasAnyHeadSpy.callCount).equal(2);
			expect(hasAnyHeadBatchSpy.callCount).equal(1);
			expect(indexIterateSpy.callCount).equal(0);
		} finally {
			hasAnyHeadBatchSpy.restore();
			hasAnyHeadSpy.restore();
			hasHeadSpy.restore();
			indexIterateSpy.restore();
			await log.close();
		}
	});

	it("plans recursive joins through the native graph", async () => {
		const source = new Log<Uint8Array>();
		const target = new Log<Uint8Array>();

		await source.open(store, signKey, { nativeGraph: true });
		await target.open(store, signKey, { nativeGraph: true });

		const { entry: present } = await source.append(new Uint8Array([1]), {
			meta: { next: [] },
		});
		await target.join([present]);

		const { entry: missing } = await source.append(new Uint8Array([2]), {
			meta: { next: [] },
		});
		const { entry: merge } = await source.append(new Uint8Array([3]), {
			meta: { next: [present, missing] },
		});

		const nativeGraph = target.entryIndex.properties.nativeGraph!.graph;
		const planJoinSpy = sinon.spy(nativeGraph, "planJoin");
		const getShallowSpy = sinon.spy(target.entryIndex, "getShallow");
		try {
			await target.join([merge]);

			expect(planJoinSpy.callCount).greaterThan(0);
			expect(planJoinSpy.firstCall.args.slice(0, 4)).to.deep.equal([
				merge.hash,
				[present.hash, missing.hash],
				merge.meta.type,
				false,
			]);
			expect(planJoinSpy.firstCall.args[4]).to.include({ gid: merge.meta.gid });
			expect(planJoinSpy.firstCall.returnValue).to.deep.equal({
				skip: false,
				missingParents: [missing.hash],
				cutChecked: true,
				coveredByCut: false,
			});
			expect(getShallowSpy.callCount).equal(0);
			expect(await target.toArray()).to.have.length(3);
		} finally {
			getShallowSpy.restore();
			planJoinSpy.restore();
			await source.close();
			await target.close();
		}
	});

	it("checks joined array membership in the native graph", async () => {
		const source = new Log<Uint8Array>();
		const target = new Log<Uint8Array>();

		await source.open(store, signKey, { nativeGraph: true });
		await target.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const present = (
			await source.append(new Uint8Array([1]), {
				meta: { next: [] },
			})
		).entry;
		const missing = (
			await source.append(new Uint8Array([2]), {
				meta: { next: [] },
			})
		).entry;
		await target.join([present]);

		const nativeGraph = target.entryIndex.properties.nativeGraph!.graph;
		const hasManySpy = sinon.spy(nativeGraph, "hasMany");
		const iterateSpy = sinon.spy(target.entryIndex.properties.index, "iterate");
		try {
			expect(await target.hasMany([present.hash, missing.hash])).to.deep.equal(
				new Set([present.hash]),
			);
			await target.join([present, missing]);

			expect(hasManySpy.callCount).equal(2);
			expect(hasManySpy.firstCall.args[0]).to.deep.equal(
				new Set([present.hash, missing.hash]),
			);
			expect(iterateSpy.callCount).equal(0);
			expect(await target.toArray()).to.have.length(2);
		} finally {
			iterateSpy.restore();
			hasManySpy.restore();
			await source.close();
			await target.close();
		}
	});

	it("batches entry metadata lookups from the native graph", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const { entry } = await log.append(new Uint8Array([1]), {
			meta: { next: [], data: absoluteReplicaData(3) },
		});

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const entryMetadataBatchSpy = sinon.spy(nativeGraph, "entryMetadataBatch");
		const indexGetSpy = sinon.spy(log.entryIndex.properties.index, "get");
		try {
			const rows = log.entryIndex.getNativeEntryMetadataBatch([
				"missing",
				entry.hash,
			]);
			expect(rows).to.not.equal(undefined);
			expect(rows![0]).equal(undefined);
			expect(rows![1]!.hash).equal(entry.hash);
			expect(rows![1]!.gid).equal(entry.meta.gid);
			expect([...(rows![1]!.data ?? [])]).to.deep.equal([0, 3, 0, 0, 0]);
			expect(entryMetadataBatchSpy.callCount).equal(1);
			expect(indexGetSpy.callCount).equal(0);
		} finally {
			indexGetSpy.restore();
			entryMetadataBatchSpy.restore();
			await log.close();
		}
	});

	it("reads memory usage from the native graph", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
		const indexSumSpy = sinon.spy(log.entryIndex.properties.index, "sum");
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const payloadSizeSumSpy = sinon.spy(nativeGraph, "payloadSizeSum");
		try {
			await log.append(new Uint8Array([1, 2, 3]), { meta: { next: [] } });

			expect(await log.entryIndex.getMemoryUsage()).greaterThan(0);
			expect(payloadSizeSumSpy.callCount).equal(1);
			expect(indexSumSpy.callCount).equal(0);
		} finally {
			payloadSizeSumSpy.restore();
			indexSumSpy.restore();
			await log.close();
		}
	});

	it("uses the native graph to plan unfiltered length trim", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			indexer: new HashmapIndices(),
			nativeGraph: true,
			trim: { type: "length", to: 2 },
		});
		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const oldestEntriesSpy = sinon.spy(nativeGraph, "oldestEntries");
		const iterateSpy = sinon.spy(log.entryIndex.properties.index, "iterate");
		try {
			const first = (await log.append(new Uint8Array([1]), { meta: { next: [] } }))
				.entry;
			await log.append(new Uint8Array([2]));
			await log.append(new Uint8Array([3]));

			expect(oldestEntriesSpy.callCount).greaterThan(0);
			expect(iterateSpy.callCount).equal(0);
			expect(await log.has(first.hash)).equal(false);
			expect(await log.entryIndex.getOldest()).to.exist;
		} finally {
			iterateSpy.restore();
			oldestEntriesSpy.restore();
			await log.close();
		}
	});

	it("plans cut recursion deletes in the native graph", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});

		const root = (
			await log.append(new Uint8Array([1]), {
				meta: { next: [] },
			})
		).entry;
		const child = (
			await log.append(new Uint8Array([2]), {
				meta: { next: [root] },
			})
		).entry;

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const planDeleteRecursivelySpy = sinon.spy(
			nativeGraph,
			"planDeleteRecursively",
		);
		const iterateSpy = sinon.spy(log.entryIndex.properties.index, "iterate");
		try {
			await log.append(new Uint8Array([3]), {
				meta: { type: EntryType.CUT, next: [child] },
			});

			expect(planDeleteRecursivelySpy.callCount).equal(1);
			expect(planDeleteRecursivelySpy.firstCall.returnValue).to.deep.equal([
				child.hash,
				root.hash,
			]);
			expect(iterateSpy.callCount).equal(0);
			expect(await log.has(root.hash)).to.equal(false);
			expect(await log.has(child.hash)).to.equal(false);
		} finally {
			iterateSpy.restore();
			planDeleteRecursivelySpy.restore();
			await log.close();
		}
	});

	it("checks cut-covered joins in the native plan", async () => {
		const sourceStore = new AnyBlockStore();
		const targetStore = new AnyBlockStore();
		const source = new Log<Uint8Array>();
		const target = new Log<Uint8Array>();

		await sourceStore.start();
		await targetStore.start();
		await source.open(sourceStore, signKey, { nativeGraph: true });
		await target.open(targetStore, signKey, { nativeGraph: true });

		const { entry: old } = await source.append(new Uint8Array([1]), {
			meta: { next: [] },
		});
		await target.append(new Uint8Array([2]), {
			meta: { type: EntryType.CUT, next: [old] },
		});

		const nativeGraph = target.entryIndex.properties.nativeGraph!.graph;
		const planJoinSpy = sinon.spy(nativeGraph, "planJoin");
		const joinHeadEntriesSpy = sinon.spy(nativeGraph, "joinHeadEntries");
		const verifySpy = sinon.spy(old, "verifySignatures");
		try {
			await target.join([old], { verifySignatures: true });

			expect(planJoinSpy.firstCall.returnValue).to.include({
				skip: false,
				cutChecked: true,
				coveredByCut: true,
			});
			expect(verifySpy.callCount).equal(1);
			expect(joinHeadEntriesSpy.callCount).equal(0);
			expect(await target.has(old.hash)).to.equal(false);
		} finally {
			verifySpy.restore();
			joinHeadEntriesSpy.restore();
			planJoinSpy.restore();
			await source.close();
			await target.close();
			await sourceStore.stop();
			await targetStore.stop();
		}
	});

	it("keeps gid removal behavior when joins use the native graph mirror", async () => {
		const source = new Log<Uint8Array>();
		const target = new Log<Uint8Array>();
		const gidsRemoved: string[][] = [];

		await source.open(store, signKey, { nativeGraph: true });
		await target.open(store, signKey, {
			nativeGraph: true,
			onGidRemoved: (gids) => {
				gidsRemoved.push(gids);
			},
		});

		await source.append(new Uint8Array([0]));
		await source.append(new Uint8Array([1]));
		await target.append(new Uint8Array([2]));
		await target.join(source);
		await target.append(new Uint8Array([3]));

		expect(gidsRemoved).to.have.length(1);
		expect(gidsRemoved[0]).to.have.length(1);
		expect(
			await target.entryIndex.countHasNext(
				(await source.getHeads().all())[0].hash,
			),
		).to.equal(1);

		await source.close();
		await target.close();
	});
});
