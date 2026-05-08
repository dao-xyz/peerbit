import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { Log } from "../src/log.js";

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
			expect(planJoinSpy.firstCall.args).to.deep.equal([
				merge.hash,
				[present.hash, missing.hash],
				merge.meta.type,
				false,
			]);
			expect(planJoinSpy.firstCall.returnValue).to.deep.equal({
				skip: false,
				missingParents: [missing.hash],
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
