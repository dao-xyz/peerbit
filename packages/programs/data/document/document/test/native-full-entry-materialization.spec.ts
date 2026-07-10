import { field, variant } from "@dao-xyz/borsh";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

@variant("native_full_entry_materialization_indexable")
class MaterializedIndexable {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	constructor(document?: Document) {
		this.id = document?.id ?? "";
		this.name = document?.name ?? "";
	}
}

describe("native full-entry materialization", () => {
	let peer: Peerbit;

	beforeEach(async () => {
		peer = await Peerbit.create({
			...createRustPeerbitOptions({ network: false }),
			libp2p: { addresses: { listen: [] } },
		});
	});

	afterEach(async () => {
		await peer.stop();
	});

	const openStore = async () => {
		const store = new TestStore<MaterializedIndexable>({
			docs: new Documents<Document, MaterializedIndexable>(),
		});
		await peer.open(store, {
			args: {
				nativeGraph: true,
				nativeBackbone: { optional: false },
				index: {
					type: MaterializedIndexable,
					cache: { resolver: 0 },
				},
			},
		});
		expect((store.docs.log as any)._nativeBackbone).to.exist;
		return store;
	};

	it("materializes a full read of a native local append", async () => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: false }),
		});
		await peer.open(store, {
			args: {
				nativeGraph: true,
				nativeBackbone: { optional: false },
			},
		});
		expect((store.docs.log as any)._nativeBackbone).to.exist;

		const document = new Document({ id: "materialize-log", name: "log value" });
		const put = await store.docs.put(document);

		const entry = await store.docs.log.log.get(put.entry.hash);
		expect(entry).to.exist;
		expect(entry).not.equal(put.entry);
		expect(entry?.createdLocally).equal(true);
		expect(entry!.getStorageBytes()).to.exist;
		expect(await entry!.getPayloadValue()).to.exist;
		expect(await store.docs.log.log.get(put.entry.hash)).equal(entry);
	});

	it("batch-materializes mixed cache states with one block read", async () => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: false }),
		});
		await peer.open(store, {
			args: {
				nativeGraph: true,
				nativeBackbone: { optional: false },
			},
		});
		expect((store.docs.log as any)._nativeBackbone).to.exist;

		const puts = [
			await store.docs.put(
				new Document({ id: "materialize-batch-1", name: "one" }),
			),
			await store.docs.put(
				new Document({ id: "materialize-batch-2", name: "two" }),
			),
			await store.docs.put(
				new Document({ id: "materialize-batch-3", name: "three" }),
			),
		];
		const full = await store.docs.log.log.get(puts[0].entry.hash);
		expect(full).to.exist;
		await store.docs.log.log.blocks.rm(puts[2].entry.hash);

		const getManySpy = sinon.spy(store.docs.log.log.blocks, "getMany");

		try {
			const entries = await store.docs.log.log.entryIndex.getMany(
				puts.map((put) => put.entry.hash),
				{ type: "full", ignoreMissing: true },
			);
			expect(entries).to.have.length(3);
			expect(entries[0]).equal(full);
			expect(entries[1]).not.equal(puts[1].entry);
			expect(entries[1]?.createdLocally).equal(true);
			expect(entries[1]!.getStorageBytes()).to.exist;
			expect(await entries[1]!.getPayloadValue()).to.exist;
			expect(entries[2]).equal(undefined);
			expect(getManySpy.callCount).equal(1);
			expect(getManySpy.firstCall.args[0]).to.deep.equal([
				puts[1].entry.hash,
				puts[2].entry.hash,
			]);

			const cached = await store.docs.log.log.entryIndex.getMany(
				puts.slice(0, 2).map((put) => put.entry.hash),
			);
			expect(cached).to.deep.equal(entries.slice(0, 2));
			expect(getManySpy.callCount).equal(1);
		} finally {
			getManySpy.restore();
		}
	});

	it("resolves a local document from a storage-hollow native entry", async () => {
		const store = await openStore();

		const document = new Document({
			id: "materialize-1",
			name: "materialized",
		});
		await store.docs.put(document);

		// With the resolver cache disabled and a non-identity index, this reaches
		// resolveDocument -> Log.get(type:"full") -> getPayloadValue. The cached
		// native local-append EntryV0 used to be hollow and throw "Missing data".
		const resolved = await store.docs.index.get(document.id, {
			local: true,
			remote: false,
		});
		expect(resolved).to.be.instanceOf(Document);
		expect(resolved?.name).equal(document.name);
	});
});
