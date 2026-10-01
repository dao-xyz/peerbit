import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { DecryptedThing } from "@peerbit/crypto";
import { SearchRequestIndexed } from "@peerbit/document-interface";
import { Entry, EntryV0 } from "@peerbit/log";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import type { Operation } from "../src/operation.js";
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

	const cacheHollowEntry = (
		docs: Documents<Document, any>,
		entry: Entry<Operation>,
	) => {
		// Keep coverage of the defensive read boundary without relying on local
		// appends to return incomplete Entries as an incidental side effect.
		const hollow = deserialize(serialize(entry), Entry) as EntryV0<Operation>;
		hollow.createdLocally = entry.createdLocally;
		hollow._payload = new DecryptedThing({});
		hollow.init(docs.log.log);
		(docs.log.log.entryIndex as any).cache.add(entry.hash, hollow);
		return hollow;
	};

	it("keeps complete local append results cached and reloads a hollow cache entry", async () => {
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
		expect(await store.docs.log.log.get(put.entry.hash)).equal(put.entry);
		const hollow = cacheHollowEntry(store.docs, put.entry);

		const entry = await store.docs.log.log.get(put.entry.hash);
		expect(entry).to.exist;
		expect(entry).not.equal(hollow);
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
		cacheHollowEntry(store.docs, puts[1].entry);
		cacheHollowEntry(store.docs, puts[2].entry);
		await store.docs.log.log.blocks.rm(puts[2].entry.hash);

		const getManySpy = sinon.spy(store.docs.log.log.blocks, "getMany");

		try {
			const entries = await store.docs.log.log.getMany(
				puts.map((put) => put.entry.hash),
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

			const cached = await store.docs.log.log.getMany(
				puts.slice(0, 2).map((put) => put.entry.hash),
			);
			expect(cached).to.deep.equal(entries.slice(0, 2));
			expect(getManySpy.callCount).equal(1);
		} finally {
			getManySpy.restore();
		}
	});

	it("batch-materializes serializable indexed RPC result heads", async () => {
		const store = await openStore();
		const puts = [];
		for (const [index, name] of ["one", "two", "three"].entries()) {
			puts.push(
				await store.docs.put(
					new Document({ id: `materialize-rpc-${index}`, name }),
				),
			);
		}
		const blocks = store.docs.log.log.blocks;
		for (const put of puts) cacheHollowEntry(store.docs, put.entry);
		const getManySpy = sinon.spy(blocks, "getMany");
		const getSpy = sinon.spy(blocks, "get");

		try {
			const response = await store.docs.index.processQuery(
				new SearchRequestIndexed({ query: [], fetch: puts.length }),
				store.node.identity.publicKey,
				false,
			);

			expect(response.results).to.have.length(puts.length);
			expect(
				response.results.map((result) => result.context.head),
			).to.have.members(puts.map((put) => put.entry.hash));
			expect(() => serialize(response)).not.to.throw();
			expect(getManySpy.callCount).to.equal(1);
			expect(getManySpy.firstCall.args[0]).to.have.members(
				puts.map((put) => put.entry.hash),
			);
			expect(getSpy.callCount).to.equal(0);
		} finally {
			getManySpy.restore();
			getSpy.restore();
		}
	});

	it("resolves a local document from a storage-hollow native entry", async () => {
		const store = await openStore();

		const document = new Document({
			id: "materialize-1",
			name: "materialized",
		});
		const put = await store.docs.put(document);
		cacheHollowEntry(store.docs, put.entry);

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
