import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { createRustPeerbitOptions } from "peerbit/rust";
import { v4 as uuid } from "uuid";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

// Regression coverage for the "Class-A" native-vs-JS divergence surfaced by the
// document conformance matrix: a peer with a NATIVE block store + native rust
// indexer, but a Documents store opened in the DEFAULT mode:"auto" (NOT
// mode:"native"), used to throw "Missing data" from `del` because the delete
// read-back resolved the prior put's payload through the in-memory JS-entry path
// (`Entry.getPayloadValue`) instead of a storage-level block read.
describe("native del read-back (auto mode)", () => {
	let session: TestSession;

	beforeEach(async () => {
		// Single native-storage + native-indexer peer, no native network plane.
		session = await TestSession.connected(1, createRustPeerbitOptions({ network: false }));
	});

	afterEach(async () => {
		await session.stop();
	});

	it("put then del does not throw and actually deletes", async () => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: false }),
		});
		// Plain args: auto document mode (no mode:"native", no nativeBackbone).
		await session.peers[0].open(store);

		const doc = new Document({ id: uuid(), name: "Hello world" });
		await store.docs.put(doc);
		expect(await store.docs.index.getSize()).equal(1);

		const deleteOperation = (await store.docs.del(doc.id)).entry;
		expect(await store.docs.index.getSize()).equal(0);
		expect((await store.docs.index.get(doc.id)) ?? undefined).to.equal(
			undefined,
		);
		// The delete operation is the only remaining head (prior put was cut).
		expect(
			(await store.docs.log.log.toArray()).map((x) => x.hash),
		).to.deep.equal([deleteOperation.hash]);
	});

	it("put, edit, then del permanently", async () => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: false }),
		});
		await session.peers[0].open(store);

		const doc = new Document({ id: uuid(), name: "Hello world" });
		const editDoc = new Document({ id: doc.id, name: "Hello world 2" });

		await store.docs.put(doc);
		const putOperation2 = (await store.docs.put(editDoc)).entry;
		expect(await store.docs.index.getSize()).equal(1);
		expect(putOperation2.meta.next).to.have.length(1);

		const deleteOperation = (await store.docs.del(doc.id)).entry;
		expect(await store.docs.index.getSize()).equal(0);
		expect(
			(await store.docs.log.log.toArray()).map((x) => x.hash),
		).to.deep.equal([deleteOperation.hash]);
	});
});
