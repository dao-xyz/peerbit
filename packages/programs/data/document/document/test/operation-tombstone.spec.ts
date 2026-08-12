// B12 retirement pins for persisted document data. Old stores hold entries
// whose payloads are the deprecated PutWithKeyOperation (tag 0, document v6
// encode) and DeleteByStringKeyOperation (tag 2). The compatibility OPTION is
// being retired, but persisted data must stay decodable and applicable
// forever: the classes, their @variant registrations and the delete-key
// coercions are permanent decode tombstones. These pins freeze the wire bytes
// and prove a store containing tag-0/tag-2 entries opens and reads back
// WITHOUT any compatibility option.
import { readFileSync, readdirSync } from "fs";
import path from "path";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	DeleteByStringKeyOperation,
	Operation,
	PutOperation,
	PutWithKeyOperation,
} from "../src/operation.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

// Frozen wire bytes, derived from the current encoding. They must never
// change again: old stores contain exactly these layouts.
// PutWithKeyOperation { key: "tombstone-doc", data: [1, 2, 3, 4] }
const PUT_WITH_KEY_TAG0_HEX =
	"00000d000000746f6d6273746f6e652d646f630400000001020304";
// DeleteByStringKeyOperation { key: "tombstone-doc" }
const DELETE_BY_STRING_KEY_TAG2_HEX = "00020d000000746f6d6273746f6e652d646f63";

describe("document operation tombstones", () => {
	describe("frozen bytes", () => {
		it("keeps the PutWithKeyOperation tag-0 layout byte-identical", () => {
			const decoded = deserialize(
				Uint8Array.from(Buffer.from(PUT_WITH_KEY_TAG0_HEX, "hex")),
				Operation,
			);
			expect(decoded).to.be.instanceOf(PutWithKeyOperation);
			const put = decoded as PutWithKeyOperation;
			expect(put.key).to.equal("tombstone-doc");
			expect([...put.data]).to.deep.equal([1, 2, 3, 4]);
			expect(Buffer.from(serialize(decoded)).toString("hex")).to.equal(
				PUT_WITH_KEY_TAG0_HEX,
			);
		});

		it("keeps the DeleteByStringKeyOperation tag-2 layout byte-identical", () => {
			const decoded = deserialize(
				Uint8Array.from(Buffer.from(DELETE_BY_STRING_KEY_TAG2_HEX, "hex")),
				Operation,
			);
			expect(decoded).to.be.instanceOf(DeleteByStringKeyOperation);
			expect((decoded as DeleteByStringKeyOperation).key).to.equal(
				"tombstone-doc",
			);
			expect(Buffer.from(serialize(decoded)).toString("hex")).to.equal(
				DELETE_BY_STRING_KEY_TAG2_HEX,
			);
		});
	});

	describe("old-store decode", () => {
		let session: TestSession;
		let store: TestStore;

		before(async () => {
			session = await TestSession.connected(1);
		});

		afterEach(async () => {
			await store?.close();
		});

		after(async () => {
			await session.stop();
		});

		it("opens persisted tag-0 and tag-2 entries without any compatibility option", async () => {
			store = new TestStore({ docs: new Documents<Document>() });
			// Deliberately NO compatibility option — this is a current-mode open.
			await session.peers[0].open(store);

			const keep = new Document({ id: "tombstone-keep", name: "kept name" });
			const removed = new Document({
				id: "tombstone-removed",
				name: "removed name",
			});

			// Write the deprecated operation layouts exactly as an old store's log
			// holds them: tag-0 puts and a tag-2 delete referencing its put.
			await store.docs.log.append(
				new PutWithKeyOperation({ key: keep.id, data: serialize(keep) }),
				{ meta: { next: [] } },
			);
			const putRemoved = await store.docs.log.append(
				new PutWithKeyOperation({ key: removed.id, data: serialize(removed) }),
				{ meta: { next: [] } },
			);
			await waitForResolved(async () =>
				expect(await store.docs.index.getSize()).to.equal(2),
			);

			await store.docs.log.append(
				new DeleteByStringKeyOperation({ key: removed.id }),
				{ meta: { next: [putRemoved.entry] } },
			);
			await waitForResolved(async () =>
				expect(await store.docs.index.getSize()).to.equal(1),
			);

			// Reopen the same store WITHOUT compatibility and read the data back:
			// old stores must stay openable after the option retires.
			await store.close();
			await session.peers[0].open(store);
			await waitForResolved(async () =>
				expect(await store.docs.index.getSize()).to.equal(1),
			);
			const readBack = await store.docs.index.get(keep.id);
			expect(readBack).to.exist;
			expect(readBack!.id).to.equal(keep.id);
			expect(readBack!.name).to.equal("kept name");
			expect(await store.docs.index.get(removed.id)).to.equal(undefined);
		});
	});

	describe("encode retirement", () => {
		// B12 stage 5: the compatibility-6 ENCODE branch (PutWithKeyOperation
		// construction at the document write path) is deleted. Writes are
		// always PutOperation; the deprecated layouts are decode-only.
		let session: TestSession;
		let store: TestStore;

		before(async () => {
			session = await TestSession.connected(1);
		});

		afterEach(async () => {
			await store?.close();
		});

		after(async () => {
			await session.stop();
		});

		it("writes PutOperation for a default put, never PutWithKeyOperation", async () => {
			store = new TestStore({ docs: new Documents<Document>() });
			await session.peers[0].open(store);
			const doc = new Document({ id: "encode-default", name: "current" });
			const { entry } = await store.docs.put(doc);
			const reloaded = await Entry.fromMultihash<Operation>(
				store.docs.log.log.blocks,
				entry.hash,
			);
			reloaded.init({
				encoding: store.docs.log.log.encoding,
				keychain: store.docs.log.log.keychain,
			});
			const payload = await reloaded.getPayloadValue();
			expect(payload).to.be.instanceOf(PutOperation);
			// PutOperation and PutWithKeyOperation are @variant SIBLINGS (tags 3
			// and 0), not a subclass relation, so `not.instanceOf` here is
			// trivially true and pins nothing. Assert the wire tag instead: byte 0
			// is Operation's own variant, byte 1 is the concrete operation tag.
			// PutWithKeyOperation's frozen tag pair is [0, 0]; a default put must
			// never produce it.
			expect([...serialize(payload).slice(0, 2)]).to.deep.equal([0, 3]);
		});

		it("has zero deprecated-operation construction sites in src", () => {
			// Source ratchet: the deprecated layouts are encode-dead. Any
			// re-added `new PutWithKeyOperation(`/`new DeleteByStringKeyOperation(`
			// in src is a retirement regression (decode registration in
			// operation.ts uses only decorators, not construction).
			const forbidden =
				/new\s+(PutWithKeyOperation|DeleteByStringKeyOperation)\s*\(/g;
			const sourceFiles = readdirSync(path.join(process.cwd(), "src"), {
				recursive: true,
			})
				.map((entry) => String(entry))
				.filter((entry) => entry.endsWith(".ts"))
				.map((entry) => path.join("src", entry));
			expect(sourceFiles.length).to.be.greaterThan(0);
			for (const file of sourceFiles) {
				const source = readFileSync(path.join(process.cwd(), file), "utf8");
				const matches = [...source.matchAll(forbidden)].map((m) => m[0]);
				expect(matches, file).to.deep.equal([]);
			}
		});

		it("has zero deprecated-operation construction sites in test", () => {
			// Test-side leg, mirroring shared-log's no-legacy-machinery ratchet.
			// The src leg alone cannot stop a spec from re-teaching the codebase
			// to write the deprecated layouts: a new test that constructs one
			// would look like sanctioned coverage. Only THIS spec is allowed to
			// construct them, and only to seed the old-store decode fixture.
			const forbidden =
				/new\s+(PutWithKeyOperation|DeleteByStringKeyOperation)\s*\(/g;
			const whitelist = new Set([
				path.join("test", "operation-tombstone.spec.ts"),
			]);
			const testFiles = readdirSync(path.join(process.cwd(), "test"), {
				recursive: true,
			})
				.map((entry) => String(entry))
				.filter((entry) => entry.endsWith(".ts"))
				.map((entry) => path.join("test", entry));
			expect(testFiles.length).to.be.greaterThan(0);
			let sawWhitelistedFile = false;
			for (const file of testFiles) {
				const source = readFileSync(path.join(process.cwd(), file), "utf8");
				const matches = [...source.matchAll(forbidden)].map((m) => m[0]);
				if (whitelist.has(file)) {
					// The sanctioned spec must actually still construct them,
					// otherwise the whitelist entry is stale cover for nothing.
					expect(matches.length, file).to.be.greaterThan(0);
					sawWhitelistedFile = true;
					continue;
				}
				expect(matches, file).to.deep.equal([]);
			}
			expect(sawWhitelistedFile).to.be.true;
		});
	});
});
