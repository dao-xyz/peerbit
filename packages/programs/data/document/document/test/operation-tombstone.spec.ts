// B12 retirement pins for persisted document data. Old stores hold entries
// whose payloads are the deprecated PutWithKeyOperation (tag 0, document v6
// encode) and DeleteByStringKeyOperation (tag 2). The compatibility OPTION is
// being retired, but persisted data must stay decodable and applicable
// forever: the classes, their @variant registrations and the delete-key
// coercions are permanent decode tombstones. These pins freeze the wire bytes
// and prove a store containing tag-0/tag-2 entries opens and reads back
// WITHOUT any compatibility option.
import { deserialize, serialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	DeleteByStringKeyOperation,
	Operation,
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
});
