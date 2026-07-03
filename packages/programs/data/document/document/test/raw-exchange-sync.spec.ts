// Reproduces the raw exchange-head onChange gap: a document store syncing
// under sync.rawExchangeHeads must land its index commits AND fire its
// program-level change consumers for every received entry. Before the raw
// receive dispatched change events, the log filled while the document index
// stayed empty.
import {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
} from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import type { DocumentsChange } from "../src/events.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

describe("raw exchange-head document sync", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("indexes documents and fires change events on the raw receive path", async () => {
		const entryCount = 32;
		const store = new TestStore({ docs: new Documents<Document>() });
		const store2 = store.clone();

		await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				sync: { rawExchangeHeads: true },
			},
		});

		// count raw vs plain exchange messages leaving peer 1 so the test
		// proves the raw path was actually exercised
		let rawExchangeHeads = 0;
		let plainExchangeHeads = 0;
		const send = store.docs.log.rpc.send.bind(store.docs.log.rpc);
		store.docs.log.rpc.send = async (message, options) => {
			if (message instanceof RawExchangeHeadsMessage) {
				rawExchangeHeads += 1;
			} else if (message instanceof ExchangeHeadsMessage) {
				plainExchangeHeads += 1;
			}
			return send(message, options);
		};

		const documents = Array.from(
			{ length: entryCount },
			(_, index) =>
				new Document({
					id: `raw-sync-${index}`,
					name: `raw-sync-name-${index}`,
				}),
		);
		await store.docs.putMany(documents, { unique: true });
		expect(store.docs.log.log.length).to.equal(entryCount);

		await session.peers[1].open(store2, {
			args: {
				replicate: { factor: 1 },
				sync: { rawExchangeHeads: true },
			},
		});
		const changes: DocumentsChange<Document, Document>[] = [];
		store2.docs.events.addEventListener("change", (evt) => {
			changes.push(evt.detail);
		});

		await waitForResolved(
			async () => {
				expect(await store2.docs.index.getSize()).to.equal(entryCount);
			},
			{ timeout: 30_000, timeoutMessage: "raw sync document index" },
		);
		expect(store2.docs.log.log.length).to.equal(entryCount);
		expect(rawExchangeHeads).to.be.greaterThan(0);
		expect(plainExchangeHeads).to.equal(0);

		// the change consumer observed every received document exactly once
		await waitForResolved(() => {
			const addedIds = changes.flatMap((change) =>
				change.added.map((doc) => doc.id),
			);
			expect(addedIds).to.have.members(documents.map((doc) => doc.id));
		});
		expect(changes.flatMap((change) => change.removed)).to.have.length(0);

		// documents are resolvable through the index
		const resolved = await store2.docs.get("raw-sync-7", {
			local: true,
			remote: false,
		});
		expect(resolved?.name).to.equal("raw-sync-name-7");
	});
});
