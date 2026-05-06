/**
 * Regression: putWithContext() throws NotStartedError on late replication
 * writes after program shutdown.
 *
 * When the index has been stopped/closed and a late replication write arrives,
 * this.index.put() throws NotStartedError which surfaces as an unhandled
 * rejection. The fix catches NotStartedError in putWithContext() and returns
 * gracefully.
 */
import { field, variant } from "@dao-xyz/borsh";
import { Context } from "@peerbit/document-interface";
import * as indexerTypes from "@peerbit/indexer-interface";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { Documents } from "../src/program.js";

@variant(0)
class TestDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	constructor(properties?: { id: string; name: string }) {
		this.id = properties?.id ?? uuid();
		this.name = properties?.name ?? "";
	}
}

@variant("test_shutdown_race")
class TestStore extends Program {
	@field({ type: Documents })
	documents: Documents<TestDocument>;

	constructor() {
		super();
		this.documents = new Documents();
	}

	async open(): Promise<void> {
		await this.documents.open({
			type: TestDocument,
			index: { idProperty: "id" },
		});
	}
}

describe("@peerbit/document — shutdown race", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("putWithContext() should ignore NotStartedError after the document index is closed", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const documentIndex = store.documents.index;
		const backingIndex = documentIndex.index as any;
		const originalPut = backingIndex.put.bind(backingIndex);
		backingIndex.put = async () => {
			throw new indexerTypes.NotStartedError();
		};

		await store.close();

		const context = new Context({
			created: 1n,
			modified: 1n,
			head: "closed-head",
			gid: "closed-gid",
			size: 0,
		});

		try {
			const result = await documentIndex.putWithContext(
				new TestDocument({ id: "doc-closed", name: "closed" }),
				indexerTypes.toId("doc-closed"),
				context,
			);

			expect(documentIndex.closed).to.equal(true);
			expect(result.context).to.equal(context);
			expect(result.indexable.id).to.equal("doc-closed");
		} finally {
			backingIndex.put = originalPut;
		}
	});

	it("closing a store after replication completes should not throw", async () => {
		session = await TestSession.connected(2);

		const store0 = await session.peers[0].open(new TestStore());
		const store1: TestStore = await session.peers[1].open(store0.clone());

		await store0.documents.waitFor(store1.documents.node.identity.publicKey);
		await store1.documents.waitFor(store0.documents.node.identity.publicKey);

		await store0.documents.put(
			new TestDocument({ id: "doc-2", name: "world" }),
		);

		await waitForResolved(async () =>
			expect(await store1.documents.index.index.count()).equal(1),
		);

		await store1.close();
		await store0.close();
	});

	it("put() should still surface NotStartedError while the store is open", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const backingIndex = store.documents.index.index as any;
		const originalPut = backingIndex.put.bind(backingIndex);
		backingIndex.put = async () => {
			throw new indexerTypes.NotStartedError();
		};

		try {
			await expect(
				store.documents.put(
					new TestDocument({ id: "doc-open", name: "still-throws" }),
				),
			).to.be.rejectedWith(indexerTypes.NotStartedError);
		} finally {
			backingIndex.put = originalPut;
		}
	});
});
