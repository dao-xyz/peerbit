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

	/**
	 * The scope pin for the recovery, and the reason it lives on
	 * `getLocalIndexedContextForChange` rather than on the shared
	 * `getLocalIndexedContext`.
	 *
	 * An earlier revision put it on the shared helper. That helper has five
	 * callers and only the change-delivery one may treat a closed index as "no
	 * prior version"; `put()` is one of the other four. With the recovery shared,
	 * a put() against a closed index returned SUCCESS, committed the entry to the
	 * log, and left the index without it - manufacturing exactly the log/index
	 * divergence the recovery exists to avoid, on the public write path, with no
	 * error to notice.
	 *
	 * Note this is the CLOSED case. The open case is pinned above and passes
	 * either way, which is why it did not catch the over-broad version.
	 */
	it("put() should still surface NotStartedError when only the index is closed", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const documents = store.documents;

		// Close the index on its own, leaving the log open: the window the
		// recovery is written for, reached through public API only.
		await documents.index.close();
		expect(documents.index.closed).to.equal(true);
		expect(documents.log.closed).to.equal(false);

		const lengthBefore = documents.log.log.length;

		await expect(
			documents.put(new TestDocument({ id: "doc-idx-closed", name: "x" })),
		).to.be.rejectedWith(indexerTypes.NotStartedError);

		// And it must not have half-committed: a rejected put that still appended
		// would be the same divergence by another route.
		expect(documents.log.log.length).to.equal(lengthBefore);
	});

	/**
	 * The read leg of the same race, and the lifecycle ordering that removes it.
	 *
	 * `Documents.handleChanges` reads the index before it writes, so the guarded
	 * put paths above were reached one call too late: a join still in flight when
	 * the sibling `DocumentIndex` stopped its indexer threw NotStartedError out of
	 * the read instead.
	 */
	it("closes the shared log before the document index", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const documentIndex = store.documents.index;

		// Sampled at the moment `Program.processEnd` enters the index child's
		// close. Without ordering, `processEnd` invokes both children in the same
		// synchronous turn, so the log cannot possibly be closed yet; with it, the
		// log close is fully awaited first. No timing involved either way.
		let logClosedWhenIndexCloseStarted: boolean | undefined;
		const originalClose = documentIndex.close.bind(documentIndex);
		(documentIndex as any).close = (from?: Program) => {
			logClosedWhenIndexCloseStarted ??= store.documents.log.closed;
			return originalClose(from);
		};

		await store.close();

		expect(logClosedWhenIndexCloseStarted).to.equal(true);
		expect(documentIndex.closed).to.equal(true);
	});

	it("getLocalIndexedContextForChange() should ignore NotStartedError after the document index is closed", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const documents = store.documents;
		const backingIndex = documents.index.index as any;
		const originalGet = backingIndex.get.bind(backingIndex);

		await store.close();

		backingIndex.get = async () => {
			throw new indexerTypes.NotStartedError();
		};

		try {
			const result = await (documents as any).getLocalIndexedContextForChange(
				indexerTypes.toId("doc-closed"),
			);

			expect(documents.index.closed).to.equal(true);
			expect(result).to.equal(undefined);
		} finally {
			backingIndex.get = originalGet;
		}
	});

	it("getLocalIndexedContextForChange() should still surface NotStartedError while the store is open", async () => {
		session = await TestSession.connected(1);

		const store = await session.peers[0].open(new TestStore());
		const documents = store.documents;
		const backingIndex = documents.index.index as any;
		const originalGet = backingIndex.get.bind(backingIndex);
		backingIndex.get = async () => {
			throw new indexerTypes.NotStartedError();
		};

		try {
			expect(documents.index.closed).to.equal(false);
			await expect(
				(documents as any).getLocalIndexedContextForChange(
					indexerTypes.toId("doc-open"),
				),
			).to.be.rejectedWith(indexerTypes.NotStartedError);
		} finally {
			backingIndex.get = originalGet;
		}
	});
});
