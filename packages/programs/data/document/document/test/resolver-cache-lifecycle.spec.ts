import { NotStartedError } from "@peerbit/indexer-interface";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

type ResolverCache = {
	size: number;
	get(id: string): Document | null | undefined;
};

const resolverCache = (index: object): ResolverCache | undefined =>
	(index as { _resolverCache?: ResolverCache })._resolverCache;

const args = {
	replicate: false,
	keep: "self" as const,
	canPerform: () => true,
};

describe("document resolver cache lifecycle", function () {
	this.timeout(30_000);
	let session: TestSession;
	let directory: string;

	beforeEach(async () => {
		// Reopen must read persistent state, not rely on an in-memory index
		// surviving close (which differs between indexer backends).
		directory = await mkdtemp(join(tmpdir(), "peerbit-resolver-lifecycle-"));
		session = await TestSession.disconnected(1, { directory });
	});

	afterEach(async () => {
		await session?.stop();
		await rm(directory, { recursive: true, force: true });
	});

	const createStore = () => new TestStore({ docs: new Documents<Document>() });
	const createDocument = () =>
		new Document({
			id: "cached",
			name: "retained document",
			data: new Uint8Array(32 * 1024).fill(37),
		});
	const put = (store: TestStore, document: Document) =>
		store.docs.put(document, { unique: true, target: "none" });
	const expectReleased = (index: object, cache: ResolverCache) => {
		expect(cache.size).to.equal(0);
		expect(cache.get("cached")).to.be.undefined;
		expect(resolverCache(index)).to.be.undefined;
	};

	it("releases the cache when the already-stopped index rejects stop", async () => {
		const store = await session.peers[0].open(createStore(), { args });
		await put(store, createDocument());
		const index = store.docs.index;
		const cache = resolverCache(index)!;
		expect(cache.size).to.equal(1);
		const backingIndex = index.index;
		const originalStop = backingIndex.stop;
		await originalStop?.call(backingIndex);
		let stopCalls = 0;
		backingIndex.stop = async () => {
			stopCalls += 1;
			throw new NotStartedError();
		};
		try {
			expect(await store.close()).to.be.true;
			expect(stopCalls).to.equal(1);
			expect(index.closed).to.be.true;
			expectReleased(index, cache);
		} finally {
			backingIndex.stop = originalStop;
		}
	});

	for (const operation of ["close", "drop"] as const) {
		it(`releases retained documents on ${operation} and ignores late cache admission`, async () => {
			const store = await session.peers[0].open(createStore(), { args });
			const document = createDocument();
			await put(store, document);
			const index = store.docs.index;
			const cache = resolverCache(index)!;
			expect(cache.size).to.equal(1);
			expect(cache.get(document.id)).to.be.instanceOf(Document);

			expect(await store[operation]()).to.be.true;
			expect(index.closed).to.be.true;
			expectReleased(index, cache);

			// This existing admission seam is also reached by delayed projection work.
			index._cacheResolvedIdentityValue("late", createDocument());
			expectReleased(index, cache);

			if (operation === "close") {
				const reopened = await session.peers[0].open(store, { args });
				expect(reopened).to.equal(store);
				expect(reopened.docs.index).to.equal(index);
				expect(resolverCache(index)).not.to.equal(cache);
				const restored = await reopened.docs.get(document.id, {
					remote: false,
				});
				expect(restored).to.be.instanceOf(Document);
				expect(restored?.id).to.equal(document.id);
				expect(restored?.name).to.equal(document.name);
				expect(Array.from(restored!.data!)).to.deep.equal(
					Array.from(document.data!),
				);
				expect(cache.size).to.equal(0);
			}
		});

		it(`keeps disabled caching disabled through ${operation}`, async () => {
			const store = await session.peers[0].open(createStore(), {
				args: { ...args, index: { cache: { resolver: 0 } } },
			});
			await put(store, createDocument());
			const index = store.docs.index;
			expect(resolverCache(index)).to.be.undefined;
			expect(await store[operation]()).to.be.true;
			index._cacheResolvedIdentityValue("late", createDocument());
			expect(resolverCache(index)).to.be.undefined;
		});

		it(`preserves the cache when another owner declines ${operation}`, async () => {
			const store = await session.peers[0].open(createStore(), { args });
			await put(store, createDocument());
			const index = store.docs.index;
			const cache = resolverCache(index)!;
			const cached = cache.get("cached");
			const root = await session.peers[0].open(index, { existing: "reuse" });
			expect(root).to.equal(index);
			expect(index.parents).to.include(store.docs);
			expect(index.parents).to.include(undefined);

			expect(await root[operation]()).to.be.false;
			expect(index.closed).to.be.false;
			expect(resolverCache(index)).to.equal(cache);
			expect(cache.get("cached")).to.equal(cached);
			expect(cache.size).to.equal(1);
			expect(await store.docs.get("cached", { remote: false })).to.equal(
				cached,
			);
		});

		it(`releases the cache if a terminal ${operation} callback rejects`, async () => {
			const store = createStore();
			const callbackError = new Error("terminal callback failure");
			let rejectCallback = true;
			const callback = (program: object) => {
				if (program === store.docs.index && rejectCallback) throw callbackError;
			};
			await session.peers[0].open(store, {
				args,
				...(operation === "close"
					? { onClose: callback }
					: { onDrop: callback }),
			});
			await put(store, createDocument());
			const index = store.docs.index;
			const cache = resolverCache(index)!;
			expect(cache.size).to.equal(1);

			try {
				let failure: unknown;
				try {
					await index[operation](store.docs);
				} catch (error) {
					failure = error;
				}
				expect(failure).to.equal(callbackError);
				expect(index.closed).to.be.true;
				expectReleased(index, cache);
				index._cacheResolvedIdentityValue("late", createDocument());
				expectReleased(index, cache);
			} finally {
				rejectCallback = false;
				await index[operation](store.docs);
			}
		});

		it(`preserves the cache if ${operation} rejects before closing`, async () => {
			const store = await session.peers[0].open(createStore(), { args });
			await put(store, createDocument());
			const index = store.docs.index;
			const cache = resolverCache(index)!;
			const cached = cache.get("cached");

			await expect(index[operation](createStore())).to.be.rejectedWith(
				"Could not find from in parents",
			);
			expect(index.closed).to.be.false;
			expect(resolverCache(index)).to.equal(cache);
			expect(cache.size).to.equal(1);
			expect(cache.get("cached")).to.equal(cached);
			expect(await store.docs.get("cached", { remote: false })).to.equal(
				cached,
			);
		});
	}
});
