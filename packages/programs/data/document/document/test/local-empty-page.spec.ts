import { field, variant } from "@dao-xyz/borsh";
import {
	CloseIteratorRequest,
	CollectNextRequest,
	IterationRequest,
} from "@peerbit/document-interface";
import {
	Sort,
	SortDirection,
	StringMatch,
	toId,
} from "@peerbit/indexer-interface";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import type { ResultsIterator } from "../src/search.js";
import { Document, TestStore } from "./data.js";

@variant("local_empty_page_indexable")
class ProjectedDocument {
	@field({ type: "string" })
	id: string;

	constructor(document?: Document) {
		this.id = document?.id ?? "";
	}
}

describe("local document empty-page continuation", () => {
	let session: TestSession | undefined;
	let store: TestStore<ProjectedDocument> | undefined;
	let sandbox: sinon.SinonSandbox;
	let iterators: ResultsIterator<Document>[];

	beforeEach(async () => {
		store = undefined;
		session = undefined;
		iterators = [];
		sandbox = sinon.createSandbox();
		session = await TestSession.disconnected(1);
	});

	afterEach(async () => {
		try {
			await Promise.all(iterators.map((iterator) => iterator.close()));
			if (store) {
				expect(store.docs.index.countIteratorsInProgress).equal(0);
			}
		} finally {
			try {
				sandbox.restore();
				await store?.close();
			} finally {
				await session?.stop();
			}
		}
	});

	const seed = async (ids: string[], missingIds: string[]) => {
		store = new TestStore<ProjectedDocument>({
			docs: new Documents<Document, ProjectedDocument>(),
		});
		await session!.peers[0].open(store, {
			args: {
				replicate: false,
				keep: "self",
				index: {
					type: ProjectedDocument,
					cache: { resolver: 0 },
				},
			},
		});
		const docs = store.docs;
		const heads = new Map<string, string>();
		for (const id of ids) {
			const put = await docs.put(new Document({ id, name: `value-${id}` }), {
				target: "none",
			});
			heads.set(id, put.entry.hash);
		}

		// Model a transient stale document-index row using only owned data.
		// Lower deletion invalidates its entry cache but does not issue a
		// Documents delete. A projection and resolver cache 0 make resolution
		// consult the real lower log instead of reconstructing/caching a value.
		for (const id of missingIds) {
			const hash = heads.get(id)!;
			expect(await docs.log.log.entryIndex.delete(hash)).to.exist;
			expect(await docs.log.log.get(hash, { remote: false })).equal(undefined);
		}
		expect(await docs.index.getSize()).equal(ids.length);
		for (const id of ids) {
			const row = await docs.index.index.get(toId(id));
			expect(row?.value.id).equal(id);
			expect(row?.value.__context.head).equal(heads.get(id));
			if (!missingIds.includes(id)) {
				expect(await docs.log.log.get(heads.get(id)!, { remote: false })).to
					.exist;
			}
		}
		return docs;
	};

	const sorted = () => ({
		sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
	});
	const track = <T extends Document>(iterator: ResultsIterator<T>) => {
		iterators.push(iterator);
		return iterator;
	};
	const drainBounded = async (
		iterator: ResultsIterator<Document>,
		max: number,
	) => {
		const ids: string[] = [];
		for (let page = 0; page < max && !iterator.done(); page++) {
			for (const value of await iterator.next(1)) ids.push(value.id);
		}
		expect(iterator.done(), "exhaustion within the fixture's page bound").equal(
			true,
		);
		return ids;
	};
	const range = (from: number, to: number) =>
		Array.from({ length: to - from }, (_, i) =>
			String(from + i).padStart(3, "0"),
		);
	// One entry per processQuery call: request kind, resolved results, kept.
	const describeCalls = (process: sinon.SinonSpy) =>
		Promise.all(
			process.getCalls().map(async (call) => {
				const [request] = call.args;
				const response = await call.returnValue;
				const kind =
					request instanceof CollectNextRequest
						? "collect"
						: request instanceof IterationRequest
							? "iterate"
							: "other";
				return `${kind}:${response.results.length}:${response.kept}`;
			}),
		);
	// Turns a drain that never settles into a failure instead of a suite
	// timeout. A passing drain settles once its keep-open wait ends.
	const settlesBeforeClose = async <T>(work: Promise<T>, label: string) => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				work,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error(`${label} did not settle without close()`)),
						30_000,
					);
				}),
			]);
		} finally {
			clearTimeout(timer);
		}
	};
	// Holds the response of the `gatedCall`-th processQuery call until
	// `release()`, after the real query has run.
	const gateProcessQuery = (
		docs: Documents<Document, ProjectedDocument>,
		gatedCall: number,
	) => {
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const original = docs.index.processQuery.bind(docs.index);
		const gate: {
			request?: unknown;
			isLocal?: boolean;
			response?: { results: unknown[]; kept: bigint };
		} = {};
		let calls = 0;
		const stub = sandbox
			.stub(docs.index, "processQuery")
			.callsFake(async (request, from, isLocal, options) => {
				const call = ++calls;
				const result = await original(request, from, isLocal, options);
				if (call === gatedCall) {
					gate.request = request;
					gate.isLocal = isLocal;
					gate.response = result;
					entered.resolve();
					await release.promise;
				}
				return result;
			});
		return {
			gate,
			stub,
			release: () => release.resolve(),
			entered: (inFlight: Promise<unknown>) =>
				Promise.race([
					entered.promise,
					inFlight.then(() => {
						throw new Error("next() completed before the response gate");
					}),
				]),
		};
	};

	it("continues after an initial empty resolved page with rows kept", async () => {
		const docs = await seed(["000", "001"], ["000"]);
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);

		expect(await iterator.next(1)).to.deep.equal([]);
		const response = await process.firstCall.returnValue;
		expect(response.results).to.have.length(0);
		expect(response.kept).equal(1n);
		expect(iterator.done()).equal(false);
		expect(await iterator.pending()).equal(1);
		expect(await drainBounded(iterator, 2)).to.deep.equal(["001"]);
		expect(await iterator.pending()).equal(0);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	it("continues after an empty local CollectNext page with a resolvable tail", async () => {
		const docs = await seed(["000", "001", "002"], ["001"]);
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);

		expect((await iterator.next(1)).map((value) => value.id)).to.deep.equal([
			"000",
		]);
		expect(await iterator.next(1)).to.deep.equal([]);
		expect(process.secondCall.args[0]).instanceOf(CollectNextRequest);
		const response = await process.secondCall.returnValue;
		expect(response.results).to.have.length(0);
		expect(response.kept).equal(1n);
		expect(iterator.done()).equal(false);
		expect(await iterator.pending()).equal(1);
		expect(await drainBounded(iterator, 2)).to.deep.equal(["002"]);
		expect(await iterator.pending()).equal(0);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	// all() and for await collect 100 rows per batch for a non-replicating
	// query. With a keep-open remote wait they wait for a join or update
	// signal between batches, and that signal's timer fires only once.
	it("all() collects past two empty local pages under a keep-open remote wait", async () => {
		// Rows 000-199 are two whole batches that resolve no documents.
		const docs = await seed(range(0, 201), range(0, 200));
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), {
				local: true,
				remote: { wait: { timeout: 500 } },
			}),
		);

		const values = await settlesBeforeClose(iterator.all(), "all()");
		expect(values.map((value) => value.id)).to.deep.equal(["200"]);
		expect(await describeCalls(process)).to.deep.equal([
			"iterate:0:101",
			"collect:0:1",
			"collect:1:0",
		]);
		expect(iterator.done()).equal(true);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	it("for await collects every local page under a keep-open remote wait", async () => {
		// A full first batch, then a batch that resolves no documents.
		const docs = await seed(range(0, 201), range(100, 200));
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), {
				local: true,
				remote: { wait: { timeout: 500 } },
			}),
		);

		const ids: string[] = [];
		await settlesBeforeClose(
			(async () => {
				for await (const value of iterator) ids.push(value.id);
			})(),
			"for await",
		);
		expect(ids).to.deep.equal([...range(0, 100), "200"]);
		expect(await describeCalls(process)).to.deep.equal([
			"iterate:100:101",
			"collect:0:1",
			"collect:1:0",
		]);
		expect(iterator.done()).equal(true);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	it("waits instead of spinning once the local cursor is gone with rows still kept", async () => {
		const docs = await seed(["000", "001", "002"], []);
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), {
				local: true,
				remote: { wait: { timeout: 1000 } },
			}),
		);
		expect((await iterator.next(1)).map((value) => value.id)).to.deep.equal([
			"000",
		]);
		expect(await iterator.pending()).equal(2);
		// Drop the local cursor as an eviction would. The iterator still
		// records two rows kept for it, but nothing can collect them.
		docs.index.processCloseIteratorRequest(
			new CloseIteratorRequest({ id: process.firstCall.args[0].id }),
			session!.peers[0].identity.publicKey,
		);
		expect(docs.index.countIteratorsInProgress).equal(0);
		const cursorChecks = sandbox.spy(
			(docs.index as any)._resumableIterators,
			"has",
		);

		expect(await settlesBeforeClose(iterator.all(), "all()")).to.deep.equal([]);
		// A drain loop that skips the wait here checks the cursor many
		// thousands of times before the 1000 ms wait ends.
		expect(cursorChecks.callCount).to.be.lessThan(20);
		expect(process.callCount).equal(1);
	});

	it("finishes an all-missing terminal page without retaining a cursor", async () => {
		const docs = await seed(["000"], ["000"]);
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);

		expect(await drainBounded(iterator, 2)).to.deep.equal([]);
		expect(process.callCount).equal(1);
		const response = await process.firstCall.returnValue;
		expect(response.results).to.have.length(0);
		expect(response.kept).equal(0n);
		expect(await iterator.pending()).equal(0);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	it("explicitly closes a retained local cursor after an empty page", async () => {
		const docs = await seed(["000", "001"], ["000"]);
		const process = sandbox.spy(docs.index, "processQuery");
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);

		expect(await iterator.next(1)).to.deep.equal([]);
		expect((await process.firstCall.returnValue).kept).equal(1n);
		expect(docs.index.countIteratorsInProgress).equal(1);
		await iterator.close();
		expect(iterator.done()).equal(true);
		expect(await iterator.pending()).equal(0);
		expect(docs.index.countIteratorsInProgress).equal(0);
	});

	it("stays closed when an in-flight initial empty response is released", async () => {
		const docs = await seed(["000", "001"], ["000"]);
		const held = gateProcessQuery(docs, 1);
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);
		const next = iterator.next(1);
		try {
			await held.entered(next);
			expect(held.stub.callCount).equal(1);
			expect(held.gate.isLocal).equal(true);
			expect(held.gate.response?.results).to.have.length(0);
			expect(held.gate.response?.kept).equal(1n);
			expect(docs.index.countIteratorsInProgress).equal(1);

			await iterator.close();
			expect(iterator.done()).equal(true);
			expect(docs.index.countIteratorsInProgress).equal(0);
			held.release();
			expect(await next).to.deep.equal([]);
			expect(iterator.done()).equal(true);
			expect(await iterator.pending()).equal(0);
			expect(docs.index.countIteratorsInProgress).equal(0);
		} finally {
			held.release();
			await next.catch(() => undefined);
			await iterator.close();
		}
	});

	it("stays closed when an in-flight non-empty first page is released", async () => {
		const docs = await seed(["000", "001", "002"], []);
		const held = gateProcessQuery(docs, 1);
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);
		const next = iterator.next(1);
		try {
			await held.entered(next);
			expect(held.gate.request).instanceOf(IterationRequest);
			expect(held.gate.isLocal).equal(true);
			expect(held.gate.response?.results).to.have.length(1);
			expect(held.gate.response?.kept).equal(2n);

			await iterator.close();
			expect(iterator.done()).equal(true);
			held.release();
			expect(await next).to.deep.equal([]);
			expect(iterator.done()).equal(true);
			expect(await iterator.pending()).equal(0);
			expect(held.stub.callCount).equal(1);
			expect(docs.index.countIteratorsInProgress).equal(0);
		} finally {
			held.release();
			await next.catch(() => undefined);
			await iterator.close();
		}
	});

	it("stays closed when an in-flight local CollectNext response is released", async () => {
		const docs = await seed(["000", "001", "002"], []);
		const held = gateProcessQuery(docs, 2);
		const iterator = track(
			docs.index.iterate(sorted(), { local: true, remote: false }),
		);
		expect((await iterator.next(1)).map((value) => value.id)).to.deep.equal([
			"000",
		]);
		const next = iterator.next(1);
		try {
			await held.entered(next);
			expect(held.gate.request).instanceOf(CollectNextRequest);
			expect(held.gate.isLocal).equal(true);
			expect(held.gate.response?.results).to.have.length(1);
			expect(held.gate.response?.kept).equal(1n);

			await iterator.close();
			expect(iterator.done()).equal(true);
			held.release();
			expect(await next).to.deep.equal([]);
			expect(iterator.done()).equal(true);
			expect(await iterator.pending()).equal(0);
			expect(held.stub.callCount).equal(2);
			expect(docs.index.countIteratorsInProgress).equal(0);
		} finally {
			held.release();
			await next.catch(() => undefined);
			await iterator.close();
		}
	});

	it("still considers remote fallback for empty local results, not readable ones", async () => {
		const docs = await seed(["000", "001"], ["000"]);
		const process = sandbox.spy(docs.index, "processQuery");
		// Call through to real cover selection. This single disconnected peer
		// checks fallback admission, not remote transport or delivery receipts.
		const cover = sandbox.spy(docs.log, "getCover");
		const empty = track(
			docs.index.iterate(sorted(), {
				local: true,
				remote: { strategy: "fallback" },
			}),
		);
		expect(await empty.next(1)).to.deep.equal([]);
		const response = await process.firstCall.returnValue;
		expect(response.results).to.have.length(0);
		expect(response.kept).equal(1n);
		expect(cover.callCount).equal(1);
		await empty.close();
		cover.resetHistory();

		const readable = track(
			docs.index.iterate(
				{ query: new StringMatch({ key: "id", value: "001" }), ...sorted() },
				{ local: true, remote: { strategy: "fallback" } },
			),
		);
		expect((await readable.next(1)).map((value) => value.id)).to.deep.equal([
			"001",
		]);
		expect(cover.callCount).equal(0);
	});
});
