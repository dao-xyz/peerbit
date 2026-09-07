import { type PublicSignKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	type AbstractSearchRequest,
	type AbstractSearchResult,
	type CanSearch,
	CloseIteratorRequest,
	CollectNextRequest,
	Documents,
	IterationRequest,
	NoAccess,
	Results,
	SearchRequest,
	SearchRequestIndexed,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

const bounded = async <T>(promise: Promise<T>, label: string): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error(`${label} timed out`)),
					10_000,
				);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
};

describe("remote query authorization", () => {
	let session: TestSession | undefined;
	let donor: TestStore;
	let observer: TestStore;
	let donorKey: PublicSignKey;
	let observerKey: PublicSignKey;
	let searchPolicy: (
		query: AbstractSearchRequest,
	) => boolean | Promise<boolean> = () => true;
	let readAllowed = true;
	const searched: { query: AbstractSearchRequest; from: PublicSignKey }[] = [];
	const read: { row: Document; from: PublicSignKey }[] = [];

	before(async () => {
		session = await bounded(
			TestSession.connected(2),
			"connect authenticated peers",
		);
		donorKey = session.peers[0].identity.publicKey;
		observerKey = session.peers[1].identity.publicKey;
		const store = new TestStore({ docs: new Documents<Document>() });
		donor = await bounded(
			session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
					timeUntilRoleMaturity: 0,
					index: {
						prefetch: false,
						canSearch: (query, from) => {
							searched.push({ query, from });
							return searchPolicy(query);
						},
						canRead: (row, from) => {
							read.push({ row, from });
							return readAllowed;
						},
					},
				},
			}),
			"open donor",
		);
		observer = await bounded(
			session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
					index: { prefetch: false },
				},
			}),
			"open observer",
		);
		await bounded(
			observer.docs.index.waitFor(donorKey, { timeout: 5_000 }),
			"query and log topic readiness",
		);
		for (const id of ["fixture-a", "fixture-b"]) {
			await bounded(donor.docs.put(new Document({ id })), "write fixture");
		}
	});

	beforeEach(() => {
		searchPolicy = () => true;
		readAllowed = true;
		searched.length = 0;
		read.length = 0;
	});

	afterEach(() => sinon.restore());
	after(async () => {
		if (session) await bounded(session.stop(), "stop fixture peers");
	});

	const remoteSearch = async (
		request: SearchRequest | SearchRequestIndexed | IterationRequest,
	) => {
		const responses: AbstractSearchResult[] = [];
		const expectedReplicationIntent =
			request instanceof SearchRequestIndexed ? request.replicate : undefined;
		const expectedId = request.id.slice();
		const options = {
			local: false,
			remote: {
				from: [donorKey.hashcode()],
				replicate: false,
				timeout: 5_000,
				retryMissingResponses: false,
				throwOnMissing: true,
				onResponse: (response: AbstractSearchResult, from?: PublicSignKey) => {
					expect(from!.equals(donorKey)).to.be.true;
					responses.push(response);
				},
			},
		};
		const rows = await bounded(
			request instanceof SearchRequestIndexed
				? observer.docs.index.search(request, { ...options, resolve: false })
				: observer.docs.index.search(request, { ...options, resolve: true }),
			"authenticated remote search",
		);
		expect(responses).to.have.length(1);
		expect(searched).to.have.length(1);
		// These are decoded wire requests, not a normalized/fabricated request.
		expect(searched[0].query).to.be.instanceOf(request.constructor);
		expect(searched[0].query.id).to.deep.equal(expectedId);
		expect(searched[0].from.equals(observerKey)).to.be.true;
		if (request instanceof SearchRequestIndexed) {
			expect((searched[0].query as SearchRequestIndexed).replicate).to.equal(
				expectedReplicationIntent,
			);
		}
		for (const call of read) {
			expect(call.from.equals(observerKey)).to.be.true;
		}
		expect(await observer.docs.index.getSize()).to.equal(0);
		expect(observer.docs.log.log.length).to.equal(0);
		return { rows, response: responses[0] };
	};

	for (const [name, create] of [
		["normal", () => new SearchRequest({ fetch: 2 })],
		["indexed", () => new SearchRequestIndexed({ fetch: 2, replicate: false })],
		[
			"indexed replication-intent",
			() => new SearchRequestIndexed({ fetch: 2, replicate: true }),
		],
		["iteration", () => new IterationRequest({ fetch: 2 })],
	] as const) {
		it(`denies an authenticated ${name} request before reading any row`, async () => {
			searchPolicy = () => false;
			const processQuery = sinon.spy(donor.docs.index, "processQuery");
			const request = create();
			const { rows, response } = await remoteSearch(request);
			expect(response).to.be.instanceOf(NoAccess);
			expect(rows).to.be.empty;
			expect(read).to.be.empty;
			expect(processQuery.called).to.be.false;
			expect((donor.docs.index as any)._resultQueue.has(request.idString)).to.be
				.false;
			expect(
				(donor.docs.index as any)._resumableIterators.has(request.idString),
			).to.be.false;
		});

		it(`allows an authenticated ${name} request`, async () => {
			const { rows, response } = await remoteSearch(create());
			expect(response).to.be.instanceOf(Results);
			expect(rows.map((row) => row.id).sort()).to.deep.equal([
				"fixture-a",
				"fixture-b",
			]);
			expect(read).to.have.length(2);
		});
	}

	for (const [name, create] of [
		["normal", () => new SearchRequest({ fetch: 2 })],
		["indexed", () => new SearchRequestIndexed({ fetch: 2 })],
	] as const) {
		it(`still filters ${name} results with canRead after canSearch allows`, async () => {
			readAllowed = false;
			const { rows, response } = await remoteSearch(create());
			expect(response).to.be.instanceOf(Results);
			expect(rows).to.be.empty;
			expect(read).to.have.length(2);
		});

		for (const kind of ["throw", "reject"] as const) {
			it(`preserves a ${kind} from canSearch for ${name} without processing the query`, async () => {
				const failure = new Error(`policy ${kind}`);
				searchPolicy = () => {
					if (kind === "throw") throw failure;
					return Promise.reject(failure);
				};
				const processQuery = sinon.spy(donor.docs.index, "processQuery");
				const request = create();
				// RPC logs callback errors without serializing them to the caller;
				// assert exact rejection at its real server boundary, not a timeout.
				let caught = false;
				try {
					await bounded(
						(donor.docs.index as any).handleSearchRequest(request, {
							from: observerKey,
						}),
						"reject authorization callback",
					);
				} catch (error) {
					caught = true;
					expect(error).to.equal(failure);
				}
				expect(caught).to.be.true;
				expect(searched).to.have.length(1);
				expect(searched[0].query).to.equal(request);
				expect(searched[0].from).to.equal(observerKey);
				expect(processQuery.called).to.be.false;
				expect(read).to.be.empty;
				expect((donor.docs.index as any)._resultQueue.has(request.idString)).to
					.be.false;
				expect(
					(donor.docs.index as any)._resumableIterators.has(request.idString),
				).to.be.false;
			});
		}
	}

	it("denies continuation but lets its owner close an existing iterator after denial", async () => {
		const index = donor.docs.index as any;
		const request = new IterationRequest({ fetch: 1 });
		const close = new CloseIteratorRequest({ id: request.id });
		try {
			const first = await bounded<Results<any>>(
				index.handleSearchRequest(request, { from: observerKey }),
				"start iterator",
			);
			expect(first.results).to.have.length(1);
			expect(index._resumableIterators.has(request.idString)).to.be.true;
			searchPolicy = () => false;
			const next = new CollectNextRequest({ id: request.id, amount: 1 });
			const denied = await bounded(
				index.handleSearchRequest(next, { from: observerKey }),
				"deny continuation",
			);
			expect(denied).to.be.instanceOf(NoAccess);
			expect(searched.at(-1)!.query).to.equal(next);
			expect(searched.at(-1)!.from).to.equal(observerKey);
			expect(read).to.have.length(1);
			const searchCount = searched.length;
			await bounded(
				index.handleSearchRequest(close, { from: donorKey }),
				"reject another identity's close",
			);
			expect(index._resumableIterators.has(request.idString)).to.be.true;
			await bounded(
				index.handleSearchRequest(close, { from: observerKey }),
				"owner closes denied iterator",
			);
			expect(searched).to.have.length(searchCount);
			expect(index._resultQueue.has(request.idString)).to.be.false;
			expect(index._resumableIterators.has(request.idString)).to.be.false;
		} finally {
			index.processCloseIteratorRequest(close, observerKey);
		}
	});

	it("types ordinary legacy callbacks and exhaustive current request handling", async () => {
		const legacy: (
			query: SearchRequest | IterationRequest | CollectNextRequest,
			from: PublicSignKey,
		) => boolean | Promise<boolean> = (query, from) =>
			Promise.resolve(query.id.length === 32 && from.equals(observerKey));
		const compatible: CanSearch = legacy;
		const exhaustive: CanSearch = (query) => {
			if (query instanceof SearchRequestIndexed) return query.replicate;
			if (
				query instanceof SearchRequest ||
				query instanceof IterationRequest ||
				query instanceof CollectNextRequest
			) {
				return true;
			}
			const unreachable: never = query;
			return unreachable;
		};
		const request = new SearchRequestIndexed({ replicate: true });
		expect(await compatible(request, observerKey)).to.be.true;
		expect(await exhaustive(request, observerKey)).to.be.true;
	});
});
