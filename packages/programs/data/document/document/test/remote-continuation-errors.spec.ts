import { deserialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	CollectNextRequest,
	Documents,
	ResultValue,
	Results,
	SearchRequest,
	Sort,
	SortDirection,
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

type Outcome = { value?: Document[]; error?: unknown };
const outcome = (promise: Promise<Document[]>): Promise<Outcome> =>
	promise.then(
		(value) => ({ value }),
		(error: unknown) => ({ error }),
	);
const expectFailure = (result: Outcome, sentinel: Error) => {
	expect(result.error).to.equal(sentinel);
	expect(result).not.to.have.property("value");
};

describe("remote continuation processing failures", () => {
	let session: TestSession | undefined;
	let donors: TestStore[];
	let observer: TestStore;
	let transform: ((document: Document) => Promise<Document>) | undefined;
	let requestSpy: sinon.SinonSpy | undefined;
	let cleanup: (() => Promise<void>) | undefined;
	const ids = [
		"continuation-a",
		"continuation-b",
		"continuation-c",
		"continuation-d",
	] as const;

	before(async () => {
		// One shared real network avoids duplicating setup. Single-donor cases
		// explicitly target only donors[0]; one case targets both full replicas.
		session = await bounded(TestSession.connected(3), "connect query peers");
		const descriptor = new TestStore({ docs: new Documents<Document>() });
		donors = [];
		for (const peer of session.peers.slice(0, 2)) {
			donors.push(
				await bounded(
					peer.open(descriptor.clone(), {
						args: {
							replicate: { factor: 1 },
							timeUntilRoleMaturity: 0,
							index: { prefetch: false, includeIndexed: false },
						},
					}),
					"open query donor",
				),
			);
		}
		observer = await bounded(
			session.peers[2].open(descriptor.clone(), {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
					index: {
						type: Document,
						transform: async (document) =>
							transform ? transform(document) : document,
						prefetch: false,
						includeIndexed: false,
						cache: { resolver: 0 },
					},
				},
			}),
			"open query observer",
		);
		for (const donor of donors) {
			await bounded(
				observer.docs.index.waitFor(donor.node.identity.publicKey, {
					timeout: 5_000,
				}),
				"donor query readiness",
			);
		}
		for (const id of ids)
			await bounded(
				donors[0].docs.put(new Document({ id })),
				"put query fixture",
			);
		await waitForResolved(
			async () => {
				expect(await donors[1].docs.index.getSize()).to.equal(ids.length);
			},
			{ timeout: 5_000 },
		);
		for (const donor of donors) {
			const local = donor.docs.index.iterate(
				{ sort: { key: "id", direction: SortDirection.ASC } },
				{ local: true, remote: false },
			);
			try {
				expect(
					(await bounded(local.all(), "verify full donor fixture")).map(
						(row) => row.id,
					),
				).to.deep.equal(ids);
			} finally {
				await bounded(local.close(), "close local verification");
			}
		}
		requestSpy = sinon.spy(observer.docs.index._query, "request");
	});

	afterEach(async () => {
		try {
			await cleanup?.();
		} finally {
			cleanup = undefined;
			transform = undefined;
			requestSpy?.resetHistory();
		}
	});
	after(async () => {
		requestSpy?.restore();
		if (session) await bounded(session.stop(), "stop query peers");
	});

	const prepare = async (
		properties: {
			strict?: boolean;
			fault?: "transform" | "onResponse" | "none";
			bothDonors?: boolean;
			signal?: AbortSignal;
		} = {},
	) => {
		const selected = properties.bothDonors ? donors : [donors[0]];
		const hashes = selected.map((donor) =>
			donor.node.identity.publicKey.hashcode(),
		);
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const healthy = pDefer<void>();
		const sentinel = new Error("later-page application callback failed");
		const fault = properties.fault ?? "transform";
		let armed = false;
		let injections = 0;
		let pending: Promise<Outcome> | undefined;
		let pendingSettled = false;
		const wire: { from: string; ids: string[] }[] = [];
		transform = async (document) => {
			if (armed && document.id === ids[1] && fault === "transform") {
				injections++;
				entered.resolve();
				await release.promise;
				throw sentinel;
			}
			if (armed && document.id === ids[2]) healthy.resolve();
			return document;
		};
		const request = new SearchRequest({
			query: [],
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});
		const iterator = observer.docs.index.iterate(request, {
			local: false,
			resolve: true,
			signal: properties.signal,
			remote: {
				from: hashes,
				...(properties.strict === undefined
					? {}
					: { throwOnMissing: properties.strict }),
				retryMissingResponses: false,
				timeout: 1_000,
				onResponse: (response, from) => {
					if (!armed) return;
					expect(response).to.be.instanceOf(Results);
					expect(from).not.to.equal(undefined);
					const received = (
						response as Results<ResultValue<Document>>
					).results.map((result) => {
						expect(result).to.be.instanceOf(ResultValue);
						// RPC invokes this hook before introduceEntries initializes the
						// value type. Decode the actual wire source, not result.value.
						return deserialize(result._source, Document).id;
					});
					wire.push({ from: from!.hashcode(), ids: received });
					if (fault === "onResponse") {
						injections++;
						throw sentinel;
					}
				},
			},
		});
		const assertRemoteClosed = async () => {
			await waitForResolved(
				async () => {
					for (const donor of selected) {
						expect(
							await donor.docs.index.getPending(request.idString),
						).to.equal(undefined);
						expect(donor.docs.index.countIteratorsInProgress).to.equal(0);
					}
				},
				{ timeout: 5_000 },
			);
		};
		cleanup = async () => {
			release.resolve();
			try {
				await bounded(iterator.close(), "close continuation iterator");
			} finally {
				if (pending) await bounded(pending, "settle pending continuation");
			}
			await assertRemoteClosed();
			expect(await observer.docs.index.getSize()).to.equal(0);
			expect(observer.docs.log.log.length).to.equal(0);
		};
		expect(
			(await bounded(iterator.next(1), "first query page")).map(
				(row) => row.id,
			),
		).to.deep.equal([ids[0]]);
		expect(iterator.done()).to.equal(false);
		for (const donor of selected) {
			expect(await donor.docs.index.getPending(request.idString)).to.equal(3);
		}
		armed = true;
		return {
			iterator,
			entered,
			release,
			healthy,
			sentinel,
			assertRemoteClosed,
			injections: () => injections,
			settled: () => pendingSettled,
			start: (amount: number | "all" = 1) => {
				pending = outcome(
					amount === "all" ? iterator.all() : iterator.next(amount),
				).then((result) => {
					pendingSettled = true;
					return result;
				});
				return pending;
			},
			assertWire: (expected: string[]) => {
				expect(wire).to.have.length(selected.length);
				expect(wire.map((response) => response.from).sort()).to.deep.equal(
					[...hashes].sort(),
				);
				for (const response of wire)
					expect(response.ids).to.deep.equal(expected);
				expect(
					requestSpy!
						.getCalls()
						.filter((call) => call.args[0] instanceof CollectNextRequest),
				).to.have.length(selected.length);
			},
		};
	};

	for (const strict of [true, false, undefined]) {
		it(`handles a failed received continuation with throwOnMissing=${String(strict)}`, async () => {
			const test = await prepare({ strict });
			const pending = test.start();
			await bounded(test.entered.promise, "public transform fault boundary");
			expect(test.injections()).to.equal(1);
			test.release.resolve();
			const result = await bounded(pending, "failed continuation result");
			test.assertWire([ids[1]]);
			if (strict) expectFailure(result, test.sentinel);
			else {
				// Compatibility control: best-effort currently suppresses this
				// local processing error. This does not certify completeness.
				expect(result).to.deep.equal({ value: [] });
			}
		});
	}

	it("returns the exact healthy second page", async () => {
		const test = await prepare({ strict: true, fault: "none" });
		const result = await bounded(test.start(), "healthy continuation");
		expect(result.error).to.equal(undefined);
		expect(result.value?.map((row) => row.id)).to.deep.equal([ids[1]]);
		expect(test.injections()).to.equal(0);
		test.assertWire([ids[1]]);
	});

	for (const strict of [false, true]) {
		it(`preserves public remote.onResponse errors with strict=${strict}`, async () => {
			const test = await prepare({ strict, fault: "onResponse" });
			expectFailure(
				await bounded(test.start(), "response callback rejection"),
				test.sentinel,
			);
			expect(test.injections()).to.equal(1);
			test.assertWire([ids[1]]);
		});
	}

	it("strict all() rejects the continuation error and closes its iterator", async () => {
		const test = await prepare({ strict: true });
		const pending = test.start("all");
		await bounded(test.entered.promise, "all transform boundary");
		expect(test.injections()).to.equal(1);
		test.release.resolve();
		expectFailure(
			await bounded(pending, "all rejection and cleanup"),
			test.sentinel,
		);
		expect(test.iterator.done()).to.equal(true);
		test.assertWire(ids.slice(1));
		await test.assertRemoteClosed();
	});

	it("strict queries reject instead of returning another donor's successful partial page", async () => {
		const test = await prepare({ strict: true, bothDonors: true });
		const pending = test.start(2);
		await bounded(test.entered.promise, "failing donor transform");
		// b is marked visited before its transform is awaited. The other
		// authenticated responder skips duplicate b and successfully transforms c.
		await bounded(test.healthy.promise, "other donor's successful transform");
		expect(test.injections()).to.equal(1);
		expect(test.settled()).to.equal(false);
		test.assertWire(ids.slice(1, 3));
		test.release.resolve();
		expectFailure(
			await bounded(pending, "strict two-donor rejection"),
			test.sentinel,
		);
	});

	for (const abortFirst of [false, true]) {
		it(`preserves a strict error after ${abortFirst ? "abort and explicit close" : "explicit close"} while its transform is gated`, async () => {
			const controller = new AbortController();
			const test = await prepare({ strict: true, signal: controller.signal });
			const pending = test.start();
			await bounded(test.entered.promise, "transform before cancellation");
			expect(test.injections()).to.equal(1);
			if (abortFirst) controller.abort();
			await bounded(test.iterator.close(), "explicit close");
			expect(test.iterator.done()).to.equal(true);
			await test.assertRemoteClosed();
			// Closing cancels protocol work, not arbitrary application promises.
			expect(test.settled()).to.equal(false);
			test.release.resolve();
			expectFailure(
				await bounded(pending, "released application rejection"),
				test.sentinel,
			);
			test.assertWire([ids[1]]);
		});
	}
});
