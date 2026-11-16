import { deserialize, serialize } from "@dao-xyz/borsh";
import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { MissingResponsesError } from "@peerbit/rpc";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	type AbstractSearchResult,
	AccessDeniedError,
	CollectNextRequest,
	Documents,
	IterationRequest,
	NoAccess,
	Results,
	SearchRequest,
	Sort,
	SortDirection,
	StringMatch,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("remote query denial", () => {
	let session: TestSession | undefined;
	let donors: TestStore[];
	let observer: TestStore;
	const denied = new Set<number>();
	const cursors = new Set<string>();
	const wire: { peer: string; denied: boolean }[] = [];
	const requests: { peer: number; next: boolean }[] = [];
	let beforeDeny: (() => Promise<void>) | undefined;
	let onResponse:
		| ((response: AbstractSearchResult, from: PublicSignKey) => void)
		| undefined;
	let release: (() => void) | undefined;
	let closeIterator: (() => Promise<void>) | undefined;
	const ids = ["denial-a", "denial-b", "denial-c"];

	before(async () => {
		session = await TestSession.connected(3);
		const descriptor = new TestStore({ docs: new Documents<Document>() });
		donors = [];
		for (const [i, peer] of session.peers.slice(0, 2).entries()) {
			donors.push(
				await peer.open(descriptor.clone(), {
					args: {
						replicate: { factor: 1 },
						timeUntilRoleMaturity: 0,
						index: {
							prefetch: false,
							canSearch: async (request, from) => {
								expect(from.equals(session!.peers[2].identity.publicKey)).to.be
									.true;
								cursors.add(request.idString);
								requests.push({
									peer: i,
									next: request instanceof CollectNextRequest,
								});
								if (!denied.has(i)) return true;
								await beforeDeny?.();
								return false;
							},
						},
					},
				}),
			);
		}
		observer = await session.peers[2].open(descriptor.clone(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				index: { prefetch: false, cache: { resolver: 0 } },
			},
		});
		for (const donor of donors) {
			await observer.docs.index.waitFor(donor.node.identity.publicKey, {
				timeout: 5_000,
			});
		}
		for (const id of ids) await donors[0].docs.put(new Document({ id }));
		await waitForResolved(
			async () => {
				expect(await donors[1].docs.index.getSize()).to.equal(ids.length);
			},
			{ timeout: 5_000 },
		);
	});

	afterEach(async () => {
		release?.();
		try {
			await closeIterator?.();
			await waitForResolved(
				async () => {
					for (const donor of donors) {
						for (const id of cursors) {
							expect(await donor.docs.index.getPending(id)).to.equal(undefined);
						}
						expect(donor.docs.index.countIteratorsInProgress).to.equal(0);
					}
				},
				{ timeout: 5_000 },
			);
			expect(await observer.docs.index.getSize()).to.equal(0);
			expect(observer.docs.log.log.length).to.equal(0);
		} finally {
			closeIterator = release = beforeDeny = onResponse = undefined;
			denied.clear();
			cursors.clear();
			wire.length = requests.length = 0;
		}
	});
	after(async () => {
		release?.();
		await session?.stop();
	});

	const peerHash = (i: number) => donors[i].node.identity.publicKey.hashcode();
	const query = () =>
		new SearchRequest({
			query: [],
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});
	const options = (strict: boolean | undefined, both = false) => ({
		local: false,
		remote: {
			from: (both ? [0, 1] : [0]).map(peerHash),
			...(strict === undefined ? {} : { throwOnMissing: strict }),
			retryMissingResponses: false,
			timeout: 1_000,
			onResponse: (response: AbstractSearchResult, from?: PublicSignKey) => {
				expect(from).not.to.equal(undefined);
				wire.push({
					peer: from!.hashcode(),
					denied: response instanceof NoAccess,
				});
				onResponse?.(response, from!);
			},
		},
	});
	const expectDenied = async (pending: Promise<unknown>) => {
		const result = await pending.then(
			(value) => ({ value, error: undefined }),
			(error: unknown) => ({ error }),
		);
		expect(result.error).to.be.instanceOf(Error);
		expect(result.error).to.be.instanceOf(AccessDeniedError);
		expect(result.error).to.have.property("name", "AccessDeniedError");
		expect(result.error)
			.to.have.property("peers")
			.that.deep.equals([peerHash(0)]);
		expect(result).not.to.have.property("value");
		expect(wire).to.deep.include({ peer: peerHash(0), denied: true });
	};

	for (const method of ["search", "next", "all"] as const) {
		it(`rejects strict ${method} on a real first-page denial`, async () => {
			denied.add(0);
			if (method === "search") {
				await expectDenied(observer.docs.index.search(query(), options(true)));
			} else {
				const iterator = observer.docs.index.iterate(query(), options(true));
				closeIterator = () => iterator.close();
				await expectDenied(
					method === "next" ? iterator.next(1) : iterator.all(),
				);
			}
		});
	}

	for (const strict of [true, false, undefined]) {
		it(`handles permission revoked before CollectNext with strict=${strict}`, async () => {
			const iterator = observer.docs.index.iterate(query(), options(strict));
			closeIterator = () => iterator.close();
			expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
				ids[0],
			]);
			denied.add(0);
			if (strict) await expectDenied(iterator.next(1));
			else expect(await iterator.next(1)).to.deep.equal([]);
			expect(requests).to.deep.include({ peer: 0, next: true });
			expect(wire).to.deep.include({ peer: peerHash(0), denied: true });
		});
	}

	for (const strict of [false, undefined]) {
		it(`preserves best-effort first-page denial with strict=${strict}`, async () => {
			denied.add(0);
			expect(
				await observer.docs.index.search(query(), options(strict)),
			).to.deep.equal([]);
			expect(wire).to.deep.equal([{ peer: peerHash(0), denied: true }]);
		});
	}

	it("does not hide a strict denial behind an earlier healthy peer response", async () => {
		const healthy = pDefer<void>();
		release = () => healthy.resolve();
		beforeDeny = () => healthy.promise;
		onResponse = (response, from) => {
			if (from.hashcode() === peerHash(1)) {
				expect(response).to.be.instanceOf(Results);
				expect((response as Results<any>).results).to.have.length(ids.length);
				healthy.resolve();
			}
		};
		denied.add(0);
		await expectDenied(
			observer.docs.index.search(query(), options(true, true)),
		);
		expect(wire.map((event) => event.peer)).to.deep.equal([
			peerHash(1),
			peerHash(0),
		]);
	});

	it("allows an authenticated empty result in strict mode", async () => {
		const request = new SearchRequest({
			query: [new StringMatch({ key: "id", value: "absent" })],
		});
		expect(
			await observer.docs.index.search(request, options(true)),
		).to.deep.equal([]);
		expect(wire).to.deep.equal([{ peer: peerHash(0), denied: false }]);
	});

	it("preserves the original public response callback error", async () => {
		denied.add(0);
		const sentinel = new Error("response observer failed");
		onResponse = () => {
			throw sentinel;
		};
		const result = await observer.docs.index
			.search(query(), options(true))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(result).to.equal(sentinel);
	});
});

describe("remote query denial from joining replicators", () => {
	// Join-triggered queries run detached from the caller's read. Their
	// failures must reach that read, not escape as unhandled rejections.
	let session: TestSession | undefined;
	let release: (() => void) | undefined;
	const unhandled: unknown[] = [];
	const onUnhandled = (reason: unknown) => {
		unhandled.push(reason);
	};
	const ids = ["join-a", "join-b", "join-c", "join-d"];

	beforeEach(() => {
		process.on("unhandledRejection", onUnhandled);
	});
	afterEach(async () => {
		release?.();
		try {
			await session?.stop();
		} finally {
			process.off("unhandledRejection", onUnhandled);
			session = release = undefined;
			unhandled.length = 0;
		}
	});

	const bounded = async <T>(promise: Promise<T>, label: string): Promise<T> => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				promise,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error(`${label} timed out`)),
						30_000,
					);
				}),
			]);
		} finally {
			clearTimeout(timer);
		}
	};
	const settle = <T>(
		promise: Promise<T>,
	): Promise<{ value?: T; error?: unknown }> =>
		promise.then(
			(value) => ({ value }),
			(error: unknown) => ({ error }),
		);
	const expectDeniedBy = (
		result: { value?: unknown; error?: unknown },
		peer: string,
	) => {
		expect(result.error).to.be.instanceOf(AccessDeniedError);
		expect(result.error).to.have.property("peers").that.deep.equals([peer]);
		expect(result).not.to.have.property("value");
	};
	const query = () =>
		new SearchRequest({
			query: [],
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});

	// Rows that only the joining replicator holds, for answer "serve".
	const joinerIds = ["join-e", "join-f", "join-g"];

	// peers[0] answers and is connected to the reader peers[2] from the start.
	// peers[1] replicates too, but joins the reader only when `join()` is called.
	// It denies, never answers, or serves its own rows until `revoke()`.
	const lateJoin = async (answer: "deny" | "hang" | "serve") => {
		session = await TestSession.disconnected(3);
		await session.connect([[session.peers[0], session.peers[2]]]);
		const descriptor = new TestStore({ docs: new Documents<Document>() });
		const healthy = await session.peers[0].open(descriptor, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				index: { prefetch: false },
			},
		});
		for (const id of ids) await healthy.docs.put(new Document({ id }));
		const hung = pDefer<boolean>();
		release = () => hung.resolve(false);
		let revoked = false;
		const joiner = await session.peers[1].open(descriptor.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				index: {
					prefetch: false,
					canSearch: () =>
						answer === "hang" ? hung.promise : answer === "serve" && !revoked,
				},
			},
		});
		if (answer === "serve") {
			for (const id of joinerIds) await joiner.docs.put(new Document({ id }));
		}
		const reader = await session.peers[2].open(descriptor.clone(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				index: { prefetch: false },
			},
		});
		await reader.docs.index.waitFor(healthy.node.identity.publicKey, {
			timeout: 10_000,
		});
		const healthyHash = healthy.node.identity.publicKey.hashcode();
		const joinerHash = joiner.node.identity.publicKey.hashcode();
		const denied = pDefer<void>();
		const iterate = (
			strict: boolean | undefined,
			waitMs = 60_000,
			scope: { from?: string[]; timeout?: number } = {},
		) =>
			reader.docs.index.iterate(query(), {
				remote: {
					...(strict === undefined ? {} : { throwOnMissing: strict }),
					...(scope.from ? { from: scope.from } : {}),
					timeout: scope.timeout ?? 5_000,
					wait: { timeout: waitMs, behavior: "keep-open" },
					reach: { eager: true },
					onResponse: (response, from) => {
						if (response instanceof NoAccess && from?.hashcode() === joinerHash)
							denied.resolve();
					},
				},
			});
		return {
			reader,
			healthyHash,
			joinerHash,
			iterate,
			// Resolves once the reader has received the joining peer's NoAccess.
			denied: () => bounded(denied.promise, "joining replicator denial"),
			join: () => session!.connect([[session!.peers[1], session!.peers[2]]]),
			revoke: () => {
				revoked = true;
			},
		};
	};

	for (const strict of [true, false, undefined]) {
		it(`handles a keep-open iterator's joining replicator denial with strict=${strict}`, async () => {
			const test = await lateJoin("deny");
			const iterator = test.iterate(strict);
			try {
				expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
					ids[0],
				]);
				await test.join();
				await test.denied();
				if (strict) {
					const result = await settle(iterator.next(10));
					expectDeniedBy(result, test.joinerHash);
					// The failure stays until close() instead of yielding the
					// healthy peer's remaining rows.
					expect(iterator.done()).to.equal(false);
					expect((await settle(iterator.next(10))).error).to.equal(
						result.error,
					);
				} else {
					expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal(
						ids.slice(1),
					);
				}
				expect(unhandled).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		});
	}

	it("wakes a waiting strict all() when a joining replicator denies", async () => {
		const test = await lateJoin("deny");
		const iterator = test.iterate(true);
		try {
			expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
				ids[0],
			]);
			const drained = settle(iterator.all());
			await test.join();
			expectDeniedBy(
				await bounded(drained, "strict all() after a joining denial"),
				test.joinerHash,
			);
			expect(unhandled).to.deep.equal([]);
		} finally {
			await iterator.close();
		}
	});

	it("does not read as done after the wait window while a strict join failure is pending", async () => {
		const waitMs = 8_000;
		const test = await lateJoin("deny");
		const opened = Date.now();
		const iterator = test.iterate(true, waitMs);
		try {
			// All rows are read, so the iterator would end with its wait window.
			expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal(ids);
			await test.join();
			await test.denied();
			await delay(Math.max(0, opened + waitMs + 500 - Date.now()));
			expect(iterator.done()).to.equal(false);
			expectDeniedBy(await settle(iterator.next(1)), test.joinerHash);
			expect(unhandled).to.deep.equal([]);
		} finally {
			await iterator.close();
		}
	});

	it("rejects the next strict read after pending() finds a denying replicator", async () => {
		const test = await lateJoin("deny");
		// Without join events, pending() is what finds the new replicator
		// (the discovery path that covers missed joins).
		(test.reader.docs.index as any).createReplicatorJoinListener =
			() => () => {};
		const iterator = test.iterate(true);
		try {
			expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal(ids);
			await test.join();
			await waitForResolved(
				async () =>
					expect([...(await test.reader.docs.log.getReplicators())]).to.include(
						test.joinerHash,
					),
				{ timeout: 10_000 },
			);
			expect(await iterator.pending()).to.equal(0);
			await test.denied();
			expectDeniedBy(await settle(iterator.next(1)), test.joinerHash);
			expect(unhandled).to.deep.equal([]);
		} finally {
			await iterator.close();
		}
	});

	it("reports a joining replicator that never answers to a strict all()", async () => {
		const test = await lateJoin("hang");
		const iterator = test.iterate(true);
		try {
			expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
				ids[0],
			]);
			const drained = settle(iterator.all());
			await test.join();
			const result = await bounded(drained, "strict all() after a silent join");
			expect(result.error).to.be.instanceOf(MissingResponsesError);
			expect(result).not.to.have.property("value");
			expect(unhandled).to.deep.equal([]);
		} finally {
			await iterator.close();
		}
	});

	it("rejects a strict get(waitFor) when the joining replicator denies", async () => {
		session = await TestSession.disconnected(2);
		const descriptor = new TestStore({ docs: new Documents<Document>() });
		const joiner = await session.peers[0].open(descriptor, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				index: { prefetch: false, canSearch: () => false },
			},
		});
		await joiner.docs.put(new Document({ id: ids[0] }));
		const reader = await session.peers[1].open(descriptor.clone(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				index: { prefetch: false },
			},
		});
		const pending = settle(
			reader.docs.index.get(ids[0], {
				waitFor: 60_000,
				remote: { throwOnMissing: true, timeout: 5_000 },
			}),
		);
		await session.connect([[session.peers[0], session.peers[1]]]);
		expectDeniedBy(
			await bounded(pending, "strict get(waitFor) after a joining denial"),
			joiner.node.identity.publicKey.hashcode(),
		);
		expect(unhandled).to.deep.equal([]);
	});

	it("rejects a strict get(waitFor) whose wait saw a joining denial before the initial result", async () => {
		session = await TestSession.disconnected(3);
		await session.connect([[session.peers[0], session.peers[2]]]);
		const descriptor = new TestStore({ docs: new Documents<Document>() });
		const healthy = await session.peers[0].open(descriptor, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				index: { prefetch: false },
			},
		});
		await healthy.docs.put(new Document({ id: ids[0] }));
		const joiner = await session.peers[1].open(descriptor.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				index: { prefetch: false, canSearch: () => false },
			},
		});
		const reader = await session.peers[2].open(descriptor.clone(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				index: { prefetch: false },
			},
		});
		await reader.docs.index.waitFor(healthy.node.identity.publicKey, {
			timeout: 10_000,
		});
		const joinerHash = joiner.node.identity.publicKey.hashcode();
		// Hold the initial query before it asks the healthy peer, so the
		// joining peer's denial is processed first.
		const entered = pDefer<void>();
		const held = pDefer<void>();
		release = () => held.resolve();
		reader.docs.log.getCover = async () => {
			entered.resolve();
			await held.promise;
			return [healthy.node.identity.publicKey.hashcode()];
		};
		const denied = pDefer<void>();
		const pending = settle(
			reader.docs.index.get(ids[0], {
				waitFor: 60_000,
				remote: {
					throwOnMissing: true,
					timeout: 5_000,
					onResponse: (response, from) => {
						if (response instanceof NoAccess && from?.hashcode() === joinerHash)
							denied.resolve();
					},
				},
			}),
		);
		await bounded(entered.promise, "initial query cover");
		await session.connect([[session.peers[1], session.peers[2]]]);
		await bounded(denied.promise, "joining replicator denial");
		held.resolve();
		// The healthy peer's document does not turn the observed denial into
		// a result.
		expectDeniedBy(
			await bounded(pending, "strict get(waitFor) after the initial result"),
			joinerHash,
		);
		expect(unhandled).to.deep.equal([]);
	});

	// A join still queries a replicator outside remote.from, as without the
	// option, but only the peers the caller asked for can fail a strict read.
	describe("outside remote.from", () => {
		it("does not reject a strict keep-open iterator when the joining replicator denies", async () => {
			const test = await lateJoin("deny");
			const iterator = test.iterate(true, 60_000, { from: [test.healthyHash] });
			try {
				expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
					ids[0],
				]);
				await test.join();
				await test.denied();
				expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal(
					ids.slice(1),
				);
				expect(unhandled).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		});

		it("does not reject a strict get(waitFor) when the joining replicator denies", async () => {
			const test = await lateJoin("deny");
			const denied = pDefer<void>();
			const pending = settle(
				test.reader.docs.index.get("join-absent", {
					waitFor: 8_000,
					remote: {
						throwOnMissing: true,
						from: [test.healthyHash],
						timeout: 5_000,
						onResponse: (response, from) => {
							if (
								response instanceof NoAccess &&
								from?.hashcode() === test.joinerHash
							)
								denied.resolve();
						},
					},
				}),
			);
			await test.join();
			await bounded(denied.promise, "joining replicator denial");
			expect(
				await bounded(pending, "strict get(waitFor) with an excluded joiner"),
			).to.deep.equal({ value: undefined });
			expect(unhandled).to.deep.equal([]);
		});

		it("retries a silent joining replicator best-effort in a strict keep-open iterator", async () => {
			const test = await lateJoin("hang");
			const rpc = test.reader.docs.index._query;
			const request = rpc.request.bind(rpc);
			const askedJoiner = pDefer<{ response: Promise<unknown> }>();
			rpc.request = ((...args: Parameters<typeof request>) => {
				const response = request(...args);
				const to = (args[1] as { mode?: { to?: string[] } } | undefined)?.mode
					?.to;
				if (to?.includes(test.joinerHash)) askedJoiner.resolve({ response });
				return response;
			}) as typeof rpc.request;
			const iterator = test.iterate(true, 60_000, {
				from: [test.healthyHash],
				timeout: 2_000,
			});
			try {
				expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
					ids[0],
				]);
				await test.join();
				// Let the join fetch time out first, so the next reads retry the
				// joining replicator before they collect the healthy peer's rows.
				const asked = await bounded(askedJoiner.promise, "join fetch");
				await Promise.allSettled([asked.response]);
				const rows: string[] = [];
				for (let read = 0; read < 6 && rows.length < 3; read++) {
					rows.push(...(await iterator.next(10)).map((row) => row.id));
				}
				expect(rows).to.deep.equal(ids.slice(1));
				expect(unhandled).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		});

		it("does not reject a strict keep-open iterator when the joining replicator denies a later page", async () => {
			const test = await lateJoin("serve");
			const iterator = test.iterate(true, 60_000, { from: [test.healthyHash] });
			try {
				expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
					ids[0],
				]);
				await test.join();
				// The join fetch buffers the joining replicator's first row and
				// leaves two more pending there, next to three on the healthy peer.
				await waitForResolved(
					async () => expect(await iterator.pending()).to.equal(6),
					{ timeout: 20_000 },
				);
				test.revoke();
				expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal([
					...ids.slice(1),
					joinerIds[0],
				]);
				await test.denied();
				expect(unhandled).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		});
	});

	// A replicator can push a predicted page when the reader joins it; a
	// push-update iterator merges matching predictions outside any read.
	for (const [strict, scoped] of [
		[true, false],
		[true, true],
		[false, false],
	] as const) {
		it(`handles a predicted denial pushed to a push-update iterator with strict=${strict}${scoped ? " and the predicting peer outside remote.from" : ""}`, async () => {
			session = await TestSession.disconnected(3);
			await session.connect([[session.peers[0], session.peers[2]]]);
			const descriptor = new TestStore({ docs: new Documents<Document>() });
			const healthy = await session.peers[0].open(descriptor, {
				args: {
					replicate: { factor: 1 },
					timeUntilRoleMaturity: 0,
					index: { prefetch: false },
				},
			});
			for (const id of ids) await healthy.docs.put(new Document({ id }));
			// Predicts the reader's own query when the reader joins, but denies it.
			let predicted: IterationRequest | undefined;
			const joiner = await session.peers[1].open(descriptor.clone(), {
				args: {
					replicate: { factor: 1 },
					timeUntilRoleMaturity: 0,
					index: {
						canSearch: () => false,
						prefetch: {
							predictor: {
								onRequest: () => ({ ignore: false }),
								predictedQuery: () => {
									if (!predicted) return undefined;
									const request = deserialize(
										serialize(predicted),
										IterationRequest,
									);
									request.id = randomBytes(32);
									return request;
								},
							},
						},
					},
				},
			});
			const reader = await session.peers[2].open(descriptor.clone(), {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
					index: { prefetch: false },
				},
			});
			await reader.docs.index.waitFor(healthy.node.identity.publicKey, {
				timeout: 10_000,
			});
			const joinerHash = joiner.node.identity.publicKey.hashcode();
			const sent = sinon.spy(reader.docs.index._query, "request");
			const iterator = reader.docs.index.iterate(query(), {
				remote: {
					throwOnMissing: strict,
					timeout: 5_000,
					...(scoped
						? { from: [healthy.node.identity.publicKey.hashcode()] }
						: {}),
				},
				updates: "remote",
			});
			try {
				expect((await iterator.next(1)).map((row) => row.id)).to.deep.equal([
					ids[0],
				]);
				predicted = sent
					.getCalls()
					.map((call) => call.args[0])
					.find((request) => request instanceof IterationRequest);
				expect(predicted).to.be.instanceOf(IterationRequest);
				const pushed = pDefer<void>();
				reader.docs.index.prefetch!.accumulator.addEventListener(
					"add",
					(event) => {
						const { consumable } = event.detail;
						if (
							consumable.from?.hashcode() === joinerHash &&
							consumable.response.results instanceof NoAccess
						)
							pushed.resolve();
					},
				);
				await session.connect([[session.peers[1], session.peers[2]]]);
				await bounded(pushed.promise, "predicted denial");
				if (strict && !scoped) {
					const result = await settle(iterator.next(10));
					expectDeniedBy(result, joinerHash);
					expect(iterator.done()).to.equal(false);
					expect((await settle(iterator.next(10))).error).to.equal(
						result.error,
					);
				} else {
					expect((await iterator.next(10)).map((row) => row.id)).to.deep.equal(
						ids.slice(1),
					);
				}
				expect(unhandled).to.deep.equal([]);
			} finally {
				sent.restore();
				await iterator.close();
			}
		});
	}
});
