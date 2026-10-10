import { type PublicSignKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import {
	type AbstractSearchResult,
	AccessDeniedError,
	CollectNextRequest,
	Documents,
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
