import type {
	IndexKeyScan,
	IndexKeyScanPage,
} from "@peerbit/indexer-interface";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import type { CoordinatePersistenceCoordinator } from "../src/coordinate-persistence.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("entry inventory authoritative coordinate keys", () => {
	let client: Peerbit | undefined;
	let store: EventStore<string, any>;
	let coordinator: CoordinatePersistenceCoordinator<"u32">;
	const probes = sinon.createSandbox();

	afterEach(async () => {
		probes.restore();
		await client?.stop();
		client = undefined;
	});

	const open = async (native = true) => {
		client = await Peerbit.create(
			native
				? createRustPeerbitOptions()
				: { indexer: () => new HashmapIndices() },
		);
		store = await client.open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				...(native ? {} : { nativeBackbone: false, nativeRangePlanner: false }),
			},
		});
		coordinator = (store.log as any)._coordinates;
		expect(coordinator.canUseNativeBackboneResidentCoordinateState()).to.equal(
			native,
		);
		const hashes: string[] = [];
		for (let i = 0; i < 3; i++) {
			hashes.push(
				(await store.add(`value-${i}`, { target: "none", meta: { next: [] } }))
					.entry.hash,
			);
		}
		return hashes;
	};

	const scan = (pageSize = 1, signal?: AbortSignal): IndexKeyScan => {
		const result = coordinator.scanAuthoritativeCoordinateKeys({
			pageSize,
			signal,
		});
		expect(result).to.exist;
		return result!;
	};

	for (const native of [false, true]) {
		it(`pages exact canonical keys without query iteration (${native ? "native resident" : "generic"})`, async () => {
			const hashes = await open(native);
			const query = probes
				.stub(store.log.entryCoordinatesIndex, "iterate")
				.throws(new Error("not a raw key scan"));
			const inventory = scan(2);
			try {
				const first = await inventory.next();
				const second = await inventory.next();
				expect(first.status).to.equal("more");
				expect(first.keys).to.have.length(2);
				expect(second.status).to.equal("complete");
				expect(second.keys).to.have.length(1);
				expect([...first.keys, ...second.keys].sort()).to.deep.equal(
					hashes.sort(),
				);
				expect(
					[...first.keys, ...second.keys].every(
						(key) => typeof key === "string",
					),
				).to.equal(true);
				expect(query.called).to.equal(false);
				query.restore();
				await store.add("after-complete", {
					target: "none",
					meta: { next: [] },
				});
				expect(await inventory.next()).to.deep.equal({
					status: "complete",
					keys: [],
				});
			} finally {
				await inventory.close();
			}
		});

		it(`invalidates at lower admission and stays busy through outer finalization (${native ? "native resident" : "generic"})`, async () => {
			await open(native);
			const inventory = scan();
			expect((await inventory.next()).status).to.equal("more");
			const index = store.log.log.entryIndex;
			const owner = await index.acquireHashMutationLocks(["no-op"]);
			let during: IndexKeyScan | undefined;
			try {
				// A finished inner transaction does not finish the enclosing lower
				// owner; durable journal/intent completion may still be pending.
				const transaction = (
					index as any
				).beginNativeCommittedAppendFactsTransaction([], owner);
				(index as any).acknowledgeNativeCommittedAppendFacts(transaction);
				during = scan();
				expect(await during.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				expect(await inventory.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
			} finally {
				index.releaseHashMutationLocks(owner);
				await inventory.close();
				await during?.close();
			}
			expect(await during!.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			const fresh = scan(3);
			try {
				expect((await fresh.next()).status).to.equal("complete");
			} finally {
				await fresh.close();
			}
		});
	}

	it("uses resident presence without a persistence journal or generic fallback", async () => {
		const hashes = await open();
		expect(coordinator._nativeBackboneCoordinatePersistence).to.equal(
			undefined,
		);
		probes
			.stub(store.log.entryCoordinatesIndex, "get")
			.throws(new Error("generic presence is not authoritative"));
		probes
			.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives")
			.value(() => {
				throw new Error("generic inventory is not authoritative");
			});
		for (const hash of hashes) {
			expect(
				(await coordinator.getAuthoritativeCoordinateEntryForInventory(hash))
					?.hash,
			).to.equal(hash);
		}
		expect(
			await coordinator.getAuthoritativeCoordinateEntryForInventory("absent"),
		).to.equal(undefined);
		const inventory = scan(3);
		try {
			expect((await inventory.next()).keys.sort()).to.deep.equal(hashes.sort());
		} finally {
			await inventory.close();
		}
	});

	it("invalidates a same-size resident replacement made through coordinate persistence", async () => {
		const hashes = await open();
		const inventory = scan();
		expect((await inventory.next()).status).to.equal("more");
		const entry = await store.log.log.get(hashes[0]!);
		expect(entry).to.exist;
		const size = coordinator._residentEntryCoordinatesByHash!.size;
		await coordinator.persistCoordinate({
			entry: entry!,
			coordinates: await (store.log as any).createCoordinates(entry, 1),
			leaders: false,
			replicas: 1,
		});
		expect(coordinator._residentEntryCoordinatesByHash!.size).to.equal(size);
		expect(await inventory.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		await inventory.close();
	});

	it("invalidates resident map replacement instead of reading the detached map", async () => {
		await open();
		const inventory = scan();
		const original = coordinator._residentEntryCoordinatesByHash!;
		try {
			coordinator._residentEntryCoordinatesByHash = new Map(original);
			expect(await inventory.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
		} finally {
			coordinator._residentEntryCoordinatesByHash = original;
			await inventory.close();
		}
	});

	it("keeps a missing generic capability unsupported without a query fallback", async () => {
		await open(false);
		probes
			.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives")
			.value(undefined);
		const query = probes
			.stub(store.log.entryCoordinatesIndex, "iterate")
			.throws(new Error("no fallback"));
		expect(
			coordinator.scanAuthoritativeCoordinateKeys({ pageSize: 2 }),
		).to.equal(undefined);
		expect(query.called).to.equal(false);
	});

	it("serializes overlapping delegated next calls and releases the cursor once", async () => {
		const hashes = await open(false);
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const next = probes.stub();
		next.onFirstCall().callsFake(async (): Promise<IndexKeyScanPage> => {
			entered.resolve();
			await release.promise;
			return { status: "more", keys: [hashes[0]!] };
		});
		next.onSecondCall().resolves({ status: "complete", keys: hashes.slice(1) });
		const close = probes.stub().resolves();
		probes
			.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives")
			.returns({ next, close });
		const inventory = scan(2);
		const first = inventory.next();
		const second = inventory.next();
		try {
			await entered.promise;
			expect(next.callCount).to.equal(1);
		} finally {
			release.resolve();
			await Promise.all([first, second]);
			await inventory.close();
		}
		expect((await first).status).to.equal("more");
		expect((await second).status).to.equal("complete");
		expect(close.callCount).to.equal(1);
		expect(await inventory.next()).to.deep.equal({
			status: "complete",
			keys: [],
		});
	});

	it("releases a delegate that aborts during cursor construction", async () => {
		await open(false);
		const controller = new AbortController();
		const close = probes.stub().resolves();
		const next = probes
			.stub()
			.throws(new Error("aborted cursor must not advance"));
		const remove = probes.spy(controller.signal, "removeEventListener");
		probes
			.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives")
			.callsFake(() => {
				controller.abort();
				return { next, close };
			});
		const inventory = scan(1, controller.signal);
		// Cleanup is scheduled by construction, without needing next or close.
		await Promise.resolve();
		expect(close.callCount).to.equal(1);
		expect(remove.calledOnce).to.equal(true);
		expect(await inventory.next()).to.deep.equal({
			status: "aborted",
			keys: [],
		});
		await inventory.close();
		expect(close.callCount).to.equal(1);
		expect(next.called).to.equal(false);
	});

	it("discards a delegated page when mutation enters during the read", async () => {
		const hashes = await open(false);
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const close = probes.stub().resolves();
		probes.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives").returns({
			next: async () => {
				entered.resolve();
				await release.promise;
				return { status: "complete", keys: hashes };
			},
			close,
		});
		const inventory = scan(3);
		const next = inventory.next();
		try {
			await entered.promise;
			const index = store.log.log.entryIndex;
			const owner = await index.acquireHashMutationLocks(["no-op"]);
			index.releaseHashMutationLocks(owner);
		} finally {
			release.resolve();
		}
		expect(await next).to.deep.equal({ status: "invalidated", keys: [] });
		await inventory.close();
		expect(close.callCount).to.equal(1);
	});

	it("preserves a read error over cleanup error and then remains failed", async () => {
		await open(false);
		const failure = new Error("read failed");
		const close = probes.stub().rejects(new Error("cleanup failed"));
		probes.stub(store.log.entryCoordinatesIndex, "scanKeyPrimitives").returns({
			next: async () => {
				throw failure;
			},
			close,
		});
		const inventory = scan();
		expect(
			await Promise.resolve(inventory.next()).catch((error) => error),
		).to.equal(failure);
		expect(await inventory.next()).to.deep.equal({
			status: "failed",
			keys: [],
		});
		await inventory.close();
		expect(close.callCount).to.equal(1);
	});

	it("releases abort listeners and keeps abort and close terminal", async () => {
		await open();
		const controller = new AbortController();
		const remove = probes.spy(controller.signal, "removeEventListener");
		const aborted = scan(1, controller.signal);
		controller.abort();
		expect(await aborted.next()).to.deep.equal({ status: "aborted", keys: [] });
		await aborted.close();
		expect(remove.calledOnce).to.equal(true);
		expect(await aborted.next()).to.deep.equal({ status: "aborted", keys: [] });
		const closed = scan();
		await store.close();
		expect(await closed.next()).to.deep.equal({ status: "closed", keys: [] });
		expect(() => scan()).to.throw();
		await closed.close();
	});

	it("fails closed on lower poison even before the ownership lifecycle aborts", async () => {
		await open();
		const inventory = scan();
		const index = store.log.log.entryIndex;
		const failure = new Error("retained durable intent");
		index.poisonNativeDurableTransactionMutations(failure);
		try {
			expect(
				await Promise.resolve(inventory.next()).catch((error) => error),
			).to.have.property("cause", failure);
			expect(await inventory.next()).to.deep.equal({
				status: "failed",
				keys: [],
			});
		} finally {
			index.clearNativeDurableTransactionMutationFailure();
			await inventory.close();
		}
	});
});
