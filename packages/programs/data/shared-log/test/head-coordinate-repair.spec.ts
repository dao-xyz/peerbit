import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { AbsoluteReplicas, decodeReplicas } from "../src/replication.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("current head coordinate repair", () => {
	let session: TestSession | undefined;
	let releases: (() => void)[];
	let pending: Promise<unknown>[];

	beforeEach(() => {
		releases = [];
		pending = [];
	});
	afterEach(async () => {
		try {
			for (const release of releases) release();
			await Promise.all(pending);
		} finally {
			sinon.restore();
			await session?.stop();
			session = undefined;
		}
	});
	const track = <T>(operation: Promise<T>) => {
		pending.push(
			operation.then(
				() => undefined,
				() => undefined,
			),
		);
		return operation;
	};
	const open = async (nativeHeads: boolean) => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				nativeGraph: nativeHeads,
				nativeBackbone: nativeHeads ? { optional: false } : false,
				nativeRangePlanner: false,
			},
		});
		const shared = store.log as any;
		const index = store.log.log.entryIndex;
		expect(index.properties.nativeGraph?.useHeads === true).to.equal(
			nativeHeads,
		);
		return { store, shared, index };
	};
	const coordinate = (shared: any, hash: string) =>
		shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(hash);
	const coldEntries = async (index: any) => {
		await index.flushPendingWrites();
		index.cache.clear();
		expect(index.cache.map.size).to.equal(0);
	};
	const observeBlockReads = (shared: any) => {
		// Probe the actual local block adapter, below EntryIndex's resolve cache.
		const blocks = shared.remoteBlocks.localStore;
		const get = sinon.spy(blocks, "get");
		const getMany = sinon.spy(blocks, "getMany");
		return {
			get,
			getMany,
			hashes: () =>
				new Set<string>([
					...get.getCalls().map((call) => call.args[0]),
					...getMany.getCalls().flatMap((call) => call.args[0]),
				]),
		};
	};
	const persistStaleCoordinate = async (shared: any, entry: any) => {
		const plan = await shared.planEntryLeaders(entry, 1, { persist: false });
		await shared._coordinates.persistCoordinate(
			{
				entry,
				coordinates: plan.coordinates,
				leaders: plan.leaders,
				replicas: plan.coordinates.length,
				assignedToRangeBoundary: plan.assignedToRangeBoundary,
			},
			shared.captureReplicationOwnershipLifecycle(),
		);
		expect(await coordinate(shared, entry.hash)).to.have.property(
			"hash",
			entry.hash,
		);
	};

	for (const nativeHeads of [false, true]) {
		const mode = nativeHeads ? "native heads" : "lower-index heads";
		it(`does not read blocks when every current head is indexed (${mode})`, async () => {
			const { store, shared, index } = await open(nativeHeads);
			const entries = [];
			for (const value of ["first", "second", "third"]) {
				entries.push(
					(await store.add(value, { target: "none", meta: { next: [] } }))
						.entry,
				);
			}
			for (const entry of entries) {
				expect(await coordinate(shared, entry.hash)).to.have.property(
					"hash",
					entry.hash,
				);
			}
			await coldEntries(index);
			const reads = observeBlockReads(shared);
			await shared.ensureCurrentHeadCoordinatesIndexed();
			expect(reads.get.callCount).to.equal(0);
			expect(reads.getMany.callCount).to.equal(0);
			for (const entry of entries) {
				expect(await coordinate(shared, entry.hash)).to.have.property(
					"hash",
					entry.hash,
				);
			}
		});

		it(`resolves only missing-coordinate heads and preserves replica metadata (${mode})`, async () => {
			const { store, shared, index } = await open(nativeHeads);
			const { entry: kept } = await store.add("kept", {
				target: "none",
				meta: { next: [] },
			});
			const { entry: missing } = await store.add("missing-coordinate", {
				target: "none",
				meta: { next: [] },
				replicas: new AbsoluteReplicas(3),
			});
			const original = await coordinate(shared, missing.hash);
			expect(original).to.exist;
			await shared._coordinates.deleteCoordinatesForHashes(
				[missing.hash],
				shared.captureReplicationOwnershipLifecycle(),
			);
			expect(await coordinate(shared, missing.hash)).to.equal(undefined);
			expect(await coordinate(shared, kept.hash)).to.exist;
			await coldEntries(index);
			const reads = observeBlockReads(shared);
			await shared.ensureCurrentHeadCoordinatesIndexed();
			expect([...reads.hashes()]).to.deep.equal([missing.hash]);
			const repaired = await coordinate(shared, missing.hash);
			expect(repaired).to.exist;
			expect(decodeReplicas(repaired).getValue(shared)).to.equal(3);
			expect(repaired.coordinates).to.deep.equal(original.coordinates);
			expect(await coordinate(shared, kept.hash)).to.exist;
		});

		it(`preserves stale coordinates when a needed head read fails (${mode})`, async () => {
			const { store, shared, index } = await open(nativeHeads);
			const { entry: stale } = await store.add("stale", {
				target: "none",
				meta: { next: [] },
			});
			const { entry: needed } = await store.add("needed", {
				target: "none",
				meta: { next: [] },
			});
			await index.delete(stale.hash);
			await persistStaleCoordinate(shared, stale);
			await shared._coordinates.deleteCoordinatesForHashes(
				[needed.hash],
				shared.captureReplicationOwnershipLifecycle(),
			);
			await coldEntries(index);
			const entered = pDefer<void>(),
				release = pDefer<void>();
			releases.push(release.resolve);
			const sentinel = new Error("needed head block read failed");
			const blocks = shared.remoteBlocks.localStore;
			const getMany = blocks.getMany.bind(blocks);
			sinon.stub(blocks, "getMany").callsFake(async (...args: any[]) => {
				const rows = await getMany(...args);
				if ((args[0] as string[]).includes(needed.hash)) {
					entered.resolve();
					await release.promise;
					throw sentinel;
				}
				return rows;
			});
			const cleanup = sinon.spy(
				shared._coordinates,
				"deleteNonHeadCoordinatesForHashes",
			);
			const repair = track(shared.ensureCurrentHeadCoordinatesIndexed());
			await Promise.race([
				entered.promise,
				repair.then(() => {
					throw new Error("Repair completed before its required block read");
				}),
			]);
			expect(cleanup.called).to.equal(false);
			release.resolve();
			expect(
				await repair.then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(sentinel);
			expect(cleanup.called).to.equal(false);
			expect(await coordinate(shared, stale.hash)).to.exist;
			expect(await coordinate(shared, needed.hash)).to.equal(undefined);
		});

		it(`skips a missing-coordinate head deleted during the coordinate scan (${mode})`, async () => {
			const { store, shared, index } = await open(nativeHeads);
			const { entry: kept } = await store.add("kept", {
				target: "none",
				meta: { next: [] },
			});
			const { entry: deleted } = await store.add("deleted", {
				target: "none",
				meta: { next: [] },
			});
			await shared._coordinates.deleteCoordinatesForHashes(
				[deleted.hash],
				shared.captureReplicationOwnershipLifecycle(),
			);
			await coldEntries(index);
			// Delete the head after the head scan, while coordinates are listed.
			let deletion: Promise<unknown> | undefined;
			const deleteOnce = () => {
				if (deletion) return;
				deletion = track(index.delete(deleted.hash));
				return deletion;
			};
			const nativeCoordinates =
				shared._nativeBackbone ?? shared._nativeSharedLogState;
			if (nativeCoordinates) {
				const list =
					nativeCoordinates.getEntryCoordinateHashes.bind(nativeCoordinates);
				sinon
					.stub(nativeCoordinates, "getEntryCoordinateHashes")
					.callsFake(() => {
						const hashes = list();
						void deleteOnce();
						return hashes;
					});
			} else {
				const coordinates = shared.entryCoordinatesIndex;
				const iterate = coordinates.iterate.bind(coordinates);
				sinon.stub(coordinates, "iterate").callsFake((...args: any[]) => {
					const iterator = iterate(...args);
					const all = iterator.all.bind(iterator);
					iterator.all = async () => {
						await deleteOnce();
						return all();
					};
					return iterator;
				});
			}
			// Resolve the missing-coordinate head only once its delete committed.
			const getMany = index.getMany.bind(index);
			sinon.stub(index, "getMany").callsFake(async (hashes, options) => {
				await deletion;
				return getMany(hashes, options);
			});
			await shared.ensureCurrentHeadCoordinatesIndexed();
			expect(deletion).to.exist;
			await deletion;
			expect(await index.getShallow(deleted.hash)).to.not.exist;
			expect(await coordinate(shared, deleted.hash)).to.equal(undefined);
			expect(await coordinate(shared, kept.hash)).to.exist;
		});
	}

	it("fails closed for a current lower-index head with neither block nor coordinate", async () => {
		const { store, shared, index } = await open(false);
		const { entry: kept } = await store.add("kept", {
			target: "none",
			meta: { next: [] },
		});
		const { entry: blockless } = await store.add("blockless", {
			target: "none",
			meta: { next: [] },
		});
		await shared._coordinates.deleteCoordinatesForHashes(
			[blockless.hash],
			shared.captureReplicationOwnershipLifecycle(),
		);
		await shared.remoteBlocks.localStore.rm(blockless.hash);
		await coldEntries(index);
		const error = await shared.ensureCurrentHeadCoordinatesIndexed().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).to.be.instanceOf(Error);
		expect((error as Error).message).to.equal(
			`Failed to load entry from head with hash: ${blockless.hash}`,
		);
		expect((await index.getShallow(blockless.hash))?.value.head).to.equal(true);
		expect(await coordinate(shared, blockless.hash)).to.equal(undefined);
		expect(await coordinate(shared, kept.hash)).to.exist;
	});

	for (const blockPresent of [false, true]) {
		const head = blockPresent
			? "a graph-only promoted head whose block is present"
			: "a block-less graph-only promoted head";
		it(`removes a stale coordinate for ${head}`, async () => {
			const { store, shared, index } = await open(true);
			const { entry: parent } = await store.add("parent", {
				target: "none",
				meta: { next: [] },
			});
			const { entry: child } = await store.add("child", {
				target: "none",
				meta: { next: [parent] },
			});
			await index.flushPendingWrites();
			expect((await index.getShallow(parent.hash))?.value.head).to.equal(false);
			// Match the native-graph block-less-head fixture: a graph deletion
			// promotes the parent without changing its lower row.
			if (!blockPresent) {
				await shared.remoteBlocks.localStore.rm(parent.hash);
			}
			const graph = index.properties.nativeGraph!.graph;
			graph.delete(child.hash);
			expect(graph.heads()).to.include(parent.hash);
			expect(await shared.remoteBlocks.localStore.has(parent.hash)).to.equal(
				blockPresent,
			);
			await persistStaleCoordinate(shared, parent);
			await coldEntries(index);
			await shared.ensureCurrentHeadCoordinatesIndexed();
			// The lower row decides head-ness, as in stale cleanup and leader
			// planning, so a present block does not keep the coordinate.
			expect(await coordinate(shared, parent.hash)).to.equal(undefined);
			// Stale cleanup must retain a current lower head even if the graph differs.
			expect((await index.getShallow(child.hash))?.value.head).to.equal(true);
			expect(await coordinate(shared, child.hash)).to.exist;
		});
	}
});
