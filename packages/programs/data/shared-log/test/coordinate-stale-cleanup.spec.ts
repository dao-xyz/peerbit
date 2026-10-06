import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

describe("stale coordinate cleanup ownership", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	it("preserves a same-CID head re-admitted while reconciliation cleanup waits", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				nativeGraph: true,
				nativeBackbone: { optional: false },
			},
		});
		const shared = store.log as any;
		const index = store.log.log.entryIndex;
		const { entry } = await store.add("same-cid", {
			target: "none",
			meta: { next: [] },
		});
		const hash = entry.hash;
		const backbone = shared._nativeBackbone;
		expect(backbone, "required native coordinate storage").to.exist;
		const plan = await shared.planEntryLeaders(entry, 1, { persist: false });
		const coordinate = {
			entry,
			coordinates: plan.coordinates,
			leaders: plan.leaders,
			replicas: plan.coordinates.length,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
		};
		const lifecycle = shared.captureReplicationOwnershipLifecycle();

		await index.delete(hash);
		expect(await index.getShallow(hash)).to.equal(undefined);
		// Reconstruct a stale metadata row that reconciliation has to remove.
		// Re-admission below uses the real lower index and native graph, not a
		// mocked head query or a fabricated replacement CID.
		await shared._coordinates.persistCoordinate(coordinate, lifecycle);
		expect(backbone.getEntryCoordinateHashes()).to.include(hash);
		const owner = await index.acquireHashMutationLocks([hash]);
		const attempted = pDefer<void>();
		const acquire = index.acquireHashMutationLocks.bind(index);
		sinon.stub(index, "acquireHashMutationLocks").callsFake((hashes) => {
			const values = [...hashes];
			const pending = acquire(values);
			if (values.includes(hash)) attempted.resolve();
			return pending;
		});
		const cleanup = shared.ensureCurrentHeadCoordinatesIndexed(lifecycle);
		void cleanup.catch(() => {});
		try {
			await Promise.race([
				attempted.promise,
				cleanup.then(() => {
					throw new Error("Cleanup did not acquire its stale hash");
				}),
			]);
			await index.put(entry, {
				unique: true,
				isHead: true,
				toMultiHash: true,
				hashMutationLockOwner: owner,
			});
			await shared._coordinates.persistCoordinate(coordinate, lifecycle, owner);
			expect((await index.getShallow(hash))?.value.head).to.equal(true);
			expect(backbone.graph.heads()).to.include(hash);
		} finally {
			index.releaseHashMutationLocks(owner);
			await Promise.allSettled([cleanup]);
		}
		await cleanup;
		expect((await index.getShallow(hash))?.value.head).to.equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.include(hash);
		expect(
			await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(hash),
		).to.have.property("hash", hash);
	});
});
