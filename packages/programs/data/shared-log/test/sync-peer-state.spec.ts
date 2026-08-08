import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { SyncPeerSlotRegistry } from "../src/sync/sync-peer-state.js";

const createSimple = () =>
	new SimpleSyncronizer<"u64">({
		rpc: { send: async () => {} } as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 128 }),
	});

const createRateless = () =>
	new RatelessIBLTSynchronizer<"u64">({
		rpc: { send: async () => {} } as any,
		rangeIndex: {} as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 128 }),
		numbers: { maxValue: 2n ** 64n - 1n } as any,
	} as any);

describe("sync-peer-state fold boundary pins", () => {
	it("keeps a detached physical permit on its old row across reconnect and open rotation", async () => {
		const sync = createSimple();
		const state = sync as any;
		let oldPermit: any;
		let successorPermit: any;
		try {
			oldPermit = state.tryAcquireCoordinateLookup("peer-a");
			expect(oldPermit).to.not.equal(undefined);
			const oldRow = oldPermit.row;
			let logicalReleases = 0;
			const releaseLogical = () => {
				logicalReleases += 1;
				oldRow.activeReleases.delete(releaseLogical);
			};
			oldRow.activeReleases.add(releaseLogical);

			sync.onPeerDisconnected("peer-a");
			expect(logicalReleases, "logical ownership released on detach").to.equal(
				1,
			);
			expect(oldRow.attached).to.equal(false);
			expect(oldRow.lookups).to.equal(1);
			expect(oldRow.activeReleases.size).to.equal(0);
			expect(state.peerSlotRows.rows.size).to.equal(0);
			expect(state.pendingCoordinateLookupCountByPeer.size).to.equal(0);
			expect(
				state.pendingCoordinateLookupCount,
				"physical work remains charged",
			).to.equal(1);

			await sync.open();
			expect(
				state.pendingCoordinateLookupCount,
				"open preserves physical charge",
			).to.equal(1);
			expect(oldRow.lookups).to.equal(1);

			successorPermit = state.tryAcquireCoordinateLookup("peer-a");
			expect(successorPermit).to.not.equal(undefined);
			const successorRow = successorPermit.row;
			expect(successorRow).to.not.equal(oldRow);
			expect(successorRow.attached).to.equal(true);
			expect(successorRow.lookups).to.equal(1);
			expect(state.peerSlotRows.rows.get("peer-a")).to.equal(successorRow);
			expect(state.pendingCoordinateLookupCount).to.equal(2);
			expect(state.pendingCoordinateLookupCountByPeer.get("peer-a")).to.equal(
				1,
			);

			// Late completion captures the old object identity. It settles that
			// row and the global physical counter, but cannot decrement or remove
			// the reconnected generation's logical/per-peer state.
			oldPermit.release();
			expect(oldRow.lookups).to.equal(0);
			expect(successorRow.lookups).to.equal(1);
			expect(state.pendingCoordinateLookupCount).to.equal(1);
			expect(state.pendingCoordinateLookupCountByPeer.get("peer-a")).to.equal(
				1,
			);
			expect(state.peerSlotRows.rows.get("peer-a")).to.equal(successorRow);

			successorPermit.release();
			expect(successorRow.lookups).to.equal(0);
			expect(state.pendingCoordinateLookupCount).to.equal(0);
			expect(state.pendingCoordinateLookupCountByPeer.size).to.equal(0);
			expect(state.peerSlotRows.rows.size).to.equal(0);
		} finally {
			oldPermit?.release();
			successorPermit?.release();
			await sync.close();
		}
	});

	it("drops attached rows only after every quota and lifecycle blocker is empty", () => {
		const blockers: Array<{
			name: string;
			block: (row: any) => () => void;
		}> = [
			{
				name: "lookups",
				block: (row) => {
					row.lookups = 1;
					return () => (row.lookups = 0);
				},
			},
			{
				name: "responses",
				block: (row) => {
					row.responses = 1;
					return () => (row.responses = 0);
				},
			},
			{
				name: "active",
				block: (row) => {
					row.active = 1;
					return () => (row.active = 0);
				},
			},
			{
				name: "pendingResponseHashes",
				block: (row) => {
					row.pendingResponseHashes = 1;
					return () => (row.pendingResponseHashes = 0);
				},
			},
			{
				name: "activeReleases",
				block: (row) => {
					const release = () => {};
					row.activeReleases.add(release);
					return () => row.activeReleases.delete(release);
				},
			},
		];

		for (const blocker of blockers) {
			const registry = new SyncPeerSlotRegistry();
			const row = registry.ensure(`peer-${blocker.name}`);
			const unblock = blocker.block(row);
			registry.maybeDropIdle(row);
			expect(registry.rows.get(row.peer), blocker.name).to.equal(row);
			unblock();
			registry.maybeDropIdle(row);
			expect(registry.rows.has(row.peer), blocker.name).to.equal(false);
		}
	});

	it("keeps Simple and Rateless rows and lifecycle namespaces isolated", async () => {
		const sync = createRateless();
		const rateless = sync as any;
		const simple = sync.simple as any;
		try {
			const simpleRow = simple.getOrCreateSyncResponseSlotRow("peer-a");
			const ratelessRow = rateless.getOrCreateRatelessResponseSlotRow("peer-a");
			expect(simple.peerSlotRows).to.not.equal(rateless.ratelessPeerSlotRows);
			expect(simpleRow).to.not.equal(ratelessRow);
			simpleRow.responses = 1;
			ratelessRow.active = 1;

			simple.detachSyncResponseSlotRow("peer-a");
			expect(simple.peerSlotRows.rows.size).to.equal(0);
			expect(rateless.ratelessPeerSlotRows.rows.get("peer-a")).to.equal(
				ratelessRow,
			);
			expect(ratelessRow.active).to.equal(1);

			const simpleSuccessor = simple.getOrCreateSyncResponseSlotRow("peer-a");
			rateless.detachRatelessResponseSlotRow("peer-a");
			expect(rateless.ratelessPeerSlotRows.rows.size).to.equal(0);
			expect(simple.peerSlotRows.rows.get("peer-a")).to.equal(simpleSuccessor);

			const simpleLifecycle = simple.captureSyncDispatchLifecycle(["peer-a"]);
			simple.finishSyncDispatchLifecycle(simpleLifecycle);
			expect(simpleLifecycle.disposed).to.equal(true);
			expect(simple.syncDispatchRegistry.activeTargets.size).to.equal(0);

			const ratelessLifecycle = rateless.captureRatelessDispatchLifecycle([
				"peer-a",
			]);
			const targetLifecycle = ratelessLifecycle.targets.get("peer-a");
			targetLifecycle.retainedByProcess = true;
			rateless.finishRatelessDispatchLifecycle(ratelessLifecycle);
			expect(ratelessLifecycle.disposed).to.equal(false);
			expect(rateless.ratelessDispatchRegistry.activeTargets.size).to.equal(1);
			targetLifecycle.retainedByProcess = false;
			rateless.maybeDisposeRatelessDispatchLifecycle(ratelessLifecycle);
			expect(ratelessLifecycle.disposed).to.equal(true);
			expect(rateless.ratelessDispatchRegistry.activeTargets.size).to.equal(0);

			simple.peerSlotRows.maybeDropIdle(simpleSuccessor);
			expect(simple.peerSlotRows.rows.size).to.equal(0);
		} finally {
			await sync.close();
		}
	});
});
