import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	RequestMaybeSyncCoordinate,
	ResponseMaybeSync,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

const deferred = <T>() => {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

describe("sync-chunking removal during admission", () => {
	let peer: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];

	before(async () => {
		peer = (await Ed25519Keypair.create()).publicKey;
	});

	const createSync = (
		hasMany: sinon.SinonStub,
		has: () => Promise<boolean> = async () => false,
	) => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count: async () => 0 } as any,
			log: { hasMany, has } as any,
			coordinateToHash,
		});
		return { sync, send, coordinateToHash };
	};

	for (const batch of [false, true]) {
		for (const coordinates of [false, true]) {
			it(`cancels only removed ${coordinates ? "known coordinate aliases" : "hashes"} during ${batch ? "batch" : "singular"} removal`, async () => {
				const lookup = deferred<string[]>();
				const hasMany = sinon.stub().resolves([]);
				hasMany.onFirstCall().returns(lookup.promise);
				const { sync, send, coordinateToHash } = createSync(hasMany);
				coordinateToHash.add(42n, "removed");
				coordinateToHash.add(43n, "removed");
				const keys = coordinates
					? [42n, 43n, "removed", "unrelated"]
					: ["removed", "unrelated"];
				const handling = sync.queueSync(keys, peer);

				try {
					expect(hasMany.calledOnce).to.equal(true);
					expect(sync.pending).to.equal(0);
					expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(
						2,
					);
					if (batch) {
						sync.onEntryRemovedHashes(["removed", "not-pending"]);
					} else {
						sync.onEntryRemoved("removed");
					}
					// Cancellation cannot release the quota of a still-running lookup.
					expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(
						2,
					);
					lookup.resolve([]);
					await handling;

					expect([...sync.syncInFlightQueue.keys()]).to.deep.equal([
						"unrelated",
					]);
					expect(send.calledOnce).to.equal(true);
					expect(send.firstCall.args[0]).to.be.instanceOf(ResponseMaybeSync);
					expect(send.firstCall.args[0].hashes).to.deep.equal(["unrelated"]);
					expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(
						0,
					);
					expect(
						(sync as any).pendingSync.pendingSyncAdmissionReservations.size,
					).to.equal(0);

					// Removal retires older work; it is not a permanent deny-list.
					const freshKey = coordinates ? 42n : "removed";
					await sync.queueSync([freshKey], peer);
					expect(sync.syncInFlightQueue.has(freshKey)).to.equal(true);
					expect(sync.syncInFlightQueue.has("unrelated")).to.equal(true);
					expect(send.callCount).to.equal(2);
					const request = send.secondCall.args[0];
					expect(request).to.be.instanceOf(
						coordinates ? RequestMaybeSyncCoordinate : ResponseMaybeSync,
					);
					expect(
						coordinates ? request.hashNumbers : request.hashes,
					).to.deep.equal([freshKey]);
				} finally {
					lookup.resolve([]);
					await Promise.allSettled([handling]);
					await sync.close();
				}
			});
		}
	}

	it("keeps fresh admission when removal interrupts a per-key lookup after bulk resolution", async () => {
		const oldPerKeyLookup = deferred<boolean>();
		const freshBulkLookup = deferred<string[]>();
		const has = sinon.stub().returns(oldPerKeyLookup.promise);
		const hasMany = sinon.stub().returns(freshBulkLookup.promise);
		const { sync, send, coordinateToHash } = createSync(hasMany, has);
		coordinateToHash.add(42n, "removed");
		const oldHandling = sync.queueSync([42n], peer);
		let freshHandling: Promise<void> | undefined;

		try {
			await waitForResolved(
				() => expect(has.calledOnceWithExactly("removed")).to.equal(true),
				{ timeout: 1_000 },
			);
			expect(hasMany.called).to.equal(false);
			sync.onEntryRemoved("removed");
			freshHandling = sync.queueSync(["removed"], peer);
			expect(hasMany.calledOnce).to.equal(true);

			oldPerKeyLookup.resolve(true);
			await oldHandling;
			expect(send.called).to.equal(false);
			expect(sync.pending).to.equal(0);
			expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(1);

			freshBulkLookup.resolve([]);
			await freshHandling;
			expect([...sync.syncInFlightQueue.keys()]).to.deep.equal(["removed"]);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[0]).to.be.instanceOf(ResponseMaybeSync);
			expect(send.firstCall.args[0].hashes).to.deep.equal(["removed"]);
			expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(0);
			expect(
				(sync as any).pendingSync.pendingSyncAdmissionReservations.size,
			).to.equal(0);
		} finally {
			oldPerKeyLookup.resolve(false);
			freshBulkLookup.resolve([]);
			await Promise.allSettled([oldHandling, freshHandling]);
			await sync.close();
		}
	});

	for (const replacementSession of [false, true]) {
		it(`does not let a retired lookup erase fresh admission in ${replacementSession ? "a replacement peer session" : "the same peer session"}`, async () => {
			const oldLookup = deferred<string[]>();
			const freshLookup = deferred<string[]>();
			const hasMany = sinon.stub();
			hasMany.onFirstCall().returns(oldLookup.promise);
			hasMany.onSecondCall().returns(freshLookup.promise);
			const { sync, send } = createSync(hasMany);
			const oldHandling = sync.queueSync(["removed"], peer);
			let freshHandling: Promise<void> | undefined;

			try {
				expect(hasMany.calledOnce).to.equal(true);
				sync.onEntryRemoved("removed");
				if (replacementSession) {
					await sync.onPeerDisconnected(peer);
				}
				freshHandling = sync.queueSync(["removed"], peer);
				expect(hasMany.calledTwice).to.equal(true);

				// A lookup started before removal may return an obsolete positive.
				oldLookup.resolve(["removed"]);
				await oldHandling;
				expect(send.called).to.equal(false);
				expect(sync.pending).to.equal(0);
				expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(1);

				freshLookup.resolve([]);
				await freshHandling;
				expect([...sync.syncInFlightQueue.keys()]).to.deep.equal(["removed"]);
				expect(send.calledOnce).to.equal(true);
				expect(send.firstCall.args[0]).to.be.instanceOf(ResponseMaybeSync);
				expect(send.firstCall.args[0].hashes).to.deep.equal(["removed"]);
				expect(send.firstCall.args[1].mode.to).to.deep.equal([peer.hashcode()]);
				expect((sync as any).pendingSync.pendingSyncAdmissionCount).to.equal(0);
				expect(
					(sync as any).pendingSync.pendingSyncAdmissionReservations.size,
				).to.equal(0);
			} finally {
				oldLookup.resolve([]);
				freshLookup.resolve([]);
				await Promise.allSettled([oldHandling, freshHandling]);
				await sync.close();
			}
		});
	}
});
