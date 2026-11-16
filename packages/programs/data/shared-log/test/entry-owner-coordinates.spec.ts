import { field, variant } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";
import { create as createSQLite } from "@peerbit/indexer-sqlite3";
import { Entry, EntryType } from "@peerbit/log";
import { Program, TerminalOperationNotStartedError } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pDefer from "p-defer";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";
import { RequestPersistedEntriesV1 } from "../src/sync/simple.js";

@variant("test_entry_owner_coordinates")
class OwnerStore extends Program<{ native: boolean }> {
	@field({ type: SharedLog })
	log = new SharedLog<Uint8Array>();
	adapter: any;

	async open(options: { native: boolean }) {
		const shared = this.log as any;
		await shared.openWithRecovery(
			{
				replicate: 1,
				timeUntilRoleMaturity: 0,
				appendDurability: "strict",
				nativeGraph: options.native,
				nativeRangePlanner: options.native ? { optional: false } : false,
				nativeBackbone: options.native ? { optional: false } : false,
				sync: { rawExchangeHeads: options.native },
			},
			async () => {
				this.adapter = shared.registerEntryOwner();
			},
		);
	}
}

// Real SharedLog storage/receipt completion, not proof authentication or a
// public Documents integration. Only the remote session handshake is bypassed.
describe("append delivery options — entry owner coordinate completion", () => {
	let session: TestSession | undefined;
	let directory: string | undefined;
	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
		if (directory) await rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	const fixture = async (native: boolean) => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-owner-coordinates-"));
		session = await TestSession.disconnected(1, {
			directory,
			storage: { storeFactory: (directory) => createStore(directory) },
			indexer: (directory) => createSQLite(directory),
		});
		const store = await session.peers[0].open(new OwnerStore(), {
			args: { native },
		});
		const shared = store.log as any;
		const lower = shared.log;
		const index = lower.entryIndex;
		expect(Boolean(shared._nativeBackbone)).equal(native);
		expect(shared.resolvePersistedReceiptStorage()).not.equal(undefined);
		const create = (
			value: number,
			next: Entry<Uint8Array>[] = [],
			cut = false,
		) =>
			lower.createAppendEntry(
				Uint8Array.of(value),
				{
					meta: {
						data: shared.createAppendReplicaMetadata().minReplicasData,
						type: cut ? EntryType.CUT : EntryType.APPEND,
					},
				},
				next,
				{ deferStore: true },
			) as Promise<Entry<Uint8Array>>;
		const commit = (
			entry: Entry<Uint8Array>,
			removed: Entry<Uint8Array>[] = [],
			afterCoordinates?: () => Promise<void>,
		) =>
			lower.withNativeCommittedAppendAdmission(async () => {
				const admission = store.adapter.beginAdmission();
				const owner = await index.acquireExclusiveMutationLock();
				let prepared: any;
				try {
					prepared = await store.adapter.prepareCoordinates(entry);
					await shared.remoteBlocks.put(
						Entry.preparedBlockFromBytes(
							Entry.getPreparedStorageBytes(entry)!,
							entry.hash,
						),
					);
					await shared.remoteBlocks.crashSafeDurability.barrier();
					await index.put(entry, {
						unique: true,
						isHead: true,
						toMultiHash: false,
						hashMutationLockOwner: owner,
					});
					await index.properties.index.crashSafeDurability.barrier();
					const targets = removed.map((value) => value.toShallow(false));
					await index.completeExactDelete(targets, owner);
					await prepared.complete({ removed: targets, owner });
					await afterCoordinates?.();
					await prepared.barrier();
				} catch (error) {
					admission.poison(error);
					throw error;
				} finally {
					index.releaseHashMutationLocks(owner);
					prepared?.release();
					admission.release();
				}
			});
		const receiptController = shared.captureReplicationOwnershipLifecycle();
		const receipt = (hash: string) =>
			shared.handleRequestPersistedEntriesV1(
				new RequestPersistedEntriesV1({
					hashes: [hash],
					expectedReceiverSession: 1n,
				}),
				{ message: { header: { session: 1n } } },
				{
					fromHash: "owner-coordinate-peer",
					ownershipLifecycleController: receiptController,
				},
			);
		sinon.stub(shared, "isPersistedReceiptRequestSessionCurrent").returns(true);
		return { store, shared, lower, index, create, commit, receipt };
	};

	for (const native of [false, true]) {
		it(`keeps actual receipt ownership poisoned when coupled durability fails (${native})`, async () => {
			const { store, shared, create, commit, receipt } = await fixture(native);
			const entry = await create(9);
			const failure = new Error("injected coordinate barrier failure");
			let armed = false;
			const durability = shared.entryCoordinatesIndex.crashSafeDurability;
			const barrier = durability.barrier.bind(durability);
			const injected = sinon.stub(durability, "barrier").callsFake(async () => {
				if (armed) throw failure;
				await barrier();
			});
			try {
				await expect(
					commit(entry, [], async () => {
						armed = true;
					}),
				).rejectedWith(failure);
			} finally {
				armed = false;
				injected.restore();
			}
			expect(
				shared._instanceLifecycle._receiveOwnershipMutationAdmissions,
			).equal(0);
			expect(() => store.adapter.beginAdmission()).throw(
				"recovery is required",
			);
			await expect(receipt(entry.hash)).rejectedWith("recovery is required");
		});

		it(`uses actual coordinate storage, removes exact targets, and fences receipts until its barrier (${native})`, async () => {
			const { shared, index, create, commit, receipt } = await fixture(native);
			const victim = await create(1);
			await commit(victim);
			expect((await receipt(victim.hash)).hashes).deep.equal([victim.hash]);
			const marker = await create(2, [victim], true);
			const entered = pDefer<void>();
			const gate = pDefer<void>();
			let armed = false;
			const durability = shared.entryCoordinatesIndex.crashSafeDurability;
			const barrier = durability.barrier.bind(durability);
			const held = sinon.stub(durability, "barrier").callsFake(async () => {
				if (armed) {
					entered.resolve();
					await gate.promise;
				}
				await barrier();
			});
			const acquire = sinon.spy(index, "acquireHashMutationLocks");
			const pending = commit(marker, [victim], async () => {
				armed = true;
			});
			void pending.catch(() => {});
			try {
				await entered.promise;
				const flush = sinon.spy(index, "flushPendingWrites");
				expect((await receipt(marker.hash)).hashes).deep.equal([]);
				expect(flush.callCount).equal(0);
				flush.restore();
				expect(acquire.callCount).equal(0);
				expect(await index.getShallow(victim.hash)).equal(undefined);
				expect(await shared.remoteBlocks.localStore.has(victim.hash)).equal(
					false,
				);
				expect(
					await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(
						victim.hash,
					),
				).equal(undefined);
			} finally {
				armed = false;
				gate.resolve();
				await pending;
				held.restore();
			}
			expect((await receipt(marker.hash)).hashes).deep.equal([marker.hash]);
			// Recovery must complete coordinate cleanup for already-absent targets,
			// not rely on the count returned by the physical lower row deletion.
			const laterMarker = await create(8, [victim], true);
			await commit(laterMarker, [victim]);
			expect((await receipt(laterMarker.hash)).hashes).deep.equal([
				laterMarker.hash,
			]);
			expect(
				await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(
					victim.hash,
				),
			).equal(undefined);
		});

		for (const terminal of ["close", "drop"] as const) {
			it(`drains physically admitted coordinates before ${terminal} tears down stores (${native})`, async () => {
				const { store, shared, create, commit } = await fixture(native);
				const entry = await create(3);
				const entered = pDefer<void>();
				const gate = pDefer<void>();
				let completed = false;
				const pending = commit(entry, [], async () => {
					entered.resolve();
					await gate.promise;
					completed = true;
				});
				void pending.catch(() => {});
				await entered.promise;
				const stop = sinon.spy(
					shared.entryCoordinatesIndex,
					terminal === "close" ? "stop" : "drop",
				);
				const ending = store[terminal]();
				void ending.catch(() => {});
				try {
					await Promise.resolve();
					expect(completed).equal(false);
					expect(stop.callCount).equal(0);
					expect(() => store.adapter.beginAdmission()).throw();
				} finally {
					gate.resolve();
					await pending;
					await ending;
				}
				expect(completed).equal(true);
				expect(stop.callCount).equal(1);
			});
		}

		it(`pins protected retention and refuses terminal callbacks without poisoning the live generation (${native})`, async () => {
			const { store, shared, lower, create } = await fixture(native);
			await expect(shared.replicate(0.5)).rejectedWith(
				"fixed full replication",
			);
			await expect(shared.unreplicate()).rejectedWith("cannot relinquish");
			await expect(shared.replicate(await create(4))).rejectedWith(
				"fixed full replication",
			);
			expect(
				shared.prune(new Map([["not-a-real-entry", {}]]), { unchecked: true }),
			).deep.equal([]);
			await lower.runWithMutationCallback(async () => {
				await expect(shared.close()).rejectedWith(
					TerminalOperationNotStartedError,
				);
				await expect(shared.drop()).rejectedWith(
					TerminalOperationNotStartedError,
				);
			});
			store.adapter.assertActive();
			await shared.replicate(1);
			expect(() => shared.registerEntryOwner()).throw("protected open");
		});
	}

	it("does not let a stale adapter poison or release the reopened generation", async () => {
		const { store, shared } = await fixture(false);
		const old = store.adapter;
		const oldAdmission = old.beginAdmission();
		await store.close();
		await session!.peers[0].open(store, { args: { native: false } });
		const current = store.adapter.beginAdmission();
		try {
			oldAdmission.poison(new Error("late old failure"));
			oldAdmission.release();
			store.adapter.assertActive();
			expect(
				shared._instanceLifecycle._receiveOwnershipMutationAdmissions,
			).equal(1);
			expect(() => old.beginAdmission()).throw("old lifecycle");
		} finally {
			current.release();
		}
	});

	it("rejects unsupported role and trim options at registration before enabling protection", async () => {
		const { store, shared } = await fixture(false);
		await store.close();
		for (const options of [
			{ replicate: false },
			{ replicate: { factor: 1 } },
			{ replicate: 0.5 },
			{ replicate: 1, trim: { type: "length", to: 1 } },
		]) {
			// Use a distinct raw child for each failed opening; a failed protected
			// generation requires terminal cleanup, not a retry around its poison.
			const child = new SharedLog<Uint8Array>() as any;
			await child.beforeOpen(session!.peers[0]);
			try {
				await expect(
					child.openWithRecovery(options, async () =>
						child.registerEntryOwner(),
					),
				).rejectedWith("protected open");
				expect(child._entryOwnerProtected).not.equal(true);
			} finally {
				await child.close();
			}
		}
		expect(shared._entryOwnerProtected).equal(true);
	});
});
