import {
	type DeleteOptions,
	Or,
	StringMatch,
} from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

const queryHashes = (options: DeleteOptions): string[] =>
	options.query instanceof Or
		? options.query.or.map((query) => {
				expect(query).to.be.instanceOf(StringMatch);
				return (query as StringMatch).value;
			})
		: [(options.query as { hash: string }).hash];

describe("coordinate persistence bounded SQLite deletes", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	const open = async () => {
		session = await TestSession.disconnected(1, {
			indexer: createSQLiteIndices,
		});
		return session.peers[0].open(new EventStore<string, any>());
	};

	const seedObsoleteCoordinates = async (count: number) => {
		const store = await open();
		const unrelated = (await store.add("unrelated", { meta: { next: [] } }))
			.entry;
		const { entries } = await store.addMany(
			Array.from({ length: count + 1 }, (_, index) => `entry-${index}`),
			{ target: "none", meta: { next: [] } },
		);
		const shared = store.log as any;
		const coordinator = shared._coordinates;
		const obsolete = entries.slice(0, -1);
		const rows = await Promise.all(
			obsolete.map(async (entry) =>
				coordinator.materializePreparedCoordinateEntry(
					coordinator.createCoordinatePersistenceEntry({
						entry,
						coordinates: await shared.createCoordinates(entry, 1),
						leaders: false,
						replicas: 1,
					}),
				),
			),
		);
		// Model obsolete persisted coordinates using real signed lower entries and
		// the real SQLite row schema, not a mock index or generated hash strings.
		await store.log.entryCoordinatesIndex.putBatch!(rows);
		return { store, shared, coordinator, obsolete, unrelated, entries };
	};

	it("appends 514 chained entries without an oversized predicate or deleting other heads", async () => {
		const store = await open();
		const unrelated = (await store.add("unrelated", { meta: { next: [] } }))
			.entry;
		const index = store.log.entryCoordinatesIndex;
		const deleting = sinon.spy(index, "del");
		const coordinator = (store.log as any)._coordinates;
		const persist = coordinator.persistCoordinatesBatch.bind(coordinator);
		const ownedBatches: string[][] = [];
		const persisting = sinon
			.stub(coordinator, "persistCoordinatesBatch")
			.callsFake(async (...args: unknown[]) => {
				const before = deleting.callCount;
				const result = await persist(...args);
				ownedBatches.push(
					...deleting
						.getCalls()
						.slice(before)
						.map((call) => queryHashes(call.args[0])),
				);
				return result;
			});
		const { entries } = await store.addMany(
			Array.from({ length: 514 }, (_, index) => `entry-${index}`),
			{ target: "none", meta: { next: [] } },
		);
		const batches = deleting
			.getCalls()
			.map((call) => queryHashes(call.args[0]));
		// Count the retained-head batch separately: later per-entry notification
		// stages recheck and remove superseded rows through their own small queries.
		expect(persisting.callCount).to.equal(1);
		expect(ownedBatches.length).to.equal(9);
		expect(batches.every((batch) => batch.length <= 64)).to.equal(true);
		const obsoleteHashes = entries
			.slice(0, -1)
			.map((entry) => entry.hash)
			.sort();
		expect(ownedBatches.flat().sort()).to.deep.equal(obsoleteHashes);
		expect([...new Set(batches.flat())].sort()).to.deep.equal(obsoleteHashes);
		expect(store.log.log.length).to.equal(515);
		expect(
			(await index.iterate({}).all()).map((row) => row.value.hash).sort(),
		).to.deep.equal([unrelated.hash, entries.at(-1)!.hash].sort());
	});

	it("keeps one hash owner across all 513 real coordinate deletions", async () => {
		const { store, coordinator, obsolete, unrelated, entries } =
			await seedObsoleteCoordinates(513);
		const index = store.log.entryCoordinatesIndex;
		const lower = store.log.log.entryIndex;
		const hashes = obsolete.map((entry) => entry.hash);
		const del = index.del.bind(index);
		const entered = pDefer<void>();
		const release = pDefer<void>();
		let calls = 0;
		let contenderEntered = false;
		sinon.stub(index, "del").callsFake(async (options) => {
			calls++;
			expect(contenderEntered, "one owner spans every SQL chunk").to.equal(
				false,
			);
			if (calls === 2) {
				entered.resolve();
				await release.promise;
			}
			return del(options);
		});
		const deleting = Promise.resolve(
			coordinator.deleteCoordinatesForHashes(hashes),
		);
		void deleting.catch(() => {});
		let contending: Promise<void> | undefined;
		try {
			await Promise.race([
				entered.promise,
				deleting.then(() => {
					throw new Error(
						"coordinate deletion finished before the second chunk",
					);
				}),
			]);
			contending = lower
				.acquireHashMutationLocks([hashes.at(-1)!])
				.then((owner) => {
					contenderEntered = true;
					lower.releaseHashMutationLocks(owner);
				});
			await Promise.resolve();
			expect(contenderEntered).to.equal(false);
			release.resolve();
			await deleting;
			await contending;
			expect(calls).to.equal(9);
			expect(contenderEntered).to.equal(true);
			expect(
				(await index.iterate({}).all()).map((row) => row.value.hash).sort(),
			).to.deep.equal([unrelated.hash, entries.at(-1)!.hash].sort());
		} finally {
			release.resolve();
			await Promise.allSettled([deleting, ...(contending ? [contending] : [])]);
		}
	});

	it("does not overwrite a promoted head after completing superseded cleanup", async () => {
		const store = await open();
		const { entry } = await store.add("parent", { target: "none" });
		const { entry: child } = await store.add("child", {
			target: "none",
			meta: { next: [entry] },
		});
		const shared = store.log as any;
		const coordinator = shared._coordinates;
		const lower = store.log.log.entryIndex;
		expect((await lower.getShallow(entry.hash))?.value.head).to.equal(false);
		const cleanup = sinon.spy(coordinator, "deleteCoordinatesForHashes");
		const persist = sinon.spy(coordinator, "persistCoordinate");
		const plan = shared.planEntryLeaders.bind(shared);
		let promotedBoundary: boolean | undefined;
		const planning = sinon
			.stub(shared, "planEntryLeaders")
			.callsFake(async (...args) => {
				const result = await plan(...args);
				// Promotion is a new lower mutation after the old cleanup released its
				// owner. Its coordinate publication must not be overwritten by old work.
				await lower.delete(child.hash);
				expect((await lower.getShallow(entry.hash))?.value.head).to.equal(true);
				promotedBoundary = !result.assignedToRangeBoundary;
				await coordinator.persistCoordinate({
					entry,
					coordinates: result.coordinates,
					leaders: result.leaders,
					replicas: result.coordinates.length,
					assignedToRangeBoundary: promotedBoundary,
				});
				return result;
			});
		const delivery = sinon
			.stub(shared, "_appendDeliverToReplicators")
			.resolves();
		await shared.processLocalAppend(entry, [], undefined, {
			minReplicasValue: 1,
			deferHeadCoordinatePersistence: false,
		});
		expect(cleanup.callCount).to.equal(1);
		expect(planning.callCount).to.equal(1);
		expect(delivery.callCount).to.equal(1);
		expect(delivery.firstCall.args[0]).to.equal(entry);
		expect(
			persist.callCount,
			"only the new head publishes coordinates",
		).to.equal(1);
		const rows = await store.log.entryCoordinatesIndex.iterate({}).all();
		expect(rows).to.have.length(1);
		expect(rows[0]!.value.hash).to.equal(entry.hash);
		expect(rows[0]!.value.assignedToRangeBoundary).to.equal(promotedBoundary);
	});

	it("stops replacement completion at the first failed later delete chunk", async () => {
		const { store, shared, coordinator, obsolete, entries } =
			await seedObsoleteCoordinates(129);
		const index = store.log.entryCoordinatesIndex;
		const del = index.del.bind(index);
		const failure = new Error("second coordinate delete chunk failed");
		let calls = 0;
		sinon.stub(index, "del").callsFake(async (options) => {
			if (++calls === 2) throw failure;
			return del(options);
		});
		const completed = sinon.spy(shared.coordinateToHash, "add");
		const entry = entries.at(-1)!;
		const error = await coordinator
			.persistCoordinate({
				entry,
				coordinates: await shared.createCoordinates(entry, 1),
				leaders: false,
				replicas: 1,
				deleteHashes: obsolete.map((entry) => entry.hash),
			})
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(error).to.equal(failure);
		expect(calls).to.equal(2);
		expect(
			completed.called,
			"failed delete cannot complete the replacement cache",
		).to.equal(false);
		const owner = await store.log.log.entryIndex.acquireHashMutationLocks([
			entry.hash,
		]);
		store.log.log.entryIndex.releaseHashMutationLocks(owner);
	});

	it("fails closed when a later mandatory removal chunk rejects after lower deletion", async () => {
		const { store, obsolete } = await seedObsoleteCoordinates(129);
		const index = store.log.entryCoordinatesIndex;
		const lower = store.log.log.entryIndex;
		const del = index.del.bind(index);
		const failure = new Error(
			"second mandatory coordinate delete chunk failed",
		);
		let calls = 0;
		sinon.stub(index, "del").callsFake(async (options) => {
			if (++calls === 2) throw failure;
			return del(options);
		});
		const error = await lower
			.deleteMany(obsolete.map((entry) => entry.toShallow(false)))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(error).to.equal(failure);
		expect(calls).to.equal(2);
		expect(() =>
			lower.throwIfNativeDurableTransactionMutationsFailed(),
		).to.throw();
		const blocked = await store.add("must not follow incomplete metadata").then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(blocked).to.be.instanceOf(Error);
	});
});
