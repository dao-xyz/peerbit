// Stage-4.5 PR-1 pinning tests (P1-P4). These pin the coordinate-persistence
// invariants most likely to break subtly when the coordinate state fields and
// the persistence method cluster move from SharedLog onto the
// CoordinatePersistenceCoordinator:
//
// P1. The native-backbone coordinate JOURNAL FLUSH predicate
//     (`shouldFlushNativeBackboneCoordinateJournalOnAppend`) honors the
//     adapter's `flushOnAppend: false` contract: flush only on the byte or
//     time threshold, measured against
//     `_nativeBackboneCoordinateJournalLastFlushMs`, and the last-flush
//     watermark advances only after the adapter's `flushJournal` settles.
//
// P2. `rollbackNativeBackboneCoordinateAppendDurably` is generation-checked
//     (the `_nativeCoordinateMutationGenerations` ratchet): a matching
//     snapshot erases (or restores) coordinate rows, the resident mirror, and
//     the native backbone state for the failed hashes — and a STALE snapshot
//     (superseded by a newer mutation generation) is a strict no-op. A retry
//     after rollback persists cleanly.
//
// P3. `deleteCoordinatesForHashes` forgets the native mirrors and the
//     resident cache BEFORE the coordinate-index delete, and re-checks the
//     ownership lifecycle controller after the index delete settles.
//
// P4. The backbone-only receive coordinate batch is atomic: finish commits
//     exactly the planned rows into the resident mirror, and a failed native
//     columns-commit rolls back leaving zero resident entries for the batch.
//
// The probes deliberately reach through the compat accessors under the
// historical field names, and resolve the private persistence methods through
// `coordinateInternals` so the assertions are identical before and after the
// state/method moves.
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import pDefer from "p-defer";
import { SharedLog } from "../src/index.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-coordinate-persistence-pins",
};

// The persistence internals live on the SharedLog host today and on its
// coordinate-persistence coordinator after the stage-4.5 method move (the
// state-only coordinator of the intermediate commit has no methods yet);
// the state fields stay reachable through host compat accessors either way.
const coordinateInternals = (log: any): any =>
	typeof log._coordinates?.deleteCoordinatesForHashes === "function"
		? log._coordinates
		: log;

describe("coordinate persistence journal flush pins", () => {
	let log: any;
	let backbone: {
		coordinatePendingJournalLength: number;
		coordinatePendingJournalByteLength: number;
		documentPendingJournalLength: number;
		documentSignerPendingJournalLength: number;
	};

	beforeEach(() => {
		log = new SharedLog();
		backbone = {
			coordinatePendingJournalLength: 0,
			coordinatePendingJournalByteLength: 0,
			documentPendingJournalLength: 0,
			documentSignerPendingJournalLength: 0,
		};
		log._nativeBackbone = backbone;
	});

	it("defaults to flush-on-append unless the adapter opts out", () => {
		log._nativeBackboneCoordinatePersistence = {
			flushJournal: async () => 0,
		};
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.true;
		log._nativeBackboneCoordinatePersistence = {
			flushOnAppend: true,
			flushJournal: async () => 0,
		};
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.true;
	});

	it("with flushOnAppend disabled, flushes only on the byte or time threshold", () => {
		log._nativeBackboneCoordinatePersistence = {
			flushOnAppend: false,
			flushMaxPendingBytes: 100,
			flushIntervalMs: 60_000,
			flushJournal: async () => 0,
		};
		// Nothing pending: never flush.
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.false;

		// Pending below both thresholds, interval not elapsed: no flush.
		backbone.coordinatePendingJournalLength = 1;
		backbone.coordinatePendingJournalByteLength = 10;
		log._nativeBackboneCoordinateJournalLastFlushMs = Date.now();
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.false;

		// Byte threshold reached: flush regardless of the interval.
		backbone.coordinatePendingJournalByteLength = 100;
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.true;

		// Time threshold elapsed (probed via the compat accessor): flush.
		backbone.coordinatePendingJournalByteLength = 10;
		log._nativeBackboneCoordinateJournalLastFlushMs = Date.now() - 60_001;
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.true;

		// No interval configured: the byte threshold is the only trigger.
		log._nativeBackboneCoordinatePersistence = {
			flushOnAppend: false,
			flushMaxPendingBytes: 100,
			flushJournal: async () => 0,
		};
		expect(
			coordinateInternals(
				log,
			).shouldFlushNativeBackboneCoordinateJournalOnAppend(),
		).to.be.false;
	});

	it("advances the last-flush watermark only after the adapter flush settles", async () => {
		const flushGate = pDefer<void>();
		let flushCalls = 0;
		log._nativeBackboneCoordinatePersistence = {
			flushJournal: async () => {
				flushCalls += 1;
				await flushGate.promise;
				return 1;
			},
		};
		log._nativeBackboneCoordinateJournalLastFlushMs = 7;

		// Nothing pending: no flush, watermark untouched.
		let result = coordinateInternals(
			log,
		).flushNativeBackboneCoordinateJournal();
		expect(result).to.equal(undefined);
		expect(flushCalls).to.equal(0);
		expect(log._nativeBackboneCoordinateJournalLastFlushMs).to.equal(7);

		backbone.coordinatePendingJournalLength = 2;
		const before = Date.now();
		result = coordinateInternals(log).flushNativeBackboneCoordinateJournal();
		expect(flushCalls).to.equal(1);
		// The adapter flush has not settled: the watermark must not have moved.
		expect(log._nativeBackboneCoordinateJournalLastFlushMs).to.equal(7);
		flushGate.resolve();
		await result;
		expect(
			log._nativeBackboneCoordinateJournalLastFlushMs,
		).to.be.greaterThanOrEqual(before);
	});

	it("never flushes after drop has started", async () => {
		let flushCalls = 0;
		log._nativeBackboneCoordinatePersistence = {
			flushJournal: async () => {
				flushCalls += 1;
				return 1;
			},
		};
		backbone.coordinatePendingJournalLength = 2;
		log._nativeBackboneDropStarted = true;
		expect(
			coordinateInternals(log).flushNativeBackboneCoordinateJournal(),
		).to.equal(undefined);
		const onAppend = coordinateInternals(
			log,
		).flushNativeBackboneCoordinateJournalOnAppend();
		if (onAppend) {
			await onAppend;
		}
		expect(flushCalls).to.equal(0);
	});

	it("prefers the adapter's flushJournalOnAppend and leaves the watermark to full flushes", async () => {
		let onAppendCalls = 0;
		let fullFlushCalls = 0;
		log._nativeBackboneCoordinatePersistence = {
			flushOnAppend: false,
			flushJournalOnAppend: () => {
				onAppendCalls += 1;
				return 0;
			},
			flushJournal: async () => {
				fullFlushCalls += 1;
				return 1;
			},
		};
		backbone.coordinatePendingJournalLength = 5;
		backbone.coordinatePendingJournalByteLength = 1e9;
		log._nativeBackboneCoordinateJournalLastFlushMs = 7;
		const result = coordinateInternals(
			log,
		).flushNativeBackboneCoordinateJournalOnAppend();
		if (result) {
			await result;
		}
		expect(onAppendCalls).to.equal(1);
		expect(fullFlushCalls).to.equal(0);
		expect(log._nativeBackboneCoordinateJournalLastFlushMs).to.equal(7);
	});
});

describe("coordinate persistence rollback and receive-batch pins", function () {
	this.timeout(120_000);

	let client: Peerbit | undefined;
	let directory: string | undefined;
	let store: EventStore<string, any> | undefined;

	beforeEach(async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-coordinate-pins-"),
		);
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});
		store = await client.open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 } },
		});
		const log = store.log as any;
		// The pins below are only meaningful against the native backbone
		// coordinate stack this repo ships by default for rust clients.
		expect(log._nativeBackbone, "native backbone").to.exist;
		expect(
			log._nativeBackboneCoordinatePersistence,
			"auto-derived coordinate persistence",
		).to.exist;
		expect(
			log._residentEntryCoordinatesByHash,
			"resident coordinate mirror (via compat accessor)",
		).to.exist;
	});

	afterEach(async () => {
		await client?.stop();
		client = undefined;
		store = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	const countIndexed = async (log: any, hash: string): Promise<number> =>
		log.entryCoordinatesIndex.count({ query: { hash } });

	const backboneHas = (log: any, hash: string): boolean =>
		[...(log._nativeBackbone.getEntryCoordinateHashes() as string[])].includes(
			hash,
		);

	it("P2: a matching rollback snapshot erases the failed append everywhere and a retry succeeds", async () => {
		const log = store!.log as any;
		const internals = coordinateInternals(log);
		const entry = (await store!.add("p2-fresh", { meta: { next: [] } })).entry;
		const hash = entry.hash;
		const coordinates = await log.createCoordinates(entry, 1);

		// Clean slate for the hash, then snapshot the pre-append state (no
		// prior entry) exactly as the append path does before its mutation.
		await internals.deleteCoordinatesForHashes([hash]);
		expect(await countIndexed(log, hash)).to.equal(0);
		const snapshot = internals.snapshotResidentCoordinateEntries([hash]);
		expect(snapshot.entries.has(hash)).to.be.false;

		// Simulate the failed append's partial work: coordinates persisted to
		// the index, the resident mirror, and the native backbone.
		await internals.persistCoordinate({
			coordinates,
			entry,
			leaders: false,
			replicas: 1,
		});
		expect(await countIndexed(log, hash)).to.equal(1);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.true;
		expect(backboneHas(log, hash)).to.be.true;

		await internals.rollbackNativeBackboneCoordinateAppendDurably(
			hash,
			snapshot,
		);
		expect(await countIndexed(log, hash)).to.equal(0);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
		expect(backboneHas(log, hash)).to.be.false;

		// Retry after rollback persists cleanly.
		const retried = await internals.persistCoordinate({
			coordinates,
			entry,
			leaders: false,
			replicas: 1,
		});
		expect(retried).to.be.true;
		expect(await countIndexed(log, hash)).to.equal(1);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.true;
		expect(backboneHas(log, hash)).to.be.true;
	});

	it("P2: a rollback snapshot with a prior entry restores it, and a stale generation is a no-op", async () => {
		const log = store!.log as any;
		const internals = coordinateInternals(log);
		const entry = (await store!.add("p2-prior", { meta: { next: [] } })).entry;
		const hash = entry.hash;
		expect(await countIndexed(log, hash)).to.equal(1);

		// Snapshot captures the persisted prior entry, then the failed
		// generation wipes the row; rollback must restore it everywhere.
		const snapshot = internals.snapshotResidentCoordinateEntries([hash]);
		expect(snapshot.entries.has(hash)).to.be.true;
		await internals.deleteCoordinatesForHashes([hash]);
		expect(await countIndexed(log, hash)).to.equal(0);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;

		await internals.rollbackNativeBackboneCoordinateAppendDurably(
			hash,
			snapshot,
		);
		expect(await countIndexed(log, hash)).to.equal(1);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.true;
		expect(backboneHas(log, hash)).to.be.true;

		// Ratchet: a snapshot superseded by a newer mutation generation must
		// not roll anything back (`_nativeCoordinateMutationGenerations` is
		// generation-checked per hash). The newer state must be visibly
		// DIFFERENT from the stale snapshot (here: the row deleted) so an
		// unconditional rollback would clobber it — with identical states a
		// no-op and an always-rollback are indistinguishable and the pin
		// has no teeth.
		const stale = internals.snapshotResidentCoordinateEntries([hash]);
		internals.snapshotResidentCoordinateEntries([hash]); // newer generation
		await internals.deleteCoordinatesForHashes([hash]); // newer visible state
		await internals.rollbackNativeBackboneCoordinateAppendDurably(hash, stale);
		expect(await countIndexed(log, hash)).to.equal(0);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
		expect(backboneHas(log, hash)).to.be.false;
	});

	it("P4: the backbone-only receive batch commits exactly the planned rows and rolls back atomically", async () => {
		const log = store!.log as any;
		const internals = coordinateInternals(log);
		const entries = [
			(await store!.add("p4-a", { meta: { next: [] } })).entry,
			(await store!.add("p4-b", { meta: { next: [] } })).entry,
		];
		const hashes = entries.map((entry) => entry.hash);
		await internals.deleteCoordinatesForHashes(hashes);

		const makeItems = async () => {
			const items: any[] = [];
			for (const entry of entries) {
				const coordinates = await log.createCoordinates(entry, 1);
				const prepared = internals.createCoordinatePersistenceEntry({
					coordinates,
					entry,
					leaders: false,
					replicas: 1,
				});
				expect(prepared).to.not.equal(false);
				items.push({
					coordinates,
					entry,
					leaders: false,
					replicas: 1,
					prepared,
				});
			}
			return items;
		};

		// Happy path: finish commits exactly the planned rows.
		const persisted = await internals.persistBackboneOnlyReceiveCoordinateBatch(
			await makeItems(),
		);
		expect(persisted, "backbone-only batch must be active for this pin").to
			.exist;
		expect([...persisted!].sort()).to.deep.equal([...hashes].sort());
		for (const hash of hashes) {
			expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.true;
			expect(backboneHas(log, hash)).to.be.true;
		}

		// Atomicity: a failed native columns-commit leaves zero resident
		// entries (and no backbone coordinates) for the batch.
		await internals.deleteCoordinatesForHashes(hashes);
		for (const hash of hashes) {
			expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
		}
		const backbone = log._nativeBackbone;
		const originalCommit =
			backbone.commitEntryCoordinatesColumnsBatch.bind(backbone);
		backbone.commitEntryCoordinatesColumnsBatch = () => {
			throw new Error("pinned native columns-commit failure");
		};
		let error: any;
		try {
			await internals.persistBackboneOnlyReceiveCoordinateBatch(
				await makeItems(),
			);
		} catch (caught) {
			error = caught;
		} finally {
			backbone.commitEntryCoordinatesColumnsBatch = originalCommit;
		}
		expect(error?.message).to.equal("pinned native columns-commit failure");
		for (const hash of hashes) {
			expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
			expect(backboneHas(log, hash)).to.be.false;
		}
	});
});

describe("coordinate persistence delete ordering pins", () => {
	it("P3: forgets native mirrors and the resident cache before the index delete, and re-checks ownership after", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const db = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false, setup },
			});
			const log = db.log as any;
			const internals = coordinateInternals(log);
			const hash = "coordinate-delete-ordering-pin";
			const events: string[] = [];
			const coordinateIndex = log.entryCoordinatesIndex;
			const hadDelIdsNoReturn = Object.prototype.hasOwnProperty.call(
				coordinateIndex,
				"delIdsNoReturn",
			);
			const originalDelIdsNoReturn = coordinateIndex.delIdsNoReturn;
			try {
				log._residentEntryCoordinatesByHash = new Map([
					[hash, { hash } as any],
				]);
				log._nativeSharedLogState = {
					deleteEntryCoordinatesBatch: () => events.push("native-state"),
				};
				log._nativeBackbone = {
					deleteEntryCoordinatesBatch: () => events.push("native-backbone"),
				};
				coordinateIndex.delIdsNoReturn = async (_values: string[]) => {
					// Both native mirrors and the resident cache must already be
					// forgotten when the index delete runs.
					expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
					events.push("index-delete");
				};

				await internals.deleteCoordinatesForHashes([hash]);
				expect(events).to.deep.equal([
					"native-state",
					"native-backbone",
					"index-delete",
				]);

				// The ownership lifecycle is re-checked AFTER the index delete
				// settles: a lifecycle stopped while the delete was in flight
				// must reject the caller.
				const controller = log.captureReplicationOwnershipLifecycle();
				coordinateIndex.delIdsNoReturn = async (_values: string[]) => {
					log.stopRepairLifecycle();
				};
				let error: any;
				try {
					await internals.deleteCoordinatesForHashes([hash], controller);
				} catch (caught) {
					error = caught;
				}
				expect(error?.message).to.match(
					/Replication ownership lifecycle is no longer active/,
				);
			} finally {
				if (hadDelIdsNoReturn) {
					coordinateIndex.delIdsNoReturn = originalDelIdsNoReturn;
				} else {
					delete coordinateIndex.delIdsNoReturn;
				}
				log._nativeSharedLogState = undefined;
				log._nativeBackbone = undefined;
				log._residentEntryCoordinatesByHash = undefined;
			}
		} finally {
			await session.stop();
		}
	});
});
