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
// The probes reach through the retained state accessors under the historical
// field names and resolve persistence methods directly on their coordinator.
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
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

const coordinateInternals = (log: any): any => log._coordinates;

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
		let result =
			coordinateInternals(log).flushNativeBackboneCoordinateJournal();
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
		const onAppend =
			coordinateInternals(log).flushNativeBackboneCoordinateJournalOnAppend();
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
		const result =
			coordinateInternals(log).flushNativeBackboneCoordinateJournalOnAppend();
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

	const withSnapshot = async (
		log: any,
		hash: string,
		operation: (snapshot: any, owner: any) => Promise<void>,
	) => {
		const internals = coordinateInternals(log);
		const index = log.log.entryIndex;
		const owner = await index.acquireHashMutationLocks([hash]);
		let snapshot: any;
		try {
			snapshot = internals.snapshotResidentCoordinateEntries([hash], owner);
			await operation(snapshot, owner);
		} finally {
			internals.settleResidentCoordinateSnapshot(snapshot);
			index.releaseHashMutationLocks(owner);
		}
	};

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
		await withSnapshot(log, hash, async (snapshot, owner) => {
			expect(snapshot.entries.has(hash)).to.be.false;

			// Simulate the failed append's partial work: coordinates persisted to
			// the index, the resident mirror, and the native backbone.
			await internals.persistCoordinate(
				{
					coordinates,
					entry,
					leaders: false,
					replicas: 1,
				},
				undefined,
				owner,
			);
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
		});

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
		await withSnapshot(log, hash, async (snapshot, owner) => {
			expect(snapshot.entries.has(hash)).to.be.true;
			await internals.deleteCoordinatesForHashes([hash], undefined, owner);
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
			const stale = internals.snapshotResidentCoordinateEntries([hash], owner);
			const newer = internals.snapshotResidentCoordinateEntries([hash], owner);
			try {
				await internals.deleteCoordinatesForHashes([hash], undefined, owner);
				await internals.rollbackNativeBackboneCoordinateAppendDurably(
					hash,
					stale,
				);
				expect(await countIndexed(log, hash)).to.equal(0);
				expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
				expect(backboneHas(log, hash)).to.be.false;
			} finally {
				internals.settleResidentCoordinateSnapshot(stale);
				internals.settleResidentCoordinateSnapshot(newer);
			}
		});
	});

	for (const mutation of ["persist", "delete"] as const) {
		it(`serializes an independent generic ${mutation} after the owning rollback`, async () => {
			const log = store!.log as any;
			const internals = coordinateInternals(log);
			const entry = (
				await store!.add(`p2-independent-${mutation}`, { meta: { next: [] } })
			).entry;
			const hash = entry.hash;
			const coordinates = await log.createCoordinates(entry, 1);
			if (mutation === "persist") {
				await internals.deleteCoordinatesForHashes([hash]);
			}
			const index = log.log.entryIndex;
			const owner = await index.acquireHashMutationLocks([hash]);
			let stale: any;
			let acquire: sinon.SinonSpy | undefined;
			let independent: Promise<unknown> | undefined;
			let completed = false;
			try {
				stale = internals.snapshotResidentCoordinateEntries([hash], owner);
				// This scope has already made partial changes that must be undone.
				if (mutation === "persist") {
					await internals.persistCoordinate(
						{ coordinates, entry, leaders: false, replicas: 1 },
						undefined,
						owner,
					);
				} else {
					await internals.deleteCoordinatesForHashes([hash], undefined, owner);
				}
				acquire = sinon.spy(index, "acquireHashMutationLocks");
				// No borrowed owner: this independent operation must queue, not
				// enter the rollback scope or require a second snapshot.
				independent = Promise.resolve(
					mutation === "persist"
						? internals.persistCoordinate({
								coordinates,
								entry,
								leaders: false,
								replicas: 1,
							})
						: internals.deleteCoordinatesForHashes([hash]),
				).then(() => {
					completed = true;
				});
				expect(
					acquire.calledOnce,
					"independent operation requests its own owner",
				).to.be.true;
				await Promise.resolve();
				expect(completed, "independent operation waits for rollback").to.be
					.false;
				await internals.rollbackNativeBackboneCoordinateAppendDurably(
					hash,
					stale,
				);
				expect(completed, "rollback retains ownership through durable flush").to
					.be.false;
				expect(await countIndexed(log, hash)).to.equal(
					mutation === "delete" ? 1 : 0,
				);
			} finally {
				internals.settleResidentCoordinateSnapshot(stale);
				index.releaseHashMutationLocks(owner);
				acquire?.restore();
				await independent;
			}
			const present = mutation === "persist";
			expect(
				await countIndexed(log, hash),
				"independent mutation committed after rollback",
			).to.equal(present ? 1 : 0);
			expect(log._residentEntryCoordinatesByHash.has(hash)).to.equal(present);
			expect(backboneHas(log, hash)).to.equal(present);
			expect(() =>
				internals.rollbackNativeBackboneCoordinateAppend(hash, stale),
			).to.throw();
			expect(log._residentEntryCoordinatesByHash.has(hash)).to.equal(present);
			expect(backboneHas(log, hash)).to.equal(present);
		});
	}

	it("rejects snapshot and rollback after the captured owner was released", async () => {
		const log = store!.log as any;
		const internals = coordinateInternals(log);
		const entry = (
			await store!.add("released-snapshot-owner", { meta: { next: [] } })
		).entry;
		const snapshot = await internals.withCoordinateMutationOwner(
			[entry.hash],
			(owner: any) =>
				internals.snapshotResidentCoordinateEntries([entry.hash], owner),
		);
		try {
			await internals.deleteCoordinatesForHashes([entry.hash]);
			expect(() =>
				internals.snapshotResidentCoordinateEntries(
					[entry.hash],
					snapshot.owner,
				),
			).to.throw();
			const error = await internals
				.rollbackNativeBackboneCoordinateAppendDurably(entry.hash, snapshot)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(
				error,
				"released owner cannot restore an old before-image",
			).to.be.instanceOf(Error);
			expect(await countIndexed(log, entry.hash)).to.equal(0);
			expect(log._residentEntryCoordinatesByHash.has(entry.hash)).to.be.false;
			expect(backboneHas(log, entry.hash)).to.be.false;
		} finally {
			internals.settleResidentCoordinateSnapshot(snapshot);
		}
	});

	it("batches parent-coordinate deletion while preserving mixed native commit flags", async () => {
		await store!.close();
		store = await client!.open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				nativeGraph: true,
				nativeBackbone: { optional: false },
				nativeRangePlanner: { optional: false },
			},
		});
		const log = store!.log as any;
		const internals = coordinateInternals(log);
		const parents = [
			(await store!.add("batch-parent-a", { meta: { next: [] } })).entry,
			(await store!.add("batch-parent-b", { meta: { next: [] } })).entry,
		];
		const children = [
			(await store!.add("batch-child-a", { meta: { next: [parents[0]!] } }))
				.entry,
			(await store!.add("batch-child-b", { meta: { next: [parents[1]!] } }))
				.entry,
		];
		const makeItem = async (entry: (typeof parents)[number]) => ({
			entry,
			coordinates: await log.createCoordinates(entry, 1),
			leaders: false,
			replicas: 1,
		});
		await internals.deleteCoordinatesForHashes(
			[...parents, ...children].map((entry) => entry.hash),
		);
		await internals.persistCoordinatesBatch(
			await Promise.all(parents.map(makeItem)),
		);
		const items = await Promise.all(children.map(makeItem));
		const index = log.entryCoordinatesIndex;
		const nativeState = log._nativeSharedLogState;
		const backbone = log._nativeBackbone;
		expect(nativeState, "real native shared-log state").to.exist;
		const probes = sinon.createSandbox();
		try {
			const batch = probes.spy(
				index,
				"putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn",
			);
			const single = probes.spy(
				index,
				"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
			);
			const nativeBatch = probes.spy(
				nativeState,
				"commitEntryCoordinatesBatch",
			);
			const nativeSingle = probes.spy(nativeState, "commitEntryCoordinates");
			const backboneBatch = probes.spy(backbone, "commitEntryCoordinatesBatch");
			const backboneSingle = probes.spy(backbone, "commitEntryCoordinates");

			expect(
				await internals.persistCoordinatesBatch([
					{ ...items[0], commitNative: false },
					{ ...items[1], commitNativeBackbone: false },
				]),
			).to.deep.equal([true, true]);
			expect(batch.callCount).to.equal(1);
			expect(single.callCount).to.equal(0);
			expect(
				batch.firstCall.args[0].map((row: any) => ({
					hash: row.fields.hash,
					deleteHashes: row.deleteHashes,
				})),
			).to.deep.equal(
				children.map((entry, i) => ({
					hash: entry.hash,
					deleteHashes: [parents[i]!.hash],
				})),
			);
			expect(nativeBatch.callCount).to.equal(1);
			expect(
				nativeBatch.firstCall.args[0].map((row: any) => row.hash),
			).to.deep.equal([children[1]!.hash]);
			expect(backboneBatch.callCount).to.equal(1);
			expect(
				backboneBatch.firstCall.args[0].map((row: any) => row.hash),
			).to.deep.equal([children[0]!.hash]);
			expect(nativeSingle.callCount).to.equal(0);
			expect(backboneSingle.callCount).to.equal(0);
			const heads = children.map((entry) => entry.hash);
			expect(
				(await index.iterate({}).all()).map((row: any) => row.value.hash),
			).to.have.members(heads);
			expect([...log._residentEntryCoordinatesByHash.keys()]).to.have.members(
				heads,
			);
			expect(backbone.graph.heads()).to.have.members(heads);
			expect(nativeState.getEntryCoordinateHashes()).to.have.members([
				parents[0]!.hash,
				children[1]!.hash,
			]);
			expect(backbone.getEntryCoordinateHashes()).to.have.members([
				children[0]!.hash,
				parents[1]!.hash,
			]);
		} finally {
			probes.restore();
		}
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
