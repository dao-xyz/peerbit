import { EntryV0, NO_ENCODING } from "@peerbit/log";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

const payload = (value: string) =>
	new TextEncoder().encode(JSON.stringify({ op: "ADD", value }));

describe("native prepare failure ownership", function () {
	this.timeout(120_000);
	let client: Peerbit | undefined;
	let directory: string | undefined;
	const probes = sinon.createSandbox();

	afterEach(async () => {
		probes.restore();
		await client?.stop();
		client = undefined;
		if (directory) await fs.rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	const open = async (replicate: false | { factor: number }) => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-native-prepare-owner-"),
		);
		client = await Peerbit.create({ directory, ...createRustPeerbitOptions() });
		const store = await client.open(new EventStore<string, any>(), {
			args: { replicate, timeUntilRoleMaturity: 0 },
		});
		const shared = store.log as any;
		expect(shared._nativeBackbone, "real native backbone").to.exist;
		expect(
			shared._nativeBackboneCoordinatePersistenceStore,
			"durable intent store",
		).to.exist;
		return { store, shared };
	};

	const append = (shared: any, value: string, properties: object = {}) =>
		Promise.resolve().then(() =>
			shared.appendLocallyPreparedPayloadCommitOnly(
				payload(value),
				{ target: "none", replicate: false, meta: { next: [] } },
				{
					resolveTrimmedEntries: false,
					skipMissingNextJoin: true,
					...properties,
				},
			),
		);

	const waitForBoundary = async (
		boundary: Promise<void>,
		operation: Promise<unknown>,
	) => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				boundary,
				operation.then(() => {
					throw new Error("Operation skipped the ownership boundary");
				}),
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error("Ownership boundary was not reached")),
						10_000,
					);
				}),
			]);
		} finally {
			clearTimeout(timer);
		}
	};

	it("does not restore a document snapshot after initial intent failure released its owner", async () => {
		const { store, shared } = await open(false);
		const backbone = shared._nativeBackbone;
		// Schema IR: version 1, untagged struct, one u32 field named value.
		backbone.configureDocumentSchemaIr(
			Uint8Array.from([
				1, 14, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 118, 97, 108, 117, 101, 1, 0,
				0, 0, 101, 0, 0, 0, 3,
			]),
		);
		const key = "same-document";
		const previous = Uint8Array.of(1, 0, 0, 0);
		const newer = Uint8Array.of(2, 0, 0, 0);
		backbone.putDocumentEncodedPartsStored(key, previous, new Uint8Array());
		probes
			.stub(shared._coordinates, "canUseBackboneOnlyCoordinatePersistence")
			.returns(false);
		const restore = probes.spy(shared, "restoreNativeBackboneDocument");
		const prepare = probes.spy(
			backbone.graph,
			"prepareEntryV0PlainEntryCommit",
		);
		const initialIntent = pDefer<void>();
		const rejectIntent = pDefer<void>();
		const failure = new Error("injected initial intent failure");
		probes
			.stub(shared, "writeNativeStrictDurableTransactionIntent")
			.callsFake(async () => {
				initialIntent.resolve();
				await rejectIntent.promise;
				throw failure;
			});
		let independent: Promise<unknown> | undefined;
		const begin = shared.beginNativeStrictDurableTransaction.bind(shared);
		probes
			.stub(shared, "beginNativeStrictDurableTransaction")
			.callsFake(async (...args: unknown[]) => {
				try {
					return await begin(...args);
				} catch (error) {
					// Force the legal ordering: begin released ownership, the queued
					// mutation commits, then the failed append's outer catch resumes.
					await independent;
					throw error;
				}
			});
		const appending = append(shared, "failed", {
			nativeBackboneDocumentIndex: {
				key,
				valuePrefixBytes: new Uint8Array(),
				byteElementIndexLimit: 0,
			},
		});
		const outcome = appending.then(
			() => undefined,
			(error: unknown) => error,
		);
		try {
			await waitForBoundary(initialIntent.promise, appending);
			independent = Promise.resolve(
				shared._coordinates.withCoordinateMutationOwner(
					["independent-document-entry"],
					() =>
						backbone.putDocumentEncodedPartsStored(
							key,
							newer,
							new Uint8Array(),
						),
				),
			);
			void independent.catch(() => {});
			expect(backbone.documentValueBytes(key)).to.deep.equal(previous);
			rejectIntent.resolve();
			expect(await outcome).to.equal(failure);
			await independent;
			expect(prepare.callCount, "no native prepare ran").to.equal(0);
			expect(restore.callCount, "no restore after lease release").to.equal(0);
			expect(backbone.documentValueBytes(key)).to.deep.equal(newer);
			expect(store.log.log.length).to.equal(0);
		} finally {
			rejectIntent.resolve();
			await Promise.allSettled([
				appending,
				...(independent ? [independent] : []),
			]);
		}
	});

	it("keeps deferred recovery exclusive and poisoned through coordinate reconciliation", async () => {
		const { shared } = await open({ factor: 1 });
		await append(shared, "seed");
		shared._nativeStrictDurableDocumentRecoveryDeferred = true;
		shared.poisonNativeStrictDurableTransaction(
			new Error("injected recovery requirement"),
		);
		const reconcileStarted = pDefer<void>();
		const releaseReconcile = pDefer<void>();
		const reconcile =
			shared.reconcileNativeCoordinatesWithLowerCommitMarkers.bind(shared);
		const reconciliation = probes
			.stub(shared, "reconcileNativeCoordinatesWithLowerCommitMarkers")
			.callsFake(async (owner: any) => {
				shared.log.entryIndex.assertHashMutationLocks(owner, [
					"recovery-owned-hash",
				]);
				expect(
					() => shared.throwIfNativeDurableCommitFailed(),
					"poison remains until reconciliation completes",
				).to.throw();
				reconcileStarted.resolve();
				await releaseReconcile.promise;
				return reconcile(owner);
			});
		const recovering =
			shared.finishNativeStrictDurableDocumentRecovery() as Promise<void>;
		void recovering.catch(() => {});
		try {
			await waitForBoundary(reconcileStarted.promise, recovering);
			const rejected = await append(shared, "during-recovery").then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(rejected).to.be.instanceOf(Error);
			releaseReconcile.resolve();
			await recovering;
			expect(reconciliation.callCount).to.equal(1);
			expect(shared._nativeStrictDurableDocumentRecoveryDeferred).to.equal(
				false,
			);
			expect(() => shared.throwIfNativeDurableCommitFailed()).not.to.throw();
			expect(await append(shared, "after-recovery")).to.exist;
		} finally {
			releaseReconcile.resolve();
			await Promise.allSettled([recovering]);
		}
	});

	it("does not manufacture a retained lower marker during close and preserves a pending parent on reopen", async () => {
		const { store, shared } = await open({ factor: 1 });
		const lower = store.log.log;
		const index = lower.entryIndex;
		const { minReplicasData } = shared.createAppendReplicaMetadata(undefined);
		const parent = await EntryV0.create({
			store: lower.blocks,
			identity: client!.identity,
			encoding: NO_ENCODING,
			data: payload("committed-pending-parent"),
			meta: { next: [], data: minReplicasData },
			deferStore: true,
		});
		await index.put(parent, {
			unique: true,
			isHead: true,
			toMultiHash: true,
			deferIndexWrite: true,
		});
		// Keep the fixture pending until the native transaction snapshots it; the
		// terminal flush under test still runs normally.
		(index as any).clearPendingIndexFlushTimer();
		expect(await index.properties.index.getSize()).to.equal(0);
		const coordinateFailure = new Error(
			"coordinate flush before marker failed",
		);
		const rollbackFailure = new Error("rollback marker write failed");
		const coordinateFlush = probes
			.stub(shared._coordinates, "flushNativeBackboneCoordinateJournal")
			.rejects(coordinateFailure);
		const rollback =
			shared.markNativeStrictDurableTransactionRollback.bind(shared);
		const writeIntent =
			shared.writeNativeStrictDurableTransactionIntent.bind(shared);
		let rollbackWrite = false;
		probes
			.stub(shared, "markNativeStrictDurableTransactionRollback")
			.callsFake(async (...args: unknown[]) => {
				rollbackWrite = true;
				return rollback(...args);
			});
		probes
			.stub(shared, "writeNativeStrictDurableTransactionIntent")
			.callsFake((...args: unknown[]) => {
				if (rollbackWrite) throw rollbackFailure;
				return writeIntent(...args);
			});
		const prepare = probes.spy(
			shared._nativeBackbone,
			"preparePlainCommittedStorageAppendTransaction",
		);
		const committedHashes = new Set<string>();
		const error = await Promise.resolve()
			.then(() =>
				shared.appendLocallyPreparedPayloadCommitOnly(
					payload("unacknowledged-child"),
					{ target: "none", replicate: false, meta: { next: [parent] } },
					{
						resolveTrimmedEntries: false,
						skipMissingNextJoin: true,
						localCommitEvidence: { committedHashes },
					},
				),
			)
			.catch((error: unknown) => error);
		expect(error).to.be.instanceOf(AggregateError);
		expect((error as AggregateError).errors)
			.to.include(coordinateFailure)
			.and.include(rollbackFailure);
		expect(prepare.calledOnce).to.equal(true);
		const childHash =
			prepare.returnValues[0].entry.cid ?? prepare.returnValues[0].entry.hash;
		expect(childHash).to.be.a("string");
		expect(committedHashes.size, "retention is not commit evidence").to.equal(
			0,
		);
		const journal = (
			await shared.loadNativeStrictDurableTransactionJournalState()
		).intent;
		expect(journal.lowerMarkerCommitted).to.equal(false);
		expect(
			journal.lowerIndexRows.find(
				(row: { hash: string }) => row.hash === parent.hash,
			),
		).to.have.property("beforePendingOnly", true);
		// The injected append failure has happened; allow ordinary close to flush
		// its journal without altering the retained lower-marker decision.
		coordinateFlush.restore();
		let durableHashesAtStop: string[] | undefined;
		const indexer = (lower as any)._indexer;
		const stop = indexer.stop.bind(indexer);
		probes.stub(indexer, "stop").callsFake(async () => {
			const rows = await index.properties.index
				.iterate({}, { shape: { hash: true } })
				.all();
			durableHashesAtStop = rows.map((row) => row.value.hash);
			return stop();
		});
		await store.close();
		expect(
			durableHashesAtStop,
			"close must not publish the retained child marker",
		).not.to.include(childHash);
		probes.restore();
		await client!.open(store, {
			args: { replicate: { factor: 1 }, timeUntilRoleMaturity: 0 },
		});
		expect(await store.log.log.has(childHash)).to.equal(false);
		expect(await store.log.log.has(parent.hash)).to.equal(true);
		expect(
			(await store.log.log.entryIndex.getShallow(parent.hash))?.value.head,
		).to.equal(true);
		expect(shared._nativeBackbone.getEntryCoordinateHashes()).not.to.include(
			childHash,
		);
		expect(
			(await shared.loadNativeStrictDurableTransactionJournalState()).intent,
		).to.equal(undefined);
		expect(await append(shared, "after-retained-recovery")).to.exist;
	});

	for (const kind of ["commit-only", "storage"] as const) {
		it(`retains recovery intent when ${kind} native preparation mutates and then throws`, async () => {
			const { store, shared } = await open(
				kind === "commit-only" ? false : { factor: 1 },
			);
			const backbone = shared._nativeBackbone;
			const target = kind === "commit-only" ? backbone.graph : backbone;
			const method =
				kind === "commit-only"
					? "prepareEntryV0PlainEntryCommit"
					: "preparePlainCommittedNoNextStorageAppendTransaction";
			const nativePrepare = target[method].bind(target);
			const failure = new Error("injected after native mutation");
			let committedHash: string | undefined;
			probes.stub(target, method).callsFake((...args: unknown[]) => {
				const result = nativePrepare(...args);
				committedHash =
					kind === "commit-only"
						? (result.cid ?? result.hash)
						: (result.entry.cid ?? result.entry.hash);
				throw failure;
			});
			const error = await append(shared, "uncertain").then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(error).to.equal(failure);
			expect(committedHash, "native operation actually ran").to.be.a("string");
			expect(store.log.log.length, "no lower commit acknowledged").to.equal(0);
			expect(
				shared._nativeStrictDurableTransactionFailure,
				"subsequent mutations fail closed",
			).to.exist;
			expect(
				(await shared.loadNativeStrictDurableTransactionJournalState()).intent,
				"uncertain intent retained",
			).to.exist;
			const rejected = await append(shared, "later").then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(rejected).to.be.instanceOf(Error);
			probes.restore();
			await store.close();
			await client!.open(store, {
				args: {
					replicate: kind === "commit-only" ? false : { factor: 1 },
					timeUntilRoleMaturity: 0,
				},
			});
			expect(store.log.log.length).to.equal(0);
			expect(shared._nativeBackbone.graph.has(committedHash)).to.equal(false);
			expect(shared._nativeBackbone.getEntryCoordinateHashes()).not.to.include(
				committedHash,
			);
			expect(
				shared._coordinates._residentEntryCoordinatesByHash?.has(committedHash),
			).not.to.equal(true);
			const afterRecovery = await append(shared, "after-recovery");
			expect(afterRecovery).to.exist;
			expect(await store.log.log.has(afterRecovery.appendCommit.hash)).to.equal(
				true,
			);
		});
	}
});
