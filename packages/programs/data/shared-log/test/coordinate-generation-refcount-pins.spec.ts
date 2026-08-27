// Pins G1-G8 for the hold-counted `_nativeCoordinateMutationGenerations` map.
//
// The map used to grow one row per distinct entry hash ever committed or
// received, forever, with no removal path anywhere. It is now refcounted:
// `snapshotResidentCoordinateEntries` takes one hold per hash per rollback
// token, and `settleResidentCoordinateSnapshot` releases it at every point
// where the token can no longer be rolled back, deleting the row at zero.
//
// The safety asymmetry these pins are built around: a MISSED settle only
// retains a row (the pre-refcount behavior, never a regression), while a
// PREMATURE settle deletes a row a live token still needs — which silently
// turns that token's rollback into a no-op, or, once the hash's generation
// numbering restarts from 1, lets a stale token clobber newer state (ABA).
// So G3, G4 and G7 carry the real weight: they assert that rollbacks that are
// still OWED continue to fire, and that supersession is still detected. G3 and
// G7 pin that shape at two different settle families — the single-append
// storage-transaction seam and the batch seam.
//
// The rollback tokens are minted only on the native-backbone local-append,
// batch-append and receive paths. The local-append paths are reached through
// the prepared payload commit-only entry point (`target: "none"`), which is
// how the Documents program appends; a non-replicating log routes it to the
// commit-only variant and a replicating log to the storage-transaction
// variant, so both local seams are covered here. The batch seam is reached
// through the prepared-payloads many-independent entry point (what
// `Documents.putMany` uses) and is covered by G7 and G8.
//
// G8 is a different kind of pin from the rest: G1-G7 pin that the settle is
// CORRECT where it runs, G8 measures how much of the map it actually covers,
// by counting both sides of the hold ledger over a mixed workload.
//
// Every assertion below is a raw value — map `.size`, `.holds`, `.generation`,
// coordinate-index counts, resident-mirror presence, native-backbone presence.
// No ratios, no identity predicates. Where a leg asserts a no-op, the states
// either side of it are made visibly different first, per the teeth rule
// stated in coordinate-persistence-pins.spec.ts (P2).
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";
import { EventStore } from "./utils/stores/event-store.js";

type GenerationRow = { generation: number; holds: number };

const coordinateInternals = (log: any): any => log._coordinates;

/** Raw map. Only safe once the mechanism is known to have run. */
const generationRows = (log: any): Map<string, GenerationRow> =>
	coordinateInternals(log)._nativeCoordinateMutationGenerations;

/** Raw row count, treating a never-created map as zero rows. */
const generationRowCount = (log: any): number =>
	coordinateInternals(log)._nativeCoordinateMutationGenerations?.size ?? 0;

const countIndexed = async (log: any, hash: string): Promise<number> =>
	log.entryCoordinatesIndex.count({ query: { hash } });

const backboneHas = (log: any, hash: string): boolean =>
	[...(log._nativeBackbone.getEntryCoordinateHashes() as string[])].includes(
		hash,
	);

const payload = (value: string): Uint8Array =>
	new TextEncoder().encode(JSON.stringify({ op: "PUT", value }));

/**
 * Drive one local append down the native-backbone prepared-payload path (the
 * same entry point the Documents program uses).
 *
 * `meta.next: []` is load-bearing: the lower log only takes the native
 * commit-only route for an entry with no explicit nexts, so without it every
 * append after the first falls back to the generic path, mints no rollback
 * token, and the bound pins below would assert an empty map that was never
 * filled.
 */
const nativeAppend = (log: any, value: string) =>
	log.appendLocallyPreparedPayloadCommitOnly(
		payload(value),
		{ target: "none", replicate: false, meta: { next: [] } },
		undefined,
	);

const writeU32 = (out: number[], value: number) => {
	out.push(
		value & 0xff,
		(value >> 8) & 0xff,
		(value >> 16) & 0xff,
		value >>> 24,
	);
};

const writeString = (out: number[], value: string) => {
	const bytes = new TextEncoder().encode(value);
	writeU32(out, bytes.byteLength);
	out.push(...bytes);
};

/**
 * The minimal native document schema IR (a document whose only field group is
 * the five `__context` fields), byte-identical to the one the native-backbone
 * suite and the document-put benchmark use.
 *
 * The batch append seam refuses to run without a configured schema IR, because
 * it always commits a native document-index row alongside the entry. The
 * Documents program configures this from its own indexed schema; a shared-log
 * pin has no Documents program, so it configures the context-only IR directly
 * and commits an empty value prefix (again exactly as the benchmark does).
 */
const contextOnlyDocumentSchemaIr = (): Uint8Array => {
	const out: number[] = [1, 14];
	writeU32(out, 1);
	out.push(0);
	writeU32(out, 5);
	writeString(out, "created");
	writeU32(out, 1);
	writeU32(out, 101);
	out.push(4);
	writeString(out, "modified");
	writeU32(out, 2);
	writeU32(out, 102);
	out.push(4);
	writeString(out, "head");
	writeU32(out, 3);
	writeU32(out, 103);
	out.push(12);
	writeString(out, "gid");
	writeU32(out, 4);
	writeU32(out, 104);
	out.push(12);
	writeString(out, "size");
	writeU32(out, 5);
	writeU32(out, 105);
	out.push(3);
	return Uint8Array.from(out);
};

/** Arm a live log's native backbone for native document-index commits. */
const enableNativeDocumentIndex = (log: any) => {
	log._nativeBackbone.configureDocumentSchemaIr(contextOnlyDocumentSchemaIr());
	log._nativeBackbone.setDocumentContextFields?.({
		created: 1,
		modified: 2,
		head: 3,
		gid: 4,
		size: 5,
	});
	log._nativeBackbone.setDocumentContextHeadField?.(3);
};

/**
 * Drive ONE native-backbone batch append (the
 * `appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch` seam,
 * which is what `Documents.putMany` reaches). The whole batch mints exactly
 * one rollback token covering every entry hash in it.
 *
 * `target: "none"` and an absent `replicate` are load-bearing: the batch seam
 * bails to the generic path for any other target, for `replicate: true`, for a
 * set `delivery`, and for non-empty prepared nexts.
 */
const nativeBatchAppend = (log: any, label: string, size: number) =>
	log.appendLocallyPreparedPayloadsManyIndependent(
		Array.from({ length: size }, (_, index) => payload(`${label}-${index}`)),
		{ target: "none" },
		{
			nativeBackboneDocumentIndexes: Array.from(
				{ length: size },
				(_, index) => ({
					key: `${label}-key-${index}`,
					valuePrefixBytes: new Uint8Array(0),
					byteElementIndexLimit: 0,
				}),
			),
		},
	);

/**
 * Wrap `snapshotResidentCoordinateEntries` on a live coordinator so a pin can
 * prove the writer actually ran (an empty map is only a bound if rows were
 * really created) and observe the largest raw size the map ever reached.
 */
const instrumentSnapshots = (log: any) => {
	const internals = coordinateInternals(log);
	const original = internals.snapshotResidentCoordinateEntries.bind(internals);
	const state = { calls: 0, maxSize: 0, tokens: [] as any[] };
	internals.snapshotResidentCoordinateEntries = (hashes: Iterable<string>) => {
		const token = original(hashes);
		state.calls++;
		if (token) {
			state.tokens.push(token);
		}
		state.maxSize = Math.max(
			state.maxSize,
			internals._nativeCoordinateMutationGenerations?.size ?? 0,
		);
		return token;
	};
	return {
		state,
		restore: () => {
			internals.snapshotResidentCoordinateEntries = original;
		},
	};
};

/**
 * Count both sides of the hold ledger on a live coordinator: every token the
 * writer mints, and every token the settle actually consumes.
 *
 * Already-`settled` tokens are counted separately from consumed ones — a
 * second settle of the same token is a no-op by design (G5), so counting it as
 * a release would inflate the released side and hide a real leak.
 */
const instrumentSettleBalance = (log: any) => {
	const internals = coordinateInternals(log);
	const originalSnapshot =
		internals.snapshotResidentCoordinateEntries.bind(internals);
	const originalSettle =
		internals.settleResidentCoordinateSnapshot.bind(internals);
	const state = {
		tokensCreated: 0,
		holdsTaken: 0,
		tokensSettled: 0,
		holdsReleased: 0,
		redundantSettles: 0,
	};
	internals.snapshotResidentCoordinateEntries = (hashes: Iterable<string>) => {
		const token = originalSnapshot(hashes);
		if (token) {
			state.tokensCreated++;
			state.holdsTaken += token.hashes.size;
		}
		return token;
	};
	internals.settleResidentCoordinateSnapshot = (rollback?: any) => {
		if (rollback) {
			if (rollback.settled) {
				state.redundantSettles++;
			} else {
				state.tokensSettled++;
				state.holdsReleased += rollback.hashes.size;
			}
		}
		return originalSettle(rollback);
	};
	return {
		state,
		restore: () => {
			internals.snapshotResidentCoordinateEntries = originalSnapshot;
			internals.settleResidentCoordinateSnapshot = originalSettle;
		},
	};
};

describe("coordinate persistence mutation-generation refcount", () => {
	// G5 and the raw refcount mechanics need nothing but the coordinator, so
	// they run as unit pins against a bare instance.
	let log: any;

	beforeEach(() => {
		log = new SharedLog();
	});

	it("G5: settling a token twice is a no-op and cannot consume another token's hold", () => {
		const internals = coordinateInternals(log);
		const hash = "g5-shared-hash";

		const tokenA = internals.snapshotResidentCoordinateEntries([hash]);
		const tokenB = internals.snapshotResidentCoordinateEntries([hash]);
		const rows = generationRows(log);
		// Two tokens hold the same hash; the generation ratcheted twice.
		expect(rows.get(hash)?.holds).to.equal(2);
		expect(rows.get(hash)?.generation).to.equal(2);
		expect(tokenA.generations.get(hash)).to.equal(1);
		expect(tokenB.generations.get(hash)).to.equal(2);

		internals.settleResidentCoordinateSnapshot(tokenA);
		expect(rows.get(hash)?.holds).to.equal(1);
		expect(rows.get(hash)?.generation).to.equal(2);

		// The second settle of the SAME token must change nothing. Without the
		// `settled` guard it would consume token B's hold and delete a row B
		// still needs.
		internals.settleResidentCoordinateSnapshot(tokenA);
		expect(rows.has(hash), "token B's row must survive A's double settle").to
			.be.true;
		expect(rows.get(hash)?.holds).to.equal(1);

		// B is the last holder: its settle is what deletes the row.
		internals.settleResidentCoordinateSnapshot(tokenB);
		expect(rows.has(hash)).to.be.false;
		expect(rows.size).to.equal(0);
	});

	it("G5: settling is per-hash, so unrelated hashes keep their holds", () => {
		const internals = coordinateInternals(log);
		const shared = "g5-shared";
		const onlyA = "g5-only-a";
		const onlyB = "g5-only-b";

		const tokenA = internals.snapshotResidentCoordinateEntries([shared, onlyA]);
		internals.snapshotResidentCoordinateEntries([shared, onlyB]);
		const rows = generationRows(log);
		expect(rows.size).to.equal(3);

		internals.settleResidentCoordinateSnapshot(tokenA);
		expect(rows.size).to.equal(2);
		expect(rows.has(onlyA)).to.be.false;
		expect(rows.get(shared)?.holds).to.equal(1);
		expect(rows.get(onlyB)?.holds).to.equal(1);
	});
});

describe("coordinate persistence mutation-generation bounds", function () {
	this.timeout(240_000);

	let client: Peerbit | undefined;
	let directory: string | undefined;
	let store: EventStore<string, any> | undefined;

	const openStore = async (args: any) => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-coordinate-generation-pins-"),
		);
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});
		store = await client.open(new EventStore<string, any>(), { args });
		const log = store.log as any;
		// These pins are only meaningful against the native backbone coordinate
		// stack: the rollback tokens are only minted there.
		expect(log._nativeBackbone, "native backbone").to.exist;
		expect(log._residentEntryCoordinatesByHash, "resident mirror").to.exist;
		return log;
	};

	afterEach(async () => {
		await client?.stop();
		client = undefined;
		store = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	const appendCount = 200;

	const boundedLocalAppendLeg = async (replicate: any, label: string) => {
		const log = await openStore({ replicate });
		const probe = instrumentSnapshots(log);
		try {
			for (let index = 0; index < appendCount; index++) {
				await nativeAppend(log, `${label}-${index}`);
			}
		} finally {
			probe.restore();
		}

		// Teeth: every append really minted a rollback token, and the map
		// really held rows during the run. Without this an append path that
		// silently stopped taking the native route would leave an empty map
		// and the bound below would pass for the wrong reason. Raw counts, not
		// a ratio against anything.
		expect(probe.state.calls, "snapshot tokens minted").to.be.at.least(
			appendCount,
		);
		expect(probe.state.maxSize, "rows resident during the run").to.be.at.least(
			1,
		);

		await waitForResolved(() => {
			expect(generationRowCount(log)).to.equal(0);
		});
	};

	it("G1: the raw map is empty again after 200 commit-only local appends", async () => {
		// A non-replicating log defers head coordinate persistence, which routes
		// the prepared payload append to the commit-only native variant.
		await boundedLocalAppendLeg(false, "g1-commit-only");
	});

	it("G1: the raw map is empty again after 200 storage-transaction local appends", async () => {
		// A replicating log takes the storage-transaction native variant.
		await boundedLocalAppendLeg({ factor: 1 }, "g1-storage-transaction");
	});

	const owedRollbackLeg = async (replicate: any, label: string) => {
		const log = await openStore({ replicate });
		const probe = instrumentSnapshots(log);
		const gidHistoryCleanup = sinon.spy(
			log as any,
			"scheduleDeadGidPeerHistoryReclaim",
		);

		// Fault injection: reject the durable lower-marker write, which runs
		// INSIDE `nativeCommittedAppendFinalizer.acknowledge` — i.e. after the
		// native coordinates are committed and before the success seam.
		const originalMarker =
			log.markNativeStrictDurableTransactionLowerMarker.bind(log);
		let injectionsFired = 0;
		let committedAtFailure: string[] = [];
		let armed = true;
		log.markNativeStrictDurableTransactionLowerMarker = async (
			...args: any[]
		) => {
			if (armed) {
				armed = false;
				injectionsFired++;
				// Snapshot the state the rollback is owed against: the native
				// coordinates are live right now, so the post-rollback probes
				// below are asserting a visibly DIFFERENT state.
				committedAtFailure = [...tokenHashes(probe.state.tokens)].filter(
					(hash) => backboneHas(log, hash),
				);
				throw new Error("pinned lower-marker durability failure");
			}
			return originalMarker(...args);
		};

		let error: any;
		try {
			await nativeAppend(log, `${label}-doomed`);
		} catch (caught) {
			error = caught;
		} finally {
			log.markNativeStrictDurableTransactionLowerMarker = originalMarker;
			probe.restore();
			gidHistoryCleanup.restore();
		}

		// The injection must actually have fired, and it must have fired while
		// the native coordinates were committed; a dead injection would make
		// every assertion below pass for the wrong reason.
		expect(injectionsFired, "fault injection fired").to.equal(1);
		expect(
			gidHistoryCleanup.callCount,
			"gid history cleanup must stay behind the strict success seam",
		).to.equal(0);
		expect(error, "the append must fail").to.exist;
		expect(
			committedAtFailure.length,
			"native coordinates were live when the append failed",
		).to.be.greaterThan(0);

		// The rollback was still owed and must have fully erased the append.
		// If the settle had run one step early (before the acknowledge await),
		// the row would be gone from the map, the generation gate would read
		// `undefined`, the rollback would silently no-op, and these three raw
		// probes would show phantom coordinates.
		for (const hash of committedAtFailure) {
			expect(await countIndexed(log, hash), `indexed ${hash}`).to.equal(0);
			expect(
				log._residentEntryCoordinatesByHash.has(hash),
				`resident ${hash}`,
			).to.be.false;
			expect(backboneHas(log, hash), `backbone ${hash}`).to.be.false;
		}
		expect(generationRowCount(log)).to.equal(0);
	};

	const tokenHashes = (tokens: any[]): Set<string> => {
		const hashes = new Set<string>();
		for (const token of tokens) {
			for (const hash of token.hashes as Set<string>) {
				hashes.add(hash);
			}
		}
		return hashes;
	};

	// Only the storage-transaction variant is pinned here. The commit-only
	// variant runs with head coordinate persistence DEFERRED, so at the same
	// injection point no coordinate row is committed yet — the leg's own
	// "native coordinates were live when the append failed" teeth assertion
	// fails there, which is why it is deliberately not run.
	it("G3: an owed rollback still fires after the success seam was added", async () => {
		await owedRollbackLeg({ factor: 1 }, "g3-storage-transaction");
	});

	// G7 pins the BATCH settle family. G3 covers the storage-transaction
	// single-append family; the batch family is the one whose success settle is
	// placed EARLIEST relative to the end of its function — it fires the moment
	// the try/catch around the coordinate persistence loop is left, ~60 lines
	// and one `throwIfReplicationOwnershipLifecycleInactive` before the method
	// returns — so it is the settle most exposed to a future edit that moves a
	// rollback consumer below it. `rollbackBatch` (the catch arm) is that
	// consumer today.
	const batchSize = 3;

	it("G7: an owed rollback still fires at the batch success seam", async () => {
		const log = await openStore({ replicate: { factor: 1 } });
		enableNativeDocumentIndex(log);

		// Half one: the success side of the seam. A clean batch append must
		// settle its token, so the map is empty again with nothing owed.
		const successProbe = instrumentSnapshots(log);
		let succeeded: any;
		try {
			succeeded = await nativeBatchAppend(log, "g7-batch-ok", batchSize);
		} finally {
			successProbe.restore();
		}
		expect(succeeded?.entries?.length, "batch committed").to.equal(batchSize);
		// Teeth: exactly ONE token for the whole batch is the batch seam's
		// signature. A fallback to the per-entry seam would mint `batchSize`
		// tokens (or none), so this is what keeps the pin honest about which
		// settle family it is exercising.
		expect(successProbe.state.calls, "one token for the whole batch").to.equal(
			1,
		);
		expect(
			successProbe.state.tokens[0]?.hashes.size,
			"the token covers every entry in the batch",
		).to.equal(batchSize);
		expect(successProbe.state.maxSize, "rows resident mid-batch").to.equal(
			batchSize,
		);
		expect(
			generationRowCount(log),
			"success settle released the rows",
		).to.equal(0);

		// Half two: the failure side. Fault-inject the durable lower-marker
		// write, which runs inside `nativeCommittedAppendFinalizer.acknowledge`
		// — i.e. after the batch's native coordinates are committed and inside
		// the try whose catch calls `rollbackBatch`, so a rollback is genuinely
		// owed at the injection point.
		const failProbe = instrumentSnapshots(log);
		const originalMarker =
			log.markNativeStrictDurableTransactionLowerMarker.bind(log);
		let injectionsFired = 0;
		let committedAtFailure: string[] = [];
		let residentAtFailure: string[] = [];
		let armed = true;
		log.markNativeStrictDurableTransactionLowerMarker = async (
			...args: any[]
		) => {
			if (armed) {
				armed = false;
				injectionsFired++;
				// Snapshot the state the rollback is owed against, so the
				// post-rollback probes below assert a visibly DIFFERENT state.
				const owed = [...tokenHashes(failProbe.state.tokens)];
				committedAtFailure = owed.filter((hash) => backboneHas(log, hash));
				residentAtFailure = owed.filter((hash) =>
					log._residentEntryCoordinatesByHash.has(hash),
				);
				throw new Error("pinned batch lower-marker durability failure");
			}
			return originalMarker(...args);
		};

		let error: any;
		try {
			await nativeBatchAppend(log, "g7-batch-doomed", batchSize);
		} catch (caught) {
			error = caught;
		} finally {
			log.markNativeStrictDurableTransactionLowerMarker = originalMarker;
			failProbe.restore();
		}

		// Teeth: the injection really fired, it fired on the batch seam (one
		// token), and it fired while the whole batch's native coordinates were
		// committed. Without these the assertions below would pass for a batch
		// that never reached the guarded path.
		expect(injectionsFired, "fault injection fired").to.equal(1);
		expect(error, "the batch append must fail").to.exist;
		expect(failProbe.state.calls, "one token for the doomed batch").to.equal(1);
		expect(
			committedAtFailure.length,
			"every batch coordinate was live in the backbone at failure",
		).to.equal(batchSize);
		expect(
			residentAtFailure.length,
			"every batch coordinate was live in the resident mirror at failure",
		).to.equal(batchSize);

		// The rollback was still owed and must have erased the whole batch.
		// With the success settle moved above `rollbackBatch`'s consumer, the
		// rows are already gone, the generation gate in
		// `rollbackNativeBackboneCoordinateAppend` reads `undefined` for every
		// hash, every branch `continue`s, and these probes report exactly
		// `batchSize` phantom coordinates instead of zero (measured).
		for (const hash of committedAtFailure) {
			expect(log._residentEntryCoordinatesByHash.has(hash), `resident ${hash}`)
				.to.be.false;
			expect(backboneHas(log, hash), `backbone ${hash}`).to.be.false;
		}
		// `countIndexed` is deliberately NOT probed here: measured against this
		// seam the generic coordinate index holds 0 rows for these hashes even
		// at failure time (the batch persists into the native journal and the
		// resident mirror), so a post-rollback `countIndexed === 0` would be
		// vacuous — it passes whether or not the rollback fired. The resident
		// mirror and the native backbone are the two probes that discriminate.
		expect(generationRowCount(log)).to.equal(0);
	});

	it("G4: a superseded token stays a strict no-op, which plain delete would break", async () => {
		const log = await openStore({ replicate: { factor: 1 } });
		const internals = coordinateInternals(log);
		const entry = (await store!.add("g4", { meta: { next: [] } })).entry;
		const hash = entry.hash;
		expect(await countIndexed(log, hash)).to.equal(1);
		expect(generationRowCount(log)).to.equal(0);

		// Token A captures the persisted row as its before-image.
		const tokenA = internals.snapshotResidentCoordinateEntries([hash]);
		expect(tokenA.entries.has(hash)).to.be.true;
		// Token B supersedes it on the same hash.
		const tokenB = internals.snapshotResidentCoordinateEntries([hash]);
		const rows = generationRows(log);
		expect(rows.get(hash)?.holds).to.equal(2);
		expect(rows.get(hash)?.generation).to.equal(2);

		// Settling B must NOT drop the row: A still holds it, and the row is
		// what remembers that A has been superseded.
		internals.settleResidentCoordinateSnapshot(tokenB);
		expect(rows.has(hash), "A's hold must keep the row alive").to.be.true;
		expect(rows.get(hash)?.holds).to.equal(1);
		expect(rows.get(hash)?.generation).to.equal(2);

		// A newer mutation on the same hash. With a hold-counted row this
		// ratchets to generation 3; if settling had DELETED the row, numbering
		// would restart at 1 and collide with A's stale generation (ABA), and
		// A's rollback would fire and restore the stale row below.
		internals.snapshotResidentCoordinateEntries([hash]);
		expect(rows.get(hash)?.generation).to.equal(3);

		// Make the newer visible state differ from A's snapshot, so a rollback
		// that wrongly fires is observable rather than indistinguishable.
		await internals.deleteCoordinatesForHashes([hash]);
		expect(await countIndexed(log, hash)).to.equal(0);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
		expect(backboneHas(log, hash)).to.be.false;

		await internals.rollbackNativeBackboneCoordinateAppendDurably(hash, tokenA);

		// A is superseded: it must restore nothing.
		expect(await countIndexed(log, hash)).to.equal(0);
		expect(log._residentEntryCoordinatesByHash.has(hash)).to.be.false;
		expect(backboneHas(log, hash)).to.be.false;
	});

	it("G6: the durable-recovery replay no longer leaves rows behind", async () => {
		const log = await openStore({ replicate: { factor: 1 } });
		const internals = coordinateInternals(log);
		await store!.add("g6-warmup", { meta: { next: [] } });
		expect(generationRowCount(log)).to.equal(0);

		const coordinateHashes = ["g6-a", "g6-b", "g6-c", "g6-d", "g6-e"];
		await log.writeNativeStrictDurableTransactionIntent({
			version: 1,
			appendHashes: [],
			trimHashes: [],
			coordinateDeleteHashes: [],
			lowerIndexRows: [],
			coordinates: coordinateHashes.map((hash) => ({ hash })),
			documents: [],
		});

		// Observe the replay's own token so the pin can prove the coordinate
		// replay branch actually ran with all K hashes.
		const originalRollback =
			internals.rollbackNativeBackboneCoordinateAppendDurably.bind(internals);
		let replayedGenerations = 0;
		let replayCalls = 0;
		internals.rollbackNativeBackboneCoordinateAppendDurably = async (
			appendHash: string,
			rollback: any,
		) => {
			replayCalls++;
			replayedGenerations = Math.max(
				replayedGenerations,
				rollback?.generations?.size ?? 0,
			);
			return originalRollback(appendHash, rollback);
		};

		let recovered: boolean;
		try {
			recovered = await log.recoverNativeStrictDurableTransactionIntent(true);
		} finally {
			internals.rollbackNativeBackboneCoordinateAppendDurably =
				originalRollback;
		}

		expect(recovered, "recovery completed").to.be.true;
		expect(replayCalls, "the coordinate replay ran").to.equal(1);
		expect(replayedGenerations, "every intent coordinate was replayed").to.equal(
			coordinateHashes.length,
		);
		expect(generationRowCount(log)).to.equal(0);
	});
});

describe("coordinate persistence mutation-generation receive bounds", function () {
	this.timeout(240_000);

	let peer1: Peerbit | undefined;
	let peer2: Peerbit | undefined;
	let directories: string[] = [];

	const createDurablePeer = async () => {
		// The backbone-only receive path — the only receive path that mints a
		// rollback token — is gated on an auto-derived durable coordinate
		// persistence adapter, which is only created when the node has a
		// directory. A memory-only node never reaches the snapshot at all and
		// would make this pin vacuous.
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-coordinate-generation-receive-pins-"),
		);
		directories.push(directory);
		return Peerbit.create({ directory, ...createRustPeerbitOptions() });
	};

	afterEach(async () => {
		await peer1?.stop();
		await peer2?.stop();
		peer1 = undefined;
		peer2 = undefined;
		for (const directory of directories) {
			await fs.rm(directory, { recursive: true, force: true });
		}
		directories = [];
	});

	it("G2: the receiver's raw map is empty again after a cold sync", async () => {
		const entryCount = 200;
		peer1 = await createDurablePeer();
		peer2 = await createDurablePeer();

		const store = new EventStore<string, any>();
		const db1 = await peer1.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		const db2 = await peer2.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		const receiver = db2.log as any;
		expect((db1.log as any)._nativeBackbone, "peer1 native backbone").to.exist;
		expect(receiver._nativeBackbone, "peer2 native backbone").to.exist;

		for (let index = 0; index < entryCount; index++) {
			await db1.add(`g2-${index}`, { meta: { next: [] } });
		}

		const probe = instrumentSnapshots(receiver);
		try {
			await peer2.dial(peer1.getMultiaddrs());
			await waitForResolved(
				() => {
					expect(db2.log.log.length).to.equal(entryCount);
				},
				{ timeout: 90_000, timeoutMessage: "receive-path cold sync" },
			);
			// Teeth: the receive path really minted tokens and the map really
			// held rows while the batches were in flight.
			expect(probe.state.calls, "receive tokens minted").to.be.greaterThan(0);
			expect(
				probe.state.maxSize,
				"rows resident during the sync",
			).to.be.at.least(1);
			await waitForResolved(() => {
				expect(generationRowCount(receiver)).to.equal(0);
			});
		} finally {
			probe.restore();
		}
	});
});

// G8 is the COMPLETENESS gate for the settle, as opposed to G1-G7 which pin
// its correctness. G1/G2 only assert that the map is empty at the end of one
// workload; that is satisfied by a settle that fires for some seams and leaks
// for others as long as the leaked seams happen not to run. G8 counts both
// sides of the hold ledger over a workload that deliberately drives every
// token-minting seam, and asserts they balance exactly.
//
// The map has exactly five writers, and the three legs below drive four:
//   1 `appendLocallyPreparedPayloadNativeBackboneCommitOnly`
//     — leg one-a, which MUST open non-replicating. Head-coordinate
//       persistence is deferred only when `!this._isReplicating` (see
//       shouldDeferHeadCoordinatePersistence), and a replicating log
//       short-circuits into writer 2 before this seam's body ever runs. This
//       is the seam a non-replicating Documents log uses, and it owns four
//       settle sites of its own, so measuring it separately is the point.
//   2 `appendLocallyPreparedPayloadNativeBackboneStorageTransaction`
//   3 `appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch`
//     — 2-3 are leg one-b, which opens replicating
//   4 `CoordinatePersistenceCoordinator.createBackboneOnlyReceiveCoordinateBatch`
//     — leg two, the receive seam
//   5 the durable-recovery intent replay, which fabricates its rows inline
//     instead of calling the snapshot writer. G6 covers that one, and it is
//     the only known producer of a settle with no matching create, which is
//     why the balance assertions below would fail loudly rather than silently
//     absorb it if it ever ran inside this workload.
//
// MEASURED at the tip of this branch (see the leg comments for the
// decomposition): 100% of created tokens settle and the raw map returns to
// zero rows, on the local-append seams, the batch seam and the receive seam
// alike. No seam leaks on a succeeding workload, so the bound asserted here
// is 0 residual rows rather than a non-zero known-residual bound.
//
// The bound is 0 for a workload that SUCCEEDS. The durable-commit failure
// arms that funnel through the shared `rollbackFailedNativeBackboneTransaction`
// sink deliberately carry no settle, so each failed durable commit can retain
// its token's rows forever. That is the accepted direction of the asymmetry —
// a retained row is the pre-refcount behavior, a premature settle is the
// silent-corruption one — and it is bounded by the number of durable commit
// failures rather than by throughput, so it is not asserted here.
//
// NOT covered by this gate, and measured explicitly rather than assumed: the
// seeded chaos suites (`test:shared-log:chaos`, and the wider `deterministic`
// grep behind `test:shared-log:chaos:all`) mint ZERO rollback tokens —
// 0 created / 0 settled across 12 coordinators, with all 41 settle calls
// receiving `undefined`. Those suites run memory-session nodes, which have no
// auto-derived durable coordinate persistence adapter and therefore never
// reach the backbone-only receive snapshot, and their appends go through the
// generic `db.add` route rather than the native prepared-payload seams. So the
// chaos suites are not a completeness signal for this map in either direction,
// and this pin is where the coverage actually lives.
describe("coordinate persistence mutation-generation settle balance", function () {
	this.timeout(240_000);

	let peers: Peerbit[] = [];
	let directories: string[] = [];

	const createDurablePeer = async () => {
		// Same durability requirement as the receive-bounds describe: a
		// memory-only node never mints a receive-path token at all.
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-coordinate-generation-balance-pins-"),
		);
		directories.push(directory);
		const peer = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});
		peers.push(peer);
		return peer;
	};

	afterEach(async () => {
		for (const peer of peers) {
			await peer.stop();
		}
		peers = [];
		for (const directory of directories) {
			await fs.rm(directory, { recursive: true, force: true });
		}
		directories = [];
	});

	const singleAppends = 40;
	const batches = 5;
	const batchWidth = 8;
	const genericAppends = 20;

	it("G8: every token minted by the commit-only append seam settles", async () => {
		// Writer 1, the backbone-only commit-only seam. It is reachable ONLY
		// from a non-replicating log: `shouldDeferHeadCoordinatePersistence`
		// begins `!this._isReplicating`, so the replicating leg below diverts
		// into the storage-transaction seam before this body runs. Without
		// this leg the balance gate would claim to cover writer 1 while never
		// executing it — a completeness gate asserting coverage it lacks.
		const peer = await createDurablePeer();
		const store = await peer.open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		expect(log._nativeBackbone, "native backbone").to.exist;
		enableNativeDocumentIndex(log);

		const probe = instrumentSettleBalance(log);
		try {
			for (let index = 0; index < singleAppends; index++) {
				await nativeAppend(log, `g8-commit-only-${index}`);
			}
			await waitForResolved(() => {
				expect(generationRowCount(log)).to.equal(0);
			});
		} finally {
			probe.restore();
		}

		// Teeth: this leg is worthless unless it actually reached writer 1.
		// A non-zero mint count is the only evidence that the commit-only
		// branch ran rather than silently diverting, which is exactly the
		// failure this leg exists to rule out.
		expect(
			probe.state.tokensCreated,
			"commit-only seam must mint tokens",
		).to.equal(singleAppends);
		expect(probe.state.holdsTaken, "holds taken").to.equal(singleAppends);
		expect(probe.state.tokensSettled, "tokens settled").to.equal(
			probe.state.tokensCreated,
		);
		expect(probe.state.holdsReleased, "holds released").to.equal(
			probe.state.holdsTaken,
		);
		expect(generationRowCount(log), "residual rows").to.equal(0);
	});

	it("G8: every token minted by a local append and batch workload settles", async () => {
		const peer = await createDurablePeer();
		const store = await peer.open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 } },
		});
		const log = store.log as any;
		expect(log._nativeBackbone, "native backbone").to.exist;
		enableNativeDocumentIndex(log);

		const probe = instrumentSettleBalance(log);
		try {
			for (let index = 0; index < singleAppends; index++) {
				await nativeAppend(log, `g8-single-${index}`);
			}
			for (let batch = 0; batch < batches; batch++) {
				await nativeBatchAppend(log, `g8-batch-${batch}`, batchWidth);
			}
			// Generic appends alongside the native seams, so the ledger is
			// measured over a mixed workload rather than a hand-picked one.
			for (let index = 0; index < genericAppends; index++) {
				await store.add(`g8-generic-${index}`, { meta: { next: [] } });
			}
			await waitForResolved(() => {
				expect(generationRowCount(log)).to.equal(0);
			});
		} finally {
			probe.restore();
		}

		// MEASURED, exactly: this log is REPLICATING, so its 40 single appends
		// take the storage-transaction seam (writer 2), not the commit-only one
		// — the non-replicating seam is measured by its own leg above. Each
		// mints one single-hash token; each of the 5 batch appends mints ONE
		// token covering all 8 entries; the 20 generic `store.add` mint none.
		// 45 tokens / 80 holds. Exact equality is deliberate: this is the gate
		// that says the ledger is fully accounted for, so a new token seam
		// appearing in this workload must be audited rather than absorbed.
		expect(probe.state.tokensCreated, "tokens minted").to.equal(
			singleAppends + batches,
		);
		expect(probe.state.holdsTaken, "holds taken").to.equal(
			singleAppends + batches * batchWidth,
		);
		// The balance itself. `rows === 0` above and these two are equivalent
		// given no other code path deletes a row, and both are asserted so a
		// future deletion path cannot make one of them silently vacuous.
		expect(probe.state.tokensSettled, "tokens settled").to.equal(
			probe.state.tokensCreated,
		);
		expect(probe.state.holdsReleased, "holds released").to.equal(
			probe.state.holdsTaken,
		);
		expect(generationRowCount(log), "residual rows").to.equal(0);
	});

	it("G8: every token minted by a cold-sync receive settles", async () => {
		const entryCount = 150;
		const peer1 = await createDurablePeer();
		const peer2 = await createDurablePeer();
		const store = new EventStore<string, any>();
		const db1 = await peer1.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		const db2 = await peer2.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		const receiver = db2.log as any;
		expect(receiver._nativeBackbone, "peer2 native backbone").to.exist;

		for (let index = 0; index < entryCount; index++) {
			await db1.add(`g8-sync-${index}`, { meta: { next: [] } });
		}

		// Only the receiver is instrumented: the sender's own `db1.add`
		// appends mint no rollback token at all (measured 0 created / 0
		// settled), so a balance assertion on the sender would be vacuous.
		const probe = instrumentSettleBalance(receiver);
		try {
			await peer2.dial(peer1.getMultiaddrs());
			await waitForResolved(
				() => {
					expect(db2.log.log.length).to.equal(entryCount);
				},
				{ timeout: 90_000, timeoutMessage: "receive-path cold sync" },
			);
			await waitForResolved(() => {
				expect(generationRowCount(receiver)).to.equal(0);
			});
		} finally {
			probe.restore();
		}

		// MEASURED: the receive seam batches, so the token COUNT is a function
		// of how the sync chunked (2 tokens covering 150 hashes each in the
		// reference run, i.e. 300 holds for 150 entries) and is asserted as a
		// bound rather than an exact number. The balance is exact.
		expect(probe.state.tokensCreated, "receive tokens minted").to.be.at.least(
			1,
		);
		expect(probe.state.holdsTaken, "receive holds taken").to.be.at.least(
			entryCount,
		);
		expect(probe.state.tokensSettled, "receive tokens settled").to.equal(
			probe.state.tokensCreated,
		);
		expect(probe.state.holdsReleased, "receive holds released").to.equal(
			probe.state.holdsTaken,
		);
		expect(generationRowCount(receiver), "residual rows").to.equal(0);
	});
});
