// Pins G1-G6 for the hold-counted `_nativeCoordinateMutationGenerations` map.
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
// So G3 and G4 carry the real weight: they assert that rollbacks that are
// still OWED continue to fire, and that supersession is still detected.
//
// The rollback tokens are minted only on the native-backbone local-append and
// receive paths. The local-append paths are reached through the prepared
// payload commit-only entry point (`target: "none"`), which is how the
// Documents program appends; a non-replicating log routes it to the
// commit-only variant and a replicating log to the storage-transaction
// variant, so both local seams are covered here.
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
		}

		// The injection must actually have fired, and it must have fired while
		// the native coordinates were committed; a dead injection would make
		// every assertion below pass for the wrong reason.
		expect(injectionsFired, "fault injection fired").to.equal(1);
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
