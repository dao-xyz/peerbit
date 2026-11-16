import assert from "node:assert/strict";
import test from "node:test";
import {
	buildCutLifecycleComparison,
	deleteHeadMatches,
	hasDurableCutBlocks,
	parseCutLifecycleCensusArgs,
	validateCutLifecycleState,
} from "./shared-log-cut-lifecycle-census-lib.mjs";
import {
	collectLifecycleDebt,
	runCheckpointedCensus,
} from "./shared-log-lifecycle-census-common.mjs";

test("parses CUT lifecycle profiles", () => {
	const defaults = parseCutLifecycleCensusArgs([], {});
	assert.equal(defaults.historyOperations, 200);
	assert.equal(defaults.keyCount, 10);
	assert.equal(defaults.batchSize, 10);
	assert.equal(defaults.deleteConcurrency, 1);
	assert.equal(defaults.runs, 1);
	const parsed = parseCutLifecycleCensusArgs(
		"--history-operations 100_000 --key-count 1_000 --batch-size 250 --delete-concurrency 16 --runs 3 --compact-max-journal-bytes 16_777_216 --compact-max-journal-records 65_536 --json".split(
			" ",
		),
		{},
	);
	assert.equal(parsed.historyOperations, 100_000);
	assert.equal(parsed.keyCount, 1_000);
	assert.equal(parsed.deleteConcurrency, 16);
	assert.equal(parsed.compactMaxJournalRecords, 65_536);
});

test("rejects incomplete CUT churn cycles", () => {
	for (const [args, message] of [
		[["--history-operations", "20", "--key-count", "10"], /must exceed/],
		[["--history-operations", "42", "--key-count", "10"], /divisible/],
		[["--key-count", "10", "--batch-size", "11"], /batch-size/],
		[["--delete-concurrency", "11"], /delete-concurrency/],
		[["--scenario", "fresh"], /worker-only/],
	]) {
		assert.throws(() => parseCutLifecycleCensusArgs(args, {}), message);
	}
});

test("compares visible state while exposing every historical log row", () => {
	const measured = (operations, cuts) => ({
		seed: { operations },
		reopen: {
			openMs: 1,
			memory: { beforeOpen: { rss: 1 }, afterOpen: { rss: 2 } },
			disk: { logicalBytes: 1, allocatedBytes: 1 },
			state: {
				cutHeadRows: cuts,
				logRows: operations,
				documentRows: 0,
				nativeDocumentIndexRows: 0,
				nativeDocumentValueRows: 0,
				enumeratedDocumentRows: 0,
				documentsFingerprint: "empty",
			},
		},
	});
	const comparison = buildCutLifecycleComparison(
		measured(20, 10),
		measured(200, 100),
	);
	assert.equal(comparison.visibleStateMatchesFresh, true);
	assert.equal(comparison.extraOperations, 180);
	assert.equal(comparison.logicalHistory.cutHeadOverhead, 90);
	assert.equal(comparison.logicalHistory.freshLogRows, 20);
	assert.equal(comparison.logicalHistory.historyLogRows, 200);
	assert.equal(comparison.logicalHistory.logRowOverhead, 180);
});

test("exact head matching catches duplicate-pair replacement", () => {
	const nativeHeads = ["a", "a", "c"];
	const remaining = new Set(nativeHeads);
	assert.equal(nativeHeads.length - remaining.size, 1);
	assert.equal(deleteHeadMatches(remaining, ["b", "b", "c"]), 2);
	assert.deepEqual([...remaining], ["a"]);
});

const validCutState = () => ({
	logRows: 4,
	graphRows: 4,
	nativeLogRows: 4,
	nativeBlockRows: 2,
	headRows: 2,
	nativeHeadRows: 2,
	cutHeadRows: 2,
	residentCoordinateRows: 2,
	coordinateIndexRows: 2,
	coordinateValueRows: 2,
	nativeCoordinateHashes: 2,
	assignedHeads: 2,
	nonCutHeadRows: 0,
	nativeHeadDuplicateRows: 0,
	lowerHeadNotNativeRows: 0,
	nativeHeadNotLowerRows: 0,
	retainedLowerShallowMissing: 0,
	retainedNativeGraphMissing: 0,
	retainedDurableBlockMissing: 0,
	rustCoordinateRows: 0,
	documentRows: 0,
	nativeDocumentIndexRows: 0,
	nativeDocumentValueRows: 0,
	enumeratedDocumentRows: 0,
	replicationRanges: 1,
	replicators: 1,
	activeReplicators: 1,
	durableBlockBytes: 100,
	debt: { nonzero: [], unobserved: [] },
});

const validateReopenedCutState = (state) =>
	validateCutLifecycleState({
		state,
		expectedOperations: 4,
		expectedCutHeads: 2,
		phase: "reopen",
	});

test("accepts a cold or warm reopen cache without weakening retained state", () => {
	for (const nativeBlockRows of [0, 1, 2]) {
		assert.ok(
			validateReopenedCutState({ ...validCutState(), nativeBlockRows }),
		);
	}
	for (const nativeBlockRows of [undefined, NaN, -1, 0.5, 3]) {
		assert.throws(
			() => validateReopenedCutState({ ...validCutState(), nativeBlockRows }),
			/nativeBlockRows/,
		);
	}
	for (const field of [
		"headRows",
		"residentCoordinateRows",
		"retainedDurableBlockMissing",
	]) {
		assert.throws(
			() => validateReopenedCutState({ ...validCutState(), [field]: 1 }),
			new RegExp(field),
		);
	}
	assert.throws(
		() =>
			validateReopenedCutState({
				...validCutState(),
				debt: {
					nonzero: [{ field: "pendingGidCleanup", value: 1 }],
					unobserved: [],
				},
			}),
		/debt did not drain/,
	);
	const seed = {
		...validCutState(),
		headRows: 4,
		nativeHeadRows: 4,
		nonCutHeadRows: 2,
	};
	assert.throws(
		() =>
			validateCutLifecycleState({
				state: seed,
				expectedOperations: 4,
				expectedCutHeads: 2,
				phase: "seed",
			}),
		/nativeBlockRows/,
	);
	assert.ok(
		validateCutLifecycleState({
			state: { ...seed, nativeBlockRows: 4 },
			expectedOperations: 4,
			expectedCutHeads: 2,
			phase: "seed",
		}),
	);
});

test("a warm native cache cannot hide a missing durable CUT block", async () => {
	const localStore = {
		hasMany: () => assert.fail("must not check the hot cache"),
		durable: {
			persisted: () => true,
			hasMany: async (hashes) => {
				assert.deepEqual(hashes, ["a", "b"]);
				return [true, false];
			},
		},
	};
	const present = await hasDurableCutBlocks(localStore, ["a", "b"]);
	assert.deepEqual(present, [true, false]);
	assert.throws(
		() =>
			validateReopenedCutState({
				...validCutState(),
				retainedDurableBlockMissing: present.filter((value) => !value).length,
			}),
		/retainedDurableBlockMissing=1/,
	);
	localStore.durable.hasMany = async () => [true, true];
	assert.deepEqual(await hasDurableCutBlocks(localStore, ["a", "b"]), [
		true,
		true,
	]);
	for (const durable of [
		undefined,
		{ persisted: () => false },
		{ persisted: () => true },
	]) {
		await assert.rejects(
			hasDurableCutBlocks({ durable }, ["a"]),
			/requires the durable block mirror/,
		);
	}
	for (const result of [undefined, [true], [true, undefined], Array(2)]) {
		await assert.rejects(
			hasDurableCutBlocks(
				{ durable: { persisted: () => true, hasMany: async () => result } },
				["a", "b"],
			),
			/not aligned/,
		);
	}
	await assert.rejects(
		hasDurableCutBlocks(
			{
				durable: {
					persisted: () => true,
					hasMany: async () => {
						throw new Error("disk failure");
					},
				},
			},
			["a"],
		),
		/disk failure/,
	);
});

test("observes every live repair debt seam", () => {
	const one = new Map([["mode", new Set(["value"])]]);
	const log = {
		_repairSweepRunning: true,
		_joinAuthoritativeRepairTimersByDelay: one,
		_joinAuthoritativeRepairPeersByDelay: one,
		_repairFrontierByMode: one,
		_repairFrontierActiveTargetsByMode: one,
		_repairFrontierBypassKnownPeersByMode: one,
		joinWarmup: {
			_repairSweepWarmupSessionByTarget: one,
			_joinWarmupRetryTimersByTarget: one,
			_joinWarmupScheduledRetriesByTarget: one,
		},
		_repairSweepOptimisticGidPeersPending: new Set(["gid"]),
		_repairSweepOptimisticGidsByPeer: new Map([["peer", new Set(["gid"])]]),
		_appendBackfillTimer: {},
		_checkedPruneAuditTimer: {},
	};
	const debt = collectLifecycleDebt(log, {}, true);
	const expectedFields = (
		"repairSweepRunning joinAuthoritativeRepairTimers joinAuthoritativeRepairPeers repairFrontierTargets repairFrontierActiveTargets " +
		"repairFrontierBypassKnownPeers repairSweepWarmupSessions joinWarmupRetryTimers joinWarmupScheduledRetryTargets repairOptimisticGids " +
		"repairOptimisticPeers appendBackfillTimer checkedPruneAuditTimer"
	).split(" ");
	assert.deepEqual(
		debt.nonzero.map(({ field }) => field).sort(),
		expectedFields.sort(),
	);
	const missing = collectLifecycleDebt({}, {}, true);
	assert.equal(missing.values.repairSweepRunning, null);
});

test("keeps completed measurements in a failure checkpoint", async () => {
	const checkpoints = [];
	const measurement = { phase: "seed", operations: 4 };
	await assert.rejects(
		runCheckpointedCensus({
			options: { runs: 1, json: true },
			scenarios: ["fresh", "history"],
			buildReport: (report) => report,
			renderHuman() {},
			logRun() {},
			onCheckpoint: (report) => checkpoints.push(structuredClone(report)),
			preserveActiveOnFailure: true,
			async runRow(_options, run, setActive) {
				await setActive({ run, completed: { fresh: { seed: measurement } } });
				throw new Error("stop after seed");
			},
		}),
		/stop after seed/,
	);
	const checkpoint = checkpoints.at(-1);
	assert.deepEqual(checkpoint.activeRow.completed.fresh.seed, measurement);
	assert.equal(checkpoint.failure.message, "stop after seed");
});
