import assert from "node:assert/strict";
import test from "node:test";
import {
	LIFECYCLE_CENSUS_NAME,
	LIFECYCLE_LIVE_FINGERPRINT_FIELDS,
	LIFECYCLE_LIVE_VALUE_FIELDS,
	buildLifecycleCensusReport,
	buildLifecycleComparison,
	parseLifecycleCensusArgs,
} from "./shared-log-lifecycle-census-lib.mjs";

test("parses the canonical lifecycle census", () => {
	assert.deepEqual(parseLifecycleCensusArgs([], {}), {
		mode: "parent",
		historyCount: 100_000,
		retain: 1_000,
		batchSize: 256,
		runs: 1,
		json: false,
	});
});

test("accepts explicit and environment lifecycle options", () => {
	assert.deepEqual(
		parseLifecycleCensusArgs(
			[
				"--history-count",
				"10_000",
				"--retain",
				"100",
				"--batch-size",
				"50",
				"--runs",
				"2",
				"--json",
			],
			{},
		),
		{
			mode: "parent",
			historyCount: 10_000,
			retain: 100,
			batchSize: 50,
			runs: 2,
			json: true,
		},
	);
	assert.equal(
		parseLifecycleCensusArgs([], {
			SHARED_LOG_LIFECYCLE_OUTPUT: "/tmp/lifecycle.json",
		}).output,
		"/tmp/lifecycle.json",
	);
});

test("parses isolated lifecycle workers", () => {
	assert.deepEqual(
		parseLifecycleCensusArgs(
			[
				"--worker",
				"--scenario",
				"history",
				"--phase",
				"reopen",
				"--run",
				"1",
				"--directory",
				"/tmp/lifecycle",
				"--probe-hash",
				"hash-a",
				"--retained-probe-hash",
				"hash-z",
			],
			{},
		),
		{
			mode: "worker",
			scenario: "history",
			phase: "reopen",
			run: 1,
			directory: "/tmp/lifecycle",
			historyCount: 100_000,
			retain: 1_000,
			batchSize: 256,
			probeHash: "hash-a",
			retainedProbeHash: "hash-z",
		},
	);
});

test("rejects workloads that cannot prove steady-state trimming", () => {
	assert.throws(
		() => parseLifecycleCensusArgs(["--retain", "0"], {}),
		/retain must be a positive integer/,
	);
	assert.throws(
		() =>
			parseLifecycleCensusArgs(
				["--history-count", "99", "--retain", "100"],
				{},
			),
		/history-count must be greater than retain/,
	);
	assert.throws(
		() =>
			parseLifecycleCensusArgs(
				["--history-count", "100", "--retain", "100"],
				{},
			),
		/history-count must be greater than retain/,
	);
	assert.throws(
		() =>
			parseLifecycleCensusArgs(["--retain", "100", "--batch-size", "101"], {}),
		/batch-size must not exceed retain/,
	);
	assert.throws(
		() => parseLifecycleCensusArgs(["--scenario", "fresh"], {}),
		/worker-only/,
	);
	assert.throws(
		() =>
			parseLifecycleCensusArgs(
				[
					"--worker",
					"--scenario",
					"fresh",
					"--phase",
					"reopen",
					"--run",
					"1",
					"--directory",
					"/tmp/lifecycle",
				],
				{},
			),
		/requires --probe-hash and --retained-probe-hash/,
	);
});

const measuredScenario = ({ disk, openMs, rss, countOffset = 0 }) => {
	const state = Object.fromEntries(
		LIFECYCLE_LIVE_VALUE_FIELDS.map((field) => [field, 100 + countOffset]),
	);
	for (const field of LIFECYCLE_LIVE_FINGERPRINT_FIELDS) {
		state[field] = `fingerprint-${countOffset}`;
	}
	return {
		reopen: {
			state,
			disk: { logicalBytes: disk, allocatedBytes: disk * 2 },
			openMs,
			memory: { beforeOpen: { rss: 1_000 }, afterValidation: { rss } },
		},
	};
};

test("compares matched live state, disk, reopen time, and memory", () => {
	const comparison = buildLifecycleComparison(
		measuredScenario({ disk: 1_000, openMs: 10, rss: 5_000 }),
		measuredScenario({ disk: 3_000, openMs: 15, rss: 6_000 }),
		900,
	);
	assert.equal(comparison.liveStateMatchesFresh, true);
	assert.deepEqual(comparison.unequalLiveValues, []);
	assert.deepEqual(comparison.unequalLiveFingerprints, []);
	assert.equal(comparison.disk.logicalDiskOverheadBytes, 2_000);
	assert.equal(comparison.disk.logicalBytesPerHistoricalEntry, 2_000 / 900);
	assert.equal(comparison.disk.logicalGrowthRatio, 3);
	assert.equal(comparison.reopen.reopenMsDelta, 5);
	assert.equal(comparison.reopen.reopenRssDeltaBytes, 1_000);
	assert.equal(comparison.reopen.freshMeasuredRssBytes, 4_000);
	assert.equal(comparison.reopen.historyMeasuredRssBytes, 5_000);
	assert.equal(comparison.reopen.measuredRssDeltaBytes, 1_000);
	assert.equal(comparison.reopen.measuredRssGrowthRatio, 1.25);

	const unequal = buildLifecycleComparison(
		measuredScenario({ disk: 1, openMs: 1, rss: 1 }),
		measuredScenario({ disk: 1, openMs: 1, rss: 1, countOffset: 1 }),
		1,
	);
	assert.equal(unequal.liveStateMatchesFresh, false);
	assert.deepEqual(unequal.unequalLiveValues, [...LIFECYCLE_LIVE_VALUE_FIELDS]);
	assert.deepEqual(unequal.unequalLiveFingerprints, [
		...LIFECYCLE_LIVE_FINGERPRINT_FIELDS,
	]);

	const fingerprintOnly = measuredScenario({ disk: 1, openMs: 1, rss: 1 });
	fingerprintOnly.reopen.state.documentsFingerprint = "different";
	const fingerprintMismatch = buildLifecycleComparison(
		measuredScenario({ disk: 1, openMs: 1, rss: 1 }),
		fingerprintOnly,
		1,
	);
	assert.equal(fingerprintMismatch.liveStateMatchesFresh, false);
	assert.deepEqual(fingerprintMismatch.unequalLiveValues, []);
	assert.deepEqual(fingerprintMismatch.unequalLiveFingerprints, [
		"documentsFingerprint",
	]);

	const unsupportedAllocation = measuredScenario({
		disk: 1,
		openMs: 1,
		rss: 1,
	});
	unsupportedAllocation.reopen.disk.allocatedBytes = null;
	const nullable = buildLifecycleComparison(
		unsupportedAllocation,
		unsupportedAllocation,
		1,
	);
	assert.equal(nullable.disk.allocatedDiskOverheadBytes, null);
	assert.equal(nullable.disk.allocatedBytesPerHistoricalEntry, null);
	assert.equal(nullable.disk.allocatedGrowthRatio, null);
});

test("builds a versioned checkpoint report", () => {
	const report = buildLifecycleCensusReport({
		historyCount: 100,
		retain: 10,
		batchSize: 5,
		runs: 1,
		rows: [{ run: 1 }],
		host: { node: "v22.0.0", platform: "linux" },
	});
	assert.equal(report.name, LIFECYCLE_CENSUS_NAME);
	assert.equal(report.schemaVersion, 1);
	assert.deepEqual(report.progress, {
		expectedRows: 1,
		completedRows: 1,
		complete: true,
		activeRow: null,
	});
});
