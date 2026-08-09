import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
	GATE_CONFIG,
	MAX_MEASURED_SAMPLES,
	compareSyncPeerStateResults,
	medianConfidenceInterval,
} from "./sync-peer-state-compare-lib.mjs";

const scenarios = [
	"simple-dispatch-quota",
	"rateless-target-lifecycle",
	"disconnect-reconnect-retained-physical",
];
const runtime = {
	node: "22.23.2",
	v8: "12.4-test",
	platform: "darwin",
	arch: "arm64",
	cpu: "fixture-cpu",
};

const median = (values) => {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[middle - 1] + sorted[middle]) / 2
		: sorted[middle];
};

const samples = (value, count = GATE_CONFIG.measuredSamples) =>
	Array.from({ length: count }, () => value);

const result = (
	valuesByScenario,
	{ config: configOverrides = {}, runtime: runtimeOverrides = {} } = {},
) => {
	const values = Array.isArray(valuesByScenario)
		? Object.fromEntries(
				scenarios.map((scenario) => [scenario, valuesByScenario]),
			)
		: valuesByScenario;
	const firstValues = values[scenarios[0]];
	const config = {
		peerCounts: [...GATE_CONFIG.peerCounts],
		warmupSamples: GATE_CONFIG.warmupSamples,
		measuredSamples: firstValues.length,
		iterations: GATE_CONFIG.iterations,
		...configOverrides,
	};
	const tasks = config.peerCounts.flatMap((peers) =>
		scenarios.map((scenario) => {
			const scenarioValues = values[scenario];
			const taskSamples = scenarioValues.map((nsPerOperation) => {
				const operations = config.iterations;
				const peerOperations = operations * peers;
				const elapsedMs = (nsPerOperation * operations) / 1e6;
				return {
					elapsedMs,
					operations,
					peerOperations,
					nsPerOperation,
					nsPerPeerOperation: (elapsedMs * 1e6) / peerOperations,
					retainedPhysicalPermits:
						scenario === "disconnect-reconnect-retained-physical"
							? Math.min(peers, 32)
							: 0,
					checksum: peerOperations,
				};
			});
			return {
				scenario,
				peers,
				samples: taskSamples,
				medianNsPerOperation: median(
					taskSamples.map((sample) => sample.nsPerOperation),
				),
				medianNsPerPeerOperation: median(
					taskSamples.map((sample) => sample.nsPerPeerOperation),
				),
			};
		}),
	);
	return {
		schemaVersion: 1,
		benchmark: "sync-peer-state",
		runtime: { ...runtime, ...runtimeOverrides },
		config,
		tasks,
	};
};

const taskValues = (simple, other) => ({
	"simple-dispatch-quota": simple,
	"rateless-target-lifecycle": other,
	"disconnect-reconnect-retained-physical": other,
});

test("computes an exact order-statistic confidence interval for the median", () => {
	assert.deepEqual(medianConfidenceInterval([1, 2, 3, 4, 5, 6, 7, 8, 9]), {
		median: 5,
		low: 2,
		high: 8,
		confidence: 0.95,
	});
});

test("accepts the maximum safe sample count and rejects overflow-sized input", () => {
	assert.doesNotThrow(() =>
		medianConfidenceInterval(
			Array.from({ length: MAX_MEASURED_SAMPLES }, () => 1),
		),
	);
	assert.throws(
		() =>
			medianConfidenceInterval(
				Array.from({ length: MAX_MEASURED_SAMPLES + 1 }, () => 1),
			),
		/7-1000 samples/,
	);
});

test("fails a separated regression above five percent", () => {
	const baselineValues = samples(100);
	const comparison = compareSyncPeerStateResults(
		result(taskValues(baselineValues, baselineValues)),
		result(taskValues(samples(106), baselineValues)),
	);
	assert.equal(comparison.status, "regression");
	assert.deepEqual(comparison.regressions, [
		"simple-dispatch-quota/peers=1",
		"simple-dispatch-quota/peers=64",
		"simple-dispatch-quota/peers=8",
	]);
});

test("does not claim a threshold regression when its interval includes less than five percent", () => {
	const baselineValues = samples(100);
	const candidateValues = [...samples(101, 7), ...samples(106, 8)];
	const comparison = compareSyncPeerStateResults(
		result(taskValues(baselineValues, baselineValues)),
		result(taskValues(candidateValues, baselineValues)),
	);
	const simple = comparison.comparisons.find(
		(comparison) => comparison.key === "simple-dispatch-quota/peers=8",
	);
	assert.equal(simple.ratio > 1.05, true);
	assert.equal(simple.candidate.low / simple.baseline.high < 1.05, true);
	assert.equal(simple.credibleRegression, false);
	assert.equal(comparison.status, "inconclusive");
});

test("calls an above-threshold estimate with overlapping intervals inconclusive", () => {
	const baselineValues = [
		...samples(90, 4),
		...samples(100, 7),
		...samples(110, 4),
	];
	const candidateValues = [
		...samples(96, 4),
		...samples(106, 7),
		...samples(116, 4),
	];
	const comparison = compareSyncPeerStateResults(
		result(baselineValues),
		result(taskValues(candidateValues, baselineValues)),
	);
	const simple = comparison.comparisons.find(
		(comparison) => comparison.key === "simple-dispatch-quota/peers=8",
	);
	assert.equal(simple.ratio > 1.05, true);
	assert.equal(simple.credibleRegression, false);
	assert.equal(comparison.status, "inconclusive");
});

test("passes an exact five-percent change but fails a strictly greater separated change", () => {
	const baselineValues = samples(100);
	const exactThreshold = compareSyncPeerStateResults(
		result(baselineValues),
		result(samples(105)),
	);
	assert.equal(exactThreshold.status, "pass");

	const beyondThreshold = compareSyncPeerStateResults(
		result(baselineValues),
		result(samples(106)),
	);
	assert.equal(beyondThreshold.status, "regression");
});

test("requires the canonical configuration for a gate result", () => {
	const canonical = result(samples(100));
	const canonicalComparison = compareSyncPeerStateResults(canonical, canonical);
	assert.equal(canonicalComparison.gate.eligible, true);
	assert.equal(canonicalComparison.status, "pass");

	const nonCanonicalDocuments = [
		result(samples(100), { config: { peerCounts: [1] } }),
		result(samples(100), { config: { warmupSamples: 1 } }),
		result(samples(100), { config: { iterations: 1 } }),
		result(samples(100, 7)),
	];
	for (const document of nonCanonicalDocuments) {
		const comparison = compareSyncPeerStateResults(document, document);
		assert.equal(comparison.gate.eligible, false);
		assert.equal(comparison.status, "inconclusive");
	}
});

test("requires Node 22 for a gate result", () => {
	const node24 = result(samples(100), {
		runtime: { node: "24.1.0", v8: "13.6-test" },
	});
	const comparison = compareSyncPeerStateResults(node24, node24);
	assert.equal(comparison.gate.eligible, false);
	assert.equal(comparison.status, "inconclusive");
	assert.deepEqual(comparison.gate.reasons, [
		"canonical gate requires Node 22",
	]);
});

test("rejects exact runtime and full configuration mismatches", () => {
	const values = samples(1);
	assert.throws(
		() =>
			compareSyncPeerStateResults(
				result(values),
				result(values, { runtime: { node: "22.23.3" } }),
			),
		/same exact Node, V8, platform, architecture, and CPU/,
	);
	assert.throws(
		() =>
			compareSyncPeerStateResults(
				result(values),
				result(values, { config: { warmupSamples: 4 } }),
			),
		/same peer counts, warmups, measured samples, and iterations/,
	);
	assert.throws(
		() => compareSyncPeerStateResults(result(values), result([...values, 1])),
		/same peer counts, warmups, measured samples, and iterations/,
	);
});

test("rejects declared sample-count mismatches and oversized configs", () => {
	const values = samples(1);
	const missingSample = structuredClone(result(values));
	missingSample.tasks[0].samples.pop();
	assert.throws(
		() => compareSyncPeerStateResults(result(values), missingSample),
		/has 14 samples; expected 15/,
	);

	const oversized = structuredClone(result(values));
	oversized.config.measuredSamples = MAX_MEASURED_SAMPLES + 1;
	assert.throws(
		() => compareSyncPeerStateResults(result(values), oversized),
		/7-1000 measured samples/,
	);
});

test("rejects reduced deterministic work counters", () => {
	const values = samples(1);
	const baseline = result(values);
	const mutations = [
		(candidate) => (candidate.tasks[0].samples[0].operations -= 1),
		(candidate) => (candidate.tasks[0].samples[0].peerOperations -= 1),
		(candidate) => (candidate.tasks[0].samples[0].checksum -= 1),
		(candidate) => (candidate.tasks[2].samples[0].retainedPhysicalPermits -= 1),
	];
	for (const mutate of mutations) {
		const candidate = structuredClone(baseline);
		mutate(candidate);
		assert.throws(
			() => compareSyncPeerStateResults(baseline, candidate),
			/performed unexpected work/,
		);
	}
});

test("rejects duplicate tasks and incomplete task sets", () => {
	const values = samples(1);
	const duplicate = structuredClone(result(values));
	duplicate.tasks.push(structuredClone(duplicate.tasks[0]));
	assert.throws(
		() => compareSyncPeerStateResults(result(values), duplicate),
		/contains duplicate task/,
	);

	const incomplete = structuredClone(result(values));
	incomplete.tasks.pop();
	assert.throws(
		() => compareSyncPeerStateResults(result(values), incomplete),
		/complete benchmark task set/,
	);
});

test("rejects missing runtime and sample fields", () => {
	const values = samples(1);
	const missingRuntime = structuredClone(result(values));
	delete missingRuntime.runtime.v8;
	assert.throws(
		() => compareSyncPeerStateResults(result(values), missingRuntime),
		/runtime\.v8 must be a non-empty string/,
	);

	const missingWork = structuredClone(result(values));
	delete missingWork.tasks[0].samples[0].checksum;
	assert.throws(
		() => compareSyncPeerStateResults(result(values), missingWork),
		/sample 0 is invalid/,
	);
});

test("loads absolute result paths while running the CLI from the package directory", async () => {
	const benchmarkDirectory = dirname(fileURLToPath(import.meta.url));
	const temporaryDirectory = await mkdtemp(
		join(tmpdir(), "peerbit-sync-bench-"),
	);
	try {
		const baselinePath = join(temporaryDirectory, "baseline.json");
		const candidatePath = join(temporaryDirectory, "candidate.json");
		const smokePath = join(temporaryDirectory, "smoke.json");
		const document = result(samples(100));
		await Promise.all([
			writeFile(baselinePath, JSON.stringify(document)),
			writeFile(candidatePath, JSON.stringify(document)),
			writeFile(
				smokePath,
				JSON.stringify(result(samples(100), { config: { peerCounts: [1] } })),
			),
		]);
		const execution = spawnSync(
			process.execPath,
			[
				join(benchmarkDirectory, "sync-peer-state-compare.mjs"),
				baselinePath,
				candidatePath,
			],
			{ cwd: dirname(benchmarkDirectory), encoding: "utf8" },
		);
		assert.equal(execution.status, 0, execution.stderr);
		assert.equal(JSON.parse(execution.stdout).status, "pass");

		const inconclusiveExecution = spawnSync(
			process.execPath,
			[
				join(benchmarkDirectory, "sync-peer-state-compare.mjs"),
				smokePath,
				smokePath,
			],
			{ cwd: dirname(benchmarkDirectory), encoding: "utf8" },
		);
		assert.equal(inconclusiveExecution.status, 2, inconclusiveExecution.stderr);
		assert.equal(
			JSON.parse(inconclusiveExecution.stdout).status,
			"inconclusive",
		);
	} finally {
		await rm(temporaryDirectory, { recursive: true, force: true });
	}
});
