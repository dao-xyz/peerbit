import assert from "node:assert/strict";
import test from "node:test";
import {
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_SCENARIOS,
	buildScaleCensusReport,
	memoryDeltas,
	parseScaleCensusArgs,
} from "./shared-log-scale-census-lib.mjs";

test("parses the canonical scale census", () => {
	assert.deepEqual(parseScaleCensusArgs([], {}), {
		mode: "parent",
		counts: [100_000, 1_000_000],
		scenarios: [...SCALE_CENSUS_SCENARIOS],
		runs: 1,
		json: false,
	});
});

test("accepts explicit and environment scale census options", () => {
	assert.deepEqual(
		parseScaleCensusArgs(
			[
				"--counts",
				"1_000,20_000",
				"--scenarios",
				"native-graph-chain,coordinate-frontier",
				"--runs",
				"3",
				"--json",
			],
			{},
		),
		{
			mode: "parent",
			counts: [1_000, 20_000],
			scenarios: ["native-graph-chain", "coordinate-frontier"],
			runs: 3,
			json: true,
		},
	);

	assert.equal(parseScaleCensusArgs([], { BENCH_JSON: "1" }).json, true);
	assert.deepEqual(parseScaleCensusArgs(["--", "--counts", "1000"], {}), {
		mode: "parent",
		counts: [1_000],
		scenarios: [...SCALE_CENSUS_SCENARIOS],
		runs: 1,
		json: false,
	});
});

test("parses one isolated worker row", () => {
	assert.deepEqual(
		parseScaleCensusArgs(
			[
				"--worker",
				"--scenario",
				"native-graph-roots",
				"--count",
				"1000",
				"--run",
				"2",
			],
			{},
		),
		{
			mode: "worker",
			scenario: "native-graph-roots",
			count: 1_000,
			run: 2,
		},
	);
});

test("rejects ambiguous or invalid workloads", () => {
	assert.throws(
		() => parseScaleCensusArgs(["--counts", "0"], {}),
		/count must be a positive integer/,
	);
	assert.throws(
		() => parseScaleCensusArgs(["--counts", "100,100"], {}),
		/counts must not contain duplicate values/,
	);
	assert.throws(
		() => parseScaleCensusArgs(["--scenarios", "unknown"], {}),
		/Unknown scale-census scenario/,
	);
	assert.throws(
		() => parseScaleCensusArgs(["--count", "100"], {}),
		/worker-only/,
	);
	assert.throws(
		() =>
			parseScaleCensusArgs(
				["--worker", "--scenario", "native-graph-chain"],
				{},
			),
		/requires --scenario, --count, and --run/,
	);
});

test("reports memory deltas and per-entry costs", () => {
	assert.deepEqual(
		memoryDeltas(
			{
				rss: 100,
				heapTotal: 200,
				heapUsed: 50,
				external: 20,
				arrayBuffers: 10,
			},
			{
				rss: 500,
				heapTotal: 300,
				heapUsed: 70,
				external: 60,
				arrayBuffers: 20,
			},
			10,
		),
		{
			rssBeforeBytes: 100,
			rssAfterBytes: 500,
			rssDeltaBytes: 400,
			rssBytesPerEntry: 40,
			heapTotalBeforeBytes: 200,
			heapTotalAfterBytes: 300,
			heapTotalDeltaBytes: 100,
			heapTotalBytesPerEntry: 10,
			heapUsedBeforeBytes: 50,
			heapUsedAfterBytes: 70,
			heapUsedDeltaBytes: 20,
			heapUsedBytesPerEntry: 2,
			externalBeforeBytes: 20,
			externalAfterBytes: 60,
			externalDeltaBytes: 40,
			externalBytesPerEntry: 4,
			arrayBuffersBeforeBytes: 10,
			arrayBuffersAfterBytes: 20,
			arrayBuffersDeltaBytes: 10,
			arrayBuffersBytesPerEntry: 1,
		},
	);
});

test("builds a versioned machine-readable report", () => {
	assert.deepEqual(
		buildScaleCensusReport({
			counts: [100],
			scenarios: ["native-graph-chain"],
			runs: 1,
			rows: [{ scenario: "native-graph-chain", count: 100 }],
			host: { node: "v22.0.0", platform: "linux" },
		}),
		{
			name: SCALE_CENSUS_NAME,
			schemaVersion: 1,
			meta: {
				counts: [100],
				scenarios: ["native-graph-chain"],
				runs: 1,
				isolation: "one fresh process per row",
				measurement:
					"resident state added after WASM and an empty index are loaded",
				node: "v22.0.0",
				platform: "linux",
			},
			rows: [{ scenario: "native-graph-chain", count: 100 }],
		},
	);
});
