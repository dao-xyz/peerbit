import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
	SCALE_CENSUS_GID_CHARACTERS,
	SCALE_CENSUS_HASH_CHARACTERS,
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_PERSISTENT_SCENARIOS,
	SCALE_CENSUS_SCENARIOS,
	buildScaleCensusReport,
	isPersistentScaleCensusScenario,
	makeScaleCensusGid,
	makeScaleCensusHash,
	memoryDeltas,
	parseScaleCensusArgs,
} from "./shared-log-scale-census-lib.mjs";

const SCALE_CENSUS_SCRIPT = fileURLToPath(
	new URL("./shared-log-scale-census.mjs", import.meta.url),
);

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
			phase: "measure",
		},
	);
});

test("parses persistent seed and reopen workers", () => {
	for (const phase of ["seed", "reopen"]) {
		assert.deepEqual(
			parseScaleCensusArgs(
				[
					"--worker",
					"--scenario",
					"persistent-coordinate-index",
					"--count",
					"1000",
					"--run",
					"1",
					"--phase",
					phase,
					"--directory",
					"/tmp/census",
				],
				{},
			),
			{
				mode: "worker",
				scenario: "persistent-coordinate-index",
				count: 1_000,
				run: 1,
				phase,
				directory: "/tmp/census",
			},
		);
	}
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
	assert.throws(
		() =>
			parseScaleCensusArgs(
				[
					"--worker",
					"--scenario",
					"persistent-coordinate-index",
					"--count",
					"100",
					"--run",
					"1",
				],
				{},
			),
		/requires --phase seed or --phase reopen/,
	);
	assert.throws(
		() =>
			parseScaleCensusArgs(
				[
					"--worker",
					"--scenario",
					"persistent-coordinate-index",
					"--count",
					"100",
					"--run",
					"1",
					"--phase",
					"seed",
				],
				{},
			),
		/requires --directory/,
	);
	assert.throws(
		() =>
			parseScaleCensusArgs(
				[
					"--worker",
					"--scenario",
					"native-graph-chain",
					"--count",
					"100",
					"--run",
					"1",
					"--directory",
					"/tmp/census",
				],
				{},
			),
		/does not accept --directory/,
	);
	assert.throws(
		() => parseScaleCensusArgs(["--phase", "seed"], {}),
		/worker-only/,
	);
});

test("builds fixed-width unique synthetic identifiers", () => {
	assert.equal(makeScaleCensusHash(0).length, SCALE_CENSUS_HASH_CHARACTERS);
	assert.equal(makeScaleCensusHash(1).length, SCALE_CENSUS_HASH_CHARACTERS);
	assert.notEqual(makeScaleCensusHash(0), makeScaleCensusHash(1));
	assert.equal(makeScaleCensusGid(0).length, SCALE_CENSUS_GID_CHARACTERS);
	assert.equal(makeScaleCensusGid(1).length, SCALE_CENSUS_GID_CHARACTERS);
	assert.notEqual(makeScaleCensusGid(0), makeScaleCensusGid(1));
	assert.ok(
		SCALE_CENSUS_PERSISTENT_SCENARIOS.every(isPersistentScaleCensusScenario),
	);
	assert.equal(isPersistentScaleCensusScenario("native-graph-chain"), false);
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
			schemaVersion: 2,
			meta: {
				counts: [100],
				scenarios: ["native-graph-chain"],
				runs: 1,
				isolation:
					"resident rows use one fresh process; persistent rows use separate seed and reopen processes",
				measurement:
					"resident growth plus persistent index disk, close, and fresh-process reopen costs",
				node: "v22.0.0",
				platform: "linux",
			},
			rows: [{ scenario: "native-graph-chain", count: 100 }],
		},
	);
});

test("round-trips compact snapshots through fresh persistent workers", () => {
	const child = spawnSync(
		process.execPath,
		[
			SCALE_CENSUS_SCRIPT,
			"--counts",
			"3",
			"--scenarios",
			SCALE_CENSUS_PERSISTENT_SCENARIOS.join(","),
			"--runs",
			"1",
			"--json",
		],
		{ encoding: "utf8", env: process.env, maxBuffer: 1024 * 1024 },
	);
	assert.equal(child.status, 0, child.stderr || child.stdout);
	const report = JSON.parse(child.stdout);
	assert.equal(report.name, SCALE_CENSUS_NAME);
	assert.equal(report.rows.length, SCALE_CENSUS_PERSISTENT_SCENARIOS.length);
	for (const row of report.rows) {
		assert.equal(row.kind, "persistent-reopen");
		assert.equal(row.count, 3);
		assert.equal(row.validation.seededEntries, 3);
		assert.equal(row.validation.reopenedEntries, 3);
		assert.equal(row.validation.logicalFootprintStable, true);
		assert.equal(row.seed.fixture, "indexer-snapshot-file-compact");
	}
});
