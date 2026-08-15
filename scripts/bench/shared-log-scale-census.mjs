#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import os from "node:os";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_SCENARIOS,
	buildScaleCensusReport,
	memoryDeltas,
	parseScaleCensusArgs,
} from "./shared-log-scale-census-lib.mjs";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const LOG_RUST_URL = new URL(
	"../../packages/log/rust/dist/src/index.js",
	import.meta.url,
);
const SHARED_LOG_RUST_URL = new URL(
	"../../packages/programs/data/shared-log/rust/dist/src/index.js",
	import.meta.url,
);

const HASH_PREFIX = "bafybeigdyrzt";
const HASH_SUFFIX_LENGTH = 59 - HASH_PREFIX.length;
const GID_PREFIX = "gid-";
const GID_SUFFIX_LENGTH = 44 - GID_PREFIX.length;

const makeHash = (index) =>
	`${HASH_PREFIX}${index.toString(36).padStart(HASH_SUFFIX_LENGTH, "0")}`;

const makeGid = (index) =>
	`${GID_PREFIX}${index.toString(36).padStart(GID_SUFFIX_LENGTH, "0")}`;

const chainGid = makeGid(0);

const collectGarbage = () => {
	if (typeof globalThis.gc !== "function") {
		throw new Error("scale-census workers require Node.js --expose-gc");
	}
	globalThis.gc();
	globalThis.gc();
};

const memorySnapshot = () => {
	const usage = process.memoryUsage();
	return {
		rss: usage.rss,
		heapTotal: usage.heapTotal,
		heapUsed: usage.heapUsed,
		external: usage.external,
		arrayBuffers: usage.arrayBuffers,
	};
};

const graphEntry = (index, topology) => ({
	hash: makeHash(index),
	gid: topology === "chain" ? chainGid : makeGid(index),
	next: topology === "chain" && index > 0 ? [makeHash(index - 1)] : [],
	type: 0,
	head: true,
	payloadSize: 1,
	clock: { timestamp: { wallTime: BigInt(index + 1), logical: 0 } },
});

const measureNativeGraph = async (scenario, count) => {
	const { createLogGraphIndex } = await import(LOG_RUST_URL);
	const graph = await createLogGraphIndex();
	collectGarbage();
	const empty = memorySnapshot();
	const topology = scenario === "native-graph-chain" ? "chain" : "roots";
	const started = performance.now();
	for (let index = 0; index < count; index++) {
		graph.put(graphEntry(index, topology));
	}
	const elapsedMs = performance.now() - started;
	if (graph.length !== count) {
		throw new Error(
			`native graph retained ${graph.length} of ${count} entries`,
		);
	}
	if (!graph.has(makeHash(0)) || !graph.has(makeHash(count - 1))) {
		throw new Error("native graph failed endpoint membership validation");
	}
	collectGarbage();
	return {
		empty,
		populated: memorySnapshot(),
		elapsedMs,
		validation: {
			retainedEntries: graph.length,
			endpointHashesPresent: true,
		},
	};
};

const measureCoordinateFrontier = async (count) => {
	const { createSharedLogState } = await import(SHARED_LOG_RUST_URL);
	const state = await createSharedLogState("u32");
	collectGarbage();
	const empty = memorySnapshot();
	const started = performance.now();
	for (let index = 0; index < count; index++) {
		state.putEntryCoordinates(
			makeHash(index),
			makeGid(index),
			[index],
			false,
			1,
			index,
		);
	}
	const elapsedMs = performance.now() - started;
	const first = state.getEntryCoordinates(makeHash(0));
	const last = state.getEntryCoordinates(makeHash(count - 1));
	if (first?.[0] !== 0 || last?.[0] !== count - 1) {
		throw new Error("coordinate frontier failed endpoint validation");
	}
	collectGarbage();
	return {
		empty,
		populated: memorySnapshot(),
		elapsedMs,
		validation: {
			attemptedRows: count,
			endpointCoordinates: [first[0], last[0]],
		},
	};
};

const runWorker = async ({ scenario, count, run }) => {
	collectGarbage();
	const processStart = memorySnapshot();
	const measured =
		scenario === "coordinate-frontier"
			? await measureCoordinateFrontier(count)
			: await measureNativeGraph(scenario, count);
	const elapsedMs = Math.round(measured.elapsedMs * 1000) / 1000;
	return {
		scenario,
		count,
		run,
		elapsedMs,
		opsPerSecond: Math.round((count / measured.elapsedMs) * 1000),
		fixedRuntimeRssBytes: measured.empty.rss - processStart.rss,
		maxRssBytes: process.resourceUsage().maxRSS * 1024,
		...memoryDeltas(measured.empty, measured.populated, count),
		validation: measured.validation,
	};
};

const runIsolatedRow = ({ scenario, count, run }) => {
	const child = spawnSync(
		process.execPath,
		[
			"--expose-gc",
			SCRIPT_PATH,
			"--worker",
			"--scenario",
			scenario,
			"--count",
			String(count),
			"--run",
			String(run),
		],
		{
			encoding: "utf8",
			env: process.env,
			maxBuffer: 1024 * 1024,
		},
	);
	if (child.status !== 0) {
		throw new Error(
			`scale-census worker failed (${scenario}, count=${count}, run=${run})\n${child.stderr || child.stdout}`,
		);
	}
	const output = child.stdout.trim();
	if (!output) {
		throw new Error("scale-census worker produced no result");
	}
	let row;
	try {
		row = JSON.parse(output);
	} catch (error) {
		throw new Error(`scale-census worker produced invalid JSON: ${output}`, {
			cause: error,
		});
	}
	if (row?.scenario !== scenario || row?.count !== count || row?.run !== run) {
		throw new Error(
			`scale-census worker returned the wrong row for ${scenario}, count=${count}, run=${run}`,
		);
	}
	return row;
};

const hostMetadata = () => ({
	node: process.version,
	v8: process.versions.v8,
	platform: process.platform,
	arch: process.arch,
	cpu: os.cpus()[0]?.model ?? "unknown",
	logicalCpus: os.cpus().length,
	totalMemoryBytes: os.totalmem(),
	hashCharacters: 59,
	gidCharacters: 44,
	payloadSizeMetadataBytes: 1,
});

const renderHuman = (report) => {
	console.log(`${SCALE_CENSUS_NAME} (${report.meta.node}, ${report.meta.cpu})`);
	console.table(
		report.rows.map((row) => ({
			scenario: row.scenario,
			entries: row.count,
			run: row.run,
			elapsedMs: row.elapsedMs,
			opsPerSecond: row.opsPerSecond,
			rssDeltaMiB: Math.round((row.rssDeltaBytes / 1024 / 1024) * 10) / 10,
			rssBytesPerEntry: row.rssBytesPerEntry,
		})),
	);
};

const usage = () => `Usage:
  node scripts/bench/shared-log-scale-census.mjs [options]

Options:
  --counts <csv>       Entry counts (default: 100000,1000000)
  --scenarios <csv>    ${SCALE_CENSUS_SCENARIOS.join(",")}
  --runs <n>           Isolated runs per scenario/count (default: 1)
  --json               Emit the versioned JSON report
  --help               Show this help
`;

const main = async () => {
	const options = parseScaleCensusArgs(process.argv.slice(2), process.env);
	if (options.mode === "help") {
		console.log(usage());
		return;
	}
	if (options.mode === "worker") {
		console.log(JSON.stringify(await runWorker(options)));
		return;
	}

	const rows = [];
	for (const scenario of options.scenarios) {
		for (const count of options.counts) {
			for (let run = 1; run <= options.runs; run++) {
				console.error(
					`[scale-census] scenario=${scenario} count=${count} run=${run}/${options.runs}`,
				);
				rows.push(runIsolatedRow({ scenario, count, run }));
			}
		}
	}
	const report = buildScaleCensusReport({
		counts: options.counts,
		scenarios: options.scenarios,
		runs: options.runs,
		rows,
		host: hostMetadata(),
	});
	if (options.json) {
		console.log(JSON.stringify(report, null, 2));
	} else {
		renderHuman(report);
	}
};

main().catch((error) => {
	console.error(error instanceof Error ? error.stack : error);
	process.exitCode = 1;
});
