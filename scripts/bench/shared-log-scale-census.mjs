#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
	SCALE_CENSUS_GID_CHARACTERS,
	SCALE_CENSUS_HASH_CHARACTERS,
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_SCENARIOS,
	buildScaleCensusReport,
	isPersistentScaleCensusScenario,
	makeScaleCensusGid,
	makeScaleCensusHash,
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

const chainGid = makeScaleCensusGid(0);

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
	hash: makeScaleCensusHash(index),
	gid: topology === "chain" ? chainGid : makeScaleCensusGid(index),
	next:
		topology === "chain" && index > 0 ? [makeScaleCensusHash(index - 1)] : [],
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
	if (
		!graph.has(makeScaleCensusHash(0)) ||
		!graph.has(makeScaleCensusHash(count - 1))
	) {
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
			makeScaleCensusHash(index),
			makeScaleCensusGid(index),
			[index],
			false,
			1,
			index,
		);
	}
	const elapsedMs = performance.now() - started;
	const first = state.getEntryCoordinates(makeScaleCensusHash(0));
	const last = state.getEntryCoordinates(makeScaleCensusHash(count - 1));
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
		phase: "measure",
		...memoryDeltas(measured.empty, measured.populated, count),
		validation: measured.validation,
	};
};

const runWorkerProcess = ({ scenario, count, run, phase, directory }) => {
	const workerArguments = [
		"--expose-gc",
		SCRIPT_PATH,
		"--worker",
		"--scenario",
		scenario,
		"--count",
		String(count),
		"--run",
		String(run),
		"--phase",
		phase,
	];
	if (directory) {
		workerArguments.push("--directory", directory);
	}
	const child = spawnSync(process.execPath, workerArguments, {
		encoding: "utf8",
		env: process.env,
		maxBuffer: 1024 * 1024,
	});
	if (child.status !== 0) {
		throw new Error(
			`scale-census worker failed (${scenario}, count=${count}, run=${run}, phase=${phase})\n${child.stderr || child.stdout}`,
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
	if (
		row?.scenario !== scenario ||
		row?.count !== count ||
		row?.run !== run ||
		row?.phase !== phase
	) {
		throw new Error(
			`scale-census worker returned the wrong row for ${scenario}, count=${count}, run=${run}, phase=${phase}`,
		);
	}
	return row;
};

const withoutWorkerIdentity = (workerRow) => {
	const metrics = { ...workerRow };
	for (const key of ["scenario", "count", "run", "phase"]) {
		delete metrics[key];
	}
	return metrics;
};

const sameLogicalFootprint = (before, after) =>
	before.logicalBytes === after.logicalBytes &&
	before.files.length === after.files.length &&
	before.files.every(
		(file, index) =>
			file.path === after.files[index].path &&
			file.logicalBytes === after.files[index].logicalBytes,
	);

const runIsolatedRow = async ({ scenario, count, run }) => {
	if (!isPersistentScaleCensusScenario(scenario)) {
		return {
			kind: "resident",
			...withoutWorkerIdentity(
				runWorkerProcess({ scenario, count, run, phase: "measure" }),
			),
			scenario,
			count,
			run,
		};
	}

	const directory = await mkdtemp(
		join(os.tmpdir(), "peerbit-shared-log-scale-census-"),
	);
	try {
		const seed = runWorkerProcess({
			scenario,
			count,
			run,
			phase: "seed",
			directory,
		});
		const reopen = runWorkerProcess({
			scenario,
			count,
			run,
			phase: "reopen",
			directory,
		});
		return {
			scenario,
			count,
			run,
			kind: "persistent-reopen",
			seed: withoutWorkerIdentity(seed),
			reopen: withoutWorkerIdentity(reopen),
			validation: {
				seededEntries: seed.validation.retainedEntriesInSnapshot,
				reopenedEntries: reopen.validation.retainedEntries,
				logicalFootprintStable: sameLogicalFootprint(seed.disk, reopen.disk),
			},
		};
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
};

const hostMetadata = () => ({
	node: process.version,
	v8: process.versions.v8,
	platform: process.platform,
	arch: process.arch,
	cpu: os.cpus()[0]?.model ?? "unknown",
	logicalCpus: os.cpus().length,
	totalMemoryBytes: os.totalmem(),
	hashCharacters: SCALE_CENSUS_HASH_CHARACTERS,
	gidCharacters: SCALE_CENSUS_GID_CHARACTERS,
	payloadSizeMetadataBytes: 1,
});

const renderHuman = (report) => {
	console.log(`${SCALE_CENSUS_NAME} (${report.meta.node}, ${report.meta.cpu})`);
	console.table(
		report.rows.map((row) =>
			row.kind === "resident"
				? {
						scenario: row.scenario,
						entries: row.count,
						run: row.run,
						operationMs: row.elapsedMs,
						opsPerSecond: row.opsPerSecond,
						rssDeltaMiB:
							Math.round((row.rssDeltaBytes / 1024 / 1024) * 10) / 10,
						rssBytesPerEntry: row.rssBytesPerEntry,
					}
				: {
						scenario: row.scenario,
						entries: row.count,
						run: row.run,
						operationMs: row.reopen.openMs,
						opsPerSecond: row.reopen.openEntriesPerSecond,
						rssDeltaMiB:
							Math.round((row.reopen.rssDeltaBytes / 1024 / 1024) * 10) / 10,
						rssBytesPerEntry: row.reopen.rssBytesPerEntry,
						diskMiB:
							Math.round((row.reopen.disk.logicalBytes / 1024 / 1024) * 10) /
							10,
						diskBytesPerEntry: row.reopen.disk.logicalBytesPerEntry,
						snapshotMs: row.seed.snapshotMs,
						reopenCloseMs: row.reopen.closeMs,
					},
		),
	);
};

const writeReport = async (path, report) => {
	const destination = resolve(path);
	await mkdir(dirname(destination), { recursive: true });
	const temporary = `${destination}.${process.pid}.tmp`;
	await writeFile(temporary, `${JSON.stringify(report, null, 2)}\n`);
	await rename(temporary, destination);
};

const usage = () => `Usage:
  node scripts/bench/shared-log-scale-census.mjs [options]

Options:
  --counts <csv>       Entry counts (default: 100000,1000000)
  --scenarios <csv>    ${SCALE_CENSUS_SCENARIOS.join(",")}
  --runs <n>           Isolated runs per scenario/count (default: 1)
  --output <path>      Atomically checkpoint the JSON report after each row
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
		if (isPersistentScaleCensusScenario(options.scenario)) {
			const { reopenPersistentScaleScenario, seedPersistentScaleScenario } =
				await import("./shared-log-scale-persistence.mjs");
			console.log(
				JSON.stringify(
					options.phase === "seed"
						? await seedPersistentScaleScenario(options)
						: await reopenPersistentScaleScenario(options),
				),
			);
			return;
		}
		console.log(JSON.stringify(await runWorker(options)));
		return;
	}

	const rows = [];
	const host = hostMetadata();
	let activeRow = null;
	let failure = null;
	const report = () =>
		buildScaleCensusReport({
			counts: options.counts,
			scenarios: options.scenarios,
			runs: options.runs,
			rows,
			host,
			activeRow,
			failure,
		});
	const checkpoint = async () => {
		if (options.output) {
			await writeReport(options.output, report());
		}
	};
	await checkpoint();
	for (const scenario of options.scenarios) {
		for (const count of options.counts) {
			for (let run = 1; run <= options.runs; run++) {
				activeRow = { scenario, count, run };
				await checkpoint();
				console.error(
					`[scale-census] scenario=${scenario} count=${count} run=${run}/${options.runs}`,
				);
				let row;
				try {
					row = await runIsolatedRow({ scenario, count, run });
				} catch (error) {
					failure = {
						...activeRow,
						message: error instanceof Error ? error.message : String(error),
					};
					activeRow = null;
					await checkpoint();
					throw error;
				}
				rows.push(row);
				activeRow = null;
				await checkpoint();
			}
		}
	}
	const completedReport = report();
	if (options.json) {
		console.log(JSON.stringify(completedReport, null, 2));
	} else {
		renderHuman(completedReport);
	}
};

main().catch((error) => {
	console.error(error instanceof Error ? error.stack : error);
	process.exitCode = 1;
});
