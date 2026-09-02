#!/usr/bin/env node

import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
	CUT_LIFECYCLE_CENSUS_SCENARIOS,
	buildCutLifecycleCensusReport,
	buildCutLifecycleComparison,
	deleteHeadMatches,
	parseCutLifecycleCensusArgs,
	validateCutLifecycleState,
} from "./shared-log-cut-lifecycle-census-lib.mjs";
import {
	collectLifecycleDebt,
	elapsed,
	fingerprint,
	lifecycleWorkerArgs,
	runCheckpointedCensus,
	runJsonWorker,
	runLifecycleScenario,
	runOpenedLifecycleWorker,
	waitForLifecycleQuiescence,
	withMatchedScenarios,
} from "./shared-log-lifecycle-census-common.mjs";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const STORE_ID = Uint8Array.from(
	{ length: 32 },
	(_, index) => (index * 17 + 23) & 0xff,
);

const collectState = async ({ store, EntryType }) => {
	const log = store.docs.log;
	const backbone = log._nativeBackbone;
	if (!backbone) {
		throw new Error("CUT lifecycle census requires a native backbone");
	}
	await waitForLifecycleQuiescence(log, { label: "CUT lifecycle cleanup" });
	let nativeHeads = backbone.graph.heads();
	const nativeHeadRows = nativeHeads.length;
	let unmatchedNativeHeads = new Set(nativeHeads);
	const nativeHeadDuplicateRows = nativeHeadRows - unmatchedNativeHeads.size;
	const nativeHeadFingerprint = fingerprint(nativeHeads);
	nativeHeads = undefined;
	let headRows = 0;
	let cutHeadRows = 0;
	let nonCutHeadRows = 0;
	let lowerHeadNotNativeRows = 0;
	let retainedLowerShallowMissing = 0;
	let retainedNativeGraphMissing = 0;
	let retainedDurableBlockMissing = 0;
	const headIterator = log.log.getHeads({
		type: "shape",
		shape: { hash: true, meta: { type: true, gid: true } },
	});
	try {
		for (;;) {
			const heads = await headIterator.next(256);
			if (heads.length === 0) break;
			const hashes = heads.map((head) => head.hash);
			const [retainedShallowRows, retainedDurableBlocks] = await Promise.all([
				Promise.all(
					hashes.map(async (hash) => (await log.log.getShallow(hash)) != null),
				),
				log.remoteBlocks.hasMany(hashes),
			]);
			const retainedNativeHashes = backbone.graph.hasMany(hashes);
			lowerHeadNotNativeRows += deleteHeadMatches(unmatchedNativeHeads, hashes);
			for (let index = 0; index < heads.length; index++) {
				const head = heads[index];
				headRows++;
				if (head.meta.type === EntryType.CUT) cutHeadRows++;
				else nonCutHeadRows++;
				if (!retainedShallowRows[index]) retainedLowerShallowMissing++;
				if (!retainedDurableBlocks[index]) retainedDurableBlockMissing++;
				if (!retainedNativeHashes.has(head.hash)) retainedNativeGraphMissing++;
			}
		}
	} finally {
		await headIterator.close();
	}
	const nativeHeadNotLowerRows = unmatchedNativeHeads.size;
	unmatchedNativeHeads.clear();
	unmatchedNativeHeads = undefined;
	let coordinateHashes = backbone.getEntryCoordinateHashes();
	const nativeCoordinateHashes = coordinateHashes.length;
	const coordinateFingerprint = fingerprint(coordinateHashes);
	coordinateHashes = undefined;
	const documents = await store.docs.index
		.iterate({}, { resolve: false, local: true, remote: false })
		.all();
	const documentValues = documents.map((document) =>
		JSON.stringify([String(document.id), String(document.name)]),
	);
	const replicationRanges = (await log.replicationIndex.iterate().all()).length;
	const debt = collectLifecycleDebt(log, backbone, true);
	return {
		logRows: log.log.length,
		graphRows: backbone.graph.length,
		nativeLogRows: backbone.logLength,
		nativeBlockRows: backbone.blockLength,
		headRows,
		nativeHeadRows,
		nativeHeadDuplicateRows,
		lowerHeadNotNativeRows,
		nativeHeadNotLowerRows,
		cutHeadRows,
		nonCutHeadRows,
		retainedLowerShallowMissing,
		retainedNativeGraphMissing,
		retainedDurableBlockMissing,
		rustCoordinateRows: await log.entryCoordinatesIndex.count(),
		residentCoordinateRows: log._residentEntryCoordinatesByHash?.size ?? null,
		coordinateIndexRows: backbone.coordinateIndexLength,
		coordinateValueRows: backbone.coordinateValueLength,
		nativeCoordinateHashes,
		documentRows: await store.docs.index.getSize(),
		nativeDocumentIndexRows: backbone.documentIndexLength,
		nativeDocumentValueRows: backbone.documentValueLength,
		enumeratedDocumentRows: documents.length,
		documentsFingerprint: fingerprint(documentValues),
		durableBlockBytes: await log.remoteBlocks.localStore.size(),
		replicationRanges,
		replicators: (await log.getReplicators()).size,
		activeReplicators: log.uniqueReplicators?.size ?? null,
		assignedHeads: await log.countAssignedHeads({ strict: true }),
		nativeHeadFingerprint,
		coordinateFingerprint,
		debt,
	};
};

const scenarioOperations = (options) =>
	options.scenario === "fresh"
		? options.keyCount * 2
		: options.historyOperations;

const seedWorker = async (options) => {
	const operations = scenarioOperations(options);
	const cycles = operations / (options.keyCount * 2);
	return runOpenedLifecycleWorker({
		options,
		storeId: STORE_ID,
		peakRssField: "throughClosePeakRssBytes",
		work: async ({ runtime, store }) => {
			let puts = 0;
			let deletes = 0;
			let nextProgress = Math.max(1_000, Math.ceil(operations / 100));
			const mutationStarted = performance.now();
			for (let cycle = 0; cycle < cycles; cycle++) {
				for (
					let start = 0;
					start < options.keyCount;
					start += options.batchSize
				) {
					const end = Math.min(start + options.batchSize, options.keyCount);
					const keys = Array.from(
						{ length: end - start },
						(_, offset) => start + offset,
					);
					const result = await store.docs.putMany(
						keys.map(
							(key) =>
								new runtime.Document({
									id: `cut-census-${key}`,
									name: `cycle-${cycle}`,
								}),
						),
						{ unique: true },
					);
					puts += result.entries.length;
					for (const key of keys) {
						await store.docs.del(`cut-census-${key}`);
						deletes++;
					}
					if (puts + deletes >= nextProgress) {
						console.error(
							`[cut-lifecycle-worker] run=${options.run} scenario=${options.scenario} phase=seed operations=${puts + deletes}/${operations}`,
						);
						nextProgress += Math.max(1_000, Math.ceil(operations / 100));
					}
				}
			}
			const mutationMs = elapsed(mutationStarted);
			if (puts + deletes !== operations) {
				throw new Error(
					`executed ${puts + deletes} mutations, expected ${operations}`,
				);
			}
			const cleanupDrainMs = await waitForLifecycleQuiescence(store.docs.log, {
				label: "CUT lifecycle cleanup",
			});
			const validationStarted = performance.now();
			const state = await collectState({
				store,
				EntryType: runtime.EntryType,
			});
			const validation = validateCutLifecycleState({
				state,
				expectedOperations: operations,
				expectedCutHeads: deletes,
				phase: "seed",
			});
			const validationMs = elapsed(validationStarted);
			return {
				operations,
				cycles,
				puts,
				deletes,
				mutationMs,
				mutationOpsPerSecond: Math.round((operations / mutationMs) * 1000),
				cleanupDrainMs,
				validationMs,
				state,
				validation,
			};
		},
	});
};

const reopenWorker = async (options) => {
	const operations = scenarioOperations(options);
	const expectedCutHeads = operations / 2;
	return runOpenedLifecycleWorker({
		options,
		storeId: STORE_ID,
		peakRssField: "throughClosePeakRssBytes",
		work: async ({ runtime, store }) => {
			const validationStarted = performance.now();
			const state = await collectState({
				store,
				EntryType: runtime.EntryType,
			});
			const validation = validateCutLifecycleState({
				state,
				expectedOperations: operations,
				expectedCutHeads,
				phase: "reopen",
			});
			const validationMs = elapsed(validationStarted);
			return {
				operations,
				validationMs,
				state,
				validation,
			};
		},
	});
};

const runWorkerProcess = (options) => {
	return runJsonWorker({
		scriptPath: SCRIPT_PATH,
		args: lifecycleWorkerArgs(options, [
			["history-operations", "historyOperations"],
			["key-count", "keyCount"],
			["batch-size", "batchSize"],
		]),
		description: `CUT lifecycle-census worker failed (${options.scenario}, ${options.phase}, run=${options.run})`,
		maxBuffer: 10 * 1024 * 1024,
		streamStderr: true,
	});
};

const runMatchedRow = async (options, run, onPhase) => {
	const order = run % 2 === 1 ? ["fresh", "history"] : ["history", "fresh"];
	const { fresh, history } = await withMatchedScenarios({
		prefix: "peerbit-cut-lifecycle-census-",
		options,
		run,
		order,
		runScenario: (rowOptions, scenario, rowRun, root) =>
			runLifecycleScenario({
				options: rowOptions,
				scenario,
				run: rowRun,
				root,
				runWorker: runWorkerProcess,
				onPhase,
				stableFields: [
					"logRows",
					"graphRows",
					"nativeLogRows",
					"cutHeadRows",
					"documentRows",
					"nativeDocumentIndexRows",
					"nativeDocumentValueRows",
					"enumeratedDocumentRows",
					"durableBlockBytes",
					"coordinateFingerprint",
				],
				validation: (stable) => ({
					stableFieldsMatchAfterProcessColdReopen: true,
					...stable,
				}),
			}),
	});
	const comparison = buildCutLifecycleComparison(fresh, history);
	if (!comparison.visibleStateMatchesFresh) {
		throw new Error(
			`historical visible state differs from fresh: ${comparison.unequalVisibleFields.join(", ")}`,
		);
	}
	return { run, executionOrder: order, fresh, history, comparison };
};

const renderHuman = (report) =>
	console.table(report.rows.map((row) => row.comparison.logicalHistory));

const usage = () =>
	`Usage: node scripts/bench/shared-log-cut-lifecycle-census.mjs [options]
See scripts/bench/SHARED_LOG_CUT_LIFECYCLE_CENSUS.md for the canonical command.`;

const main = async () => {
	const options = parseCutLifecycleCensusArgs(
		process.argv.slice(2),
		process.env,
	);
	if (options.mode === "help") {
		console.log(usage());
		return;
	}
	if (options.mode === "worker") {
		console.log(
			JSON.stringify(
				options.phase === "seed"
					? await seedWorker(options)
					: await reopenWorker(options),
			),
		);
		return;
	}

	await runCheckpointedCensus({
		options,
		scenarios: CUT_LIFECYCLE_CENSUS_SCENARIOS,
		buildReport: buildCutLifecycleCensusReport,
		renderHuman,
		preserveActiveOnFailure: true,
		logRun: (run) =>
			console.error(`[cut-lifecycle-census] run=${run}/${options.runs}`),
		runRow: async (rowOptions, run, setActive) => {
			const completed = {};
			return runMatchedRow(
				rowOptions,
				run,
				async (scenario, phase, measurement) => {
					(completed[scenario] ??= {})[phase] = measurement;
					await setActive({ run, completed });
				},
			);
		},
	});
};

if (process.argv[1] === SCRIPT_PATH) {
	main().catch((error) => {
		console.error(error instanceof Error ? error.stack : error);
		process.exitCode = 1;
	});
}
