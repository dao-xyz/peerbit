#!/usr/bin/env node

import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
	collectLifecycleDebt,
	elapsed,
	fingerprint,
	lifecycleWorkerArgs,
	mapEdges,
	optionalSize,
	runCheckpointedCensus,
	runJsonWorker,
	runLifecycleScenario,
	runOpenedLifecycleWorker,
	waitForLifecycleQuiescence,
	withMatchedScenarios,
} from "./shared-log-lifecycle-census-common.mjs";
import {
	LIFECYCLE_CENSUS_NAME,
	LIFECYCLE_CENSUS_SCENARIOS,
	buildLifecycleCensusReport,
	buildLifecycleComparison,
	parseLifecycleCensusArgs,
} from "./shared-log-lifecycle-census-lib.mjs";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const STORE_ID = Uint8Array.from(
	{ length: 32 },
	(_, index) => (index * 13 + 7) & 0xff,
);

const documentFingerprintValue = ({ id, name }) =>
	JSON.stringify([String(id), String(name)]);

const normalizeRange = (range) => ({
	hash: range.hash,
	start1: String(range.start1),
	end1: String(range.end1),
	start2: String(range.start2),
	end2: String(range.end2),
	width: String(range.width),
	mode: range.mode,
});

const collectState = async ({
	store,
	firstDocumentIndex,
	oldestRetainedIndex,
	newestRetainedIndex,
	probeHash,
	retainedProbeHash,
	liveGids,
}) => {
	const log = store.docs.log;
	const backbone = log._nativeBackbone;
	if (!backbone) throw new Error("Lifecycle census requires a native backbone");
	await waitForLifecycleQuiescence(log, { cleanup: false });
	const heads = await log.log
		.getHeads({ type: "shape", shape: { hash: true } })
		.all();
	const hashes = heads.map((head) => head.hash);
	const [retainedShallowRows, retainedDurableBlocks] = await Promise.all([
		Promise.all(
			hashes.map(async (hash) => (await log.log.getShallow(hash)) != null),
		),
		log.remoteBlocks.hasMany(hashes),
	]);
	const nativeHeads = backbone.graph.heads();
	const retainedNativeHashes = backbone.graph.hasMany(hashes);
	const coordinateHashes = backbone.getEntryCoordinateHashes();
	const rangeResults = await log.replicationIndex.iterate().all();
	const ranges = rangeResults.map((result) => normalizeRange(result.value));
	const gidHistory = log._gidPeersHistory;
	const cleanup = log._gidPeerHistoryCleanupState;
	const firstDocumentId = `doc-${firstDocumentIndex}`;
	const oldestRetainedId = `doc-${oldestRetainedIndex}`;
	const newestRetainedId = `doc-${newestRetainedIndex}`;
	const documents = await store.docs.index
		.iterate({}, { resolve: false, local: true, remote: false })
		.all();
	const documentsById = new Map(
		documents.map((document) => [String(document.id), document]),
	);
	const firstDocument = documentsById.get(firstDocumentId);
	const oldestRetained = documentsById.get(oldestRetainedId);
	const newestRetained = documentsById.get(newestRetainedId);
	const documentValues = documents.map(documentFingerprintValue);
	const observedDocuments = new Set(documentValues);
	const expectedDocumentValues = Array.from(
		{ length: newestRetainedIndex - oldestRetainedIndex + 1 },
		(_, offset) => {
			const index = oldestRetainedIndex + offset;
			return documentFingerprintValue({
				id: `doc-${index}`,
				name: `value-${index}`,
			});
		},
	);
	const expectedDocuments = new Set(expectedDocumentValues);
	return {
		logRows: log.log.length,
		graphRows: backbone.graph.length,
		nativeLogRows: backbone.logLength,
		nativeBlockRows: backbone.blockLength,
		retainedLowerShallowMissing: retainedShallowRows.reduce(
			(missing, present) => missing + (present ? 0 : 1),
			0,
		),
		retainedNativeGraphMissing: hashes.length - retainedNativeHashes.size,
		retainedDurableBlockMissing: hashes.reduce(
			(missing, _, index) =>
				missing + (retainedDurableBlocks[index] === true ? 0 : 1),
			0,
		),
		headRows: heads.length,
		nativeHeadRows: nativeHeads.length,
		rustCoordinateRows: await log.entryCoordinatesIndex.count(),
		residentCoordinateRows: log._residentEntryCoordinatesByHash?.size ?? null,
		coordinateIndexRows: backbone.coordinateIndexLength,
		coordinateValueRows: backbone.coordinateValueLength,
		nativeCoordinateHashes: coordinateHashes.length,
		documentRows: await store.docs.index.getSize(),
		nativeDocumentIndexRows: backbone.documentIndexLength,
		nativeDocumentValueRows: backbone.documentValueLength,
		enumeratedDocumentRows: documents.length,
		documentsFingerprint: fingerprint(documentValues),
		expectedDocumentsFingerprint: fingerprint(expectedDocumentValues),
		unexpectedDocumentRows: documentValues.filter(
			(value) => !expectedDocuments.has(value),
		).length,
		missingDocumentRows: expectedDocumentValues.filter(
			(value) => !observedDocuments.has(value),
		).length,
		duplicateDocumentRows: documentValues.length - observedDocuments.size,
		durableBlockBytes: await log.remoteBlocks.localStore.size(),
		replicationRanges: ranges.length,
		rangeRows: ranges,
		replicators: (await log.getReplicators()).size,
		activeReplicators: log.uniqueReplicators?.size ?? null,
		assignedHeads: await log.countAssignedHeads({ strict: true }),
		entriesFingerprint: fingerprint(hashes),
		headsFingerprint: fingerprint(nativeHeads),
		coordinatesFingerprint: fingerprint(coordinateHashes),
		// A full-ring range chooses an arbitrary wrap point on each open. Hash,
		// width, and mode describe the stable ownership coverage for this workload.
		rangesFingerprint: fingerprint(
			ranges.map((range) =>
				JSON.stringify({
					hash: range.hash,
					width: range.width,
					mode: range.mode,
				}),
			),
		),
		gidHistoryRows: gidHistory.size,
		gidHistoryEdges: mapEdges(gidHistory),
		gidHistoryStaleRows: liveGids
			? [...gidHistory.keys()].filter((gid) => !liveGids.has(gid)).length
			: null,
		gidHistoryUntrackedLiveRows: liveGids
			? [...liveGids].filter((gid) => !gidHistory.has(gid)).length
			: null,
		knownPeerRows: optionalSize(log._entryKnownPeers),
		knownPeerEdges: mapEdges(log._entryKnownPeers),
		knownPeerObservedRows: optionalSize(log._entryKnownPeerObservedAt),
		knownPeerObservedEdges: mapEdges(log._entryKnownPeerObservedAt),
		gidCleanupHighWater: cleanup?.highWater ?? 0,
		...collectLifecycleDebt(log, backbone).values,
		probe: {
			lowerShallow: (await log.log.getShallow(probeHash)) != null,
			nativeLog: backbone.hasLogEntry(probeHash),
			durableBlock: await log.remoteBlocks.has(probeHash),
			nativeBlock: backbone.hasBlock(probeHash),
			firstDocument: firstDocument
				? { id: firstDocument.id, name: firstDocument.name }
				: null,
			oldestRetained: oldestRetained
				? { id: oldestRetained.id, name: oldestRetained.name }
				: null,
			newestRetained: newestRetained
				? { id: newestRetained.id, name: newestRetained.name }
				: null,
			retained: {
				lowerShallow: (await log.log.getShallow(retainedProbeHash)) != null,
				nativeLog: backbone.hasLogEntry(retainedProbeHash),
				durableBlock: await log.remoteBlocks.has(retainedProbeHash),
				nativeBlock: backbone.hasBlock(retainedProbeHash),
			},
		},
	};
};

const validateState = ({
	state,
	phase,
	scenario,
	retain,
	firstDocumentIndex,
	oldestRetainedIndex,
	newestRetainedIndex,
}) => {
	const failures = [];
	const exactRetain = [
		"logRows",
		"graphRows",
		"nativeLogRows",
		"headRows",
		"nativeHeadRows",
		"residentCoordinateRows",
		"coordinateIndexRows",
		"coordinateValueRows",
		"nativeCoordinateHashes",
		"documentRows",
		"nativeDocumentIndexRows",
		"nativeDocumentValueRows",
		"enumeratedDocumentRows",
	];
	for (const field of exactRetain) {
		if (state[field] !== retain) {
			failures.push(`${field}=${state[field]}, expected ${retain}`);
		}
	}
	if (
		(phase === "seed" && state.nativeBlockRows !== retain) ||
		state.nativeBlockRows > retain
	) {
		failures.push(
			`nativeBlockRows=${state.nativeBlockRows}, expected ${phase === "seed" ? retain : `at most ${retain}`}`,
		);
	}
	if (state.durableBlockBytes <= 0) {
		failures.push(
			`durableBlockBytes=${state.durableBlockBytes}, expected a positive live footprint`,
		);
	}
	if (state.documentsFingerprint !== state.expectedDocumentsFingerprint) {
		failures.push(
			`documentsFingerprint=${state.documentsFingerprint}, expected ${state.expectedDocumentsFingerprint}`,
		);
	}
	if (state.rustCoordinateRows !== 0) {
		failures.push(
			`rustCoordinateRows=${state.rustCoordinateRows}, expected 0 with native WAL persistence`,
		);
	}
	if (
		state.replicationRanges !== 1 ||
		state.replicators !== 1 ||
		state.activeReplicators !== 1
	) {
		failures.push(
			`replication ranges/replicators/active=${state.replicationRanges}/${state.replicators}/${state.activeReplicators}, expected 1/1/1`,
		);
	}
	if (state.assignedHeads !== retain) {
		failures.push(`assignedHeads=${state.assignedHeads}, expected ${retain}`);
	}
	for (const field of [
		"retainedLowerShallowMissing",
		"retainedNativeGraphMissing",
		"retainedDurableBlockMissing",
		"unexpectedDocumentRows",
		"missingDocumentRows",
		"duplicateDocumentRows",
		"knownPeerRows",
		"knownPeerEdges",
		"knownPeerObservedRows",
		"knownPeerObservedEdges",
		"pendingGidCleanup",
		"pendingIHave",
		"pendingIHaveCallbacks",
		"pendingMaturityOwners",
		"pendingMaturityRanges",
		"repairRetryTimers",
		"repairPendingModes",
		"repairPendingPeers",
		"repairFrontierTargets",
		"repairFrontierActiveTargets",
		"repairFrontierBypassKnownPeers",
		"repairOptimisticGids",
		"repairOptimisticPeers",
		"appendBackfillTargets",
		"appendBackfillRows",
		"checkedPrunePendingDeletes",
		"checkedPruneRetries",
		"writeThroughPendingDurableWrites",
		"writeThroughDeleteTombstones",
		"writeThroughPendingDeleteCleanup",
		"writeThroughStagedDeleteBatches",
		"writeThroughCommitOwnerships",
		"coordinatePendingJournalRows",
		"coordinatePendingJournalBytes",
		"documentPendingJournalRows",
		"documentPendingJournalBytes",
		"documentSignerPendingJournalRows",
		"documentSignerPendingJournalBytes",
	]) {
		if (state[field] !== 0) {
			failures.push(`${field}=${state[field]}, expected 0`);
		}
	}
	if (state.gidHistoryStaleRows != null && state.gidHistoryStaleRows !== 0) {
		failures.push(
			`gidHistoryStaleRows=${state.gidHistoryStaleRows}, expected 0`,
		);
	}
	const expectedHistoryRows = phase === "seed" ? retain : 0;
	if (state.gidHistoryRows !== expectedHistoryRows) {
		failures.push(
			`gidHistoryRows=${state.gidHistoryRows}, expected ${expectedHistoryRows}`,
		);
	}
	if (state.gidHistoryEdges !== expectedHistoryRows) {
		failures.push(
			`gidHistoryEdges=${state.gidHistoryEdges}, expected ${expectedHistoryRows}`,
		);
	}
	if (state.gidCleanupHighWater > 4096) {
		failures.push(
			`gidCleanupHighWater=${state.gidCleanupHighWater}, exceeds the 4096 cap`,
		);
	}
	if (phase === "seed" && state.gidHistoryUntrackedLiveRows !== 0) {
		failures.push(
			`gidHistoryUntrackedLiveRows=${state.gidHistoryUntrackedLiveRows}, expected 0`,
		);
	}
	const probeShouldExist = scenario === "fresh";
	for (const [field, exists] of Object.entries({
		"probe.lowerShallow": state.probe.lowerShallow,
		"probe.nativeLog": state.probe.nativeLog,
		"probe.durableBlock": state.probe.durableBlock,
		"probe.nativeBlock": state.probe.nativeBlock,
	})) {
		if (exists !== probeShouldExist) {
			failures.push(`${field}=${exists}, expected ${probeShouldExist}`);
		}
	}
	if ((state.probe.firstDocument !== null) !== probeShouldExist) {
		failures.push(
			`first document presence=${state.probe.firstDocument !== null}, expected ${probeShouldExist}`,
		);
	}
	if (
		probeShouldExist &&
		(state.probe.firstDocument?.id !== `doc-${firstDocumentIndex}` ||
			state.probe.firstDocument?.name !== `value-${firstDocumentIndex}`)
	) {
		failures.push(
			`first document 'doc-${firstDocumentIndex}' was not readable`,
		);
	}
	for (const [field, exists] of Object.entries({
		"probe.retained.lowerShallow": state.probe.retained.lowerShallow,
		"probe.retained.nativeLog": state.probe.retained.nativeLog,
		"probe.retained.durableBlock": state.probe.retained.durableBlock,
	})) {
		if (exists !== true) failures.push(`${field}=${exists}, expected true`);
	}
	const oldestId = `doc-${oldestRetainedIndex}`;
	const newestId = `doc-${newestRetainedIndex}`;
	if (
		state.probe.oldestRetained?.id !== oldestId ||
		state.probe.oldestRetained?.name !== `value-${oldestRetainedIndex}`
	) {
		failures.push(`oldest retained document '${oldestId}' was not readable`);
	}
	if (
		state.probe.newestRetained?.id !== newestId ||
		state.probe.newestRetained?.name !== `value-${newestRetainedIndex}`
	) {
		failures.push(`newest retained document '${newestId}' was not readable`);
	}
	if (
		state.entriesFingerprint !== state.headsFingerprint ||
		state.entriesFingerprint !== state.coordinatesFingerprint
	) {
		failures.push(
			"retained lower heads, native heads, and coordinate hashes do not match",
		);
	}
	if (failures.length > 0) {
		throw new Error(
			`${scenario} ${phase} lifecycle validation failed: ${failures.join("; ")}`,
		);
	}
	return { correct: true, exactRetainedRows: retain, probeShouldExist };
};

const seedWorker = async (options) => {
	const count =
		options.scenario === "fresh" ? options.retain : options.historyCount;
	const documentOffset =
		options.scenario === "fresh" ? options.historyCount - options.retain : 0;
	const oldestRetainedIndex = options.historyCount - options.retain;
	const newestRetainedIndex = options.historyCount - 1;
	return runOpenedLifecycleWorker({
		options,
		storeId: STORE_ID,
		peakRssField: "maxRssBytes",
		work: async ({ runtime, client, store }) => {
			const historyPeer = client.identity.publicKey.hashcode();
			let probeHash;
			let retainedProbeHash;
			const liveGidQueue = [];
			const appendStarted = performance.now();
			for (let start = 0; start < count; start += options.batchSize) {
				const end = Math.min(count, start + options.batchSize);
				const result = await store.docs.putMany(
					Array.from({ length: end - start }, (_, offset) => {
						const index = documentOffset + start + offset;
						return new runtime.Document({
							id: `doc-${index}`,
							name: `value-${index}`,
						});
					}),
					{ unique: true },
				);
				for (const entry of result.entries) {
					probeHash ??= entry.hash;
					retainedProbeHash = entry.hash;
					store.docs.log.addPeersToGidPeerHistory(entry.meta.gid, [
						historyPeer,
					]);
					liveGidQueue.push(entry.meta.gid);
				}
				if (liveGidQueue.length > options.retain) {
					liveGidQueue.splice(0, liveGidQueue.length - options.retain);
				}
			}
			const appendMs = elapsed(appendStarted);
			const cleanupDrainMs = await waitForLifecycleQuiescence(store.docs.log, {
				storage: false,
				label: "GID-history cleanup",
			});
			const validationStarted = performance.now();
			const state = await collectState({
				store,
				firstDocumentIndex: documentOffset,
				oldestRetainedIndex,
				newestRetainedIndex,
				probeHash,
				retainedProbeHash,
				liveGids: new Set(liveGidQueue),
			});
			const validation = validateState({
				state,
				phase: "seed",
				scenario: options.scenario,
				retain: options.retain,
				firstDocumentIndex: documentOffset,
				oldestRetainedIndex,
				newestRetainedIndex,
			});
			const validationMs = elapsed(validationStarted);
			return {
				count,
				documentOffset,
				probeHash,
				retainedProbeHash,
				appendMs,
				appendOpsPerSecond: Math.round((count / appendMs) * 1000),
				cleanupDrainMs,
				validationMs,
				state,
				validation,
			};
		},
	});
};

const reopenWorker = async (options) => {
	const count =
		options.scenario === "fresh" ? options.retain : options.historyCount;
	const documentOffset =
		options.scenario === "fresh" ? options.historyCount - options.retain : 0;
	const oldestRetainedIndex = options.historyCount - options.retain;
	const newestRetainedIndex = options.historyCount - 1;
	return runOpenedLifecycleWorker({
		options,
		storeId: STORE_ID,
		peakRssField: "maxRssBytes",
		work: async ({ store }) => {
			const validationStarted = performance.now();
			const state = await collectState({
				store,
				firstDocumentIndex: documentOffset,
				oldestRetainedIndex,
				newestRetainedIndex,
				probeHash: options.probeHash,
				retainedProbeHash: options.retainedProbeHash,
				liveGids: null,
			});
			const validation = validateState({
				state,
				phase: "reopen",
				scenario: options.scenario,
				retain: options.retain,
				firstDocumentIndex: documentOffset,
				oldestRetainedIndex,
				newestRetainedIndex,
			});
			const validationMs = elapsed(validationStarted);
			return {
				count,
				documentOffset,
				validationMs,
				state,
				validation,
			};
		},
	});
};

const runWorkerProcess = (options) => {
	const args = lifecycleWorkerArgs(options, [
		["history-count", "historyCount"],
		["retain", "retain"],
		["batch-size", "batchSize"],
	]);
	if (options.probeHash) args.push("--probe-hash", options.probeHash);
	if (options.retainedProbeHash) {
		args.push("--retained-probe-hash", options.retainedProbeHash);
	}
	return runJsonWorker({
		scriptPath: SCRIPT_PATH,
		args,
		description: `lifecycle-census worker failed (${options.scenario}, ${options.phase}, run=${options.run})`,
		maxBuffer: 5 * 1024 * 1024,
	});
};

const runScenario = async (options, scenario, run, root) => {
	return runLifecycleScenario({
		options,
		scenario,
		run,
		root,
		runWorker: runWorkerProcess,
		reopenOptions: (seed) => ({
			probeHash: seed.probeHash,
			retainedProbeHash: seed.retainedProbeHash,
		}),
		stableFields: [
			"entriesFingerprint",
			"headsFingerprint",
			"coordinatesFingerprint",
			"rangesFingerprint",
			"documentsFingerprint",
			"durableBlockBytes",
		],
		validation: () => ({
			coldReopenMatchesSeed: true,
			changedFingerprints: [],
			changedStableFields: [],
		}),
	});
};

const runMatchedRow = async (options, run) => {
	const { fresh, history } = await withMatchedScenarios({
		prefix: "peerbit-lifecycle-census-",
		options,
		run,
		order: ["fresh", "history"],
		runScenario,
	});
	const comparison = buildLifecycleComparison(
		fresh,
		history,
		options.historyCount - options.retain,
	);
	if (!comparison.liveStateMatchesFresh) {
		throw new Error(
			`historical reopen differs from the matched fresh live state: ${[
				...comparison.unequalLiveValues,
				...comparison.unequalLiveFingerprints,
			].join(", ")}`,
		);
	}
	return { run, fresh, history, comparison };
};

const renderHuman = (report) => {
	console.log(
		`${LIFECYCLE_CENSUS_NAME} (${report.meta.node}, ${report.meta.cpu})`,
	);
	console.table(
		report.rows.map((row) => ({
			run: row.run,
			freshAppends: row.fresh.seed.count,
			historyAppends: row.history.seed.count,
			retained: row.history.reopen.state.logRows,
			freshAppendOpsPerSecond: row.fresh.seed.appendOpsPerSecond,
			historyAppendOpsPerSecond: row.history.seed.appendOpsPerSecond,
			freshReopenMs: row.fresh.reopen.openMs,
			historyReopenMs: row.history.reopen.openMs,
			freshDiskMiB:
				Math.round((row.fresh.reopen.disk.logicalBytes / 1024 / 1024) * 10) /
				10,
			historyDiskMiB:
				Math.round((row.history.reopen.disk.logicalBytes / 1024 / 1024) * 10) /
				10,
			diskGrowthRatio:
				Math.round(row.comparison.disk.logicalGrowthRatio * 100) / 100,
			liveStateMatches: row.comparison.liveStateMatchesFresh,
		})),
	);
};

const usage = () => `Usage:
  node scripts/bench/shared-log-lifecycle-census.mjs [options]

Options:
  --history-count <n>  Historical append count (default: 100000)
  --retain <n>         Live entries retained in both controls (default: 1000)
  --batch-size <n>     Append batch size, at most retain (default: 256)
  --compact-max-journal-bytes <n>
                       Compact the three coupled WALs at this total byte count
  --compact-max-journal-records <n>
                       Compact the three coupled WALs at this total record count
  --runs <n>           Matched isolated runs (default: 1)
  --output <path>      Atomically checkpoint the JSON report after each run
  --json               Emit the versioned JSON report
  --help               Show this help
`;

const main = async () => {
	const options = parseLifecycleCensusArgs(process.argv.slice(2), process.env);
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
		scenarios: LIFECYCLE_CENSUS_SCENARIOS,
		buildReport: buildLifecycleCensusReport,
		runRow: runMatchedRow,
		renderHuman,
		logRun: (run) =>
			console.error(
				`[lifecycle-census] run=${run}/${options.runs} fresh=${options.retain} history=${options.historyCount} retain=${options.retain}`,
			),
	});
};

if (process.argv[1] === SCRIPT_PATH) {
	main().catch((error) => {
		console.error(error instanceof Error ? error.stack : error);
		process.exitCode = 1;
	});
}
