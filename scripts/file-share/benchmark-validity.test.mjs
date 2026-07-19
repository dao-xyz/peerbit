import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createBenchmarkInvocation } from "./benchmark-invocation.mjs";
import {
	BENCHMARK_RESULT_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
	SEEDER_DROP_POLICY,
} from "./benchmark-orchestration.mjs";
import {
	assertPlaywrightSucceeded,
	calculateSinkAwaitSubtractedDiagnosticMs,
	loadAndValidateBenchmarkResult,
	parseBenchmarkResult,
	validateBenchmarkResult,
	validateBenchmarkResultEnvelope,
} from "./benchmark-validity.mjs";
import {
	DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
	DOWNLOAD_MEMORY_HOST_ATTRIBUTION,
	DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS,
	DOWNLOAD_MEMORY_HOST_SCOPE,
	DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
	DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES,
	DOWNLOAD_MEMORY_NODE_SCOPE,
	DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	DOWNLOAD_MEMORY_PROFILE,
	DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
	DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS,
	DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS,
	DOWNLOAD_MEMORY_WINDOW_DEFINITION,
	calculateDownloadMemoryMaxLiveSampleGapMs,
	calculateDownloadMemoryMaxSamples,
} from "./templates/download-memory-telemetry.mjs";

const RUN_NONCE = "123e4567-e89b-42d3-a456-426614174000";
const FILE_NAME = `file-share-benchmark-adaptive-${RUN_NONCE}.bin`;
const FILE_ID = "benchmark-large-file-id";
const SHA256 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const OTHER_SHA256 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=";
const FILE_MB = 5;
const FILE_SIZE_BYTES = FILE_MB * 1024 * 1024;
const SAMPLE_COUNT_DEFINITION =
	"observation-density divisor: planned interval is min(sampleMs, floor(readyTimeoutMs/sampleCount)) clamped to 1ms; convergence may finish early";
const POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION =
	"max(250ms, pollMs + 250ms) for the final poll and event-loop scheduling";
const POST_TRANSFER_SOAK_SCHEDULING_TOLERANCE_DEFINITION =
	"max(250ms, pollMs + 250ms) for the requested post-transfer timer and event-loop scheduling";
const POST_TRANSFER_SOAK_DEFINITION =
	"idle observation window beginning after transfer integrity and any requested terminal-topology validation, ending before terminal resource capture and peer shutdown";
const TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION =
	"max(5000ms, pollMs + 1000ms) for browser actions and event-loop scheduling";
const TIME_TO_WRITER_READY_DEFINITION =
	"upload-input-set-to-writer-ready-manifest-listed";
const TIME_TO_READER_READY_DEFINITION =
	"upload-input-set-to-reader-ready-manifest-listed";
const LISTING_DURATION_DEFINITION =
	"post-upload-settlement-to-both-writer-and-reader-ready-manifests-listed; excludes upload time";
const DOWNLOAD_DURATION_DEFINITION =
	"reader-download-click-to-selected-backpressured-sink-complete";
const SINK_WRITE_DURATION_DEFINITION =
	"sum of browser writable.write wall-clock durations; library read diagnostics provide the authoritative awaited sink-write interval";
const LIBRARY_STREAM_WALL_DEFINITION =
	"library-large-file-stream-start-to-finish including awaited sink writes";
const SINK_WRITE_AWAIT_DEFINITION =
	"sum of per-chunk library wall-clock intervals awaiting writable.write";
const SINK_AWAIT_SUBTRACTED_DIAGNOSTIC_DEFINITION =
	"arithmetic library stream wall-clock duration minus summed awaited writable.write intervals; overlap-sensitive and not a sink-independent Peerbit duration";
const PRIMARY_DOWNLOAD_METRIC_DEFINITION =
	"authoritative only within one fixed download-sink cohort; hash-only is the standardized primary cohort and includes awaited sink writes plus any overlapping read-ahead";
const DEMAND_WAIT_DEFINITION =
	"wall-clock time each sequential stream consumer awaited its scheduled chunk";
const RESOURCE_STORAGE_DEFINITION =
	"Peerbit logical usage and browser origin-wide navigator.storage estimates; deltas are later minus earlier";
const RESOURCE_EAGER_DEFINITION =
	"deltas of monotonic eager-cache admission, hit, eviction, expiration, and rejection counters; null when eager telemetry is disabled or unavailable";
const EAGER_COUNTERS = [
	"evictions",
	"expirations",
	"admitted",
	"hits",
	"rejectedCid",
	"rejectedCodec",
	"rejectedSize",
	"rejectedPending",
	"rejectedIntegrity",
	"rejectedLifecycle",
];
const PROVENANCE = {
	harness: {
		requestedRef: "harness-worktree",
		resolvedCommit: "d".repeat(40),
		dirty: true,
		worktreeDigest: "e".repeat(64),
	},
	peerbit: {
		requestedRef: "candidate",
		resolvedCommit: "a".repeat(40),
		dirty: false,
		worktreeDigest: null,
	},
	examples: {
		requestedRef: "66e250f1",
		resolvedCommit: "b".repeat(40),
		lockfileSha256: "c".repeat(64),
		dirty: false,
		worktreeDigest: null,
	},
};

const INVOCATION = createBenchmarkInvocation({
	scenario: "upload",
	mode: "adaptive",
	network: "local",
	integrationMode: "link",
	fileMb: FILE_MB,
	fixtureSeed: "fixture-seed",
	uploadTimeoutMs: 600_000,
	downloadTimeoutMs: 600_000,
	postUploadMonitorMs: 50,
	postTransferSoakMs: 50,
	pollMs: 1_000,
	minReadySeeders: 2,
	readyTimeoutMs: 180_000,
});

const options = {
	scenario: "upload",
	expectedMode: "adaptive",
	expectedFileMb: FILE_MB,
	expectedNetwork: "local",
	expectedFixtureSeed: "fixture-seed",
	expectedRunNonce: RUN_NONCE,
	expectedProvenance: PROVENANCE,
	expectedInvocation: INVOCATION,
};

const calculateSamplingWindowBudgetMs = (invocation) => {
	const transferToleranceMs = Math.max(5_000, invocation.pollMs + 1_000);
	const localityBudgetMs =
		invocation.readerLocalChunkTarget === null
			? 0
			: 3 * invocation.readyTimeoutMs +
				2 * 5_000 +
				(invocation.readerLocalChunkTarget > 0
					? invocation.downloadTimeoutMs + transferToleranceMs
					: 0);
	return Math.max(
		20 * 60 * 1_000,
		3 * invocation.readyTimeoutMs +
			invocation.uploadTimeoutMs +
			invocation.downloadTimeoutMs +
			localityBudgetMs +
			invocation.postUploadMonitorMs +
			invocation.postTransferSoakMs +
			5 * 60 * 1_000,
	);
};

const createDownloadMemoryTelemetry = (
	readStartedAt,
	readFinishedAt,
	invocation = INVOCATION,
	{
		afterSoakStartedAt = readFinishedAt + 61,
		shutdownFinishedAt = readFinishedAt + 73,
	} = {},
) => {
	const schedulingToleranceMs = Math.max(5_000, invocation.pollMs + 1_000);
	const samplingWindowBudgetMs = calculateSamplingWindowBudgetMs(invocation);
	const maxSamplesPerSeries = calculateDownloadMemoryMaxSamples({
		samplingWindowBudgetMs,
	});
	const liveSampleMaxGapMs = calculateDownloadMemoryMaxLiveSampleGapMs({
		schedulingToleranceMs,
	});
	const manualSampleAt = afterSoakStartedAt - 1;
	const terminalSampleAt = shutdownFinishedAt + 1;
	const seriesSamplingFields = {
		maxSamples: maxSamplesPerSeries,
		periodicSampleLimit:
			maxSamplesPerSeries - DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
		periodicSampleCount: 1,
		capacityExhaustedBeforeTerminal: false,
		samplingCapacitySufficient: true,
		manualSampleCount: 1,
		lastManualSampleAt: manualSampleAt,
		terminalSampleAttempted: true,
		terminalSampleCaptured: true,
		terminalSampleAt,
	};
	const heap = (scope, startedOffset, finishedOffset, bytes) => {
		const samples = [
			{
				capturedAt: readStartedAt - 1,
				sampleKind: "initial",
				usedBytes: bytes,
				totalBytes: bytes + 200,
				embedderHeapUsedBytes: bytes + 20,
				backingStorageBytes: bytes + 30,
			},
			{
				capturedAt: readFinishedAt + 1,
				sampleKind: "periodic",
				usedBytes: bytes + 20,
				totalBytes: bytes + 240,
				embedderHeapUsedBytes: bytes + 25,
				backingStorageBytes: bytes + 35,
			},
			{
				capturedAt: manualSampleAt,
				sampleKind: "manual",
				usedBytes: bytes + 15,
				totalBytes: bytes + 230,
				embedderHeapUsedBytes: bytes + 24,
				backingStorageBytes: bytes + 34,
			},
			{
				capturedAt: terminalSampleAt,
				sampleKind: "terminal",
				usedBytes: bytes + 10,
				totalBytes: bytes + 220,
				embedderHeapUsedBytes: bytes + 22,
				backingStorageBytes: bytes + 32,
			},
		];
		const first = samples[0];
		const last = samples.at(-1);
		const peak = (key) => Math.max(...samples.map((sample) => sample[key]));
		return {
			memoryKind: "runtime-heap",
			scope,
			metric: "Runtime.getHeapUsage",
			unit: "bytes",
			sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
			startedAt: readStartedAt - startedOffset,
			finishedAt: shutdownFinishedAt + finishedOffset,
			sampleCount: samples.length,
			startBytes: first.usedBytes,
			endBytes: last.usedBytes,
			peakBytes: peak("usedBytes"),
			startUsedBytes: first.usedBytes,
			endUsedBytes: last.usedBytes,
			peakUsedBytes: peak("usedBytes"),
			startTotalBytes: first.totalBytes,
			endTotalBytes: last.totalBytes,
			peakTotalBytes: peak("totalBytes"),
			startEmbedderHeapUsedBytes: first.embedderHeapUsedBytes,
			endEmbedderHeapUsedBytes: last.embedderHeapUsedBytes,
			peakEmbedderHeapUsedBytes: peak("embedderHeapUsedBytes"),
			startBackingStorageBytes: first.backingStorageBytes,
			endBackingStorageBytes: last.backingStorageBytes,
			peakBackingStorageBytes: peak("backingStorageBytes"),
			samples,
			samplingErrors: [],
			samplingErrorOverflowCount: 0,
			cleanupWarnings: [],
			cleanupWarningOverflowCount: 0,
			...seriesSamplingFields,
		};
	};
	const readerJsHeap = heap("reader-renderer", 3, 3, 100);
	const writerJsHeap = heap("writer-renderer", 2, 4, 200);
	const hostSamples = [
		{
			capturedAt: readStartedAt - 1,
			sampleKind: "initial",
			browserInstanceCount: 2,
			browserRootProcessCount: 2,
			browserBytes: 1_000,
			nodeBytes: 500,
			nodeExternalBytes: 100,
			nodeArrayBuffersBytes: 50,
			combinedBytes: 1_500,
			browserProcessCount: 2,
			browserRoleBytes: { browser: 400, renderer: 600 },
		},
		{
			capturedAt: readFinishedAt + 1,
			sampleKind: "periodic",
			browserInstanceCount: 2,
			browserRootProcessCount: 2,
			browserBytes: 1_200,
			nodeBytes: 550,
			nodeExternalBytes: 120,
			nodeArrayBuffersBytes: 60,
			combinedBytes: 1_750,
			browserProcessCount: 3,
			browserRoleBytes: { browser: 450, renderer: 750 },
		},
		{
			capturedAt: manualSampleAt,
			sampleKind: "manual",
			browserInstanceCount: 2,
			browserRootProcessCount: 2,
			browserBytes: 1_150,
			nodeBytes: 540,
			nodeExternalBytes: 115,
			nodeArrayBuffersBytes: 58,
			combinedBytes: 1_690,
			browserProcessCount: 3,
			browserRoleBytes: { browser: 440, renderer: 710 },
		},
		{
			capturedAt: terminalSampleAt,
			sampleKind: "terminal",
			browserInstanceCount: 2,
			browserRootProcessCount: 2,
			browserBytes: 1_100,
			nodeBytes: 525,
			nodeExternalBytes: 110,
			nodeArrayBuffersBytes: 55,
			combinedBytes: 1_625,
			browserProcessCount: 2,
			browserRoleBytes: { browser: 425, renderer: 675 },
		},
	];
	const hostRss = {
		memoryKind: "resident-set-size",
		scope: DOWNLOAD_MEMORY_HOST_SCOPE,
		nodeScope: DOWNLOAD_MEMORY_NODE_SCOPE,
		metric: "RSS",
		attribution: DOWNLOAD_MEMORY_HOST_ATTRIBUTION,
		attributionLimitations: DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS,
		unit: "bytes",
		browserInstanceCount: 2,
		browserSessionCount: 2,
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		startedAt: readStartedAt - 1,
		finishedAt: shutdownFinishedAt + 5,
		sampleCount: hostSamples.length,
		startBrowserBytes: 1_000,
		endBrowserBytes: 1_100,
		peakBrowserBytes: 1_200,
		startNodeBytes: 500,
		endNodeBytes: 525,
		peakNodeBytes: 550,
		startNodeExternalBytes: 100,
		endNodeExternalBytes: 110,
		peakNodeExternalBytes: 120,
		startNodeArrayBuffersBytes: 50,
		endNodeArrayBuffersBytes: 55,
		peakNodeArrayBuffersBytes: 60,
		startCombinedBytes: 1_500,
		endCombinedBytes: 1_625,
		peakCombinedBytes: 1_750,
		startBrowserProcessCount: 2,
		endBrowserProcessCount: 2,
		peakBrowserProcessCount: 3,
		startBrowserRoleBytes: { browser: 400, renderer: 600 },
		endBrowserRoleBytes: { browser: 425, renderer: 675 },
		peakBrowserRoleBytes: { browser: 450, renderer: 750 },
		samples: hostSamples,
		samplingErrors: [],
		samplingErrorOverflowCount: 0,
		cleanupWarnings: [],
		cleanupWarningOverflowCount: 0,
		...seriesSamplingFields,
	};
	return {
		profile: DOWNLOAD_MEMORY_PROFILE,
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		windowDefinition: DOWNLOAD_MEMORY_WINDOW_DEFINITION,
		downloadTimeoutMs: invocation.downloadTimeoutMs,
		schedulingToleranceMs,
		operationTimeoutMs: DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
		postTransferSoakMs: invocation.postTransferSoakMs,
		samplingWindowBudgetMs,
		liveSampleMaxGapMs,
		liveSampleCoverageDefinition:
			DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
		endpointSampleAllowance: DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
		maxSamplesPerSeries,
		capacityExhaustedBeforeTerminal: false,
		samplingCapacitySufficient: true,
		manualCheckpointComplete: true,
		terminalCheckpointComplete: true,
		complete: true,
		cleanupComplete: true,
		startedAt: readerJsHeap.startedAt,
		finishedAt: hostRss.finishedAt,
		readerJsHeap,
		writerJsHeap,
		hostRss,
	};
};

const createUploadDiagnostics = (fileName = FILE_NAME) => {
	const chunkSize = 3 * 1024 * 1024;
	const chunkCount = Math.ceil(FILE_SIZE_BYTES / chunkSize);
	const startedAt = 1_001;
	const firstChunkFinishedAt = 1_030;
	const lastChunkFinishedAt = 1_040;
	const milestones = [
		{
			basisPoints: 0,
			targetBytes: 0,
			completedBytes: 0,
			reachedAt: startedAt,
			chunkIndex: null,
		},
	];
	let completedBytes = 0;
	let nextMilestoneIndex = 1;
	for (const completion of [
		{ chunkIndex: 0, bytes: chunkSize, reachedAt: firstChunkFinishedAt },
		{
			chunkIndex: 1,
			bytes: FILE_SIZE_BYTES - chunkSize,
			reachedAt: lastChunkFinishedAt,
		},
	]) {
		completedBytes += completion.bytes;
		while (nextMilestoneIndex < 21) {
			const basisPoints = nextMilestoneIndex * 500;
			const targetBytes = Number(
				(BigInt(FILE_SIZE_BYTES) * BigInt(basisPoints) + 9_999n) / 10_000n,
			);
			if (completedBytes < targetBytes) {
				break;
			}
			milestones.push({
				basisPoints,
				targetBytes,
				completedBytes,
				reachedAt: completion.reachedAt,
				chunkIndex: completion.chunkIndex,
			});
			nextMilestoneIndex += 1;
		}
	}
	return {
		transferId: FILE_ID,
		uploadId: FILE_ID,
		fileName,
		sizeBytes: FILE_SIZE_BYTES,
		chunkSize,
		chunkCount,
		startedAt,
		manifestStartedAt: 1_002,
		manifestFinishedAt: 1_003,
		firstChunkStartedAt: 1_004,
		firstChunkFinishedAt,
		lastChunkFinishedAt,
		chunkPutCount: chunkCount,
		readyManifestStartedAt: 1_041,
		readyManifestFinishedAt: 1_042,
		finishedAt: 1_043,
		failureAt: null,
		failureMessage: null,
		progressTelemetry: {
			schemaVersion: 1,
			kind: "upload-chunk-commit",
			clock: "unix-epoch-ms",
			milestones,
		},
	};
};

const createReceiverProgress = (diagnostics, downloadSink) => {
	const indices = Object.keys(diagnostics.chunkByteLength)
		.map(Number)
		.toSorted((left, right) => left - right);
	const chunkBytes = indices.map((index) => diagnostics.chunkByteLength[index]);
	const totalBytes = chunkBytes.reduce((total, bytes) => total + bytes, 0);
	const milestones = (source) => {
		let contiguousBytes = 0;
		let contiguousConfirmedAt = diagnostics.startedAt;
		const prefixBytes = [];
		const prefixConfirmedAt = [];
		for (const [offset, index] of indices.entries()) {
			contiguousBytes += chunkBytes[offset];
			contiguousConfirmedAt = Math.max(
				contiguousConfirmedAt,
				diagnostics[source][index],
			);
			prefixBytes.push(contiguousBytes);
			prefixConfirmedAt.push(contiguousConfirmedAt);
		}
		return Array.from({ length: 21 }, (_, index) => index * 5).map(
			(percent) => {
				if (percent === 0) {
					return {
						percent,
						targetBytes: 0,
						contiguousBytes: 0,
						chunkIndex: null,
						confirmedAt: diagnostics.startedAt,
						elapsedMs: 0,
					};
				}
				const targetBytes = Math.ceil((totalBytes * percent) / 100);
				const chunkIndex = prefixBytes.findIndex(
					(bytes) => bytes >= targetBytes,
				);
				return {
					percent,
					targetBytes,
					contiguousBytes: prefixBytes[chunkIndex],
					chunkIndex,
					confirmedAt: prefixConfirmedAt[chunkIndex],
					elapsedMs: prefixConfirmedAt[chunkIndex] - diagnostics.startedAt,
				};
			},
		);
	};
	const sourceCounts = {};
	if (diagnostics.persistChunkReads) {
		for (const index of indices) {
			const source = diagnostics.chunkPersistenceConfirmationSource[index];
			sourceCounts[source] = (sourceCounts[source] ?? 0) + 1;
		}
	}
	return {
		percentages: Array.from({ length: 21 }, (_, index) => index * 5),
		available: {
			definition:
				"contiguous file-prefix bytes materialized and available to the receiver library",
			source: "chunkMaterializeFinishedAt",
			milestones: milestones("chunkMaterializeFinishedAt"),
		},
		peerbitDurable: {
			definition:
				"contiguous file-prefix bytes whose exact signed manifest-entry blocks were confirmed in the receiver's local Peerbit block store",
			source: "chunkPersistenceConfirmedAt",
			claimed: diagnostics.persistChunkReads,
			sourceCounts: Object.fromEntries(
				Object.entries(sourceCounts).sort(([left], [right]) =>
					left.localeCompare(right),
				),
			),
			milestones: diagnostics.persistChunkReads
				? milestones("chunkPersistenceConfirmedAt")
				: null,
		},
		sinkAccepted: {
			definition:
				"contiguous file-prefix bytes accepted by the configured benchmark sink; this is not a Peerbit or filesystem durability claim",
			source: "chunkWriteFinishedAt",
			sink: downloadSink,
			durable: false,
			milestones: milestones("chunkWriteFinishedAt"),
		},
	};
};

const refreshReceiverProgress = (result) => {
	result.readTransfer.receiverProgress = createReceiverProgress(
		result.readerDiagnostics.lastReadDiagnostics,
		result.downloadSink,
	);
};

const createEagerTelemetry = (step) => ({
	entries: 2 + step,
	bytes: 2_000 + step * 100,
	peakEntries: 3 + step,
	peakBytes: 3_000 + step * 100,
	evictions: step,
	expirations: step,
	pendingEntries: 0,
	pendingBytes: 0,
	peakPendingEntries: 1 + step,
	peakPendingBytes: 500 + step * 10,
	admitted: 5 + step,
	hits: 3 + step,
	rejectedCid: step,
	rejectedCodec: step,
	rejectedSize: step,
	rejectedPending: step,
	rejectedIntegrity: step,
	rejectedLifecycle: step,
	limits: {
		maxEntries: 100,
		maxBytes: 10_000,
		maxBlockBytes: 5_000,
		ttlMs: 10_000,
		validationConcurrency: 2,
		maxPendingBytes: 5_000,
		maxPendingEntries: 10,
	},
});

const createRuntimeIdentity = (role) => ({
	programAddress: "benchmark-program-address",
	peerId: `${role}-peer-id`,
	peerHash: `${role}-peer-hash`,
	sessionId: `${role}-session-id`,
});

const createResourcePage = ({ role, startedAt, capturedAt, step }) => ({
	role,
	capturedAt,
	storage: {
		capturedAt: startedAt,
		origin: "http://127.0.0.1:4173",
		backend: {
			requestedMode: "memory",
			directoryConfigured: false,
			directoryConfigurationError: null,
			persistence: {
				navigatorStorage: {
					api: "navigator.storage.persisted",
					available: true,
					persisted: false,
					error: null,
				},
				peerStorage: {
					api: "peer.storage.persisted",
					available: true,
					persisted: false,
					error: null,
				},
				peerBlocks: {
					api: "peer.services.blocks.persisted",
					available: true,
					persisted: false,
					error: null,
				},
				peerIndexer: {
					api: "peer.indexer.persisted",
					available: true,
					persisted: false,
					error: null,
				},
			},
		},
		peerbitLog: {
			api: "SharedLog.getMemoryUsage",
			scope: "file-share-log-logical-usage",
			available: true,
			usageBytes: (role === "writer" ? 100 : 80) + step * 10,
			error: null,
		},
		backingStorage: {
			api: "navigator.storage.estimate",
			scope: "browser-origin-aggregate",
			available: true,
			usageBytes: (role === "writer" ? 1_000 : 900) + step * 50,
			quotaBytes: 100_000,
			usageDetails: { indexedDB: 500 + step * 25 },
			error: null,
		},
	},
	runtime: {
		capturedAt: Math.min(startedAt + 1, capturedAt),
		programReady: true,
		identity: createRuntimeIdentity(role),
		nativeGraph: { active: true, useHeads: true },
		eagerBlocks: {
			telemetryAvailable: true,
			enabled: true,
			telemetry: createEagerTelemetry(step),
		},
		pubsub: {
			runtimeSnapshotAvailable: true,
			snapshot: {
				fanout: {
					root: { uploadLimitBps: 5_000_000 },
					node: { uploadLimitBps: 20_000_000 },
				},
			},
			error: null,
		},
	},
});

const createResourceSnapshotSet = ({ label, startedAt, finishedAt, step }) => ({
	label,
	startedAt,
	finishedAt,
	writer: createResourcePage({
		role: "writer",
		startedAt,
		capturedAt: finishedAt,
		step,
	}),
	reader: createResourcePage({
		role: "reader",
		startedAt,
		capturedAt: finishedAt,
		step,
	}),
});

const createResourceEvidence = ({
	beforeTimedRead = [1_168, 1_170],
	afterSink = [1_202, 1_204],
	beforeSoak = [1_205, 1_210],
	afterSoak = [1_261, 1_264],
} = {}) => {
	const snapshots = {
		beforeTimedRead: createResourceSnapshotSet({
			label: "beforeTimedRead",
			startedAt: beforeTimedRead[0],
			finishedAt: beforeTimedRead[1],
			step: 0,
		}),
		afterSink: createResourceSnapshotSet({
			label: "afterSink",
			startedAt: afterSink[0],
			finishedAt: afterSink[1],
			step: 1,
		}),
		beforeSoak: createResourceSnapshotSet({
			label: "beforeSoak",
			startedAt: beforeSoak[0],
			finishedAt: beforeSoak[1],
			step: 2,
		}),
		afterSoak: createResourceSnapshotSet({
			label: "afterSoak",
			startedAt: afterSoak[0],
			finishedAt: afterSoak[1],
			step: 3,
		}),
	};
	const interval = (before, after) => {
		const storage = (role) => ({
			role,
			peerbitLogUsageDeltaBytes:
				after[role].storage.peerbitLog.usageBytes -
				before[role].storage.peerbitLog.usageBytes,
			backingStorageUsageDeltaBytes:
				after[role].storage.backingStorage.usageBytes -
				before[role].storage.backingStorage.usageBytes,
		});
		const eager = (role) =>
			Object.fromEntries(
				EAGER_COUNTERS.map((key) => [
					key,
					after[role].runtime.eagerBlocks.telemetry[key] -
						before[role].runtime.eagerBlocks.telemetry[key],
				]),
			);
		return {
			from: before.label,
			to: after.label,
			writerStorage: storage("writer"),
			readerStorage: storage("reader"),
			writerEager: eager("writer"),
			readerEager: eager("reader"),
		};
	};
	return {
		schemaVersion: 2,
		storageDefinition: RESOURCE_STORAGE_DEFINITION,
		eagerDefinition: RESOURCE_EAGER_DEFINITION,
		snapshots,
		intervals: {
			timedReadEnvelope: interval(
				snapshots.beforeTimedRead,
				snapshots.afterSink,
			),
			postTransferWork: interval(snapshots.afterSink, snapshots.beforeSoak),
			soak: interval(snapshots.beforeSoak, snapshots.afterSoak),
			total: interval(snapshots.beforeTimedRead, snapshots.afterSoak),
		},
	};
};

const createShutdownOutcomes = ({
	writerStartedAt = 1_266,
	writerFinishedAt = 1_271,
	readerStartedAt = 1_266,
	readerFinishedAt = 1_273,
} = {}) => ({
	writer: {
		role: "writer",
		status: "fulfilled",
		startedAt: writerStartedAt,
		finishedAt: writerFinishedAt,
		durationMs: writerFinishedAt - writerStartedAt,
		programClosed: true,
		peerStopped: true,
		identity: createRuntimeIdentity("writer"),
		error: null,
	},
	reader: {
		role: "reader",
		status: "fulfilled",
		startedAt: readerStartedAt,
		finishedAt: readerFinishedAt,
		durationMs: readerFinishedAt - readerStartedAt,
		programClosed: true,
		peerStopped: true,
		identity: createRuntimeIdentity("reader"),
		error: null,
	},
});

const shiftTimedDownloadEvidence = (result, offset) => {
	for (const key of [
		"downloadStartedAt",
		"downloadFinishedAt",
		"downloadCompletionObservedAt",
		"postTransferSoakStartedAt",
		"postTransferSoakFinishedAt",
	]) {
		result.timestamps[key] += offset;
	}
	result.integrityVerifiedAt += offset;
	result.snapshots.find(({ label }) => label === "terminal").at += offset;
	const diagnostics = result.readerDiagnostics.lastReadDiagnostics;
	diagnostics.startedAt += offset;
	diagnostics.finishedAt += offset;
	for (const key of [
		"chunkResolveStartedAt",
		"chunkResolveFinishedAt",
		"chunkWriteStartedAt",
		"chunkWriteFinishedAt",
		"chunkMaterializeStartedAt",
		"chunkMaterializeFinishedAt",
		"chunkHashStartedAt",
		"chunkHashFinishedAt",
		"chunkPersistenceConfirmedAt",
	]) {
		if (!diagnostics[key]) {
			continue;
		}
		for (const index of Object.keys(diagnostics[key])) {
			diagnostics[key][index] += offset;
		}
	}
	refreshReceiverProgress(result);
	for (const snapshot of Object.values(result.resourceEvidence.snapshots)) {
		snapshot.startedAt += offset;
		snapshot.finishedAt += offset;
		for (const role of ["writer", "reader"]) {
			snapshot[role].capturedAt += offset;
			snapshot[role].storage.capturedAt += offset;
			snapshot[role].runtime.capturedAt += offset;
		}
	}
	for (const outcome of Object.values(result.shutdownOutcomes)) {
		outcome.startedAt += offset;
		outcome.finishedAt += offset;
	}
	const telemetry = result.downloadMemoryTelemetry;
	telemetry.startedAt += offset;
	telemetry.finishedAt += offset;
	for (const series of [
		telemetry.readerJsHeap,
		telemetry.writerJsHeap,
		telemetry.hostRss,
	]) {
		series.startedAt += offset;
		series.finishedAt += offset;
		series.lastManualSampleAt += offset;
		series.terminalSampleAt += offset;
		for (const sample of series.samples) {
			sample.capturedAt += offset;
		}
	}
};

const validResult = () => {
	const result = {
		schema: { ...BENCHMARK_RESULT_SCHEMA },
		runNonce: RUN_NONCE,
		invocation: structuredClone(INVOCATION),
		provenance: structuredClone(PROVENANCE),
		status: "passed",
		mode: "adaptive",
		readerLocalChunkTarget: null,
		readerLocalChunkMaxOvershoot: null,
		readerTerminalTopology: null,
		readerPersistChunkReads: null,
		browserStorageMode: "memory",
		readerLocalChunkBlockCount: null,
		readerLocalChunkIndexRowCount: null,
		readerLocalityCohortKey: null,
		readerLocalityControl: null,
		networkMode: "local",
		fileName: FILE_NAME,
		fileSizeMb: FILE_MB,
		integrityVerified: true,
		integrityVerifiedAt: 1_205,
		integrity: {
			fixtureMode: "deterministic",
			fixtureFormat: "aes-256-ctr-v1",
			fixtureSeed: "fixture-seed",
			expectedSizeBytes: FILE_SIZE_BYTES,
			sourceSizeBytes: FILE_SIZE_BYTES,
			manifestSizeBytes: FILE_SIZE_BYTES,
			downloadedSizeBytes: FILE_SIZE_BYTES,
			sourceSha256Base64: SHA256,
			manifestSha256Base64: SHA256,
			libraryComputedSha256Base64: SHA256,
			downloadedSha256Base64: null,
			sourceCrc32Hex: "00000000",
			downloadedCrc32Hex: "00000000",
			downloadSink: "hash-only",
			sinkPersistence: "none",
			sinkPersistenceVerified: null,
			sizeVerified: true,
			manifestVerified: true,
			sha256Verified: true,
			librarySha256Verified: true,
			persistedSinkSha256Verified: null,
			crc32Verified: true,
			verified: true,
		},
		uploadDurationMs: 101,
		timeToWriterReadyMs: 100,
		timeToWriterReadyDefinition: TIME_TO_WRITER_READY_DEFINITION,
		timeToReaderReadyMs: 120,
		timeToReaderReadyDefinition: TIME_TO_READER_READY_DEFINITION,
		listingDurationMs: 19,
		listingDurationDefinition: LISTING_DURATION_DEFINITION,
		postUploadMonitorDurationMs: 50,
		postUploadMonitorSchedulingToleranceMs: 1_250,
		postUploadMonitorSchedulingToleranceDefinition:
			POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION,
		postTransferSoakActualMs: 50,
		postTransferSoakDefinition: POST_TRANSFER_SOAK_DEFINITION,
		postTransferSoakSchedulingToleranceMs: 1_250,
		postTransferSoakSchedulingToleranceDefinition:
			POST_TRANSFER_SOAK_SCHEDULING_TOLERANCE_DEFINITION,
		postUploadMonitorMs: 50,
		postTransferSoakMs: 50,
		pollMs: 1_000,
		uploadTimeoutMs: 600_000,
		downloadTimeoutMs: 600_000,
		minReadySeeders: 2,
		readyTimeoutMs: 180_000,
		downloadDurationMs: 30,
		downloadDurationDefinition: DOWNLOAD_DURATION_DEFINITION,
		transferTimeoutSchedulingToleranceMs: 5_000,
		transferTimeoutSchedulingToleranceDefinition:
			TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION,
		downloadSink: "hash-only",
		requestedDownloadSink: "hash-only",
		downloadMemoryTelemetry: createDownloadMemoryTelemetry(1_170, 1_200),
		resourceEvidence: createResourceEvidence(),
		shutdownOutcomes: createShutdownOutcomes(),
		sinkWriteCalls: 2,
		sinkWriteDurationMs: 8.5,
		sinkWriteDurationDefinition: SINK_WRITE_DURATION_DEFINITION,
		sinkServerWriteCalls: null,
		sinkServerWriteDurationMs: null,
		sinkServerWriteDurationDefinition: null,
		readTransfer: {
			chunkCount: 2,
			totalBytes: FILE_SIZE_BYTES,
			sources: { remote: { chunkCount: 2, bytes: FILE_SIZE_BYTES } },
			demandWait: {
				definition: DEMAND_WAIT_DEFINITION,
				sampleCount: 2,
				sumMs: 12,
				p50Ms: 4,
				p95Ms: 8,
				p99Ms: 8,
				maxMs: 8,
				over1sCount: 0,
				over5sCount: 0,
				over10sCount: 0,
			},
			stages: {
				libraryStreamWallMs: 30,
				sinkWriteAwaitMs: 9,
				sinkAwaitSubtractedDiagnosticMs: 21,
				demandWaitMs: 12,
				materializeMs: 4,
				contentHashMs: 3,
				otherStreamReadMs: 2,
			},
		},
		libraryStreamWallMs: 30,
		libraryStreamWallDefinition: LIBRARY_STREAM_WALL_DEFINITION,
		primaryDownloadMetric: "libraryStreamWallMs",
		primaryDownloadAuthoritative: true,
		primaryDownloadMetricDefinition: PRIMARY_DOWNLOAD_METRIC_DEFINITION,
		sinkWriteAwaitMs: 9,
		sinkWriteAwaitDefinition: SINK_WRITE_AWAIT_DEFINITION,
		sinkAwaitSubtractedDiagnosticMs: 21,
		sinkAwaitSubtractedDiagnosticDefinition:
			SINK_AWAIT_SUBTRACTED_DIAGNOSTIC_DEFINITION,
		phaseDurationsMs: {
			timeToUploadSettled: 101,
			timeToWriterReady: 100,
			timeToReaderReady: 120,
			writerListingLag: 0,
			readerListingLag: 19,
			readerAfterWriter: 20,
			postUploadMonitor: 50,
			download: 30,
		},
		timestamps: {
			uploadStartedAt: 1000,
			progressSettledAt: 1090,
			writerListedAt: 1100,
			uploadSettledAt: 1101,
			readerListedAt: 1120,
			postMonitorStartedAt: 1120,
			postMonitorFinishedAt: 1170,
			downloadStartedAt: 1170,
			downloadFinishedAt: 1200,
			downloadCompletionObservedAt: 1201,
			postTransferSoakStartedAt: 1210,
			postTransferSoakFinishedAt: 1260,
		},
		baselineWriterSeeders: 2,
		baselineReaderSeeders: 2,
		seederDropPolicy: { ...SEEDER_DROP_POLICY },
		droppedSeeders: false,
		unexpectedSeederDrop: false,
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: [...KNOWN_PEERBIT_FAILURE_SIGNATURES],
		errorCollectionComplete: true,
		errorCount: 0,
		errors: [],
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete: true,
		requestFailureCount: 0,
		requestFailures: [],
		writerDiagnostics: {
			lastUploadDiagnostics: createUploadDiagnostics(),
		},
		readerDiagnostics: {
			lastReadDiagnostics: {
				transferId: "benchmark-read-transfer-id",
				fileId: FILE_ID,
				fileName: FILE_NAME,
				startedAt: 1_170,
				finishedAt: 1_200,
				persistChunkReads: false,
				computedFinalHash: SHA256,
				chunkResolved: { 0: "remote", 1: "remote" },
				chunkByteLength: { 0: 3 * 1024 * 1024, 1: 2 * 1024 * 1024 },
				chunkDemandWaitMs: { 0: 8, 1: 4 },
				chunkResolveStartedAt: { 0: 1_170, 1: 1_185 },
				chunkResolveFinishedAt: { 0: 1_171, 1: 1_186 },
				chunkWriteStartedAt: { 0: 1_180, 1: 1_192 },
				chunkWriteFinishedAt: { 0: 1_185, 1: 1_196 },
				chunkMaterializeStartedAt: { 0: 1_171, 1: 1_186 },
				chunkMaterializeFinishedAt: { 0: 1_173, 1: 1_188 },
				chunkHashStartedAt: { 0: 1_173, 1: 1_188 },
				chunkHashFinishedAt: { 0: 1_174, 1: 1_190 },
				chunkPersistenceConfirmedAt: {},
				chunkPersistenceConfirmationSource: {},
			},
		},
		writerManifestEvidence: {
			capturedAt: 1_100,
			fileId: FILE_ID,
			fileName: FILE_NAME,
			sizeBytes: FILE_SIZE_BYTES,
			finalHash: SHA256,
		},
		readerManifestEvidence: {
			capturedAt: 1_120,
			fileId: FILE_ID,
			fileName: FILE_NAME,
			sizeBytes: FILE_SIZE_BYTES,
			finalHash: SHA256,
		},
		snapshots: [
			{
				label: "seeders-ready",
				writerSeeders: 2,
				readerSeeders: 2,
				at: 900,
			},
			{
				label: "after-1",
				writerSeeders: 2,
				readerSeeders: 2,
				at: 1130,
			},
			{
				label: "terminal",
				writerSeeders: 2,
				readerSeeders: 2,
				at: 1265,
			},
		],
	};
	refreshReceiverProgress(result);
	return result;
};

const createSinkFixture = (downloadSink) => {
	const invocation = createBenchmarkInvocation({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		integrationMode: "link",
		fileMb: FILE_MB,
		fixtureSeed: "fixture-seed",
		downloadSink,
		uploadTimeoutMs: 600_000,
		downloadTimeoutMs: 600_000,
		postUploadMonitorMs: 50,
		postTransferSoakMs: 50,
		pollMs: 1_000,
		minReadySeeders: 2,
		readyTimeoutMs: 180_000,
	});
	const result = validResult();
	result.invocation = structuredClone(invocation);
	result.downloadSink = downloadSink;
	result.requestedDownloadSink = downloadSink;
	result.integrity.downloadSink = downloadSink;
	result.primaryDownloadAuthoritative = downloadSink === "hash-only";
	if (["opfs", "node-file"].includes(downloadSink)) {
		result.integrity.downloadedSha256Base64 = SHA256;
		result.integrity.persistedSinkSha256Verified = true;
		result.integrity.sinkPersistence = downloadSink;
		result.integrity.sinkPersistenceVerified = true;
	}
	if (downloadSink === "node-file") {
		result.sinkServerWriteCalls = 2;
		result.sinkServerWriteDurationMs = 7;
		result.sinkServerWriteDurationDefinition =
			"loopback-request-body-receive-and-node-filesystem-write-only";
	}
	refreshReceiverProgress(result);
	return {
		result,
		options: { ...options, expectedInvocation: invocation },
	};
};

const createReaderLocalityFixture = ({
	target = 1,
	maxOvershoot = 1,
	actualBlockCount = 1,
	actualIndexRowCount = 0,
	readerTerminalTopology = "observer",
	readerPersistChunkReads = true,
} = {}) => {
	const invocation = createBenchmarkInvocation({
		scenario: "upload",
		mode: "fixed1",
		network: "local",
		integrationMode: "link",
		fileMb: FILE_MB,
		fixtureSeed: "fixture-seed",
		uploadTimeoutMs: 600_000,
		downloadTimeoutMs: 600_000,
		postUploadMonitorMs: 50,
		postTransferSoakMs: 50,
		pollMs: 1_000,
		minReadySeeders: 1,
		readyTimeoutMs: 180_000,
		readerLocalChunkTarget: target,
		readerLocalChunkMaxOvershoot: maxOvershoot,
		readerTerminalTopology,
		readerPersistChunkReads,
	});
	const result = validResult();
	const fileName = `file-share-benchmark-fixed1-${RUN_NONCE}.bin`;
	const idleScheduler = {
		activeCount: 0,
		activeBytes: 0,
		queuedCount: 0,
	};
	const observation = ({
		capturedAt,
		persistChunkReads,
		blocks = [],
		indexed = [],
	}) => ({
		capturedAt,
		fileId: FILE_ID,
		chunkCount: 2,
		indexRowCount: indexed.length,
		indexedChunkIndices: indexed,
		blockCount: blocks.length,
		blockChunkIndices: blocks,
		persistChunkReads,
		activeTransfers: [],
		downloadScheduler: { ...idleScheduler },
	});
	const topology = (
		capturedAt,
		selfInReplicatorSet,
		{
			peerHash = selfInReplicatorSet ? "writer-peer" : "reader-peer",
			peerId = `${peerHash}-id`,
			replicatorCount = 1,
			replicatorHashes = ["writer-peer"],
			transportStreams,
		} = {},
	) => ({
		capturedAt,
		peerHash,
		peerId,
		replicatorCount,
		replicatorHashes,
		selfInReplicatorSet,
		...(transportStreams ? { transportStreams } : {}),
	});
	const pubsubTransportStream = ({
		direction,
		remotePeerHash,
		remotePeer,
		bytes,
		connectionId,
		id,
		multiplexer = "/webrtc",
	}) => ({
		service: "pubsub",
		remotePeerHash,
		peerHashIdentityMatch: true,
		serviceProtocol: "/peerbit/topic-control-plane/2.0.0",
		protocol: "/peerbit/topic-control-plane/2.0.0",
		expectedProtocol: "/peerbit/topic-control-plane/2.0.0",
		protocolIdentityMatch: true,
		remotePeer,
		id,
		direction,
		bytes,
		aborted: direction === "outbound" ? false : null,
		counterStreamIdentityMatch: true,
		connectionIdentityMatchCount: 1,
		connectionId,
		multiplexer,
	});
	const transportTopologyObservation = (capturedAt, bytes) => ({
		capturedAt,
		writerTopology: topology(capturedAt - 1, true, {
			peerHash: "writer-peer",
			transportStreams: [
				pubsubTransportStream({
					direction: "outbound",
					remotePeerHash: "reader-peer",
					remotePeer: "reader-peer-id",
					bytes,
					connectionId: "writer-connection",
					id: "writer-stream-id",
				}),
				pubsubTransportStream({
					direction: "outbound",
					remotePeerHash: "reader-peer",
					remotePeer: "reader-peer-id",
					bytes: bytes + 50,
					connectionId: "writer-connection-2",
					id: "writer-stream-id-2",
					multiplexer: "/peerbit/yamux/1.0.0",
				}),
			],
		}),
		readerTopology: topology(capturedAt - 1, false, {
			peerHash: "reader-peer",
			transportStreams: [
				pubsubTransportStream({
					direction: "inbound",
					remotePeerHash: "writer-peer",
					remotePeer: "writer-peer-id",
					bytes,
					connectionId: "reader-connection",
					id: "reader-stream-id",
					multiplexer: "/peerbit/yamux/1.0.0",
				}),
				pubsubTransportStream({
					direction: "inbound",
					remotePeerHash: "writer-peer",
					remotePeer: "writer-peer-id",
					bytes: bytes + 50,
					connectionId: "reader-connection-2",
					id: "reader-stream-id-2",
					multiplexer: "/webrtc",
				}),
			],
		}),
	});
	const preTimedReadTopologyObservations = [1_382, 1_481, 1_580].map(
		(capturedAt) => transportTopologyObservation(capturedAt, 100),
	);
	const postTimedReadTopologyObservations = [1_635, 1_734, 1_833].map(
		(capturedAt) => transportTopologyObservation(capturedAt, 200),
	);
	const terminalReplicatorHashes =
		readerTerminalTopology === "replicator"
			? ["reader-peer", "writer-peer"]
			: ["writer-peer"];
	const terminalReplicatorCount = terminalReplicatorHashes.length;
	const terminalTopology = (capturedAt, peerHash) =>
		topology(
			capturedAt,
			peerHash === "writer-peer" || readerTerminalTopology === "replicator",
			{
				peerHash,
				replicatorCount: terminalReplicatorCount,
				replicatorHashes: terminalReplicatorHashes,
			},
		);
	result.invocation = structuredClone(invocation);
	result.mode = "fixed1";
	result.fileName = fileName;
	result.readerLocalChunkTarget = target;
	result.readerLocalChunkMaxOvershoot = maxOvershoot;
	result.readerTerminalTopology = readerTerminalTopology;
	result.readerPersistChunkReads = readerPersistChunkReads;
	result.readerLocalChunkBlockCount = actualBlockCount;
	result.readerLocalChunkIndexRowCount = actualIndexRowCount;
	const cohortKey = `observer-${readerPersistChunkReads ? "persistent" : "transient"}-memory-prefix-b${actualBlockCount}-i${actualIndexRowCount}`;
	result.readerLocalityCohortKey = cohortKey;
	result.minReadySeeders = 1;
	result.readerDiagnostics.lastReadDiagnostics.fileName = fileName;
	result.readerDiagnostics.programAddress = "reader-program-address";
	result.readerDiagnostics.persistChunkReads = readerPersistChunkReads;
	result.readerDiagnostics.peerHash = "reader-peer";
	result.readerDiagnostics.replicatorCount = terminalReplicatorCount;
	result.readerDiagnostics.replicationSetSize = readerPersistChunkReads ? 1 : 0;
	result.readerDiagnostics.timings = {
		initialRole: "observer",
		updateRoleCount: 0,
		lastAppliedRole: null,
	};
	Object.assign(result.readerDiagnostics.lastReadDiagnostics, {
		persistChunkReads: readerPersistChunkReads,
		programPersistChunkReads: readerPersistChunkReads,
		initialLocalChunkIndexRowCount: readerPersistChunkReads
			? actualIndexRowCount
			: null,
		initialLocalChunkCount: readerPersistChunkReads
			? actualIndexRowCount
			: null,
		initialLocalChunkBlockCount: readerPersistChunkReads
			? actualBlockCount
			: null,
		startedAt: 1_600,
		finishedAt: 1_630,
		chunkResolveStartedAt: { 0: 1_600, 1: 1_615 },
		chunkResolveFinishedAt: { 0: 1_601, 1: 1_616 },
		chunkWriteStartedAt: { 0: 1_610, 1: 1_622 },
		chunkWriteFinishedAt: { 0: 1_615, 1: 1_626 },
		chunkMaterializeStartedAt: { 0: 1_601, 1: 1_616 },
		chunkMaterializeFinishedAt: { 0: 1_603, 1: 1_618 },
		chunkHashStartedAt: { 0: 1_603, 1: 1_618 },
		chunkHashFinishedAt: { 0: 1_604, 1: 1_620 },
		chunkPersistenceConfirmedAt: readerPersistChunkReads
			? { 0: 1_605, 1: 1_621 }
			: {},
		chunkPersistenceConfirmationSource: readerPersistChunkReads
			? {
					0: "manifest-head-batch-local",
					1: "manifest-head-batch-remote",
				}
			: {},
	});
	result.writerManifestEvidence.fileName = fileName;
	result.readerManifestEvidence.fileName = fileName;
	result.writerDiagnostics.peerHash = "writer-peer";
	result.writerDiagnostics.replicatorCount = terminalReplicatorCount;
	result.writerDiagnostics.replicationSetSize = 1;
	result.writerDiagnostics.lastUploadDiagnostics.fileName = fileName;
	result.timestamps.downloadStartedAt = 1_600;
	result.timestamps.downloadFinishedAt = 1_630;
	result.timestamps.downloadCompletionObservedAt = 1_631;
	result.timestamps.postTransferSoakStartedAt = 2_041;
	result.timestamps.postTransferSoakFinishedAt = 2_091;
	result.integrityVerifiedAt = 1_837;
	result.downloadMemoryTelemetry = createDownloadMemoryTelemetry(
		1_600,
		1_630,
		invocation,
		{ afterSoakStartedAt: 2_092, shutdownFinishedAt: 2_101 },
	);
	result.resourceEvidence = createResourceEvidence({
		beforeTimedRead: [1_377, 1_380],
		afterSink: [1_836, 1_837],
		beforeSoak: [2_040, 2_041],
		afterSoak: [2_092, 2_094],
	});
	result.shutdownOutcomes = createShutdownOutcomes({
		writerStartedAt: 2_096,
		writerFinishedAt: 2_099,
		readerStartedAt: 2_096,
		readerFinishedAt: 2_101,
	});
	result.downloadMemoryTelemetry.startedAt = 1_375;
	for (const series of [
		result.downloadMemoryTelemetry.readerJsHeap,
		result.downloadMemoryTelemetry.writerJsHeap,
		result.downloadMemoryTelemetry.hostRss,
	]) {
		series.startedAt = 1_375;
		series.samples[0].capturedAt = 1_376;
	}
	result.readerLocalityControl = {
		profile: "observer-topology-exact-manifest-prefix",
		provisioningMethod: "exact-manifest-head-import",
		requestedLocalChunkBlockCount: target,
		maxSpeculativeOvershootChunkCount: maxOvershoot,
		countMetric: "exact local Documents index rows and manifest entry blocks",
		writerUploadRole: "fixed1",
		readerUploadRole: "observer",
		readerTimedReadPolicy: readerPersistChunkReads
			? "persist-chunk-reads"
			: "transient-chunk-reads",
		expectedTerminalTopology: readerTerminalTopology,
		stabilityPollIntervalMs: 100,
		requiredStableObservationCount: 3,
		transportCounterStabilityPollIntervalMs: 100,
		transportCounterStabilityTimeoutMs: 5_000,
		transportCounterRequiredStableObservationCount: 3,
		transportCounterMaxCounterpartByteSkew: 1024 * 1024,
		transportCounterSampleClockToleranceMs: 1,
		transportCounterPreReadStartToleranceMs: 1_000,
		transportCounterPostReadCaptureMaxDelayMs: 9_000,
		status: "complete",
		readerInitialRoleEvidence: {
			capturedAt: 989,
			programAddress: "reader-program-address",
			persistChunkReads: false,
			initialRole: "observer",
			updateRoleCount: 0,
			lastAppliedRole: null,
		},
		writerTopologyBeforeUpload: topology(990, true),
		readerTopologyBeforeUpload: topology(991, false),
		writerTopologyBeforeTimedRead: structuredClone(
			preTimedReadTopologyObservations.at(-1).writerTopology,
		),
		readerTopologyBeforeTimedRead: structuredClone(
			preTimedReadTopologyObservations.at(-1).readerTopology,
		),
		preTimedReadTopologyStartedAt: 1_381,
		preTimedReadTopologyDeadlineAt: 6_381,
		preTimedReadTopologyFinishedAt: 1_583,
		preTimedReadTopologyObservations,
		writerTopologyAfterTimedRead: structuredClone(
			postTimedReadTopologyObservations.at(-1).writerTopology,
		),
		readerTopologyAfterTimedRead: structuredClone(
			postTimedReadTopologyObservations.at(-1).readerTopology,
		),
		postTimedReadTopologyStartedAt: 1_634,
		postTimedReadTopologyDeadlineAt: 6_634,
		postTimedReadTopologyFinishedAt: 1_836,
		postTimedReadTopologyCaptureDelayMs: 205,
		postTimedReadTopologyObservations,
		beforePreloadObservation: observation({
			capturedAt: 1_171,
			persistChunkReads: false,
		}),
		preloadEvidence: {
			startedAt: 1_172,
			finishedAt: 1_175,
			fileId: FILE_ID,
			provisioningMethod: "exact-manifest-head-import",
			transferId: null,
			aggregateTimeoutMs: target === 0 ? null : invocation.downloadTimeoutMs,
			aggregateDeadlineAt:
				target === 0 ? null : 1_172 + invocation.downloadTimeoutMs,
			aggregateTimedOut: false,
			requestedManifestEntryCount: target,
			importedManifestEntryCount: target,
			importedManifestEntryIndices: Array.from(
				{ length: target },
				(_, index) => index,
			),
			localManifestEntryIndicesAfter: Array.from(
				{ length: target },
				(_, index) => index,
			),
			rawFetchedByteCount: target === 0 ? 0 : 3 * 1024 * 1024,
			maxConcurrentImports: 8,
			persistChunkReads: readerPersistChunkReads,
			activeTransfersAfterClose: [],
			downloadSchedulerAfterClose: { ...idleScheduler },
			readDiagnostics: null,
		},
		stabilityObservations: [1_176, 1_276, 1_376].map((capturedAt) =>
			observation({
				capturedAt,
				persistChunkReads: readerPersistChunkReads,
				blocks: Array.from({ length: actualBlockCount }, (_, index) => index),
				indexed: Array.from(
					{ length: actualIndexRowCount },
					(_, index) => index,
				),
			}),
		),
		preDownloadObservation: observation({
			capturedAt: 1_376,
			persistChunkReads: readerPersistChunkReads,
			blocks: Array.from({ length: actualBlockCount }, (_, index) => index),
			indexed: Array.from({ length: actualIndexRowCount }, (_, index) => index),
		}),
		integrityVerifiedAt: 1_837,
		terminalIdleObservation: observation({
			capturedAt: 1_838,
			persistChunkReads: readerPersistChunkReads,
			blocks: readerPersistChunkReads ? [0, 1] : [],
			indexed:
				readerPersistChunkReads && readerTerminalTopology === "replicator"
					? [0, 1]
					: [],
		}),
		terminalTopologyStartedAt: 1_838,
		terminalTopologyDeadlineAt: 181_838,
		terminalTopologyFinishedAt: 2_040,
		terminalTopologyRole: readerTerminalTopology,
		terminalTopologyExpectationSatisfied: true,
		terminalTopologyObservations: [1_839, 1_939, 2_039].map((capturedAt) => ({
			capturedAt,
			writerTopology: terminalTopology(capturedAt - 1, "writer-peer"),
			readerTopology: terminalTopology(capturedAt - 1, "reader-peer"),
		})),
		actualLocalChunkBlockCount: actualBlockCount,
		actualLocalChunkIndexRowCount: actualIndexRowCount,
		speculativeOvershootChunkCount: 0,
		cohortKey,
		failure: null,
	};
	result.snapshots = [
		{
			...result.snapshots[0],
			writerSeeders: 1,
			readerSeeders: 1,
		},
		{
			label: "after-1",
			writerSeeders: 1,
			readerSeeders: 1,
			at: 1_130,
		},
		{
			label: "terminal",
			writerSeeders: 1,
			readerSeeders: 1,
			at: 2_095,
		},
	];
	result.baselineWriterSeeders = 1;
	result.baselineReaderSeeders = 1;
	result.droppedSeeders = false;
	result.unexpectedSeederDrop = false;
	refreshReceiverProgress(result);
	return {
		result,
		options: {
			...options,
			expectedMode: "fixed1",
			expectedInvocation: invocation,
		},
	};
};

test("accepts a complete deterministic transfer result", () => {
	assert.equal(
		validateBenchmarkResult(validResult(), options).status,
		"passed",
	);
});

test("requires exact receiver progress and rejects false durability claims", () => {
	for (const mutate of [
		(result) => {
			result.readTransfer.receiverProgress.percentages[1] = 6;
		},
		(result) => {
			result.readTransfer.receiverProgress.available.milestones[10].confirmedAt += 1;
		},
		(result) => {
			delete result.readTransfer.receiverProgress.available.milestones[10]
				.elapsedMs;
		},
		(result) => {
			result.readTransfer.receiverProgress.available.source =
				"chunkWriteFinishedAt";
		},
		(result) => {
			result.readTransfer.receiverProgress.sinkAccepted.durable = true;
		},
		(result) => {
			result.readTransfer.receiverProgress.sinkAccepted.sink = "opfs";
		},
		(result) => {
			result.readTransfer.receiverProgress.peerbitDurable.sourceCounts = {
				fabricated: 1,
			};
		},
		(result) => {
			result.readTransfer.receiverProgress.sinkAccepted.milestones.at(
				-1,
			).contiguousBytes -= 1;
		},
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(
			() => validateBenchmarkResult(result, options),
			/read-transfer timing decomposition is inconsistent/,
		);
	}

	const observerWithDurability = validResult();
	observerWithDurability.readerDiagnostics.lastReadDiagnostics.chunkPersistenceConfirmedAt =
		{ 0: 1_180 };
	observerWithDurability.readerDiagnostics.lastReadDiagnostics.chunkPersistenceConfirmationSource =
		{ 0: "fabricated" };
	assert.throws(
		() => validateBenchmarkResult(observerWithDurability, options),
		/chunkPersistenceConfirmedAt contains unexpected or missing fields/,
	);

	const durableSourceMismatch = createReaderLocalityFixture();
	durableSourceMismatch.result.readTransfer.receiverProgress.peerbitDurable.sourceCounts =
		{ fabricated: 2 };
	assert.throws(
		() =>
			validateBenchmarkResult(
				durableSourceMismatch.result,
				durableSourceMismatch.options,
			),
		/read-transfer timing decomposition is inconsistent/,
	);

	const durableOutsideRead = createReaderLocalityFixture();
	durableOutsideRead.result.readerDiagnostics.lastReadDiagnostics.chunkPersistenceConfirmedAt[1] =
		durableOutsideRead.result.readerDiagnostics.lastReadDiagnostics.finishedAt +
		1;
	assert.throws(
		() =>
			validateBenchmarkResult(
				durableOutsideRead.result,
				durableOutsideRead.options,
			),
		/outside the canonical library read window/,
	);

	const unknownDurabilitySource = createReaderLocalityFixture();
	unknownDurabilitySource.result.readerDiagnostics.lastReadDiagnostics.chunkPersistenceConfirmationSource[0] =
		"unknown-source";
	assert.throws(
		() =>
			validateBenchmarkResult(
				unknownDurabilitySource.result,
				unknownDurabilitySource.options,
			),
		/not a recognized persistence source/,
	);
});

test("requires a trustworthy aggregate-integrity timestamp before the final snapshot", () => {
	for (const [mutate, pattern] of [
		[
			(result) => {
				result.readerTerminalTopology = "replicator";
			},
			/readerTerminalTopology does not match the requested invocation/,
		],
		[
			(result) => {
				delete result.integrityVerifiedAt;
			},
			/integrityVerifiedAt/,
		],
		[
			(result) => {
				result.integrityVerifiedAt = 0;
			},
			/non-positive integrityVerifiedAt/,
		],
		[
			(result) => {
				result.integrityVerifiedAt = Number.MAX_SAFE_INTEGER + 1;
			},
			/invalid integrityVerifiedAt/,
		],
		[
			(result) => {
				result.integrityVerifiedAt =
					result.timestamps.downloadCompletionObservedAt - 1;
			},
			/aggregate integrity gate precedes download completion/,
		],
		[
			(result) => {
				result.snapshots.at(-1).at = result.integrityVerifiedAt - 1;
			},
			/precedes aggregate integrity verification/,
		],
		[
			(result) => {
				result.snapshots.push({
					label: "during-99",
					writerSeeders: 2,
					readerSeeders: 2,
					at: 1_265,
				});
			},
			/must end with exactly one terminal seeder snapshot/,
		],
		[
			(result) => {
				result.snapshots.pop();
			},
			/must end with exactly one terminal seeder snapshot/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("requires the terminal snapshot after controlled-locality topology", () => {
	const beforeTopology = createReaderLocalityFixture();
	beforeTopology.result.snapshots.at(-1).at =
		beforeTopology.result.readerLocalityControl.terminalTopologyFinishedAt - 1;
	assert.throws(
		() =>
			validateBenchmarkResult(beforeTopology.result, beforeTopology.options),
		/precedes terminal topology completion/,
	);

	const sameMillisecond = createReaderLocalityFixture();
	sameMillisecond.result.readerLocalityControl.terminalTopologyFinishedAt =
		sameMillisecond.result.resourceEvidence.snapshots.beforeSoak.startedAt;
	assert.equal(
		validateBenchmarkResult(sameMillisecond.result, sameMillisecond.options)
			.status,
		"passed",
	);
});

test("accepts explicit disabled eager-cache evidence", () => {
	const result = validResult();
	for (const snapshot of Object.values(result.resourceEvidence.snapshots)) {
		for (const role of ["writer", "reader"]) {
			snapshot[role].runtime.eagerBlocks.enabled = false;
			snapshot[role].runtime.eagerBlocks.telemetry = null;
		}
	}
	for (const interval of Object.values(result.resourceEvidence.intervals)) {
		interval.writerEager = null;
		interval.readerEager = null;
	}
	assert.equal(validateBenchmarkResult(result, options).status, "passed");
});

test("requires measured memory and OPFS backend evidence", () => {
	const opfs = validResult();
	const opfsInvocation = {
		...structuredClone(INVOCATION),
		browserStorageMode: "opfs",
	};
	opfs.invocation = structuredClone(opfsInvocation);
	opfs.browserStorageMode = "opfs";
	for (const snapshot of Object.values(opfs.resourceEvidence.snapshots)) {
		for (const role of ["writer", "reader"]) {
			const backend = snapshot[role].storage.backend;
			backend.requestedMode = "opfs";
			backend.directoryConfigured = true;
			for (const key of ["peerStorage", "peerBlocks", "peerIndexer"]) {
				backend.persistence[key].persisted = true;
			}
			// OPFS availability and eviction protection are separate facts.
			backend.persistence.navigatorStorage.persisted = false;
		}
	}
	const opfsOptions = { ...options, expectedInvocation: opfsInvocation };
	assert.equal(validateBenchmarkResult(opfs, opfsOptions).status, "passed");

	for (const mutate of [
		(result) => {
			result.resourceEvidence.snapshots.afterSink.reader.storage.backend.directoryConfigured = false;
		},
		(result) => {
			result.resourceEvidence.snapshots.afterSink.reader.storage.backend.persistence.peerBlocks.persisted = false;
		},
		(result) => {
			result.resourceEvidence.snapshots.afterSink.reader.storage.backend.requestedMode =
				"memory";
		},
	]) {
		const invalid = structuredClone(opfs);
		mutate(invalid);
		assert.throws(
			() => validateBenchmarkResult(invalid, opfsOptions),
			/backend.*contract is invalid|peerBlocks.*contract is invalid/,
		);
	}
});

test("requires ordered exact resource, soak, and shutdown evidence", () => {
	for (const [mutate, pattern] of [
		[
			(result) => {
				delete result.resourceEvidence;
			},
			/missing resourceEvidence/,
		],
		[
			(result) => {
				result.resourceEvidence.schemaVersion = 1;
			},
			/resource evidence contract is invalid/,
		],
		[
			(result) => {
				delete result.resourceEvidence.snapshots.beforeSoak;
			},
			/resourceEvidence\.snapshots contains unexpected or missing fields/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.extra = {};
			},
			/resourceEvidence\.snapshots contains unexpected or missing fields/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeTimedRead.finishedAt =
					result.timestamps.downloadStartedAt + 1;
			},
			/resource snapshots, soak, integrity, and terminal evidence are out of order/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeSoak.startedAt =
					result.integrityVerifiedAt - 1;
			},
			/resource snapshots, soak, integrity, and terminal evidence are out of order/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeSoak.finishedAt =
					result.timestamps.postTransferSoakStartedAt + 1;
			},
			/resource snapshots, soak, integrity, and terminal evidence are out of order/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.startedAt =
					result.resourceEvidence.snapshots.beforeTimedRead.startedAt + 1;
			},
			/resource snapshots, soak, integrity, and terminal evidence are out of order/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeTimedRead.finishedAt =
					result.resourceEvidence.snapshots.beforeTimedRead.startedAt + 20_000;
			},
			/capture window is invalid or unbounded/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.writer.storage.peerbitLog.scope =
					"origin-wide";
			},
			/peerbitLog contract is invalid/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.reader.storage.backingStorage.available = false;
			},
			/backingStorage contract is invalid/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.reader.storage.backingStorage.usageDetails =
					Object.fromEntries(
						Array.from({ length: 65 }, (_, index) => [`kind-${index}`, 1]),
					);
			},
			/too many storage categories/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.writer.runtime.nativeGraph.useHeads =
					null;
			},
			/nativeGraph contract is invalid/,
		],
		[
			(result) => {
				delete result.resourceEvidence.snapshots.afterSink.writer.runtime
					.identity.sessionId;
			},
			/identity contains unexpected or missing fields/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.writer.runtime.identity.sessionId =
					"replacement-session";
			},
			/writer runtime provenance changed/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeSoak.reader.storage.origin =
					"http://127.0.0.1:4174";
			},
			/reader runtime provenance changed/,
		],
		[
			(result) => {
				for (const snapshot of Object.values(
					result.resourceEvidence.snapshots,
				)) {
					snapshot.reader.runtime.identity.peerId =
						snapshot.writer.runtime.identity.peerId;
				}
				result.shutdownOutcomes.reader.identity.peerId =
					result.shutdownOutcomes.writer.identity.peerId;
			},
			/does not identify two distinct peers/,
		],
		[
			(result) => {
				for (const snapshot of Object.values(
					result.resourceEvidence.snapshots,
				)) {
					snapshot.reader.runtime.identity.sessionId =
						snapshot.writer.runtime.identity.sessionId;
				}
				result.shutdownOutcomes.reader.identity.sessionId =
					result.shutdownOutcomes.writer.identity.sessionId;
			},
			/does not identify two distinct peers/,
		],
		[
			(result) => {
				for (const snapshot of Object.values(
					result.resourceEvidence.snapshots,
				)) {
					snapshot.reader.runtime.identity.programAddress = "different-program";
				}
				result.shutdownOutcomes.reader.identity.programAddress =
					"different-program";
			},
			/does not identify two distinct peers in one program/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.reader.runtime.pubsub.snapshot.fanout.root.uploadLimitBps = 0;
			},
			/fanout\.root\.uploadLimitBps/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.writer.runtime.pubsub.snapshot.fanout.node.uploadLimitBps += 1;
			},
			/writer runtime provenance changed/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.afterSink.writer.runtime.eagerBlocks.telemetry.admitted = 0;
			},
			/writer eager-cache admitted regressed/,
		],
		[
			(result) => {
				result.resourceEvidence.snapshots.beforeSoak.reader.runtime.eagerBlocks.telemetry.hits = 0;
			},
			/reader eager-cache hits regressed/,
		],
		[
			(result) => {
				result.resourceEvidence.intervals.timedReadEnvelope.writerStorage.peerbitLogUsageDeltaBytes += 1;
			},
			/resource interval deltas contradict/,
		],
		[
			(result) => {
				result.resourceEvidence.intervals.postTransferWork.from =
					"beforeTimedRead";
			},
			/resource interval deltas contradict/,
		],
		[
			(result) => {
				result.resourceEvidence.intervals.soak.readerStorage.backingStorageUsageDeltaBytes += 1;
			},
			/resource interval deltas contradict/,
		],
		[
			(result) => {
				result.timestamps.postTransferSoakFinishedAt -= 1;
				result.postTransferSoakActualMs -= 1;
			},
			/post-transfer soak duration or scheduling contract is invalid/,
		],
		[
			(result) => {
				result.timestamps.postTransferSoakStartedAt =
					result.integrityVerifiedAt - 1;
				result.timestamps.postTransferSoakFinishedAt =
					result.timestamps.postTransferSoakStartedAt + 50;
			},
			/post-transfer soak duration or scheduling contract is invalid/,
		],
		[
			(result) => {
				result.postTransferSoakMs -= 1;
			},
			/postTransferSoakMs does not match the requested invocation/,
		],
		[
			(result) => {
				result.shutdownOutcomes.writer.status = "rejected";
				result.shutdownOutcomes.writer.error = "failed";
			},
			/not a successful bounded shutdown/,
		],
		[
			(result) => {
				result.shutdownOutcomes.reader.durationMs += 1;
			},
			/not a successful bounded shutdown/,
		],
		[
			(result) => {
				result.shutdownOutcomes.writer.programClosed = false;
			},
			/not a successful bounded shutdown/,
		],
		[
			(result) => {
				result.shutdownOutcomes.reader.identity.sessionId = "stale-session";
			},
			/shutdown identities do not match/,
		],
		[
			(result) => {
				const outcome = result.shutdownOutcomes.reader;
				outcome.finishedAt = outcome.startedAt + 31_251;
				outcome.durationMs = 31_251;
			},
			/not a successful bounded shutdown/,
		],
		[
			(result) => {
				result.shutdownOutcomes.writer.startedAt = 1_278;
				result.shutdownOutcomes.writer.finishedAt = 1_280;
				result.shutdownOutcomes.writer.durationMs = 2;
				result.shutdownOutcomes.reader.startedAt = 1_278;
				result.shutdownOutcomes.reader.finishedAt = 1_285;
				result.shutdownOutcomes.reader.durationMs = 7;
			},
			/invalid live or terminal checkpoint ordering/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("accepts one recovered seeder dip under the v11 policy", () => {
	const result = validResult();
	result.snapshots[1].writerSeeders = 1;
	result.droppedSeeders = true;
	assert.equal(validateBenchmarkResult(result, options).status, "passed");
});

test("rejects two consecutive below-baseline seeder snapshots", () => {
	const result = validResult();
	result.snapshots[1].writerSeeders = 1;
	result.snapshots.splice(2, 0, {
		label: "after-2",
		writerSeeders: 1,
		readerSeeders: 2,
		at: 1140,
	});
	result.droppedSeeders = true;
	result.unexpectedSeederDrop = true;
	assert.throws(
		() => validateBenchmarkResult(result, options),
		/unexpected seeder drop/,
	);
});

test("rejects a terminal below-baseline seeder snapshot", () => {
	const result = validResult();
	result.snapshots.at(-1).readerSeeders = 1;
	result.droppedSeeders = true;
	result.unexpectedSeederDrop = true;
	assert.throws(
		() => validateBenchmarkResult(result, options),
		/unexpected seeder drop/,
	);
});

test("rejects missing, altered, and contradictory v11 seeder-drop evidence", () => {
	for (const [mutate, pattern] of [
		[
			(result) => {
				delete result.seederDropPolicy;
			},
			/missing seederDropPolicy/,
		],
		[
			(result) => {
				result.seederDropPolicy.consecutiveBelowBaselineSnapshotThreshold = 1;
			},
			/unsupported seeder-drop policy/,
		],
		[
			(result) => {
				result.snapshots[1].writerSeeders = 1;
				result.droppedSeeders = true;
				result.unexpectedSeederDrop = true;
			},
			/unexpectedSeederDrop claim contradicts/,
		],
		[
			(result) => {
				result.snapshots[1].writerSeeders = 1;
				result.snapshots.splice(2, 0, {
					label: "after-2",
					writerSeeders: 1,
					readerSeeders: 2,
					at: 1140,
				});
				result.droppedSeeders = true;
			},
			/unexpectedSeederDrop claim contradicts/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("requires exact bounded upload chunk-commit progress telemetry", () => {
	for (const [mutate, pattern] of [
		[
			(result) => {
				delete result.writerDiagnostics;
			},
			/missing writerDiagnostics/,
		],
		[
			(result) => {
				delete result.writerDiagnostics.lastUploadDiagnostics;
			},
			/missing writerDiagnostics\.lastUploadDiagnostics/,
		],
		[
			(result) => {
				delete result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry;
			},
			/missing writerDiagnostics\.lastUploadDiagnostics\.progressTelemetry/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.extra = true;
			},
			/upload progress telemetry contains unexpected or missing fields/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones[1].extra = true;
			},
			/upload progress milestone 1 contains unexpected or missing fields/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.schemaVersion = 2;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.kind =
					"wire-bytes";
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones.pop();
			},
			/exactly 21 milestones/,
		],
		[
			(result) => {
				const telemetry =
					result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry;
				telemetry.milestones.push(structuredClone(telemetry.milestones.at(-1)));
			},
			/exactly 21 milestones/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones[5].basisPoints += 1;
			},
			/exact 5% target/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones[5].targetBytes += 1;
			},
			/exact 5% target/,
		],
		[
			(result) => {
				const milestone =
					result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry
						.milestones[5];
				milestone.completedBytes = milestone.targetBytes - 1;
			},
			/invalid aggregate completed bytes/,
		],
		[
			(result) => {
				const milestone =
					result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry
						.milestones[1];
				milestone.completedBytes = milestone.targetBytes;
			},
			/not a possible aggregate of completed chunks/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				const milestone = diagnostics.progressTelemetry.milestones[1];
				milestone.completedBytes =
					milestone.targetBytes + diagnostics.chunkSize;
			},
			/invalid aggregate completed bytes/,
		],
		[
			(result) => {
				const milestones =
					result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry
						.milestones;
				milestones[2].completedBytes = 2 * 1024 * 1024;
			},
			/invalid aggregate completed bytes/,
		],
		[
			(result) => {
				const milestones =
					result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry
						.milestones;
				milestones[2].reachedAt = milestones[0].reachedAt;
			},
			/invalid completion time/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				diagnostics.progressTelemetry.milestones[1].reachedAt =
					diagnostics.startedAt;
			},
			/predates the first completed chunk/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				for (const milestone of diagnostics.progressTelemetry.milestones.slice(
					1,
					13,
				)) {
					milestone.reachedAt = diagnostics.firstChunkFinishedAt + 5;
				}
			},
			/contradicts the first completed chunk timestamp/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				const milestone = diagnostics.progressTelemetry.milestones[1];
				milestone.completedBytes = 2 * 1024 * 1024;
				milestone.chunkIndex = 0;
			},
			/contradicts its triggering chunk completion/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones[2].chunkIndex = 1;
			},
			/contradicts its shared chunk-completion event/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				for (const milestone of diagnostics.progressTelemetry.milestones.slice(
					9,
				)) {
					milestone.completedBytes = FILE_SIZE_BYTES;
					milestone.reachedAt = diagnostics.lastChunkFinishedAt;
					milestone.chunkIndex = 1;
				}
			},
			/already crossed before its triggering chunk completion/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.progressTelemetry.milestones[1].chunkIndex =
					null;
			},
			/invalid upload progress milestone 1\.chunkIndex/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				diagnostics.progressTelemetry.milestones[1].chunkIndex =
					diagnostics.chunkCount;
			},
			/out-of-range chunk index/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.lastChunkFinishedAt += 1;
			},
			/invalid completion milestone/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.uploadId = "stale-file";
			},
			/do not identify the canonical uploaded file/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.fileName = "stale.bin";
			},
			/do not identify the canonical uploaded file/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.sizeBytes -= 1;
			},
			/do not identify the canonical uploaded file/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.chunkPutCount -= 1;
			},
			/do not prove a successful complete chunk upload/,
		],
		[
			(result) => {
				const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
				diagnostics.chunkSize = FILE_SIZE_BYTES;
				diagnostics.chunkCount = 1;
				diagnostics.chunkPutCount = 1;
				for (const milestone of diagnostics.progressTelemetry.milestones.slice(
					1,
				)) {
					milestone.completedBytes = FILE_SIZE_BYTES;
					milestone.reachedAt = diagnostics.lastChunkFinishedAt;
					milestone.chunkIndex = 0;
				}
			},
			/upload chunk geometry contradicts canonical read evidence/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.failureAt = 1_044;
			},
			/do not prove a successful complete chunk upload/,
		],
		[
			(result) => {
				result.writerDiagnostics.lastUploadDiagnostics.finishedAt =
					result.timestamps.uploadSettledAt + 1;
			},
			/lifecycle timestamps are inconsistent/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("accepts upload progress milestones with out-of-order chunk completion", () => {
	const result = validResult();
	const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
	const milestones = diagnostics.progressTelemetry.milestones;
	for (let index = 1; index <= 8; index++) {
		milestones[index].completedBytes = 2 * 1024 * 1024;
		milestones[index].chunkIndex = 1;
	}
	for (let index = 9; index < milestones.length; index++) {
		milestones[index].completedBytes = FILE_SIZE_BYTES;
		milestones[index].reachedAt = diagnostics.lastChunkFinishedAt;
		milestones[index].chunkIndex = 0;
	}
	assert.equal(validateBenchmarkResult(result, options).status, "passed");
});

test("rejects milestones crossed before a partial-tail trigger", () => {
	const result = validResult();
	const chunkSize = 1_700_000;
	const chunkByteLengths = [
		chunkSize,
		chunkSize,
		chunkSize,
		FILE_SIZE_BYTES - 3 * chunkSize,
	];
	const reader = result.readerDiagnostics.lastReadDiagnostics;
	const indices = chunkByteLengths.map((_, index) => index);
	reader.chunkResolved = Object.fromEntries(
		indices.map((index) => [index, "remote"]),
	);
	reader.chunkByteLength = Object.fromEntries(
		chunkByteLengths.map((bytes, index) => [index, bytes]),
	);
	reader.chunkDemandWaitMs = Object.fromEntries(
		indices.map((index) => [index, 0]),
	);
	for (const key of [
		"chunkResolveStartedAt",
		"chunkResolveFinishedAt",
		"chunkWriteStartedAt",
		"chunkWriteFinishedAt",
		"chunkMaterializeStartedAt",
		"chunkMaterializeFinishedAt",
		"chunkHashStartedAt",
		"chunkHashFinishedAt",
	]) {
		reader[key] = Object.fromEntries(indices.map((index) => [index, 1_171]));
	}
	result.readTransfer = {
		chunkCount: 4,
		totalBytes: FILE_SIZE_BYTES,
		sources: { remote: { chunkCount: 4, bytes: FILE_SIZE_BYTES } },
		demandWait: {
			definition: DEMAND_WAIT_DEFINITION,
			sampleCount: 4,
			sumMs: 0,
			p50Ms: 0,
			p95Ms: 0,
			p99Ms: 0,
			maxMs: 0,
			over1sCount: 0,
			over5sCount: 0,
			over10sCount: 0,
		},
		stages: {
			libraryStreamWallMs: 30,
			sinkWriteAwaitMs: 0,
			sinkAwaitSubtractedDiagnosticMs: 30,
			demandWaitMs: 0,
			materializeMs: 0,
			contentHashMs: 0,
			otherStreamReadMs: 30,
		},
	};
	result.sinkWriteCalls = 4;
	result.sinkWriteDurationMs = 0;
	result.sinkWriteAwaitMs = 0;
	result.sinkAwaitSubtractedDiagnosticMs = 30;
	refreshReceiverProgress(result);

	const diagnostics = result.writerDiagnostics.lastUploadDiagnostics;
	diagnostics.chunkSize = chunkSize;
	diagnostics.chunkCount = 4;
	diagnostics.chunkPutCount = 4;
	diagnostics.firstChunkFinishedAt = 1_029;
	diagnostics.lastChunkFinishedAt = 1_040;
	const completionEvents = [
		{
			completedBytes: chunkSize + chunkByteLengths[3],
			reachedAt: 1_030,
			chunkIndex: 3,
		},
		{
			completedBytes: 2 * chunkSize + chunkByteLengths[3],
			reachedAt: 1_035,
			chunkIndex: 1,
		},
		{
			completedBytes: FILE_SIZE_BYTES,
			reachedAt: 1_040,
			chunkIndex: 2,
		},
	];
	diagnostics.progressTelemetry.milestones = [
		{
			basisPoints: 0,
			targetBytes: 0,
			completedBytes: 0,
			reachedAt: diagnostics.startedAt,
			chunkIndex: null,
		},
		...Array.from({ length: 20 }, (_, offset) => {
			const basisPoints = (offset + 1) * 500;
			const targetBytes = Number(
				(BigInt(FILE_SIZE_BYTES) * BigInt(basisPoints) + 9_999n) / 10_000n,
			);
			const event = completionEvents.find(
				(value) => value.completedBytes >= targetBytes,
			);
			return { basisPoints, targetBytes, ...event };
		}),
	];

	assert.throws(
		() => validateBenchmarkResult(result, options),
		/already crossed before its triggering chunk completion/,
	);
});

test("requires bounded, error-free memory telemetry covering the canonical read", () => {
	const cleanupTimeoutWarning = validResult();
	cleanupTimeoutWarning.downloadMemoryTelemetry.readerJsHeap.cleanupWarnings.push(
		"cleanup-timeout: Page JS heap Performance.disable exceeded 4000ms",
	);
	assert.equal(
		validateBenchmarkResult(cleanupTimeoutWarning, options).status,
		"passed",
	);
	for (const [mutate, pattern] of [
		[
			(result) => {
				delete result.downloadMemoryTelemetry;
			},
			/missing downloadMemoryTelemetry/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.complete = false;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.cleanupComplete = false;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.postTransferSoakMs += 1;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.operationTimeoutMs += 1;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.liveSampleMaxGapMs += 1;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samplingErrors.push("boom");
			},
			/contains memory sampling errors/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samplingErrors = Array.from(
					{ length: 17 },
					() => "bounded-message",
				);
			},
			/contains unbounded sampling errors/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samplingErrors = [
					"x".repeat(513),
				];
			},
			/contains unbounded sampling errors/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.cleanupWarnings = [
					"ordinary cleanup failure",
				];
			},
			/contains invalid cleanup warnings/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.sampleCount = 5;
			},
			/invalid bounded sample series/,
		],
		[
			(result) => {
				const samples = result.downloadMemoryTelemetry.readerJsHeap.samples;
				samples[0].capturedAt = 1_199;
				samples[1].capturedAt = 1_171;
			},
			/reorder the sampler window/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samples[0].capturedAt = 1_171;
			},
			/does not bracket the click-to-post-shutdown observation window/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.writerJsHeap.samples.at(-1).capturedAt =
					1_201;
			},
			/reorder the sampler window/,
		],
		[
			(result) => {
				shiftTimedDownloadEvidence(result, 100_000);
				const series = result.downloadMemoryTelemetry.readerJsHeap;
				series.startedAt =
					result.timestamps.downloadStartedAt -
					DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS -
					1;
				series.samples[0].capturedAt = series.startedAt;
			},
			/begins before the bounded telemetry setup window/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.writerJsHeap;
				series.samples.at(-1).capturedAt =
					Math.max(
						result.shutdownOutcomes.writer.finishedAt,
						result.shutdownOutcomes.reader.finishedAt,
					) +
					DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS +
					1;
				series.finishedAt = series.samples.at(-1).capturedAt + 1;
				series.terminalSampleAt = series.samples.at(-1).capturedAt;
			},
			/ends after the bounded telemetry cleanup window/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.peakBytes += 1;
			},
			/heap summaries are inconsistent/,
		],
		[
			(result) => {
				delete result.downloadMemoryTelemetry.readerJsHeap.startTotalBytes;
			},
			/unexpected or missing fields/,
		],
		[
			(result) => {
				const sample = result.downloadMemoryTelemetry.readerJsHeap.samples[0];
				sample.totalBytes = sample.usedBytes - 1;
			},
			/heap totals are inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.writerJsHeap.peakBackingStorageBytes += 1;
			},
			/heap summaries are inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].combinedBytes += 1;
			},
			/RSS totals are inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.peakNodeExternalBytes += 1;
			},
			/host RSS summaries are inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].nodeArrayBuffersBytes =
					-1;
			},
			/nodeArrayBuffersBytes/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].browserRoleBytes.renderer += 1;
			},
			/RSS totals are inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].browserRoleBytes =
					Object.fromEntries(
						Array.from({ length: 33 }, (_, index) => [`role-${index}`, 1]),
					);
			},
			/invalid browser-role count/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.browserSessionCount = 1;
			},
			/host RSS attribution contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].browserRootProcessCount = 1;
			},
			/does not prove both browser instances and root processes were sampled/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[0].browserProcessCount =
					DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES + 1;
			},
			/browser-process count cap/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.finishedAt += 1;
			},
			/window summary is inconsistent/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samples[0].unbounded = "x";
			},
			/unexpected or missing fields/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.readerJsHeap;
				series.samples = Array.from(
					{ length: result.downloadMemoryTelemetry.maxSamplesPerSeries + 1 },
					() => ({ capturedAt: 1_170, usedBytes: 100 }),
				);
				series.sampleCount = series.samples.length;
			},
			/invalid bounded sample series/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("accepts a short phase with no periodic sample when live endpoints cover it", () => {
	const result = validResult();
	const series = result.downloadMemoryTelemetry.readerJsHeap;
	series.samples = series.samples.filter(
		(sample) => sample.sampleKind !== "periodic",
	);
	series.sampleCount = series.samples.length;
	series.periodicSampleCount = 0;
	const peak = (key) =>
		Math.max(...series.samples.map((sample) => sample[key]));
	series.peakBytes = peak("usedBytes");
	series.peakUsedBytes = peak("usedBytes");
	series.peakTotalBytes = peak("totalBytes");
	series.peakEmbedderHeapUsedBytes = peak("embedderHeapUsedBytes");
	series.peakBackingStorageBytes = peak("backingStorageBytes");
	assert.equal(validateBenchmarkResult(result, options).status, "passed");
});

test("rejects a long live phase with an uncovered periodic-sampling gap", () => {
	const result = validResult();
	result.timestamps.postTransferSoakStartedAt = 20_000;
	result.timestamps.postTransferSoakFinishedAt = 20_050;
	result.resourceEvidence = createResourceEvidence({
		beforeTimedRead: [1_168, 1_170],
		afterSink: [1_202, 1_204],
		beforeSoak: [19_995, 20_000],
		afterSoak: [20_051, 20_054],
	});
	result.snapshots.at(-1).at = 20_055;
	result.shutdownOutcomes = createShutdownOutcomes({
		writerStartedAt: 20_056,
		writerFinishedAt: 20_061,
		readerStartedAt: 20_056,
		readerFinishedAt: 20_063,
	});
	result.downloadMemoryTelemetry = createDownloadMemoryTelemetry(
		1_170,
		1_200,
		INVOCATION,
		{ afterSoakStartedAt: 20_051, shutdownFinishedAt: 20_063 },
	);
	assert.throws(
		() => validateBenchmarkResult(result, options),
		/live-sample gap/,
	);
});

test("requires sufficient full-window capacity and distinct memory checkpoints", () => {
	for (const [mutate, pattern] of [
		[
			(result) => {
				result.downloadMemoryTelemetry.samplingWindowBudgetMs += 1;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.endpointSampleAllowance = 2;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.capacityExhaustedBeforeTerminal = true;
				result.downloadMemoryTelemetry.samplingCapacitySufficient = false;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.manualCheckpointComplete = false;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.terminalCheckpointComplete = false;
			},
			/telemetry contract is invalid/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.maxSamples -= 1;
			},
			/invalid bounded sample series/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.writerJsHeap.periodicSampleLimit += 1;
			},
			/invalid bounded sample series/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.periodicSampleCount += 1;
			},
			/invalid bounded sample series/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.readerJsHeap;
				series.capacityExhaustedBeforeTerminal = true;
				series.samplingCapacitySufficient = false;
			},
			/invalid bounded sample series/,
		],
		[
			(result) => {
				delete result.downloadMemoryTelemetry.readerJsHeap.samples[0]
					.sampleKind;
			},
			/invalid sample kind/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.readerJsHeap.samples[1].sampleKind =
					"initial";
			},
			/invalid live or terminal checkpoint ordering/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.writerJsHeap.samples[1].sampleKind =
					"manual";
			},
			/duplicate manual samples/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples.at(-1).sampleKind =
					"periodic";
			},
			/invalid live or terminal checkpoint ordering/,
		],
		[
			(result) => {
				result.downloadMemoryTelemetry.hostRss.samples[1].sampleKind =
					"terminal";
			},
			/duplicate terminal samples/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.readerJsHeap;
				const manual = series.samples.find(
					(sample) => sample.sampleKind === "manual",
				);
				manual.capturedAt = result.shutdownOutcomes.reader.finishedAt + 1;
				series.lastManualSampleAt = manual.capturedAt;
			},
			/invalid live or terminal checkpoint ordering/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.writerJsHeap;
				const manual = series.samples.find(
					(sample) => sample.sampleKind === "manual",
				);
				manual.capturedAt =
					result.resourceEvidence.snapshots.afterSoak.startedAt + 1;
				series.lastManualSampleAt = manual.capturedAt;
			},
			/invalid live or terminal checkpoint ordering/,
		],
		[
			(result) => {
				const series = result.downloadMemoryTelemetry.hostRss;
				const terminal = series.samples.at(-1);
				terminal.capturedAt = result.shutdownOutcomes.reader.finishedAt - 1;
				series.terminalSampleAt = terminal.capturedAt;
			},
			/invalid live or terminal checkpoint ordering/,
		],
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	}
});

test("accepts exact observer-locality control and rejects contradictory evidence", () => {
	const fixture = createReaderLocalityFixture();
	assert.equal(
		validateBenchmarkResult(fixture.result, fixture.options).status,
		"passed",
	);
	for (const [mutate, pattern] of [
		[
			(result) => {
				result.readerLocalityControl.expectedTerminalTopology = "replicator";
			},
			/locality control contract is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.transportCounterSampleClockToleranceMs = 2;
			},
			/locality control contract is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.transportCounterPreReadStartToleranceMs = 999;
			},
			/locality control contract is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.transportCounterPostReadCaptureMaxDelayMs = 10_000;
			},
			/locality control contract is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.beforePreloadObservation.chunkCount = 3;
				for (const observation of [
					...result.readerLocalityControl.stabilityObservations,
					result.readerLocalityControl.preDownloadObservation,
				]) {
					observation.chunkCount = 3;
				}
			},
			/manifest chunk count contradicts the canonical completed read/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerInitialRoleEvidence.initialRole =
					"replicator-default";
			},
			/did not initialize the reader as an observer/,
		],
		[
			(result) => {
				result.readerDiagnostics.timings.updateRoleCount = 1;
			},
			/diagnostics contradict its initial observer-role evidence/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerTopologyBeforeTimedRead.peerHash =
					"writer-peer";
			},
			/does not preserve two distinct peer identities/,
		],
		[
			(result) => {
				const control = result.readerLocalityControl;
				control.writerTopologyBeforeTimedRead.peerHash = "new-writer";
				control.writerTopologyBeforeTimedRead.replicatorHashes = ["new-writer"];
				control.readerTopologyBeforeTimedRead.peerHash = "new-reader";
				control.readerTopologyBeforeTimedRead.replicatorHashes = ["new-writer"];
			},
			/does not preserve two distinct peer identities/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerTopologyBeforeTimedRead.replicatorHashes =
					["third-peer"];
			},
			/does not agree on the writer as the exact singleton replicator/,
		],
		[
			(result) => {
				delete result.readerLocalityControl.writerTopologyAfterTimedRead;
			},
			/writerTopologyAfterTimedRead/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerTopologyAfterTimedRead.peerHash =
					"other-reader";
			},
			/does not preserve two distinct peer identities/,
		],
		[
			(result) => {
				result.readerLocalityControl.writerTopologyAfterTimedRead.replicatorHashes =
					["reader-peer", "writer-peer"];
				result.readerLocalityControl.writerTopologyAfterTimedRead.replicatorCount = 2;
			},
			/immediate post-read topology evidence is inconsistent/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].writerTopology.transportStreams[0].protocol =
					"/wrong/protocol";
			},
			/relevant pubsub stream 0 is not authoritative/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].readerTopology.transportStreams[0].remotePeerHash =
					"wrong-writer-hash";
			},
			/relevant pubsub stream 0 is not authoritative/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].writerTopology.transportStreams[0].remotePeer =
					"wrong-reader-id";
			},
			/relevant pubsub stream 0 is not authoritative/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].writerTopology.transportStreams.push(
					null,
				);
			},
			/contains malformed transport stream diagnostics/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].writerTopology.transportStreams[0].peerHashIdentityMatch = false;
			},
			/relevant pubsub stream 0 is not authoritative/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyObservations[1].writerTopology.transportStreams[0].connectionId =
					"changed-writer-connection";
			},
			/do not prove stable counterpart pubsub counters/,
		],
		[
			(result) => {
				const streams =
					result.readerLocalityControl.preTimedReadTopologyObservations[1]
						.writerTopology.transportStreams;
				streams[1] = structuredClone(streams[0]);
			},
			/contains a duplicate pubsub counter key/,
		],
		[
			(result) => {
				const observations =
					result.readerLocalityControl.preTimedReadTopologyObservations;
				observations[0].writerTopology.transportStreams[0].bytes = 101;
				observations[1].writerTopology.transportStreams[0].bytes = 100;
			},
			/counters decrease for an unchanged key set/,
		],
		[
			(result) => {
				const control = result.readerLocalityControl;
				for (const observation of control.postTimedReadTopologyObservations) {
					observation.writerTopology.transportStreams[0].connectionId =
						"replacement-writer-connection";
				}
				control.writerTopologyAfterTimedRead.transportStreams[0].connectionId =
					"replacement-writer-connection";
			},
			/writer pubsub counter key set changed during timed read/,
		],
		[
			(result) => {
				const control = result.readerLocalityControl;
				for (const observation of control.postTimedReadTopologyObservations) {
					observation.writerTopology.transportStreams[0].bytes = 90;
					observation.writerTopology.transportStreams[1].bytes = 360;
				}
				control.writerTopologyAfterTimedRead.transportStreams[0].bytes = 90;
				control.writerTopologyAfterTimedRead.transportStreams[1].bytes = 360;
			},
			/writer pubsub counter decreased during timed read/,
		],
		[
			(result) => {
				const control = result.readerLocalityControl;
				for (const observation of control.postTimedReadTopologyObservations) {
					observation.readerTopology.transportStreams[0].bytes =
						2 * 1024 * 1024;
				}
				control.readerTopologyAfterTimedRead.transportStreams[0].bytes =
					2 * 1024 * 1024;
			},
			/do not prove stable counterpart pubsub counters/,
		],
		[
			(result) => {
				result.readerLocalityControl.postTimedReadTopologyObservations.pop();
			},
			/stability window is invalid or unbounded/,
		],
		[
			(result) => {
				result.readerLocalityControl.preTimedReadTopologyDeadlineAt -= 1;
			},
			/stability window is invalid or unbounded/,
		],
		[
			(result) => {
				result.readerLocalityControl.postTimedReadTopologyFinishedAt =
					result.timestamps.downloadCompletionObservedAt + 9_001;
			},
			/exceeded its bounded post-read capture delay/,
		],
		[
			(result) => {
				result.readerLocalityControl.postTimedReadTopologyCaptureDelayMs += 1;
			},
			/post-timed-read topology capture delay is inconsistent or unbounded/,
		],
		[
			(result) => {
				const observations =
					result.readerLocalityControl.preTimedReadTopologyObservations;
				observations[1].capturedAt = observations[0].capturedAt + 98;
				observations[1].writerTopology.capturedAt =
					observations[1].capturedAt - 1;
				observations[1].readerTopology.capturedAt =
					observations[1].capturedAt - 1;
			},
			/observations have inconsistent identities or timestamps/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerTopologyAfterTimedRead.capturedAt =
					result.timestamps.downloadCompletionObservedAt - 1;
			},
			/postTimedReadTopology observations do not prove stable counterpart pubsub counters/,
		],
		[
			(result) => {
				result.readerLocalityControl.writerTopologyAfterTimedRead.capturedAt =
					result.integrityVerifiedAt + 1;
			},
			/postTimedReadTopology observations do not prove stable counterpart pubsub counters/,
		],
		[
			(result) => {
				result.readerLocalityControl.stabilityObservations[1].capturedAt = 1_200;
			},
			/stable locality observations do not prove one exact prefix/,
		],
		[
			(result) => {
				result.readerLocalityControl.stabilityObservations[0].blockChunkIndices =
					[1];
			},
			/stable locality observations do not prove one exact prefix/,
		],
		[
			(result) => {
				result.readerLocalityControl.speculativeOvershootChunkCount = 1;
			},
			/locality cohort count or key is inconsistent/,
		],
		[
			(result) => {
				const control = result.readerLocalityControl;
				for (const observation of [
					...control.stabilityObservations,
					control.preDownloadObservation,
				]) {
					observation.blockCount = 2;
					observation.blockChunkIndices = [0, 1];
				}
				control.actualLocalChunkBlockCount = 2;
				control.speculativeOvershootChunkCount = 1;
				control.cohortKey = "observer-persistent-memory-prefix-b2-i0";
				result.readerLocalChunkBlockCount = 2;
				result.readerLocalityCohortKey = control.cohortKey;
				result.readerDiagnostics.lastReadDiagnostics.initialLocalChunkBlockCount = 2;
			},
			/locality cohort count or key is inconsistent/,
		],
		[
			(result) => {
				result.readerDiagnostics.lastReadDiagnostics.initialLocalChunkBlockCount = 0;
			},
			/timed read diagnostics do not match its exact locality cohort/,
		],
		[
			(result) => {
				result.readerLocalityControl.terminalTopologyExpectationSatisfied = false;
			},
			/terminal reader evidence does not match the requested persistence and topology policy/,
		],
		[
			(result) => {
				result.readerLocalityControl.terminalIdleObservation.activeTransfers = [
					"still-active",
				];
			},
			/terminalIdleObservation has inconsistent or non-idle locality evidence/,
		],
		[
			(result) => {
				const terminalIdle =
					result.readerLocalityControl.terminalIdleObservation;
				terminalIdle.blockCount = 1;
				terminalIdle.blockChunkIndices = [0];
			},
			/terminal reader evidence does not match the requested persistence and topology policy/,
		],
		[
			(result) => {
				const terminalIdle =
					result.readerLocalityControl.terminalIdleObservation;
				terminalIdle.indexRowCount = 1;
				terminalIdle.indexedChunkIndices = [0];
			},
			/terminal reader evidence does not match the requested persistence and topology policy/,
		],
		[
			(result) => {
				result.readerLocalityControl.terminalTopologyObservations.pop();
			},
			/exactly three stable terminal topology observations/,
		],
		[
			(result) => {
				result.readerLocalityControl.terminalTopologyObservations[1].readerTopology.replicatorHashes =
					["reader-peer"];
			},
			/terminal topology observations do not prove the requested stable topology/,
		],
		[
			(result) => {
				const observation =
					result.readerLocalityControl.terminalTopologyObservations[1];
				observation.capturedAt = 1_500;
				observation.writerTopology.capturedAt = 1_499;
				observation.readerTopology.capturedAt = 1_499;
			},
			/terminal topology observations do not prove the requested stable topology/,
		],
		[
			(result) => {
				result.writerDiagnostics.peerHash = "other-writer";
			},
			/final peer diagnostics contradict the terminal topology evidence/,
		],
		[
			(result) => {
				result.readerDiagnostics.replicationSetSize = 2;
			},
			/final peer diagnostics contradict the terminal topology evidence/,
		],
		[
			(result) => {
				result.readerLocalityControl.integrityVerifiedAt = 1_430;
			},
			/locality control timestamps are inconsistent/,
		],
		[
			(result) => {
				result.readerLocalityControl.terminalTopologyDeadlineAt -= 1;
			},
			/locality control timestamps are inconsistent/,
		],
		[
			(result) => {
				result.readerLocalityControl.readerTopologyBeforeTimedRead.selfInReplicatorSet = true;
			},
			/does not prove the requested topology role/,
		],
		[
			(result) => {
				result.readerLocalityControl.preloadEvidence.activeTransfersAfterClose =
					["still-active"];
			},
			/locality preload evidence is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.preloadEvidence.aggregateTimedOut = true;
			},
			/preload aggregate deadline evidence is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.preloadEvidence.aggregateTimeoutMs += 1;
			},
			/preload aggregate deadline evidence is invalid/,
		],
		[
			(result) => {
				result.readerLocalityControl.preloadEvidence.aggregateDeadlineAt += 1;
			},
			/preload aggregate deadline evidence is invalid/,
		],
		[
			(result) => {
				const preload = result.readerLocalityControl.preloadEvidence;
				preload.finishedAt =
					preload.aggregateDeadlineAt +
					result.transferTimeoutSchedulingToleranceMs +
					1;
			},
			/preload aggregate deadline evidence is invalid/,
		],
		[
			(result) => {
				result.snapshots[1].writerSeeders = 0;
				result.snapshots[2].writerSeeders = 0;
				result.droppedSeeders = true;
				result.unexpectedSeederDrop = true;
			},
			/seeder drop/i,
		],
	]) {
		const rejected = createReaderLocalityFixture();
		mutate(rejected.result);
		assert.throws(
			() => validateBenchmarkResult(rejected.result, rejected.options),
			pattern,
		);
	}
	const fullFileTarget = createReaderLocalityFixture();
	for (const invocation of [
		fullFileTarget.result.invocation,
		fullFileTarget.options.expectedInvocation,
	]) {
		invocation.readerLocalChunkTarget = 2;
	}
	fullFileTarget.result.readerLocalChunkTarget = 2;
	fullFileTarget.result.readerLocalityControl.requestedLocalChunkBlockCount = 2;
	assert.throws(
		() =>
			validateBenchmarkResult(fullFileTarget.result, fullFileTarget.options),
		/target must be a partial prefix of the canonical completed read/,
	);
});

test("accepts the historical reader-replicator terminal topology as an explicit cohort", () => {
	const fixture = createReaderLocalityFixture({
		readerTerminalTopology: "replicator",
	});
	assert.equal(
		validateBenchmarkResult(fixture.result, fixture.options).status,
		"passed",
	);
	assert.equal(
		fixture.result.readerLocalityControl.preDownloadObservation.indexRowCount,
		0,
	);
	assert.equal(
		fixture.result.readerLocalityControl.terminalIdleObservation.indexRowCount,
		2,
	);
});

test("accepts the explicit cold observer-persistent b0-i0 locality cohort", () => {
	const fixture = createReaderLocalityFixture({
		target: 0,
		maxOvershoot: 0,
		actualBlockCount: 0,
		actualIndexRowCount: 0,
	});
	assert.equal(
		validateBenchmarkResult(fixture.result, fixture.options).status,
		"passed",
	);
});

test("accepts the explicit cold observer-transient memory cohort", () => {
	const fixture = createReaderLocalityFixture({
		target: 0,
		maxOvershoot: 0,
		actualBlockCount: 0,
		actualIndexRowCount: 0,
		readerPersistChunkReads: false,
	});
	assert.equal(
		validateBenchmarkResult(fixture.result, fixture.options).status,
		"passed",
	);
	assert.equal(
		fixture.result.readerLocalityCohortKey,
		"observer-transient-memory-prefix-b0-i0",
	);
	assert.equal(
		fixture.result.readTransfer.receiverProgress.peerbitDurable.claimed,
		false,
	);
	assert.deepEqual(
		fixture.result.readerDiagnostics.lastReadDiagnostics
			.chunkPersistenceConfirmedAt,
		{},
	);
	assert.equal(
		fixture.result.readerDiagnostics.lastReadDiagnostics
			.initialLocalChunkBlockCount,
		null,
	);
	assert.equal(fixture.result.readerDiagnostics.replicationSetSize, 0);
	fixture.result.readerDiagnostics.lastReadDiagnostics.initialLocalChunkBlockCount = 0;
	assert.throws(
		() => validateBenchmarkResult(fixture.result, fixture.options),
		/timed read diagnostics do not match its exact locality cohort/,
	);
});

test("rejects stale read diagnostics outside the clicked download window", () => {
	for (const mutate of [
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.startedAt = 1_100;
			result.readerDiagnostics.lastReadDiagnostics.finishedAt = 1_130;
		},
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.finishedAt = 1_201;
		},
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(
			() => validateBenchmarkResult(result, options),
			/outside the clicked download window/,
		);
	}
});

test("rejects read diagnostics for a different file name or manifest id", () => {
	for (const mutate of [
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.fileName = "other.bin";
		},
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.fileId = "other-file-id";
		},
	]) {
		const result = validResult();
		mutate(result);
		assert.throws(
			() => validateBenchmarkResult(result, options),
			/read identity does not match the clicked file manifest/,
		);
	}
});

test("accepts hash-only, OPFS, and Node-file integrity cohorts", () => {
	for (const downloadSink of ["hash-only", "opfs", "node-file"]) {
		const fixture = createSinkFixture(downloadSink);
		assert.equal(
			validateBenchmarkResult(fixture.result, fixture.options).status,
			"passed",
		);
	}
});

test("enforces hash-only result-level primary download authority", () => {
	for (const [downloadSink, invalidAuthority] of [
		["hash-only", false],
		["opfs", true],
		["node-file", true],
	]) {
		const fixture = createSinkFixture(downloadSink);
		fixture.result.primaryDownloadAuthoritative = invalidAuthority;
		assert.throws(
			() => validateBenchmarkResult(fixture.result, fixture.options),
			/read-transfer timing definitions are invalid/,
		);
	}
});

test("requires persisted SHA-256 for OPFS and Node-file cohorts", () => {
	for (const downloadSink of ["opfs", "node-file"]) {
		const fixture = createSinkFixture(downloadSink);
		fixture.result.integrity.downloadedSha256Base64 = null;
		assert.throws(
			() => validateBenchmarkResult(fixture.result, fixture.options),
			/integrity.downloadedSha256Base64/,
		);
	}
});

test("rejects a tampered persisted OPFS SHA-256 readback", () => {
	const fixture = createSinkFixture("opfs");
	fixture.result.integrity.downloadedSha256Base64 = OTHER_SHA256;
	assert.throws(
		() => validateBenchmarkResult(fixture.result, fixture.options),
		/persisted opfs SHA-256 integrity gate/,
	);
});

test("rejects an unverified persisted OPFS readback", () => {
	const fixture = createSinkFixture("opfs");
	fixture.result.integrity.persistedSinkSha256Verified = false;
	fixture.result.integrity.sinkPersistenceVerified = false;
	assert.throws(
		() => validateBenchmarkResult(fixture.result, fixture.options),
		/persisted opfs SHA-256 integrity gate/,
	);
});

test("models sink-await subtraction as an overlap-sensitive diagnostic", () => {
	const noSinkDelay = calculateSinkAwaitSubtractedDiagnosticMs({
		libraryStreamWallMs: 50,
		sinkWriteAwaitMs: 0,
	});
	// In this controlled timeline, 30ms of the same 50ms read path progresses
	// during a 40ms sink wait. Wall time becomes 60ms, not 90ms; subtracting the
	// whole wait therefore reports 20ms and proves this is not a counterfactual
	// sink-free duration.
	const overlappingSinkDelay = calculateSinkAwaitSubtractedDiagnosticMs({
		libraryStreamWallMs: 60,
		sinkWriteAwaitMs: 40,
	});
	assert.equal(noSinkDelay, 50);
	assert.equal(overlappingSinkDelay, 20);
	assert.ok(overlappingSinkDelay < noSinkDelay);
});

test("bounds Node-file server timing against its browser sink timing", () => {
	const fixture = createSinkFixture("node-file");
	fixture.result.sinkServerWriteDurationMs = 10.501;
	assert.throws(
		() => validateBenchmarkResult(fixture.result, fixture.options),
		/Node-file server duration exceeds its browser sink duration/,
	);
});

test("rejects a status-only passed payload at the v11 evidence envelope", () => {
	assert.throws(
		() => validateBenchmarkResultEnvelope({ status: "passed" }, options),
		/missing schema/,
	);
});

test("requires explicit error evidence on failed v11 envelopes", () => {
	const completeFailure = validResult();
	completeFailure.status = "failed";
	completeFailure.failure = { message: "synthetic browser failure" };
	assert.equal(
		validateBenchmarkResultEnvelope(completeFailure, options).status,
		"failed",
	);
	const partialUploadFailure = structuredClone(completeFailure);
	const partialDiagnostics =
		partialUploadFailure.writerDiagnostics.lastUploadDiagnostics;
	partialDiagnostics.progressTelemetry.milestones =
		partialDiagnostics.progressTelemetry.milestones.slice(0, 4);
	partialDiagnostics.failureAt = 1_020;
	partialDiagnostics.failureMessage = "synthetic chunk failure";
	partialDiagnostics.finishedAt = null;
	assert.equal(
		validateBenchmarkResultEnvelope(partialUploadFailure, options).status,
		"failed",
	);

	const incompleteFailure = structuredClone(completeFailure);
	incompleteFailure.errorCollectionComplete = false;
	incompleteFailure.errorCount = null;
	incompleteFailure.errors = null;
	incompleteFailure.requestFailureCollectionComplete = false;
	incompleteFailure.requestFailureCount = null;
	incompleteFailure.requestFailures = null;
	assert.equal(
		validateBenchmarkResultEnvelope(incompleteFailure, options).status,
		"failed",
	);

	delete incompleteFailure.errorCollectionComplete;
	assert.throws(
		() => validateBenchmarkResultEnvelope(incompleteFailure, options),
		/incomplete error collection/,
	);
});

test("validates early and late failed-upload integrity claims", () => {
	const earlyFailure = validResult();
	earlyFailure.status = "failed";
	earlyFailure.failure = { message: "failed before integrity" };
	earlyFailure.integrity = null;
	earlyFailure.integrityVerified = false;
	earlyFailure.integrityVerifiedAt = null;
	assert.equal(
		validateBenchmarkResultEnvelope(earlyFailure, options).status,
		"failed",
	);

	const lateFailure = validResult();
	lateFailure.status = "failed";
	lateFailure.failure = { message: "terminal topology did not converge" };
	assert.equal(
		validateBenchmarkResultEnvelope(lateFailure, options).status,
		"failed",
	);

	for (const [mutate, pattern] of [
		[
			(result) => {
				result.integrityVerifiedAt = null;
			},
			/integrityVerifiedAt/,
		],
		[
			(result) => {
				result.integrity.sourceCrc32Hex = "ffffffff";
			},
			/CRC-32 integrity gate/,
		],
		[
			(result) => {
				result.integrityVerified = false;
				result.integrityVerifiedAt = null;
			},
			/contradicts its unverified claim/,
		],
	]) {
		const result = structuredClone(lateFailure);
		mutate(result);
		assert.throws(
			() => validateBenchmarkResultEnvelope(result, options),
			pattern,
		);
	}

	const falseWithTimestamp = structuredClone(earlyFailure);
	falseWithTimestamp.integrityVerifiedAt = 1_205;
	assert.throws(
		() => validateBenchmarkResultEnvelope(falseWithTimestamp, options),
		/must use a null integrityVerifiedAt/,
	);
});

test("retains request failures as non-fatal diagnostics", () => {
	const result = validResult();
	result.requestFailures.push("writer:requestfailed:{}");
	result.requestFailureCount = result.requestFailures.length;
	assert.equal(validateBenchmarkResult(result, options).status, "passed");
});

test("rejects failed, malformed, and stale Playwright outcomes", async () => {
	assert.throws(() => assertPlaywrightSucceeded(1), /unsuccessfully/);
	assert.throws(() => assertPlaywrightSucceeded(null), /unsuccessfully/);
	assert.throws(() => parseBenchmarkResult("{"), /Malformed JSON/);
	assert.throws(
		() =>
			validateBenchmarkResult({ ...validResult(), status: "failed" }, options),
		/status is not passed/,
	);

	const directory = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-bench-test-"),
	);
	try {
		const resultFile = path.join(directory, "result.json");
		await writeFile(resultFile, JSON.stringify(validResult()));
		await assert.rejects(
			loadAndValidateBenchmarkResult({
				resultFile,
				exitCode: 2,
				...options,
			}),
			/unsuccessfully/,
		);
		await assert.rejects(
			loadAndValidateBenchmarkResult({
				resultFile: path.join(directory, "missing.json"),
				exitCode: 0,
				...options,
			}),
			/did not produce/,
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

for (const [name, mutate, pattern] of [
	[
		"schema",
		(result) => {
			result.schema.version = 1;
		},
		/unsupported schema/,
	],
	[
		"integrity envelope",
		(result) => {
			delete result.integrity;
		},
		/missing integrity/,
	],
	[
		"fixture mode",
		(result) => {
			result.integrity.fixtureMode = "sparse";
		},
		/deterministic fixture/,
	],
	[
		"SHA-256 format",
		(result) => {
			result.integrity.sourceSha256Base64 = "x";
			result.integrity.manifestSha256Base64 = "x";
			result.integrity.downloadedSha256Base64 = "x";
		},
		/malformed integrity.sourceSha256Base64/,
	],
	[
		"CRC-32 match",
		(result) => {
			result.integrity.downloadedCrc32Hex = "ffffffff";
		},
		/CRC-32/,
	],
	[
		"manifest size",
		(result) => {
			delete result.integrity.manifestSizeBytes;
		},
		/exact-size/,
	],
	[
		"aggregate integrity",
		(result) => {
			result.integrity.verified = false;
		},
		/aggregate integrity/,
	],
	[
		"download sink mismatch",
		(result) => {
			result.downloadSink = "opfs";
		},
		/download sink does not match/,
	],
	[
		"library SHA-256 evidence",
		(result) => {
			result.integrity.libraryComputedSha256Base64 = null;
		},
		/integrity.libraryComputedSha256Base64/,
	],
	[
		"hash-only persistence claim",
		(result) => {
			result.integrity.downloadedSha256Base64 = SHA256;
			result.integrity.persistedSinkSha256Verified = true;
		},
		/must not claim persisted SHA-256/,
	],
	[
		"non-Node server timing",
		(result) => {
			result.sinkServerWriteCalls = 1;
		},
		/Node-only server timing/,
	],
	[
		"sink write count",
		(result) => {
			result.sinkWriteCalls = 3;
		},
		/sink write count/,
	],
	[
		"inflated sink helper timing",
		(result) => {
			result.sinkWriteDurationMs = 11.001;
		},
		/helper duration exceeds canonical read evidence/,
	],
	[
		"stream timing decomposition",
		(result) => {
			result.sinkAwaitSubtractedDiagnosticMs += 1;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"raw demand-tail summary",
		(result) => {
			result.readTransfer.demandWait.p95Ms += 1;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"raw demand-tail threshold count",
		(result) => {
			result.readTransfer.demandWait.over1sCount += 1;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"read source byte attribution",
		(result) => {
			result.readTransfer.sources.remote.bytes -= 1;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"raw materialization summary",
		(result) => {
			result.readTransfer.stages.materializeMs += 1;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"extra read-transfer field",
		(result) => {
			result.readTransfer.unverified = true;
		},
		/timing decomposition is inconsistent/,
	],
	[
		"noncanonical resolved chunk key",
		(result) => {
			const resolved =
				result.readerDiagnostics.lastReadDiagnostics.chunkResolved;
			resolved["01"] = resolved[1];
			delete resolved[1];
		},
		/contiguous canonical chunk indices/,
	],
	[
		"single giant large-file chunk",
		(result) => {
			delete result.readerDiagnostics.lastReadDiagnostics.chunkResolved[1];
		},
		/at least two contiguous canonical chunk indices/,
	],
	[
		"empty resolved chunk source",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkResolved[0] = "";
		},
		/invalid readerDiagnostics\.chunkResolved\[0\]/,
	],
	[
		"missing canonical demand key",
		(result) => {
			delete result.readerDiagnostics.lastReadDiagnostics.chunkDemandWaitMs[1];
		},
		/exact canonical chunk keys/,
	],
	[
		"extra noncanonical timing key",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkHashStartedAt["01"] =
				1_188;
		},
		/exact canonical chunk keys/,
	],
	[
		"chunk write before read window",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkWriteStartedAt[0] = 1_169;
		},
		/outside the library read window/,
	],
	[
		"chunk write after read window",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkWriteFinishedAt[1] = 1_201;
		},
		/outside the library read window/,
	],
	[
		"overlapping chunk writes",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkWriteStartedAt[1] = 1_184;
		},
		/ordered and non-overlapping/,
	],
	[
		"resolve completes after materialization starts",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkResolveFinishedAt[0] = 1_172;
		},
		/lifecycle is not causal/,
	],
	[
		"materialization completes after hashing starts",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkMaterializeFinishedAt[0] = 1_174;
		},
		/lifecycle is not causal/,
	],
	[
		"hashing completes after sink writing starts",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkHashFinishedAt[0] = 1_181;
		},
		/lifecycle is not causal/,
	],
	[
		"next chunk materializes before the previous sink write finishes",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkMaterializeStartedAt[1] = 1_184;
		},
		/lifecycle is not causal/,
	],
	[
		"raw read byte coverage",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.chunkByteLength[0] -= 1;
		},
		/do not cover the requested file size/,
	],
	[
		"raw reader SHA-256 contradiction",
		(result) => {
			result.readerDiagnostics.lastReadDiagnostics.computedFinalHash =
				SHA256.replace(/^A/, "B");
		},
		/raw reader SHA-256 contradicts/,
	],
	[
		"upload arithmetic",
		(result) => {
			result.uploadDurationMs += result.postUploadMonitorDurationMs;
		},
		/uploadDurationMs must end/,
	],
	[
		"phase ordering",
		(result) => {
			result.timestamps.downloadStartedAt = 1100;
		},
		/not monotonic/,
	],
	[
		"missing completion-observation timestamp",
		(result) => {
			delete result.timestamps.downloadCompletionObservedAt;
		},
		/downloadCompletionObservedAt/,
	],
	[
		"unsafe sink-completion epoch",
		(result) => {
			result.timestamps.downloadFinishedAt = Number.MAX_SAFE_INTEGER + 1;
		},
		/invalid timestamps.downloadFinishedAt/,
	],
	[
		"sink completion after observation",
		(result) => {
			result.timestamps.downloadFinishedAt = 1_202;
		},
		/not monotonic/,
	],
	[
		"sink completion before download start",
		(result) => {
			result.timestamps.downloadFinishedAt = 1_169;
		},
		/not monotonic/,
	],
	[
		"writer readiness arithmetic",
		(result) => {
			result.timeToWriterReadyMs += 1;
		},
		/phase duration arithmetic is inconsistent/,
	],
	[
		"reader readiness phase propagation",
		(result) => {
			result.phaseDurationsMs.timeToReaderReady += 1;
		},
		/phase duration arithmetic is inconsistent/,
	],
	[
		"readiness definition",
		(result) => {
			result.timeToReaderReadyDefinition = "post-settlement-only";
		},
		/readiness duration definitions are invalid/,
	],
	[
		"provenance",
		(result) => {
			result.provenance.peerbit.resolvedCommit = "d".repeat(40);
		},
		/provenance does not match/,
	],
	[
		"invocation",
		(result) => {
			result.invocation.postUploadMonitorMs = 1;
		},
		/invocation does not match/,
	],
	[
		"ready timeout propagation",
		(result) => {
			result.readyTimeoutMs -= 1;
		},
		/readyTimeoutMs does not match the requested invocation/,
	],
	[
		"reader readiness after the post-writer deadline",
		(result) => {
			const readerListedAt =
				result.timestamps.uploadSettledAt +
				result.invocation.readyTimeoutMs +
				1;
			result.timestamps.readerListedAt = readerListedAt;
			result.timeToReaderReadyMs =
				readerListedAt - result.timestamps.uploadStartedAt;
			result.phaseDurationsMs.timeToReaderReady = result.timeToReaderReadyMs;
			result.phaseDurationsMs.readerListingLag =
				readerListedAt - result.timestamps.uploadSettledAt;
			result.phaseDurationsMs.readerAfterWriter =
				readerListedAt - result.timestamps.writerListedAt;
			result.listingDurationMs = result.phaseDurationsMs.readerListingLag;
			result.timestamps.postMonitorStartedAt = readerListedAt;
			result.timestamps.postMonitorFinishedAt = readerListedAt + 50;
			result.timestamps.downloadStartedAt = readerListedAt + 50;
			result.timestamps.downloadFinishedAt = readerListedAt + 80;
			result.timestamps.downloadCompletionObservedAt = readerListedAt + 81;
		},
		/requested post-writer deadline/,
	],
	[
		"short monitor",
		(result) => {
			result.postUploadMonitorDurationMs = 49;
			result.phaseDurationsMs.postUploadMonitor = 49;
			result.timestamps.postMonitorFinishedAt = 1169;
			result.timestamps.downloadStartedAt = 1169;
			result.timestamps.downloadFinishedAt = 1199;
		},
		/outside the requested duration/,
	],
	[
		"long monitor",
		(result) => {
			result.postUploadMonitorDurationMs = 1_301;
			result.phaseDurationsMs.postUploadMonitor = 1_301;
			result.timestamps.postMonitorFinishedAt = 2_421;
			result.timestamps.downloadStartedAt = 2_421;
			result.timestamps.downloadFinishedAt = 2_451;
			result.timestamps.downloadCompletionObservedAt = 2_452;
		},
		/outside the requested duration/,
	],
	[
		"post-monitor tolerance",
		(result) => {
			result.postUploadMonitorSchedulingToleranceMs += 1;
		},
		/monitor scheduling tolerance/,
	],
	[
		"upload timeout",
		(result) => {
			result.uploadDurationMs = 605_001;
			result.phaseDurationsMs.timeToUploadSettled = 605_001;
			result.phaseDurationsMs.writerListingLag = 0;
			result.phaseDurationsMs.readerListingLag = 0;
			result.listingDurationMs = 0;
			result.timestamps.uploadSettledAt = 606_001;
			result.timestamps.postMonitorStartedAt = 606_001;
			result.timestamps.postMonitorFinishedAt = 606_051;
			result.timestamps.downloadStartedAt = 606_051;
			result.timestamps.downloadFinishedAt = 606_081;
			result.timestamps.downloadCompletionObservedAt = 606_082;
		},
		/requested timeout/,
	],
	[
		"download timeout",
		(result) => {
			result.downloadDurationMs = 605_001;
			result.phaseDurationsMs.download = 605_001;
			result.timestamps.downloadFinishedAt =
				result.timestamps.downloadStartedAt + 605_001;
			result.timestamps.downloadCompletionObservedAt =
				result.timestamps.downloadFinishedAt + 1;
		},
		/requested timeout/,
	],
	[
		"transfer tolerance",
		(result) => {
			result.transferTimeoutSchedulingToleranceMs += 1;
		},
		/transfer scheduling tolerance/,
	],
	[
		"recorded errors",
		(result) => {
			result.errors.push("writer:seeder-count:failed");
		},
		/recorded errors/,
	],
	[
		"error collection definition",
		(result) => {
			result.errorCollectionDefinition = "selected errors only";
		},
		/invalid error collection definition/,
	],
	[
		"error collection completeness",
		(result) => {
			result.errorCollectionComplete = false;
		},
		/incomplete error collection/,
	],
	[
		"known Peerbit signatures",
		(result) => {
			result.knownPeerbitFailureSignatures.pop();
		},
		/invalid known Peerbit failure signatures/,
	],
	[
		"error count arithmetic",
		(result) => {
			result.errorCount = 1;
		},
		/inconsistent recorded errors/,
	],
	[
		"request-failure definition",
		(result) => {
			result.requestFailureCollectionDefinition = "fatal network failures";
		},
		/invalid request-failure collection definition/,
	],
	[
		"request-failure completeness",
		(result) => {
			result.requestFailureCollectionComplete = false;
		},
		/incomplete request-failure collection/,
	],
	[
		"request-failure count arithmetic",
		(result) => {
			result.requestFailureCount = 1;
		},
		/inconsistent request-failure diagnostics/,
	],
	[
		"non-numeric seeder sample",
		(result) => {
			result.snapshots[0].writerSeeders = "error:failed";
		},
		/invalid snapshots\[0\]\.writerSeeders/,
	],
	[
		"missing monitor sample",
		(result) => {
			result.snapshots = result.snapshots.filter(
				(snapshot) => !snapshot.label.startsWith("after-"),
			);
		},
		/missing a numeric post-monitor snapshot/,
	],
	[
		"monitor sample bound",
		(result) => {
			result.snapshots[1].at = result.timestamps.postMonitorFinishedAt + 1;
		},
		/outside the monitor window/,
	],
	[
		"duplicate sample label",
		(result) => {
			result.snapshots[1].label = "seeders-ready";
		},
		/labels are invalid or duplicated/,
	],
	[
		"missing ready baseline snapshot",
		(result) => {
			result.snapshots = result.snapshots.filter(
				(snapshot) => snapshot.label !== "seeders-ready",
			);
		},
		/exactly one ready-seeder baseline snapshot/,
	],
	[
		"ready baseline not first",
		(result) => {
			[result.snapshots[0], result.snapshots[1]] = [
				result.snapshots[1],
				result.snapshots[0],
			];
			result.snapshots[0].at = 1120;
			result.snapshots[1].at = 1130;
		},
		/exactly one ready-seeder baseline snapshot/,
	],
	[
		"missing baseline field",
		(result) => {
			delete result.baselineWriterSeeders;
		},
		/invalid baselineWriterSeeders/,
	],
	[
		"contradictory baseline field",
		(result) => {
			result.baselineReaderSeeders = 1;
		},
		/baseline fields contradict/,
	],
	[
		"ready baseline below requested minimum",
		(result) => {
			result.baselineWriterSeeders = 1;
			result.snapshots[0].writerSeeders = 1;
		},
		/below the requested minimum/,
	],
	[
		"unreported seeder drop",
		(result) => {
			result.snapshots[1].writerSeeders = 1;
		},
		/droppedSeeders claim contradicts/,
	],
	[
		"unsupported seeder drop claim",
		(result) => {
			result.droppedSeeders = true;
		},
		/droppedSeeders claim contradicts/,
	],
	[
		"observed seeder drop in passed result",
		(result) => {
			result.snapshots[2].readerSeeders = 1;
			result.droppedSeeders = true;
			result.unexpectedSeederDrop = true;
		},
		/contains an unexpected seeder drop/,
	],
]) {
	test(`rejects invalid ${name}`, () => {
		const result = validResult();
		mutate(result);
		assert.throws(() => validateBenchmarkResult(result, options), pattern);
	});
}

const createSeederProbeFixture = () => {
	const invocation = createBenchmarkInvocation({
		scenario: "seeder-probe",
		mode: "adaptive",
		network: "local",
		integrationMode: "link",
		fileMb: FILE_MB,
		fixtureSeed: "fixture-seed",
		readyTimeoutMs: 10,
		sampleMs: 2,
		sampleCount: 3,
		targetSeeders: 2,
	});
	const seederResult = {
		schema: { ...BENCHMARK_RESULT_SCHEMA },
		runNonce: RUN_NONCE,
		invocation,
		provenance: structuredClone(PROVENANCE),
		status: "passed",
		mode: "adaptive",
		networkMode: "local",
		readyTimeoutMs: 10,
		sampleMs: 2,
		sampleCount: 3,
		targetSeeders: 2,
		effectiveSampleIntervalMs: 2,
		sampleCountDefinition: SAMPLE_COUNT_DEFINITION,
		probeStartedAt: 1000,
		readyDeadlineAt: 1010,
		probeFinishedAt: 1002,
		probeDurationMs: 2,
		reachedTarget: true,
		timeToTargetMs: 2,
		targetSampleLabel: "sample-1",
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: [...KNOWN_PEERBIT_FAILURE_SIGNATURES],
		errorCollectionComplete: true,
		errorCount: 0,
		errors: [],
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete: true,
		requestFailureCount: 0,
		requestFailures: [],
		samples: [
			{
				index: 1,
				label: "sample-1",
				capturedAt: 1002,
				elapsedMs: 2,
				writerSeeders: 3,
				readerSeeders: 2,
			},
		],
	};
	const seederOptions = {
		scenario: "seeder-probe",
		expectedMode: "adaptive",
		expectedFileMb: FILE_MB,
		expectedNetwork: "local",
		expectedFixtureSeed: "fixture-seed",
		expectedRunNonce: RUN_NONCE,
		expectedProvenance: PROVENANCE,
		expectedInvocation: invocation,
	};
	return { seederResult, seederOptions };
};

test("accepts bounded numeric seeder convergence evidence", () => {
	const { seederResult, seederOptions } = createSeederProbeFixture();
	assert.doesNotThrow(() =>
		validateBenchmarkResult(seederResult, seederOptions),
	);
});

test("keeps the non-transfer seeder probe fail closed", () => {
	const { seederResult, seederOptions } = createSeederProbeFixture();
	assert.throws(
		() =>
			validateBenchmarkResult(
				{
					...seederResult,
					reachedTarget: false,
				},
				seederOptions,
			),
		/did not reach/,
	);
	assert.throws(
		() =>
			validateBenchmarkResult(
				{
					...seederResult,
					runNonce: "223e4567-e89b-42d3-a456-426614174000",
				},
				seederOptions,
			),
		/run nonce/,
	);
});

for (const [name, mutate, pattern] of [
	[
		"target counts",
		(result) => {
			result.samples[0].writerSeeders = 1;
		},
		/target evidence/,
	],
	[
		"inflated time to target",
		(result) => {
			result.samples.push({
				index: 2,
				label: "sample-2",
				capturedAt: 1004,
				elapsedMs: 4,
				writerSeeders: 3,
				readerSeeders: 2,
			});
			result.targetSampleLabel = "sample-2";
			result.probeFinishedAt = 1004;
			result.probeDurationMs = 4;
			result.timeToTargetMs = 4;
		},
		/first observed target/,
	],
	[
		"target arithmetic",
		(result) => {
			result.timeToTargetMs = 1;
		},
		/deadline\/duration arithmetic/,
	],
	[
		"deadline arithmetic",
		(result) => {
			result.readyDeadlineAt += 1;
		},
		/deadline\/duration arithmetic/,
	],
	[
		"deadline bound",
		(result) => {
			result.probeFinishedAt = 1011;
			result.probeDurationMs = 11;
			result.timeToTargetMs = 11;
			result.samples[0].capturedAt = 1011;
			result.samples[0].elapsedMs = 11;
		},
		/deadline\/duration arithmetic/,
	],
	[
		"numeric sample count",
		(result) => {
			result.samples[0].readerSeeders = "2";
		},
		/invalid samples\[0\]\.readerSeeders/,
	],
	[
		"monotonic sample time",
		(result) => {
			result.samples.unshift({
				index: 1,
				label: "sample-1",
				capturedAt: 1003,
				elapsedMs: 3,
				writerSeeders: 1,
				readerSeeders: 1,
			});
			result.samples[1].index = 2;
			result.samples[1].label = "sample-2";
			result.targetSampleLabel = "sample-2";
		},
		/not monotonic and bounded/,
	],
	[
		"sample labels",
		(result) => {
			result.samples[0].label = "target";
			result.targetSampleLabel = "target";
		},
		/labels or indices/,
	],
	[
		"sample-count semantics",
		(result) => {
			result.effectiveSampleIntervalMs = 3;
		},
		/effective sample interval/,
	],
	[
		"seeder errors",
		(result) => {
			result.errors.push("reader:seeder-count:failed");
		},
		/recorded errors/,
	],
]) {
	test(`rejects invalid seeder ${name}`, () => {
		const { seederResult, seederOptions } = createSeederProbeFixture();
		mutate(seederResult);
		assert.throws(
			() => validateBenchmarkResult(seederResult, seederOptions),
			pattern,
		);
	});
}
