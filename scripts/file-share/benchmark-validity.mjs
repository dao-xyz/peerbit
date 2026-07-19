import fsp from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import {
	BENCHMARK_RESULT_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
	SEEDER_DROP_POLICY,
	projectUploadIntegrityEvidence,
} from "./benchmark-orchestration.mjs";
import {
	DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
	DOWNLOAD_MEMORY_HOST_ATTRIBUTION,
	DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS,
	DOWNLOAD_MEMORY_HOST_SCOPE,
	DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
	DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES,
	DOWNLOAD_MEMORY_MAX_BROWSER_ROLES,
	DOWNLOAD_MEMORY_MAX_BROWSER_ROLE_NAME_LENGTH,
	DOWNLOAD_MEMORY_MAX_CLEANUP_WARNINGS,
	DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
	DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS,
	DOWNLOAD_MEMORY_NODE_SCOPE,
	DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	DOWNLOAD_MEMORY_PROFILE,
	DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
	DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS,
	DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS,
	DOWNLOAD_MEMORY_WINDOW_DEFINITION,
	assertDownloadMemoryLiveSampleCoverage,
	calculateDownloadMemoryMaxLiveSampleGapMs,
	calculateDownloadMemoryMaxSamples,
} from "./templates/download-memory-telemetry.mjs";

const SHA256_BASE64_PATTERN = /^[A-Za-z0-9+/]{43}=$/;
const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/;
const GIT_COMMIT_PATTERN = /^[0-9a-f]{40}$/;
const CRC32_HEX_PATTERN = /^[0-9a-f]{8}$/;
const UUID_PATTERN =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
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
const SINK_SERVER_WRITE_DURATION_DEFINITION =
	"loopback-request-body-receive-and-node-filesystem-write-only";
const LIBRARY_STREAM_WALL_DEFINITION =
	"library-large-file-stream-start-to-finish including awaited sink writes";
const SINK_WRITE_AWAIT_DEFINITION =
	"sum of per-chunk library wall-clock intervals awaiting writable.write";
const SINK_AWAIT_SUBTRACTED_DIAGNOSTIC_DEFINITION =
	"arithmetic library stream wall-clock duration minus summed awaited writable.write intervals; overlap-sensitive and not a sink-independent Peerbit duration";
const PRIMARY_DOWNLOAD_METRIC = "libraryStreamWallMs";
const PRIMARY_DOWNLOAD_METRIC_DEFINITION =
	"authoritative only within one fixed download-sink cohort; hash-only is the standardized primary cohort and includes awaited sink writes plus any overlapping read-ahead";
const TRANSPORT_COUNTER_STABILITY_POLL_MS = 100;
const TRANSPORT_COUNTER_STABILITY_TIMEOUT_MS = 5_000;
const TRANSPORT_COUNTER_STABLE_SAMPLE_COUNT = 3;
const TRANSPORT_COUNTER_MAX_COUNTERPART_BYTE_SKEW = 1024 * 1024;
const TRANSPORT_COUNTER_SAMPLE_CLOCK_TOLERANCE_MS = 1;
const TRANSPORT_COUNTER_PRE_READ_START_TOLERANCE_MS = 1_000;
const TRANSPORT_COUNTER_POST_READ_CAPTURE_MAX_DELAY_MS = 9_000;
const PUBSUB_PROTOCOL = "/peerbit/topic-control-plane/2.0.0";
const DEMAND_WAIT_DEFINITION =
	"wall-clock time each sequential stream consumer awaited its scheduled chunk";
const RECEIVER_PROGRESS_PERCENTAGES = Object.freeze(
	Array.from({ length: 21 }, (_, index) => index * 5),
);
const RECEIVER_PROGRESS_MILESTONE_KEYS = Object.freeze([
	"percent",
	"targetBytes",
	"contiguousBytes",
	"chunkIndex",
	"confirmedAt",
	"elapsedMs",
]);
const PERSISTENCE_CONFIRMATION_SOURCES = new Set([
	"manifest-head-batch-local",
	"manifest-head-batch-remote",
	"manifest-entry-local",
	"manifest-entry-import",
]);
const RESOURCE_SNAPSHOT_TIMEOUT_MS = 15_000;
const PAGE_SHUTDOWN_TIMEOUT_MS = 30_000;
const RESOURCE_STORAGE_DEFINITION =
	"Peerbit logical usage and browser origin-wide navigator.storage estimates; deltas are later minus earlier";
const RESOURCE_EAGER_DEFINITION =
	"deltas of monotonic eager-cache admission, hit, eviction, expiration, and rejection counters; null when eager telemetry is disabled or unavailable";
const RESOURCE_MAX_STORAGE_DETAIL_KEYS = 64;
const RESOURCE_MAX_STORAGE_DETAIL_KEY_LENGTH = 128;
const EAGER_MONOTONIC_COUNTERS = Object.freeze([
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
]);
const EAGER_TELEMETRY_KEYS = Object.freeze([
	"entries",
	"bytes",
	"peakEntries",
	"peakBytes",
	...EAGER_MONOTONIC_COUNTERS.slice(0, 2),
	"pendingEntries",
	"pendingBytes",
	"peakPendingEntries",
	"peakPendingBytes",
	...EAGER_MONOTONIC_COUNTERS.slice(2),
	"limits",
]);
const EAGER_LIMIT_KEYS = Object.freeze([
	"maxEntries",
	"maxBytes",
	"maxBlockBytes",
	"ttlMs",
	"validationConcurrency",
	"maxPendingBytes",
	"maxPendingEntries",
]);
const BENCHMARK_MIN_OUTER_TIMEOUT_MS = 20 * 60 * 1_000;
const BENCHMARK_OUTER_TIMEOUT_SAFETY_MS = 5 * 60 * 1_000;

const calculateUploadSamplingWindowBudgetMs = (invocation) => {
	const transferToleranceMs = Math.max(5_000, invocation.pollMs + 1_000);
	const localityBudgetMs =
		invocation.readerLocalChunkTarget === null
			? 0
			: 3 * invocation.readyTimeoutMs +
				2 * TRANSPORT_COUNTER_STABILITY_TIMEOUT_MS +
				(invocation.readerLocalChunkTarget > 0
					? invocation.downloadTimeoutMs + transferToleranceMs
					: 0);
	const requestedBudgetMs =
		3 * invocation.readyTimeoutMs +
		invocation.uploadTimeoutMs +
		invocation.downloadTimeoutMs +
		localityBudgetMs +
		invocation.postUploadMonitorMs +
		invocation.postTransferSoakMs +
		BENCHMARK_OUTER_TIMEOUT_SAFETY_MS;
	return requirePositiveSafeInteger(
		Math.max(BENCHMARK_MIN_OUTER_TIMEOUT_MS, requestedBudgetMs),
		"download memory sampling window budget",
	);
};
// Date.now() endpoints can undercount the nested performance.now() helper
// interval by less than one millisecond for each chunk.
const SINK_WRITE_QUANTIZATION_ALLOWANCE_MS_PER_CHUNK = 1;
// Browser performance.now() and Node hrtime measure nested intervals on
// independent monotonic clocks; keep their aggregate drift finite per call.
const SINK_SERVER_CLOCK_TOLERANCE_MS_PER_CHUNK = 1;
const UPLOAD_PROGRESS_MILESTONE_BASIS_POINTS = Object.freeze(
	Array.from({ length: 21 }, (_, index) => index * 500),
);
const UPLOAD_PROGRESS_TELEMETRY_KEYS = Object.freeze([
	"schemaVersion",
	"kind",
	"clock",
	"milestones",
]);
const UPLOAD_PROGRESS_MILESTONE_KEYS = Object.freeze([
	"basisPoints",
	"targetBytes",
	"completedBytes",
	"reachedAt",
	"chunkIndex",
]);
const SEEDER_DROP_POLICY_KEYS = Object.freeze(Object.keys(SEEDER_DROP_POLICY));

const isRecord = (value) =>
	value != null && typeof value === "object" && !Array.isArray(value);

const requireRecord = (value, label) => {
	if (!isRecord(value)) {
		throw new Error(`Benchmark result is missing ${label}`);
	}
	return value;
};

const requireExactRecordKeys = (value, expectedKeys, label) => {
	const actualKeys = Object.keys(value).toSorted();
	const sortedExpectedKeys = [...expectedKeys].toSorted();
	if (!isDeepStrictEqual(actualKeys, sortedExpectedKeys)) {
		throw new Error(`Benchmark ${label} contains unexpected or missing fields`);
	}
	return value;
};

const requireString = (value, label) => {
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

const requireNonNegativeNumber = (value, label) => {
	if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

export const calculateSinkAwaitSubtractedDiagnosticMs = ({
	libraryStreamWallMs,
	sinkWriteAwaitMs,
}) => {
	requireNonNegativeNumber(libraryStreamWallMs, "libraryStreamWallMs");
	requireNonNegativeNumber(sinkWriteAwaitMs, "sinkWriteAwaitMs");
	if (sinkWriteAwaitMs > libraryStreamWallMs) {
		throw new Error(
			"Benchmark sink-write intervals exceed the library stream wall time",
		);
	}
	return libraryStreamWallMs - sinkWriteAwaitMs;
};

const requirePositiveNumber = (value, label) => {
	const parsed = requireNonNegativeNumber(value, label);
	if (parsed <= 0) {
		throw new Error(`Benchmark result has non-positive ${label}`);
	}
	return parsed;
};

const requireFiniteNumber = (value, label) => {
	if (typeof value !== "number" || !Number.isFinite(value)) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

const requireSafeInteger = (value, label) => {
	if (!Number.isSafeInteger(value)) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

const requireNonNegativeSafeInteger = (value, label) => {
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

const requirePositiveSafeInteger = (value, label) => {
	const parsed = requireNonNegativeSafeInteger(value, label);
	if (parsed <= 0) {
		throw new Error(`Benchmark result has non-positive ${label}`);
	}
	return parsed;
};

const requirePattern = (value, pattern, label) => {
	const string = requireString(value, label);
	if (!pattern.test(string)) {
		throw new Error(`Benchmark result has malformed ${label}`);
	}
	return string;
};

const validateSeederDropPolicy = (result) => {
	const policy = requireExactRecordKeys(
		requireRecord(result.seederDropPolicy, "seederDropPolicy"),
		SEEDER_DROP_POLICY_KEYS,
		"seederDropPolicy",
	);
	if (!isDeepStrictEqual(policy, SEEDER_DROP_POLICY)) {
		throw new Error("Benchmark result has an unsupported seeder-drop policy");
	}
	return policy;
};

export const assertPlaywrightSucceeded = (exitCode) => {
	if (!Number.isInteger(exitCode) || exitCode !== 0) {
		throw new Error(
			`Playwright benchmark exited unsuccessfully (exitCode=${String(exitCode)})`,
		);
	}
};

export const parseBenchmarkResult = (
	contents,
	resultFile = "benchmark result",
) => {
	let result;
	try {
		result = JSON.parse(contents);
	} catch (error) {
		throw new Error(`Malformed JSON in ${resultFile}`, { cause: error });
	}
	if (!isRecord(result)) {
		throw new Error(`${resultFile} must contain a JSON object`);
	}
	return result;
};

const validateUploadIntegrity = (
	result,
	expectedFileMb,
	expectedFixtureSeed,
	invocation,
	{ requireCompletedEvidence = true } = {},
) => {
	const integrity = requireRecord(result.integrity, "integrity");
	if (integrity.fixtureMode !== "deterministic") {
		throw new Error("Benchmark result did not use deterministic fixture bytes");
	}
	if (integrity.fixtureFormat !== "aes-256-ctr-v1") {
		throw new Error("Benchmark result has an unsupported fixture format");
	}
	if (integrity.fixtureSeed !== expectedFixtureSeed) {
		throw new Error("Benchmark result fixture seed does not match the request");
	}
	const expectedSizeBytes = expectedFileMb * 1024 * 1024;
	if (!Number.isSafeInteger(expectedSizeBytes) || expectedSizeBytes < 0) {
		throw new Error(`Invalid expected benchmark size ${expectedFileMb} MiB`);
	}
	if (
		integrity.expectedSizeBytes !== expectedSizeBytes ||
		integrity.sourceSizeBytes !== expectedSizeBytes ||
		integrity.manifestSizeBytes !== expectedSizeBytes ||
		integrity.downloadedSizeBytes !== expectedSizeBytes ||
		integrity.sizeVerified !== true ||
		integrity.manifestVerified !== true
	) {
		throw new Error("Benchmark result failed its exact-size integrity gate");
	}

	const sourceSha256 = requirePattern(
		integrity.sourceSha256Base64,
		SHA256_BASE64_PATTERN,
		"integrity.sourceSha256Base64",
	);
	const manifestSha256 = requirePattern(
		integrity.manifestSha256Base64,
		SHA256_BASE64_PATTERN,
		"integrity.manifestSha256Base64",
	);
	const librarySha256 = requirePattern(
		integrity.libraryComputedSha256Base64,
		SHA256_BASE64_PATTERN,
		"integrity.libraryComputedSha256Base64",
	);
	if (
		integrity.sha256Verified !== true ||
		integrity.librarySha256Verified !== true ||
		sourceSha256 !== manifestSha256 ||
		sourceSha256 !== librarySha256
	) {
		throw new Error("Benchmark result failed its SHA-256 integrity gate");
	}
	if (
		(requireCompletedEvidence &&
			(result.downloadSink !== invocation.downloadSink ||
				result.requestedDownloadSink !== invocation.downloadSink)) ||
		integrity.downloadSink !== invocation.downloadSink
	) {
		throw new Error(
			"Benchmark result download sink does not match the requested invocation",
		);
	}
	if (["opfs", "node-file"].includes(invocation.downloadSink)) {
		const downloadedSha256 = requirePattern(
			integrity.downloadedSha256Base64,
			SHA256_BASE64_PATTERN,
			"integrity.downloadedSha256Base64",
		);
		if (
			downloadedSha256 !== sourceSha256 ||
			integrity.persistedSinkSha256Verified !== true ||
			integrity.sinkPersistence !== invocation.downloadSink ||
			integrity.sinkPersistenceVerified !== true
		) {
			throw new Error(
				`Benchmark result failed its persisted ${invocation.downloadSink} SHA-256 integrity gate`,
			);
		}
	} else {
		if (
			integrity.downloadedSha256Base64 !== null ||
			integrity.persistedSinkSha256Verified !== null
		) {
			throw new Error(
				"Hash-only benchmark sink must not claim persisted SHA-256 evidence",
			);
		}
		if (
			invocation.downloadSink !== "hash-only" ||
			integrity.sinkPersistence !== "none" ||
			integrity.sinkPersistenceVerified !== null
		) {
			throw new Error(
				"Hash-only benchmark sink must not claim file persistence",
			);
		}
	}

	const sourceCrc32 = requirePattern(
		integrity.sourceCrc32Hex,
		CRC32_HEX_PATTERN,
		"integrity.sourceCrc32Hex",
	);
	const downloadedCrc32 = requirePattern(
		integrity.downloadedCrc32Hex,
		CRC32_HEX_PATTERN,
		"integrity.downloadedCrc32Hex",
	);
	if (integrity.crc32Verified !== true || sourceCrc32 !== downloadedCrc32) {
		throw new Error("Benchmark result failed its CRC-32 integrity gate");
	}
	if (integrity.verified !== true || result.integrityVerified !== true) {
		throw new Error("Benchmark result is missing the aggregate integrity gate");
	}
	const integrityVerifiedAt = requirePositiveSafeInteger(
		result.integrityVerifiedAt,
		"integrityVerifiedAt",
	);
	return integrityVerifiedAt;
};

const validateFailedUploadIntegrityEvidence = (
	result,
	expectedFileMb,
	expectedInvocation,
) => {
	const hasRunnerProjection =
		Object.hasOwn(result, "browserResult") ||
		Object.hasOwn(result, "resultEvidence");
	if (hasRunnerProjection) {
		const hasExplicitResultEvidence = Object.hasOwn(result, "resultEvidence");
		const hasParsedResultEvidence =
			isRecord(result.resultEvidence) &&
			result.resultEvidence.kind === "parsed";
		if (hasExplicitResultEvidence && !hasParsedResultEvidence) {
			if (
				result.browserResult !== null ||
				result.integrity !== null ||
				result.integrityVerified !== false ||
				result.integrityVerifiedAt !== null
			) {
				throw new Error(
					"Runner failure without parsed browser evidence must use a null integrity projection",
				);
			}
		} else if (isRecord(result.browserResult)) {
			const projected = projectUploadIntegrityEvidence(result.browserResult, {
				fileMb: expectedFileMb,
				fixtureSeed: expectedInvocation.fixtureSeed,
				downloadSink: expectedInvocation.downloadSink,
			});
			if (
				!isDeepStrictEqual(result.integrity, projected.integrity) ||
				result.integrityVerified !== projected.integrityVerified ||
				result.integrityVerifiedAt !== projected.integrityVerifiedAt
			) {
				throw new Error(
					"Runner failure integrity projection does not exactly match its parsed browser result",
				);
			}
		} else if (
			hasParsedResultEvidence ||
			result.browserResult !== null ||
			result.integrity !== null ||
			result.integrityVerified !== false ||
			result.integrityVerifiedAt !== null
		) {
			throw new Error(
				"Runner failure without parsed browser evidence must use a null integrity projection",
			);
		}
	}

	if (typeof result.integrityVerified !== "boolean") {
		throw new Error("Failed upload has an invalid integrityVerified claim");
	}
	if (result.integrityVerified) {
		validateUploadIntegrity(
			result,
			expectedFileMb,
			expectedInvocation.fixtureSeed,
			expectedInvocation,
			{ requireCompletedEvidence: false },
		);
		return;
	}
	if (result.integrityVerifiedAt !== null) {
		throw new Error(
			"Failed upload without verified integrity must use a null integrityVerifiedAt",
		);
	}
	if (result.integrity !== null) {
		const integrity = requireRecord(result.integrity, "integrity");
		if (integrity.verified !== false) {
			throw new Error(
				"Failed upload integrity evidence contradicts its unverified claim",
			);
		}
	}
};

const requireExactDiagnosticSeries = (
	diagnostics,
	name,
	indices,
	requireValue = requireNonNegativeNumber,
) => {
	const record = requireRecord(diagnostics[name], `readerDiagnostics.${name}`);
	const expectedKeys = indices.map(String);
	const expectedKeySet = new Set(expectedKeys);
	const actualKeys = Object.keys(record);
	if (
		actualKeys.length !== expectedKeys.length ||
		expectedKeys.some((key) => !Object.hasOwn(record, key)) ||
		actualKeys.some((key) => !expectedKeySet.has(key))
	) {
		throw new Error(
			`Benchmark readerDiagnostics.${name} must use the exact canonical chunk keys`,
		);
	}
	return indices.map((index) =>
		requireValue(record[index], `readerDiagnostics.${name}[${index}]`),
	);
};

const nearestRank = (values, percentile) => {
	const sorted = [...values].toSorted((left, right) => left - right);
	return sorted[Math.ceil((percentile / 100) * sorted.length) - 1];
};

const sum = (values) => values.reduce((total, value) => total + value, 0);

const requireBoundedDiagnosticIntervals = ({
	diagnostics,
	indices,
	startedName,
	finishedName,
	readStartedAt,
	readFinishedAt,
	ordered = false,
}) => {
	const started = requireExactDiagnosticSeries(
		diagnostics,
		startedName,
		indices,
		requireNonNegativeSafeInteger,
	);
	const finished = requireExactDiagnosticSeries(
		diagnostics,
		finishedName,
		indices,
		requireNonNegativeSafeInteger,
	);
	const durations = [];
	for (const [offset, index] of indices.entries()) {
		if (
			started[offset] < readStartedAt ||
			finished[offset] > readFinishedAt ||
			finished[offset] < started[offset]
		) {
			throw new Error(
				`Benchmark ${startedName}/${finishedName} interval for chunk ${index} is outside the library read window`,
			);
		}
		if (ordered && offset > 0 && started[offset] < finished[offset - 1]) {
			throw new Error(
				"Benchmark chunk sink-write intervals must be ordered and non-overlapping",
			);
		}
		durations.push(finished[offset] - started[offset]);
	}
	return { started, finished, durations };
};

const buildReceiverProgressMilestones = ({
	chunkBytes,
	confirmedAt,
	readStartedAt,
	readFinishedAt,
	label,
}) => {
	if (confirmedAt.length !== chunkBytes.length) {
		throw new Error(`Benchmark ${label} must cover every read chunk`);
	}
	const prefixBytes = [];
	const prefixConfirmedAt = [];
	let cumulativeBytes = 0;
	let cumulativeConfirmedAt = readStartedAt;
	for (const [index, bytes] of chunkBytes.entries()) {
		const timestamp = requireNonNegativeSafeInteger(
			confirmedAt[index],
			`${label}[${index}]`,
		);
		if (timestamp < readStartedAt || timestamp > readFinishedAt) {
			throw new Error(
				`Benchmark ${label}[${index}] is outside the canonical library read window`,
			);
		}
		cumulativeBytes += bytes;
		cumulativeConfirmedAt = Math.max(cumulativeConfirmedAt, timestamp);
		prefixBytes.push(cumulativeBytes);
		prefixConfirmedAt.push(cumulativeConfirmedAt);
	}
	const totalBytes = sum(chunkBytes);
	return RECEIVER_PROGRESS_PERCENTAGES.map((percent) => {
		if (percent === 0) {
			return {
				percent,
				targetBytes: 0,
				contiguousBytes: 0,
				chunkIndex: null,
				confirmedAt: readStartedAt,
				elapsedMs: 0,
			};
		}
		const targetBytes = Math.ceil((totalBytes * percent) / 100);
		const chunkIndex = prefixBytes.findIndex((bytes) => bytes >= targetBytes);
		if (chunkIndex < 0) {
			throw new Error(`Benchmark ${label} did not reach ${percent}%`);
		}
		const milestone = {
			percent,
			targetBytes,
			contiguousBytes: prefixBytes[chunkIndex],
			chunkIndex,
			confirmedAt: prefixConfirmedAt[chunkIndex],
			elapsedMs: prefixConfirmedAt[chunkIndex] - readStartedAt,
		};
		requireExactRecordKeys(
			milestone,
			RECEIVER_PROGRESS_MILESTONE_KEYS,
			`${label} ${percent}% milestone`,
		);
		return milestone;
	});
};

const validateReadTransferEvidence = (
	result,
	invocation,
	{ downloadStartedAt, downloadFinishedAt, downloadCompletionObservedAt },
) => {
	const readerDiagnostics = requireRecord(
		result.readerDiagnostics,
		"readerDiagnostics",
	);
	const diagnostics = requireRecord(
		readerDiagnostics.lastReadDiagnostics,
		"readerDiagnostics.lastReadDiagnostics",
	);
	const resolved = requireRecord(
		diagnostics.chunkResolved,
		"readerDiagnostics.chunkResolved",
	);
	const resolvedKeys = Object.keys(resolved);
	const indices = resolvedKeys
		.map((key) => Number(key))
		.toSorted((left, right) => left - right);
	if (
		indices.length < 2 ||
		resolvedKeys.some((key) => !/^(?:0|[1-9]\d*)$/.test(key)) ||
		indices.some(
			(index, offset) => !Number.isSafeInteger(index) || index !== offset,
		)
	) {
		throw new Error(
			"Benchmark large-file read diagnostics must contain at least two contiguous canonical chunk indices",
		);
	}
	const startedAt = requirePositiveSafeInteger(
		diagnostics.startedAt,
		"readerDiagnostics.startedAt",
	);
	const finishedAt = requirePositiveSafeInteger(
		diagnostics.finishedAt,
		"readerDiagnostics.finishedAt",
	);
	if (
		startedAt < downloadStartedAt ||
		finishedAt < startedAt ||
		finishedAt > downloadFinishedAt
	) {
		throw new Error(
			"Benchmark library read window is outside the clicked download window",
		);
	}
	const expectedFileName = `file-share-benchmark-${result.mode}-${result.runNonce}.bin`;
	const writerManifest = requireRecord(
		result.writerManifestEvidence,
		"writerManifestEvidence",
	);
	const readerManifest = requireRecord(
		result.readerManifestEvidence,
		"readerManifestEvidence",
	);
	const writerFileId = requireString(
		writerManifest.fileId,
		"writerManifestEvidence.fileId",
	);
	const readerFileId = requireString(
		readerManifest.fileId,
		"readerManifestEvidence.fileId",
	);
	if (
		result.fileName !== expectedFileName ||
		diagnostics.fileName !== expectedFileName ||
		writerManifest.fileName !== expectedFileName ||
		readerManifest.fileName !== expectedFileName ||
		writerFileId !== readerFileId ||
		diagnostics.fileId !== readerFileId
	) {
		throw new Error(
			"Benchmark library read identity does not match the clicked file manifest",
		);
	}
	requireString(diagnostics.transferId, "readerDiagnostics.transferId");

	const chunkBytes = requireExactDiagnosticSeries(
		diagnostics,
		"chunkByteLength",
		indices,
		requireNonNegativeSafeInteger,
	);
	if (chunkBytes.some((value) => value <= 0)) {
		throw new Error(
			"Benchmark read diagnostics contain invalid chunk byte lengths",
		);
	}
	const totalBytes = chunkBytes.reduce((sum, value) => sum + value, 0);
	if (totalBytes !== invocation.fileSizeBytes) {
		throw new Error(
			"Benchmark read diagnostics do not cover the requested file size",
		);
	}
	const sourceSummary = {};
	for (const [offset, index] of indices.entries()) {
		const source = requireString(
			resolved[index],
			`readerDiagnostics.chunkResolved[${index}]`,
		);
		const current = (sourceSummary[source] ??= { chunkCount: 0, bytes: 0 });
		current.chunkCount += 1;
		current.bytes += chunkBytes[offset];
	}
	const demandWaitMs = requireExactDiagnosticSeries(
		diagnostics,
		"chunkDemandWaitMs",
		indices,
		requireNonNegativeSafeInteger,
	);
	const resolveIntervals = requireBoundedDiagnosticIntervals({
		diagnostics,
		indices,
		startedName: "chunkResolveStartedAt",
		finishedName: "chunkResolveFinishedAt",
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
	});
	const writeIntervals = requireBoundedDiagnosticIntervals({
		diagnostics,
		indices,
		startedName: "chunkWriteStartedAt",
		finishedName: "chunkWriteFinishedAt",
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
		ordered: true,
	});
	const materializeIntervals = requireBoundedDiagnosticIntervals({
		diagnostics,
		indices,
		startedName: "chunkMaterializeStartedAt",
		finishedName: "chunkMaterializeFinishedAt",
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
	});
	const hashIntervals = requireBoundedDiagnosticIntervals({
		diagnostics,
		indices,
		startedName: "chunkHashStartedAt",
		finishedName: "chunkHashFinishedAt",
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
	});
	for (const [offset, index] of indices.entries()) {
		if (
			resolveIntervals.finished[offset] >
				materializeIntervals.started[offset] ||
			materializeIntervals.finished[offset] > hashIntervals.started[offset] ||
			hashIntervals.finished[offset] > writeIntervals.started[offset] ||
			(offset > 0 &&
				materializeIntervals.started[offset] <
					writeIntervals.finished[offset - 1])
		) {
			throw new Error(
				`Benchmark chunk ${index} resolve/materialize/hash/write lifecycle is not causal`,
			);
		}
	}
	const libraryStreamWallMs = finishedAt - startedAt;
	const sinkWriteAwaitMs = sum(writeIntervals.durations);
	const sinkAwaitSubtractedDiagnosticMs =
		calculateSinkAwaitSubtractedDiagnosticMs({
			libraryStreamWallMs,
			sinkWriteAwaitMs,
		});
	const demandWaitSumMs = sum(demandWaitMs);
	const materializeSumMs = sum(materializeIntervals.durations);
	const contentHashSumMs = sum(hashIntervals.durations);
	const sortedSourceSummary = Object.fromEntries(
		Object.entries(sourceSummary).sort(([left], [right]) =>
			left.localeCompare(right),
		),
	);
	if (typeof diagnostics.persistChunkReads !== "boolean") {
		throw new Error(
			"Benchmark readerDiagnostics.persistChunkReads must be a boolean",
		);
	}
	const availableMilestones = buildReceiverProgressMilestones({
		chunkBytes,
		confirmedAt: materializeIntervals.finished,
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
		label: "readerDiagnostics.chunkMaterializeFinishedAt",
	});
	const sinkAcceptedMilestones = buildReceiverProgressMilestones({
		chunkBytes,
		confirmedAt: writeIntervals.finished,
		readStartedAt: startedAt,
		readFinishedAt: finishedAt,
		label: "readerDiagnostics.chunkWriteFinishedAt",
	});
	let peerbitDurableMilestones = null;
	const peerbitDurableSourceCounts = {};
	if (diagnostics.persistChunkReads) {
		const peerbitDurableAt = requireExactDiagnosticSeries(
			diagnostics,
			"chunkPersistenceConfirmedAt",
			indices,
			requireNonNegativeSafeInteger,
		);
		const persistenceSources = requireRecord(
			diagnostics.chunkPersistenceConfirmationSource,
			"readerDiagnostics.chunkPersistenceConfirmationSource",
		);
		requireExactRecordKeys(
			persistenceSources,
			indices.map(String),
			"readerDiagnostics.chunkPersistenceConfirmationSource",
		);
		for (const index of indices) {
			const source = requireString(
				persistenceSources[index],
				`readerDiagnostics.chunkPersistenceConfirmationSource[${index}]`,
			);
			if (!PERSISTENCE_CONFIRMATION_SOURCES.has(source)) {
				throw new Error(
					`Benchmark readerDiagnostics.chunkPersistenceConfirmationSource[${index}] is not a recognized persistence source`,
				);
			}
			peerbitDurableSourceCounts[source] =
				(peerbitDurableSourceCounts[source] ?? 0) + 1;
		}
		peerbitDurableMilestones = buildReceiverProgressMilestones({
			chunkBytes,
			confirmedAt: peerbitDurableAt,
			readStartedAt: startedAt,
			readFinishedAt: finishedAt,
			label: "readerDiagnostics.chunkPersistenceConfirmedAt",
		});
	} else {
		for (const name of [
			"chunkPersistenceConfirmedAt",
			"chunkPersistenceConfirmationSource",
		]) {
			requireExactRecordKeys(
				requireRecord(diagnostics[name], `readerDiagnostics.${name}`),
				[],
				`readerDiagnostics.${name}`,
			);
		}
	}
	const expectedReadTransfer = {
		chunkCount: indices.length,
		totalBytes,
		sources: sortedSourceSummary,
		demandWait: {
			definition: DEMAND_WAIT_DEFINITION,
			sampleCount: demandWaitMs.length,
			sumMs: demandWaitSumMs,
			p50Ms: nearestRank(demandWaitMs, 50),
			p95Ms: nearestRank(demandWaitMs, 95),
			p99Ms: nearestRank(demandWaitMs, 99),
			maxMs: Math.max(...demandWaitMs),
			over1sCount: demandWaitMs.filter((value) => value > 1_000).length,
			over5sCount: demandWaitMs.filter((value) => value > 5_000).length,
			over10sCount: demandWaitMs.filter((value) => value > 10_000).length,
		},
		stages: {
			libraryStreamWallMs,
			sinkWriteAwaitMs,
			sinkAwaitSubtractedDiagnosticMs,
			demandWaitMs: demandWaitSumMs,
			materializeMs: materializeSumMs,
			contentHashMs: contentHashSumMs,
			otherStreamReadMs: Math.max(
				0,
				sinkAwaitSubtractedDiagnosticMs -
					demandWaitSumMs -
					materializeSumMs -
					contentHashSumMs,
			),
		},
		receiverProgress: {
			percentages: [...RECEIVER_PROGRESS_PERCENTAGES],
			available: {
				definition:
					"contiguous file-prefix bytes materialized and available to the receiver library",
				source: "chunkMaterializeFinishedAt",
				milestones: availableMilestones,
			},
			peerbitDurable: {
				definition:
					"contiguous file-prefix bytes whose exact signed manifest-entry blocks were confirmed in the receiver's local Peerbit block store",
				source: "chunkPersistenceConfirmedAt",
				claimed: diagnostics.persistChunkReads,
				sourceCounts: Object.fromEntries(
					Object.entries(peerbitDurableSourceCounts).sort(([left], [right]) =>
						left.localeCompare(right),
					),
				),
				milestones: peerbitDurableMilestones,
			},
			sinkAccepted: {
				definition:
					"contiguous file-prefix bytes accepted by the configured benchmark sink; this is not a Peerbit or filesystem durability claim",
				source: "chunkWriteFinishedAt",
				sink: invocation.downloadSink,
				durable: false,
				milestones: sinkAcceptedMilestones,
			},
		},
	};
	if (
		!isDeepStrictEqual(result.readTransfer, expectedReadTransfer) ||
		result.libraryStreamWallMs !== libraryStreamWallMs ||
		result.sinkWriteAwaitMs !== sinkWriteAwaitMs ||
		result.sinkAwaitSubtractedDiagnosticMs !== sinkAwaitSubtractedDiagnosticMs
	) {
		throw new Error(
			"Benchmark result read-transfer timing decomposition is inconsistent",
		);
	}
	const computedFinalHash = requirePattern(
		diagnostics.computedFinalHash,
		SHA256_BASE64_PATTERN,
		"readerDiagnostics.computedFinalHash",
	);
	if (computedFinalHash !== result.integrity.libraryComputedSha256Base64) {
		throw new Error(
			"Benchmark raw reader SHA-256 contradicts its integrity evidence",
		);
	}
	if (
		result.libraryStreamWallDefinition !== LIBRARY_STREAM_WALL_DEFINITION ||
		result.sinkWriteAwaitDefinition !== SINK_WRITE_AWAIT_DEFINITION ||
		result.sinkAwaitSubtractedDiagnosticDefinition !==
			SINK_AWAIT_SUBTRACTED_DIAGNOSTIC_DEFINITION ||
		result.primaryDownloadMetric !== PRIMARY_DOWNLOAD_METRIC ||
		result.primaryDownloadAuthoritative !==
			(invocation.downloadSink === "hash-only") ||
		result.primaryDownloadMetricDefinition !==
			PRIMARY_DOWNLOAD_METRIC_DEFINITION
	) {
		throw new Error(
			"Benchmark result read-transfer timing definitions are invalid",
		);
	}
	const sinkWriteCalls = requireNonNegativeSafeInteger(
		result.sinkWriteCalls,
		"sinkWriteCalls",
	);
	if (sinkWriteCalls !== indices.length || sinkWriteCalls === 0) {
		throw new Error(
			"Benchmark sink write count does not match the read chunk count",
		);
	}
	if (result.sinkWriteDurationDefinition !== SINK_WRITE_DURATION_DEFINITION) {
		throw new Error("Benchmark sink-write duration definition is invalid");
	}
	const sinkWriteDurationMs = requireNonNegativeNumber(
		result.sinkWriteDurationMs,
		"sinkWriteDurationMs",
	);
	if (
		sinkWriteDurationMs >
		sinkWriteAwaitMs +
			indices.length * SINK_WRITE_QUANTIZATION_ALLOWANCE_MS_PER_CHUNK
	) {
		throw new Error(
			"Benchmark sink-write helper duration exceeds canonical read evidence plus its per-chunk clock allowance",
		);
	}
	if (invocation.downloadSink === "node-file") {
		if (
			requireNonNegativeSafeInteger(
				result.sinkServerWriteCalls,
				"sinkServerWriteCalls",
			) !== sinkWriteCalls ||
			result.sinkServerWriteDurationDefinition !==
				SINK_SERVER_WRITE_DURATION_DEFINITION
		) {
			throw new Error("Benchmark Node-file server write evidence is invalid");
		}
		const sinkServerWriteDurationMs = requireNonNegativeNumber(
			result.sinkServerWriteDurationMs,
			"sinkServerWriteDurationMs",
		);
		if (
			sinkServerWriteDurationMs >
			sinkWriteDurationMs +
				indices.length * SINK_SERVER_CLOCK_TOLERANCE_MS_PER_CHUNK
		) {
			throw new Error(
				"Benchmark Node-file server duration exceeds its browser sink duration plus the bounded clock tolerance",
			);
		}
	} else if (
		result.sinkServerWriteCalls !== null ||
		result.sinkServerWriteDurationMs !== null ||
		result.sinkServerWriteDurationDefinition !== null
	) {
		throw new Error(
			"Non-Node benchmark sink contains Node-only server timing evidence",
		);
	}
	return {
		startedAt,
		finishedAt,
		downloadStartedAt,
		downloadFinishedAt,
		downloadCompletionObservedAt,
	};
};

const validateMemorySamplingErrors = (series, label) => {
	if (
		!Array.isArray(series.samplingErrors) ||
		series.samplingErrors.length > DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS ||
		series.samplingErrors.some(
			(message) =>
				typeof message !== "string" ||
				message.length === 0 ||
				message.length > DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
		) ||
		!Number.isSafeInteger(series.samplingErrorOverflowCount) ||
		series.samplingErrorOverflowCount < 0
	) {
		throw new Error(`Benchmark ${label} contains unbounded sampling errors`);
	}
	if (
		series.samplingErrors.length !== 0 ||
		series.samplingErrorOverflowCount !== 0
	) {
		throw new Error(`Benchmark ${label} contains memory sampling errors`);
	}
	if (
		!Array.isArray(series.cleanupWarnings) ||
		series.cleanupWarnings.length > DOWNLOAD_MEMORY_MAX_CLEANUP_WARNINGS ||
		series.cleanupWarnings.some(
			(message) =>
				typeof message !== "string" ||
				message.length === 0 ||
				message.length > DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH ||
				!/^cleanup-timeout: .+ exceeded [1-9]\d*ms(?:; .+ exceeded [1-9]\d*ms)*$/.test(
					message,
				),
		) ||
		!Number.isSafeInteger(series.cleanupWarningOverflowCount) ||
		series.cleanupWarningOverflowCount < 0
	) {
		throw new Error(`Benchmark ${label} contains invalid cleanup warnings`);
	}
};

const validateMemorySeriesWindow = (
	series,
	label,
	maxSamples,
	{
		startedAt: readStartedAt,
		finishedAt: readFinishedAt,
		downloadStartedAt,
		downloadFinishedAt,
		downloadCompletionObservedAt,
		postTransferSoakStartedAt,
		postTransferSoakFinishedAt,
		afterSoakStartedAt,
		shutdownStartedAt,
		shutdownFinishedAt,
		liveSampleMaxGapMs,
	},
) => {
	const startedAt = requirePositiveSafeInteger(
		series.startedAt,
		`${label}.startedAt`,
	);
	const finishedAt = requirePositiveSafeInteger(
		series.finishedAt,
		`${label}.finishedAt`,
	);
	if (
		series.sampleIntervalMs !== DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS ||
		!Array.isArray(series.samples) ||
		series.sampleCount !== series.samples.length ||
		!Number.isSafeInteger(series.sampleCount) ||
		series.sampleCount < DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE ||
		series.sampleCount > maxSamples ||
		series.maxSamples !== maxSamples ||
		series.periodicSampleLimit !==
			maxSamples - DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE ||
		!Number.isSafeInteger(series.periodicSampleCount) ||
		series.periodicSampleCount < 0 ||
		series.periodicSampleCount > series.periodicSampleLimit ||
		series.sampleCount !==
			series.periodicSampleCount + DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE ||
		series.capacityExhaustedBeforeTerminal !== false ||
		series.samplingCapacitySufficient !== true ||
		series.manualSampleCount !== 1 ||
		series.terminalSampleAttempted !== true ||
		series.terminalSampleCaptured !== true ||
		finishedAt < startedAt
	) {
		throw new Error(`Benchmark ${label} has an invalid bounded sample series`);
	}
	let previousCapturedAt = null;
	let initialSampleCount = 0;
	let periodicSampleCount = 0;
	let manualSample = null;
	let terminalSample = null;
	for (const [index, sampleValue] of series.samples.entries()) {
		const sample = requireRecord(sampleValue, `${label}.samples[${index}]`);
		const capturedAt = requirePositiveSafeInteger(
			sample.capturedAt,
			`${label}.samples[${index}].capturedAt`,
		);
		if (
			capturedAt < startedAt ||
			capturedAt > finishedAt ||
			(previousCapturedAt !== null && capturedAt < previousCapturedAt)
		) {
			throw new Error(
				`Benchmark ${label} sample timestamps are outside or reorder the sampler window`,
			);
		}
		if (sample.sampleKind === "initial") {
			initialSampleCount += 1;
		} else if (sample.sampleKind === "periodic") {
			periodicSampleCount += 1;
		} else if (sample.sampleKind === "manual") {
			if (manualSample !== null) {
				throw new Error(`Benchmark ${label} contains duplicate manual samples`);
			}
			manualSample = { index, capturedAt };
		} else if (sample.sampleKind === "terminal") {
			if (terminalSample !== null) {
				throw new Error(
					`Benchmark ${label} contains duplicate terminal samples`,
				);
			}
			terminalSample = { index, capturedAt };
		} else {
			throw new Error(`Benchmark ${label} contains an invalid sample kind`);
		}
		previousCapturedAt = capturedAt;
	}
	const lastManualSampleAt = requirePositiveSafeInteger(
		series.lastManualSampleAt,
		`${label}.lastManualSampleAt`,
	);
	const terminalSampleAt = requirePositiveSafeInteger(
		series.terminalSampleAt,
		`${label}.terminalSampleAt`,
	);
	if (
		initialSampleCount !== 1 ||
		series.samples[0].sampleKind !== "initial" ||
		periodicSampleCount !== series.periodicSampleCount ||
		manualSample === null ||
		manualSample.index === 0 ||
		manualSample.index === series.samples.length - 1 ||
		manualSample.capturedAt !== lastManualSampleAt ||
		manualSample.capturedAt < postTransferSoakFinishedAt ||
		manualSample.capturedAt > afterSoakStartedAt ||
		manualSample.capturedAt - postTransferSoakFinishedAt >
			liveSampleMaxGapMs - DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS ||
		manualSample.capturedAt > shutdownStartedAt ||
		terminalSample === null ||
		terminalSample.index !== series.samples.length - 1 ||
		series.samples.at(-1).sampleKind !== "terminal" ||
		terminalSample.capturedAt !== terminalSampleAt ||
		terminalSample.capturedAt < shutdownFinishedAt ||
		manualSample.index >= terminalSample.index
	) {
		throw new Error(
			`Benchmark ${label} has invalid live or terminal checkpoint ordering`,
		);
	}
	const firstCapturedAt = series.samples[0].capturedAt;
	const lastCapturedAt = series.samples.at(-1).capturedAt;
	if (
		startedAt > downloadStartedAt ||
		firstCapturedAt > downloadStartedAt ||
		lastCapturedAt < downloadCompletionObservedAt ||
		finishedAt < downloadCompletionObservedAt ||
		lastCapturedAt < shutdownFinishedAt ||
		finishedAt < shutdownFinishedAt
	) {
		throw new Error(
			`Benchmark ${label} does not bracket the click-to-post-shutdown observation window`,
		);
	}
	if (
		startedAt < downloadStartedAt - DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS ||
		firstCapturedAt < downloadStartedAt - DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS
	) {
		throw new Error(
			`Benchmark ${label} begins before the bounded telemetry setup window`,
		);
	}
	if (
		lastCapturedAt >
			shutdownFinishedAt + DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS ||
		finishedAt > shutdownFinishedAt + DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS
	) {
		throw new Error(
			`Benchmark ${label} ends after the bounded telemetry cleanup window`,
		);
	}
	if (lastCapturedAt < downloadFinishedAt) {
		throw new Error(
			`Benchmark ${label} does not cover the selected sink download window`,
		);
	}
	if (firstCapturedAt > readStartedAt || lastCapturedAt < readFinishedAt) {
		throw new Error(
			`Benchmark ${label} does not cover the canonical library read window`,
		);
	}
	validateMemorySamplingErrors(series, label);
	for (const [phase, phaseStartedAt, phaseFinishedAt] of [
		["transfer", downloadStartedAt, downloadFinishedAt],
		["soak", postTransferSoakStartedAt, postTransferSoakFinishedAt],
	]) {
		assertDownloadMemoryLiveSampleCoverage({
			samples: series.samples,
			phaseStartedAt,
			phaseFinishedAt,
			maxGapMs: liveSampleMaxGapMs,
			label: `${label}.${phase}`,
		});
	}
	return { startedAt, finishedAt };
};

const validateHeapMemorySeries = (
	seriesValue,
	label,
	expectedScope,
	maxSamples,
	readWindow,
) => {
	const series = requireRecord(seriesValue, label);
	requireExactRecordKeys(
		series,
		[
			"memoryKind",
			"scope",
			"metric",
			"unit",
			"sampleIntervalMs",
			"startedAt",
			"finishedAt",
			"sampleCount",
			"startBytes",
			"endBytes",
			"peakBytes",
			"startUsedBytes",
			"endUsedBytes",
			"peakUsedBytes",
			"startTotalBytes",
			"endTotalBytes",
			"peakTotalBytes",
			"startEmbedderHeapUsedBytes",
			"endEmbedderHeapUsedBytes",
			"peakEmbedderHeapUsedBytes",
			"startBackingStorageBytes",
			"endBackingStorageBytes",
			"peakBackingStorageBytes",
			"samples",
			"samplingErrors",
			"samplingErrorOverflowCount",
			"cleanupWarnings",
			"cleanupWarningOverflowCount",
			"maxSamples",
			"periodicSampleLimit",
			"periodicSampleCount",
			"capacityExhaustedBeforeTerminal",
			"samplingCapacitySufficient",
			"manualSampleCount",
			"lastManualSampleAt",
			"terminalSampleAttempted",
			"terminalSampleCaptured",
			"terminalSampleAt",
		],
		label,
	);
	if (
		series.memoryKind !== "runtime-heap" ||
		series.scope !== expectedScope ||
		series.metric !== "Runtime.getHeapUsage" ||
		series.unit !== "bytes"
	) {
		throw new Error(`Benchmark ${label} has invalid heap attribution`);
	}
	const window = validateMemorySeriesWindow(
		series,
		label,
		maxSamples,
		readWindow,
	);
	const samples = series.samples.map((sample, index) => {
		requireExactRecordKeys(
			sample,
			[
				"capturedAt",
				"sampleKind",
				"usedBytes",
				"totalBytes",
				"embedderHeapUsedBytes",
				"backingStorageBytes",
			],
			`${label}.samples[${index}]`,
		);
		const sampleLabel = `${label}.samples[${index}]`;
		const values = {
			usedBytes: requireNonNegativeSafeInteger(
				sample.usedBytes,
				`${sampleLabel}.usedBytes`,
			),
			totalBytes: requireNonNegativeSafeInteger(
				sample.totalBytes,
				`${sampleLabel}.totalBytes`,
			),
			embedderHeapUsedBytes: requireNonNegativeSafeInteger(
				sample.embedderHeapUsedBytes,
				`${sampleLabel}.embedderHeapUsedBytes`,
			),
			backingStorageBytes: requireNonNegativeSafeInteger(
				sample.backingStorageBytes,
				`${sampleLabel}.backingStorageBytes`,
			),
		};
		if (values.usedBytes > values.totalBytes) {
			throw new Error(`Benchmark ${sampleLabel} heap totals are inconsistent`);
		}
		return values;
	});
	const first = samples[0];
	const last = samples.at(-1);
	const peak = (name) => Math.max(...samples.map((sample) => sample[name]));
	if (
		series.startBytes !== first.usedBytes ||
		series.endBytes !== last.usedBytes ||
		series.peakBytes !== peak("usedBytes") ||
		series.startUsedBytes !== first.usedBytes ||
		series.endUsedBytes !== last.usedBytes ||
		series.peakUsedBytes !== peak("usedBytes") ||
		series.startTotalBytes !== first.totalBytes ||
		series.endTotalBytes !== last.totalBytes ||
		series.peakTotalBytes !== peak("totalBytes") ||
		series.startEmbedderHeapUsedBytes !== first.embedderHeapUsedBytes ||
		series.endEmbedderHeapUsedBytes !== last.embedderHeapUsedBytes ||
		series.peakEmbedderHeapUsedBytes !== peak("embedderHeapUsedBytes") ||
		series.startBackingStorageBytes !== first.backingStorageBytes ||
		series.endBackingStorageBytes !== last.backingStorageBytes ||
		series.peakBackingStorageBytes !== peak("backingStorageBytes")
	) {
		throw new Error(`Benchmark ${label} heap summaries are inconsistent`);
	}
	return window;
};

const validateBrowserRoleBytes = (value, label) => {
	const roles = requireRecord(value, label);
	const entries = Object.entries(roles);
	if (
		entries.length === 0 ||
		entries.length > DOWNLOAD_MEMORY_MAX_BROWSER_ROLES
	) {
		throw new Error(`Benchmark ${label} has an invalid browser-role count`);
	}
	let total = 0;
	for (const [role, bytes] of entries) {
		if (
			role.length === 0 ||
			role.length > DOWNLOAD_MEMORY_MAX_BROWSER_ROLE_NAME_LENGTH ||
			!/^[\x20-\x7e]+$/.test(role)
		) {
			throw new Error(`Benchmark ${label} has an invalid browser-role name`);
		}
		total += requirePositiveSafeInteger(bytes, `${label}.${role}`);
	}
	return { roles, total };
};

const validateHostRssSeries = (seriesValue, maxSamples, readWindow) => {
	const label = "downloadMemoryTelemetry.hostRss";
	const series = requireRecord(seriesValue, label);
	requireExactRecordKeys(
		series,
		[
			"memoryKind",
			"scope",
			"nodeScope",
			"metric",
			"attribution",
			"attributionLimitations",
			"unit",
			"browserInstanceCount",
			"browserSessionCount",
			"sampleIntervalMs",
			"startedAt",
			"finishedAt",
			"sampleCount",
			"startBrowserBytes",
			"endBrowserBytes",
			"peakBrowserBytes",
			"startNodeBytes",
			"endNodeBytes",
			"peakNodeBytes",
			"startNodeExternalBytes",
			"endNodeExternalBytes",
			"peakNodeExternalBytes",
			"startNodeArrayBuffersBytes",
			"endNodeArrayBuffersBytes",
			"peakNodeArrayBuffersBytes",
			"startCombinedBytes",
			"endCombinedBytes",
			"peakCombinedBytes",
			"startBrowserProcessCount",
			"endBrowserProcessCount",
			"peakBrowserProcessCount",
			"startBrowserRoleBytes",
			"endBrowserRoleBytes",
			"peakBrowserRoleBytes",
			"samples",
			"samplingErrors",
			"samplingErrorOverflowCount",
			"cleanupWarnings",
			"cleanupWarningOverflowCount",
			"maxSamples",
			"periodicSampleLimit",
			"periodicSampleCount",
			"capacityExhaustedBeforeTerminal",
			"samplingCapacitySufficient",
			"manualSampleCount",
			"lastManualSampleAt",
			"terminalSampleAttempted",
			"terminalSampleCaptured",
			"terminalSampleAt",
		],
		label,
	);
	if (
		series.memoryKind !== "resident-set-size" ||
		series.scope !== DOWNLOAD_MEMORY_HOST_SCOPE ||
		series.nodeScope !== DOWNLOAD_MEMORY_NODE_SCOPE ||
		series.metric !== "RSS" ||
		series.attribution !== DOWNLOAD_MEMORY_HOST_ATTRIBUTION ||
		series.attributionLimitations !==
			DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS ||
		series.unit !== "bytes" ||
		series.browserInstanceCount !== 2 ||
		series.browserSessionCount !== 2
	) {
		throw new Error("Benchmark host RSS attribution contract is invalid");
	}
	const window = validateMemorySeriesWindow(
		series,
		label,
		maxSamples,
		readWindow,
	);
	const peakBrowserRoleBytes = {};
	const samples = series.samples.map((sample, index) => {
		const sampleLabel = `${label}.samples[${index}]`;
		requireExactRecordKeys(
			sample,
			[
				"capturedAt",
				"sampleKind",
				"browserInstanceCount",
				"browserRootProcessCount",
				"browserBytes",
				"nodeBytes",
				"nodeExternalBytes",
				"nodeArrayBuffersBytes",
				"combinedBytes",
				"browserProcessCount",
				"browserRoleBytes",
			],
			sampleLabel,
		);
		if (
			sample.browserInstanceCount !== 2 ||
			sample.browserRootProcessCount !== 2
		) {
			throw new Error(
				`Benchmark ${sampleLabel} does not prove both browser instances and root processes were sampled`,
			);
		}
		const browserBytes = requirePositiveSafeInteger(
			sample.browserBytes,
			`${sampleLabel}.browserBytes`,
		);
		const nodeBytes = requirePositiveSafeInteger(
			sample.nodeBytes,
			`${sampleLabel}.nodeBytes`,
		);
		const nodeExternalBytes = requireNonNegativeSafeInteger(
			sample.nodeExternalBytes,
			`${sampleLabel}.nodeExternalBytes`,
		);
		const nodeArrayBuffersBytes = requireNonNegativeSafeInteger(
			sample.nodeArrayBuffersBytes,
			`${sampleLabel}.nodeArrayBuffersBytes`,
		);
		const combinedBytes = requirePositiveSafeInteger(
			sample.combinedBytes,
			`${sampleLabel}.combinedBytes`,
		);
		const browserProcessCount = requirePositiveSafeInteger(
			sample.browserProcessCount,
			`${sampleLabel}.browserProcessCount`,
		);
		if (browserProcessCount > DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES) {
			throw new Error(
				`Benchmark ${sampleLabel} exceeds the browser-process count cap`,
			);
		}
		if (browserProcessCount < sample.browserRootProcessCount) {
			throw new Error(
				`Benchmark ${sampleLabel} browser-process count excludes a browser root`,
			);
		}
		const { roles: browserRoleBytes, total: roleTotal } =
			validateBrowserRoleBytes(
				sample.browserRoleBytes,
				`${sampleLabel}.browserRoleBytes`,
			);
		if (
			combinedBytes !== browserBytes + nodeBytes ||
			roleTotal !== browserBytes
		) {
			throw new Error(`Benchmark ${sampleLabel} RSS totals are inconsistent`);
		}
		for (const [role, bytes] of Object.entries(browserRoleBytes)) {
			peakBrowserRoleBytes[role] = Math.max(
				peakBrowserRoleBytes[role] ?? 0,
				bytes,
			);
		}
		return {
			browserInstanceCount: sample.browserInstanceCount,
			browserRootProcessCount: sample.browserRootProcessCount,
			browserBytes,
			nodeBytes,
			nodeExternalBytes,
			nodeArrayBuffersBytes,
			combinedBytes,
			browserProcessCount,
			browserRoleBytes,
		};
	});
	const first = samples[0];
	const last = samples.at(-1);
	const peak = (name) => Math.max(...samples.map((sample) => sample[name]));
	if (
		series.startBrowserBytes !== first.browserBytes ||
		series.endBrowserBytes !== last.browserBytes ||
		series.peakBrowserBytes !== peak("browserBytes") ||
		series.startNodeBytes !== first.nodeBytes ||
		series.endNodeBytes !== last.nodeBytes ||
		series.peakNodeBytes !== peak("nodeBytes") ||
		series.startNodeExternalBytes !== first.nodeExternalBytes ||
		series.endNodeExternalBytes !== last.nodeExternalBytes ||
		series.peakNodeExternalBytes !== peak("nodeExternalBytes") ||
		series.startNodeArrayBuffersBytes !== first.nodeArrayBuffersBytes ||
		series.endNodeArrayBuffersBytes !== last.nodeArrayBuffersBytes ||
		series.peakNodeArrayBuffersBytes !== peak("nodeArrayBuffersBytes") ||
		series.startCombinedBytes !== first.combinedBytes ||
		series.endCombinedBytes !== last.combinedBytes ||
		series.peakCombinedBytes !== peak("combinedBytes") ||
		series.startBrowserProcessCount !== first.browserProcessCount ||
		series.endBrowserProcessCount !== last.browserProcessCount ||
		series.peakBrowserProcessCount !== peak("browserProcessCount") ||
		!isDeepStrictEqual(series.startBrowserRoleBytes, first.browserRoleBytes) ||
		!isDeepStrictEqual(series.endBrowserRoleBytes, last.browserRoleBytes) ||
		!isDeepStrictEqual(series.peakBrowserRoleBytes, peakBrowserRoleBytes)
	) {
		throw new Error("Benchmark host RSS summaries are inconsistent");
	}
	return window;
};

export const validateDownloadMemoryTelemetry = (
	result,
	invocation,
	readWindow,
) => {
	const telemetry = requireRecord(
		result.downloadMemoryTelemetry,
		"downloadMemoryTelemetry",
	);
	requireExactRecordKeys(
		telemetry,
		[
			"profile",
			"sampleIntervalMs",
			"windowDefinition",
			"downloadTimeoutMs",
			"schedulingToleranceMs",
			"operationTimeoutMs",
			"postTransferSoakMs",
			"samplingWindowBudgetMs",
			"liveSampleMaxGapMs",
			"liveSampleCoverageDefinition",
			"endpointSampleAllowance",
			"maxSamplesPerSeries",
			"capacityExhaustedBeforeTerminal",
			"samplingCapacitySufficient",
			"manualCheckpointComplete",
			"terminalCheckpointComplete",
			"complete",
			"cleanupComplete",
			"startedAt",
			"finishedAt",
			"readerJsHeap",
			"writerJsHeap",
			"hostRss",
		],
		"downloadMemoryTelemetry",
	);
	const expectedSchedulingToleranceMs = Math.max(
		5_000,
		invocation.pollMs + 1_000,
	);
	const expectedSamplingWindowBudgetMs =
		calculateUploadSamplingWindowBudgetMs(invocation);
	const expectedLiveSampleMaxGapMs = calculateDownloadMemoryMaxLiveSampleGapMs({
		schedulingToleranceMs: expectedSchedulingToleranceMs,
	});
	const expectedMaxSamples = calculateDownloadMemoryMaxSamples({
		samplingWindowBudgetMs: expectedSamplingWindowBudgetMs,
	});
	if (
		telemetry.profile !== DOWNLOAD_MEMORY_PROFILE ||
		telemetry.sampleIntervalMs !== DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS ||
		telemetry.windowDefinition !== DOWNLOAD_MEMORY_WINDOW_DEFINITION ||
		telemetry.downloadTimeoutMs !== invocation.downloadTimeoutMs ||
		telemetry.schedulingToleranceMs !== expectedSchedulingToleranceMs ||
		telemetry.operationTimeoutMs !== DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS ||
		telemetry.postTransferSoakMs !== invocation.postTransferSoakMs ||
		telemetry.samplingWindowBudgetMs !== expectedSamplingWindowBudgetMs ||
		telemetry.liveSampleMaxGapMs !== expectedLiveSampleMaxGapMs ||
		telemetry.liveSampleCoverageDefinition !==
			DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION ||
		telemetry.endpointSampleAllowance !==
			DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE ||
		telemetry.maxSamplesPerSeries !== expectedMaxSamples ||
		telemetry.capacityExhaustedBeforeTerminal !== false ||
		telemetry.samplingCapacitySufficient !== true ||
		telemetry.manualCheckpointComplete !== true ||
		telemetry.terminalCheckpointComplete !== true ||
		telemetry.complete !== true ||
		telemetry.cleanupComplete !== true
	) {
		throw new Error("Benchmark download memory telemetry contract is invalid");
	}
	const readerWindow = validateHeapMemorySeries(
		telemetry.readerJsHeap,
		"downloadMemoryTelemetry.readerJsHeap",
		"reader-renderer",
		expectedMaxSamples,
		{ ...readWindow, liveSampleMaxGapMs: expectedLiveSampleMaxGapMs },
	);
	const writerWindow = validateHeapMemorySeries(
		telemetry.writerJsHeap,
		"downloadMemoryTelemetry.writerJsHeap",
		"writer-renderer",
		expectedMaxSamples,
		{ ...readWindow, liveSampleMaxGapMs: expectedLiveSampleMaxGapMs },
	);
	const hostWindow = validateHostRssSeries(
		telemetry.hostRss,
		expectedMaxSamples,
		{ ...readWindow, liveSampleMaxGapMs: expectedLiveSampleMaxGapMs },
	);
	const expectedStartedAt = Math.min(
		readerWindow.startedAt,
		writerWindow.startedAt,
		hostWindow.startedAt,
	);
	const expectedFinishedAt = Math.max(
		readerWindow.finishedAt,
		writerWindow.finishedAt,
		hostWindow.finishedAt,
	);
	if (
		telemetry.startedAt !== expectedStartedAt ||
		telemetry.finishedAt !== expectedFinishedAt
	) {
		throw new Error(
			"Benchmark download memory telemetry window summary is inconsistent",
		);
	}
	return telemetry;
};

const validateUploadTimings = (result, invocation, integrityVerifiedAt) => {
	if (result.downloadDurationDefinition !== DOWNLOAD_DURATION_DEFINITION) {
		throw new Error("Benchmark download duration definition is invalid");
	}
	const phases = requireRecord(result.phaseDurationsMs, "phaseDurationsMs");
	const uploadSettledMs = requireNonNegativeNumber(
		phases.timeToUploadSettled,
		"phaseDurationsMs.timeToUploadSettled",
	);
	const phaseTimeToWriterReadyMs = requireNonNegativeNumber(
		phases.timeToWriterReady,
		"phaseDurationsMs.timeToWriterReady",
	);
	const phaseTimeToReaderReadyMs = requireNonNegativeNumber(
		phases.timeToReaderReady,
		"phaseDurationsMs.timeToReaderReady",
	);
	const timeToWriterReadyMs = requireNonNegativeNumber(
		result.timeToWriterReadyMs,
		"timeToWriterReadyMs",
	);
	const timeToReaderReadyMs = requireNonNegativeNumber(
		result.timeToReaderReadyMs,
		"timeToReaderReadyMs",
	);
	requireNonNegativeNumber(
		phases.writerListingLag,
		"phaseDurationsMs.writerListingLag",
	);
	requireNonNegativeNumber(
		phases.readerListingLag,
		"phaseDurationsMs.readerListingLag",
	);
	requireNonNegativeNumber(result.listingDurationMs, "listingDurationMs");
	requireNonNegativeNumber(
		result.postUploadMonitorDurationMs,
		"postUploadMonitorDurationMs",
	);
	requireNonNegativeNumber(result.downloadDurationMs, "downloadDurationMs");
	const uploadDurationMs = requirePositiveNumber(
		result.uploadDurationMs,
		"uploadDurationMs",
	);
	if (uploadDurationMs !== uploadSettledMs) {
		throw new Error(
			"uploadDurationMs must end at upload settlement and exclude listing/post-monitor/download",
		);
	}
	if (
		result.timeToWriterReadyDefinition !== TIME_TO_WRITER_READY_DEFINITION ||
		result.timeToReaderReadyDefinition !== TIME_TO_READER_READY_DEFINITION ||
		result.listingDurationDefinition !== LISTING_DURATION_DEFINITION
	) {
		throw new Error("Benchmark readiness duration definitions are invalid");
	}
	const timestamps = requireRecord(result.timestamps, "timestamps");
	const uploadStartedAt = requirePositiveNumber(
		timestamps.uploadStartedAt,
		"timestamps.uploadStartedAt",
	);
	const uploadSettledAt = requirePositiveNumber(
		timestamps.uploadSettledAt,
		"timestamps.uploadSettledAt",
	);
	const progressSettledAt = requirePositiveNumber(
		timestamps.progressSettledAt,
		"timestamps.progressSettledAt",
	);
	const writerListedAt = requirePositiveNumber(
		timestamps.writerListedAt,
		"timestamps.writerListedAt",
	);
	const readerListedAt = requirePositiveNumber(
		timestamps.readerListedAt,
		"timestamps.readerListedAt",
	);
	const postMonitorStartedAt = requirePositiveNumber(
		timestamps.postMonitorStartedAt,
		"timestamps.postMonitorStartedAt",
	);
	const postMonitorFinishedAt = requirePositiveNumber(
		timestamps.postMonitorFinishedAt,
		"timestamps.postMonitorFinishedAt",
	);
	const downloadStartedAt = requirePositiveSafeInteger(
		timestamps.downloadStartedAt,
		"timestamps.downloadStartedAt",
	);
	const downloadFinishedAt = requirePositiveSafeInteger(
		timestamps.downloadFinishedAt,
		"timestamps.downloadFinishedAt",
	);
	const downloadCompletionObservedAt = requirePositiveSafeInteger(
		timestamps.downloadCompletionObservedAt,
		"timestamps.downloadCompletionObservedAt",
	);
	const postTransferSoakStartedAt = requirePositiveSafeInteger(
		timestamps.postTransferSoakStartedAt,
		"timestamps.postTransferSoakStartedAt",
	);
	const postTransferSoakFinishedAt = requirePositiveSafeInteger(
		timestamps.postTransferSoakFinishedAt,
		"timestamps.postTransferSoakFinishedAt",
	);
	if (
		uploadSettledAt <= uploadStartedAt ||
		progressSettledAt < uploadStartedAt ||
		progressSettledAt > uploadSettledAt ||
		writerListedAt < uploadStartedAt ||
		readerListedAt < uploadStartedAt ||
		writerListedAt > uploadSettledAt ||
		postMonitorStartedAt <
			Math.max(uploadSettledAt, writerListedAt, readerListedAt) ||
		postMonitorFinishedAt < postMonitorStartedAt ||
		downloadStartedAt < postMonitorFinishedAt ||
		downloadFinishedAt < downloadStartedAt ||
		downloadFinishedAt > downloadCompletionObservedAt
	) {
		throw new Error("Benchmark phase timestamps are not monotonic");
	}
	if (readerListedAt > uploadSettledAt + invocation.readyTimeoutMs) {
		throw new Error(
			"Benchmark reader readiness exceeded the requested post-writer deadline",
		);
	}
	const progressVisibleAt =
		timestamps.progressVisibleAt == null
			? undefined
			: requirePositiveNumber(
					timestamps.progressVisibleAt,
					"timestamps.progressVisibleAt",
				);
	if (
		progressVisibleAt != null &&
		(progressVisibleAt < uploadStartedAt || progressVisibleAt > uploadSettledAt)
	) {
		throw new Error("Benchmark phase timestamps are not monotonic");
	}
	const expectedListingDuration = Math.max(
		0,
		Math.max(writerListedAt, readerListedAt) - uploadSettledAt,
	);
	if (
		uploadDurationMs !== uploadSettledAt - uploadStartedAt ||
		timeToWriterReadyMs !== writerListedAt - uploadStartedAt ||
		timeToReaderReadyMs !== readerListedAt - uploadStartedAt ||
		phaseTimeToWriterReadyMs !== timeToWriterReadyMs ||
		phaseTimeToReaderReadyMs !== timeToReaderReadyMs ||
		result.listingDurationMs !== expectedListingDuration ||
		result.postUploadMonitorDurationMs !==
			postMonitorFinishedAt - postMonitorStartedAt ||
		result.downloadDurationMs !== downloadFinishedAt - downloadStartedAt ||
		phases.writerListingLag !== Math.max(0, writerListedAt - uploadSettledAt) ||
		phases.readerListingLag !== Math.max(0, readerListedAt - uploadSettledAt) ||
		phases.readerAfterWriter !== readerListedAt - writerListedAt ||
		phases.postUploadMonitor !== result.postUploadMonitorDurationMs ||
		phases.download !== result.downloadDurationMs
	) {
		throw new Error("Benchmark phase duration arithmetic is inconsistent");
	}
	requireFiniteNumber(
		phases.readerAfterWriter,
		"phaseDurationsMs.readerAfterWriter",
	);
	if (progressVisibleAt == null) {
		if (phases.timeToProgressVisible != null || phases.activeUpload != null) {
			throw new Error("Benchmark progress duration arithmetic is inconsistent");
		}
	} else if (
		phases.timeToProgressVisible !== progressVisibleAt - uploadStartedAt ||
		phases.activeUpload !== uploadSettledAt - progressVisibleAt
	) {
		throw new Error("Benchmark progress duration arithmetic is inconsistent");
	}

	const expectedPostMonitorTolerance = Math.max(250, invocation.pollMs + 250);
	if (
		result.postUploadMonitorSchedulingToleranceMs !==
			expectedPostMonitorTolerance ||
		result.postUploadMonitorSchedulingToleranceDefinition !==
			POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION
	) {
		throw new Error(
			"Benchmark post-upload monitor scheduling tolerance is invalid",
		);
	}
	if (
		result.postUploadMonitorDurationMs < invocation.postUploadMonitorMs ||
		result.postUploadMonitorDurationMs >
			invocation.postUploadMonitorMs + expectedPostMonitorTolerance
	) {
		throw new Error(
			"Measured post-upload monitor duration is outside the requested duration and scheduling tolerance",
		);
	}

	const expectedTransferTolerance = Math.max(5_000, invocation.pollMs + 1_000);
	if (
		result.transferTimeoutSchedulingToleranceMs !== expectedTransferTolerance ||
		result.transferTimeoutSchedulingToleranceDefinition !==
			TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION
	) {
		throw new Error("Benchmark transfer scheduling tolerance is invalid");
	}
	if (
		uploadDurationMs > invocation.uploadTimeoutMs + expectedTransferTolerance ||
		result.downloadDurationMs >
			invocation.downloadTimeoutMs + expectedTransferTolerance
	) {
		throw new Error(
			"Measured transfer duration exceeded its requested timeout and scheduling tolerance",
		);
	}
	const expectedPostTransferSoakTolerance = Math.max(
		250,
		invocation.pollMs + 250,
	);
	const postTransferSoakActualMs = requireNonNegativeSafeInteger(
		result.postTransferSoakActualMs,
		"postTransferSoakActualMs",
	);
	if (
		postTransferSoakStartedAt < downloadCompletionObservedAt ||
		postTransferSoakStartedAt < integrityVerifiedAt ||
		postTransferSoakFinishedAt < postTransferSoakStartedAt ||
		result.postTransferSoakDefinition !== POST_TRANSFER_SOAK_DEFINITION ||
		result.postTransferSoakSchedulingToleranceMs !==
			expectedPostTransferSoakTolerance ||
		result.postTransferSoakSchedulingToleranceDefinition !==
			POST_TRANSFER_SOAK_SCHEDULING_TOLERANCE_DEFINITION ||
		postTransferSoakActualMs !==
			postTransferSoakFinishedAt - postTransferSoakStartedAt ||
		postTransferSoakActualMs < invocation.postTransferSoakMs ||
		postTransferSoakActualMs >
			invocation.postTransferSoakMs + expectedPostTransferSoakTolerance
	) {
		throw new Error(
			"Benchmark post-transfer soak duration or scheduling contract is invalid",
		);
	}
	return {
		...validateReadTransferEvidence(result, invocation, {
			downloadStartedAt,
			downloadFinishedAt,
			downloadCompletionObservedAt,
		}),
		postTransferSoakStartedAt,
		postTransferSoakFinishedAt,
	};
};

const validateUploadProgressTelemetry = (result, invocation) => {
	const writerDiagnostics = requireRecord(
		result.writerDiagnostics,
		"writerDiagnostics",
	);
	const diagnostics = requireRecord(
		writerDiagnostics.lastUploadDiagnostics,
		"writerDiagnostics.lastUploadDiagnostics",
	);
	const writerManifest = requireRecord(
		result.writerManifestEvidence,
		"writerManifestEvidence",
	);
	const timestamps = requireRecord(result.timestamps, "timestamps");
	const manifestFileId = requireString(
		writerManifest.fileId,
		"writerManifestEvidence.fileId",
	);
	const resultFileName = requireString(result.fileName, "fileName");
	requireString(
		diagnostics.transferId,
		"writerDiagnostics.lastUploadDiagnostics.transferId",
	);
	const uploadId = requireString(
		diagnostics.uploadId,
		"writerDiagnostics.lastUploadDiagnostics.uploadId",
	);
	const fileName = requireString(
		diagnostics.fileName,
		"writerDiagnostics.lastUploadDiagnostics.fileName",
	);
	const sizeBytes = requirePositiveSafeInteger(
		invocation.fileSizeBytes,
		"invocation.fileSizeBytes",
	);
	const chunkSize = requirePositiveSafeInteger(
		diagnostics.chunkSize,
		"writerDiagnostics.lastUploadDiagnostics.chunkSize",
	);
	const chunkCount = requirePositiveSafeInteger(
		diagnostics.chunkCount,
		"writerDiagnostics.lastUploadDiagnostics.chunkCount",
	);
	const chunkPutCount = requireNonNegativeSafeInteger(
		diagnostics.chunkPutCount,
		"writerDiagnostics.lastUploadDiagnostics.chunkPutCount",
	);
	const readTransfer = requireRecord(result.readTransfer, "readTransfer");
	const readChunkCount = requirePositiveSafeInteger(
		readTransfer.chunkCount,
		"readTransfer.chunkCount",
	);
	const readerDiagnostics = requireRecord(
		result.readerDiagnostics,
		"readerDiagnostics",
	);
	const lastReadDiagnostics = requireRecord(
		readerDiagnostics.lastReadDiagnostics,
		"readerDiagnostics.lastReadDiagnostics",
	);
	const readChunkByteLength = requireRecord(
		lastReadDiagnostics.chunkByteLength,
		"readerDiagnostics.lastReadDiagnostics.chunkByteLength",
	);
	const uploadStartedAt = requirePositiveSafeInteger(
		timestamps.uploadStartedAt,
		"timestamps.uploadStartedAt",
	);
	const uploadSettledAt = requirePositiveSafeInteger(
		timestamps.uploadSettledAt,
		"timestamps.uploadSettledAt",
	);
	const startedAt = requirePositiveSafeInteger(
		diagnostics.startedAt,
		"writerDiagnostics.lastUploadDiagnostics.startedAt",
	);
	const manifestStartedAt = requirePositiveSafeInteger(
		diagnostics.manifestStartedAt,
		"writerDiagnostics.lastUploadDiagnostics.manifestStartedAt",
	);
	const manifestFinishedAt = requirePositiveSafeInteger(
		diagnostics.manifestFinishedAt,
		"writerDiagnostics.lastUploadDiagnostics.manifestFinishedAt",
	);
	const firstChunkStartedAt = requirePositiveSafeInteger(
		diagnostics.firstChunkStartedAt,
		"writerDiagnostics.lastUploadDiagnostics.firstChunkStartedAt",
	);
	const firstChunkFinishedAt = requirePositiveSafeInteger(
		diagnostics.firstChunkFinishedAt,
		"writerDiagnostics.lastUploadDiagnostics.firstChunkFinishedAt",
	);
	const lastChunkFinishedAt = requirePositiveSafeInteger(
		diagnostics.lastChunkFinishedAt,
		"writerDiagnostics.lastUploadDiagnostics.lastChunkFinishedAt",
	);
	const readyManifestStartedAt = requirePositiveSafeInteger(
		diagnostics.readyManifestStartedAt,
		"writerDiagnostics.lastUploadDiagnostics.readyManifestStartedAt",
	);
	const readyManifestFinishedAt = requirePositiveSafeInteger(
		diagnostics.readyManifestFinishedAt,
		"writerDiagnostics.lastUploadDiagnostics.readyManifestFinishedAt",
	);
	const finishedAt = requirePositiveSafeInteger(
		diagnostics.finishedAt,
		"writerDiagnostics.lastUploadDiagnostics.finishedAt",
	);
	if (
		uploadId !== manifestFileId ||
		fileName !== resultFileName ||
		diagnostics.sizeBytes !== sizeBytes
	) {
		throw new Error(
			"Benchmark upload diagnostics do not identify the canonical uploaded file",
		);
	}
	if (
		chunkCount !== Math.ceil(sizeBytes / chunkSize) ||
		chunkPutCount !== chunkCount ||
		diagnostics.failureAt !== null ||
		diagnostics.failureMessage !== null
	) {
		throw new Error(
			"Benchmark upload diagnostics do not prove a successful complete chunk upload",
		);
	}
	if (chunkCount !== readChunkCount) {
		throw new Error(
			"Benchmark upload chunk geometry contradicts canonical read evidence",
		);
	}
	for (let index = 0; index < chunkCount; index++) {
		const expectedChunkBytes =
			index === chunkCount - 1 ? sizeBytes - index * chunkSize : chunkSize;
		const observedChunkBytes = requirePositiveSafeInteger(
			readChunkByteLength[index],
			`readerDiagnostics.lastReadDiagnostics.chunkByteLength[${index}]`,
		);
		if (observedChunkBytes !== expectedChunkBytes) {
			throw new Error(
				"Benchmark upload chunk geometry contradicts canonical read evidence",
			);
		}
	}
	if (
		uploadStartedAt > startedAt ||
		startedAt > manifestStartedAt ||
		manifestStartedAt > manifestFinishedAt ||
		manifestFinishedAt > firstChunkStartedAt ||
		firstChunkStartedAt > firstChunkFinishedAt ||
		firstChunkFinishedAt > lastChunkFinishedAt ||
		lastChunkFinishedAt > readyManifestStartedAt ||
		readyManifestStartedAt > readyManifestFinishedAt ||
		readyManifestFinishedAt > finishedAt ||
		// The DOM progress helper may return before React renders the progress
		// element. Writer readiness also awaits the canonical ready manifest, so
		// uploadSettledAt is the reliable harness-side upper bound.
		finishedAt > uploadSettledAt
	) {
		throw new Error(
			"Benchmark upload diagnostics lifecycle timestamps are inconsistent",
		);
	}

	const telemetry = requireExactRecordKeys(
		requireRecord(
			diagnostics.progressTelemetry,
			"writerDiagnostics.lastUploadDiagnostics.progressTelemetry",
		),
		UPLOAD_PROGRESS_TELEMETRY_KEYS,
		"upload progress telemetry",
	);
	if (
		telemetry.schemaVersion !== 1 ||
		telemetry.kind !== "upload-chunk-commit" ||
		telemetry.clock !== "unix-epoch-ms"
	) {
		throw new Error("Benchmark upload progress telemetry contract is invalid");
	}
	if (
		!Array.isArray(telemetry.milestones) ||
		telemetry.milestones.length !==
			UPLOAD_PROGRESS_MILESTONE_BASIS_POINTS.length
	) {
		throw new Error(
			"Benchmark upload progress telemetry must contain exactly 21 milestones",
		);
	}

	const partialTailBytes = sizeBytes % chunkSize;
	const hasPartialTail = partialTailBytes !== 0;
	const fullChunkCount = chunkCount - (hasPartialTail ? 1 : 0);
	const decodeCompletionState = (completedBytes, label) => {
		if (completedBytes === 0) {
			return { includedFullChunks: 0, tailIncluded: false };
		}
		const remainder = completedBytes % chunkSize;
		let includedFullChunks;
		let tailIncluded;
		if (remainder === 0) {
			includedFullChunks = completedBytes / chunkSize;
			tailIncluded = false;
		} else if (hasPartialTail && remainder === partialTailBytes) {
			includedFullChunks = (completedBytes - partialTailBytes) / chunkSize;
			tailIncluded = true;
		} else {
			throw new Error(
				`Benchmark ${label} is not a possible aggregate of completed chunks`,
			);
		}
		if (
			!Number.isSafeInteger(includedFullChunks) ||
			includedFullChunks < 0 ||
			includedFullChunks > fullChunkCount
		) {
			throw new Error(
				`Benchmark ${label} is not a possible aggregate of completed chunks`,
			);
		}
		return { includedFullChunks, tailIncluded };
	};

	let previousCompletedBytes = 0;
	let previousReachedAt = startedAt;
	let previousChunkIndex = null;
	let previousCompletionState = {
		includedFullChunks: 0,
		tailIncluded: false,
	};
	const recordedTriggerEvents = new Map();
	for (const [index, value] of telemetry.milestones.entries()) {
		const label = `upload progress milestone ${index}`;
		const milestone = requireExactRecordKeys(
			requireRecord(value, label),
			UPLOAD_PROGRESS_MILESTONE_KEYS,
			label,
		);
		const basisPoints = UPLOAD_PROGRESS_MILESTONE_BASIS_POINTS[index];
		const expectedTargetBytes = Number(
			(BigInt(sizeBytes) * BigInt(basisPoints) + 9_999n) / 10_000n,
		);
		const targetBytes = requireNonNegativeSafeInteger(
			milestone.targetBytes,
			`${label}.targetBytes`,
		);
		const completedBytes = requireNonNegativeSafeInteger(
			milestone.completedBytes,
			`${label}.completedBytes`,
		);
		const reachedAt = requirePositiveSafeInteger(
			milestone.reachedAt,
			`${label}.reachedAt`,
		);
		if (
			milestone.basisPoints !== basisPoints ||
			targetBytes !== expectedTargetBytes
		) {
			throw new Error(`Benchmark ${label} does not use the exact 5% target`);
		}
		if (
			completedBytes < targetBytes ||
			completedBytes > sizeBytes ||
			completedBytes - targetBytes >= chunkSize ||
			completedBytes < previousCompletedBytes
		) {
			throw new Error(
				`Benchmark ${label} has invalid aggregate completed bytes`,
			);
		}
		if (
			reachedAt < startedAt ||
			reachedAt > lastChunkFinishedAt ||
			reachedAt < previousReachedAt
		) {
			throw new Error(`Benchmark ${label} has invalid completion time`);
		}
		const completionState = decodeCompletionState(completedBytes, label);
		if (index === 0) {
			if (
				targetBytes !== 0 ||
				completedBytes !== 0 ||
				reachedAt !== startedAt ||
				milestone.chunkIndex !== null
			) {
				throw new Error(
					"Benchmark upload progress telemetry has an invalid zero milestone",
				);
			}
		} else {
			if (reachedAt < firstChunkFinishedAt) {
				throw new Error(
					`Benchmark ${label} predates the first completed chunk`,
				);
			}
			const chunkIndex = requireNonNegativeSafeInteger(
				milestone.chunkIndex,
				`${label}.chunkIndex`,
			);
			if (chunkIndex >= chunkCount) {
				throw new Error(`Benchmark ${label} has an out-of-range chunk index`);
			}
			if (
				completedBytes === previousCompletedBytes &&
				(reachedAt !== previousReachedAt || chunkIndex !== previousChunkIndex)
			) {
				throw new Error(
					`Benchmark ${label} contradicts its shared chunk-completion event`,
				);
			}
			const triggerChunkBytes = requirePositiveSafeInteger(
				readChunkByteLength[chunkIndex],
				`readerDiagnostics.lastReadDiagnostics.chunkByteLength[${chunkIndex}]`,
			);
			const preTriggerBytes = completedBytes - triggerChunkBytes;
			if (preTriggerBytes >= targetBytes) {
				throw new Error(
					`Benchmark ${label} was already crossed before its triggering chunk completion`,
				);
			}
			if (
				completedBytes !== previousCompletedBytes &&
				preTriggerBytes === 0 &&
				reachedAt !== firstChunkFinishedAt
			) {
				throw new Error(
					`Benchmark ${label} contradicts the first completed chunk timestamp`,
				);
			}
			if (
				previousCompletedBytes >= targetBytes &&
				completedBytes !== previousCompletedBytes
			) {
				throw new Error(
					`Benchmark ${label} was not recorded at its first qualifying chunk-completion event`,
				);
			}
			if (completedBytes !== previousCompletedBytes) {
				if (
					completionState.includedFullChunks <
						previousCompletionState.includedFullChunks ||
					(previousCompletionState.tailIncluded &&
						!completionState.tailIncluded)
				) {
					throw new Error(
						`Benchmark ${label} regresses its completed-chunk set`,
					);
				}
				const triggersPartialTail =
					hasPartialTail && chunkIndex === chunkCount - 1;
				if (
					triggersPartialTail
						? !completionState.tailIncluded ||
							previousCompletionState.tailIncluded
						: chunkIndex >= fullChunkCount ||
							completionState.includedFullChunks <=
								previousCompletionState.includedFullChunks
				) {
					throw new Error(
						`Benchmark ${label} contradicts its triggering chunk completion`,
					);
				}
				if (recordedTriggerEvents.has(chunkIndex)) {
					throw new Error(
						`Benchmark ${label} reuses a completed chunk as a later trigger`,
					);
				}
				recordedTriggerEvents.set(chunkIndex, {
					completedBytes,
					reachedAt,
				});
				previousCompletionState = completionState;
			}
		}
		previousCompletedBytes = completedBytes;
		previousReachedAt = reachedAt;
		previousChunkIndex = milestone.chunkIndex;
	}

	const finalMilestone = telemetry.milestones.at(-1);
	if (
		finalMilestone.targetBytes !== sizeBytes ||
		finalMilestone.completedBytes !== sizeBytes ||
		finalMilestone.reachedAt !== lastChunkFinishedAt
	) {
		throw new Error(
			"Benchmark upload progress telemetry has an invalid completion milestone",
		);
	}
	return telemetry;
};

const validateGitProvenance = (value, label) => {
	const provenance = requireRecord(value, label);
	requireString(provenance.requestedRef, `${label}.requestedRef`);
	requirePattern(
		provenance.resolvedCommit,
		GIT_COMMIT_PATTERN,
		`${label}.resolvedCommit`,
	);
	if (typeof provenance.dirty !== "boolean") {
		throw new Error(`Benchmark result has invalid ${label}.dirty`);
	}
	if (provenance.dirty) {
		requirePattern(
			provenance.worktreeDigest,
			SHA256_HEX_PATTERN,
			`${label}.worktreeDigest`,
		);
	} else if (provenance.worktreeDigest !== null) {
		throw new Error(`Clean ${label} has a worktree digest`);
	}
	return provenance;
};

const validateEnvelope = (
	result,
	{ expectedRunNonce, expectedProvenance, expectedNetwork, expectedInvocation },
) => {
	const schema = requireRecord(result.schema, "schema");
	if (
		schema.id !== BENCHMARK_RESULT_SCHEMA.id ||
		schema.version !== BENCHMARK_RESULT_SCHEMA.version
	) {
		throw new Error("Benchmark result has an unsupported schema");
	}
	if (
		result.runNonce !== expectedRunNonce ||
		!UUID_PATTERN.test(String(result.runNonce))
	) {
		throw new Error("Benchmark result run nonce does not match the invocation");
	}
	if (result.networkMode !== expectedNetwork) {
		throw new Error("Benchmark result network does not match the invocation");
	}
	if (!isDeepStrictEqual(result.invocation, expectedInvocation)) {
		throw new Error("Benchmark result invocation does not match the request");
	}
	const invocation = requireRecord(result.invocation, "invocation");
	const invocationSchema = requireRecord(
		invocation.schema,
		"invocation.schema",
	);
	if (
		invocationSchema.id !== "peerbit-file-share-benchmark-invocation" ||
		invocationSchema.version !== 6
	) {
		throw new Error("Benchmark result has an unsupported invocation schema");
	}
	if (invocation.scenario === "upload") {
		validateSeederDropPolicy(result);
	}
	if (!isDeepStrictEqual(result.provenance, expectedProvenance)) {
		throw new Error(
			"Benchmark result provenance does not match the invocation",
		);
	}
	const provenance = requireRecord(result.provenance, "provenance");
	validateGitProvenance(provenance.harness, "provenance.harness");
	validateGitProvenance(provenance.peerbit, "provenance.peerbit");
	const examples = validateGitProvenance(
		provenance.examples,
		"provenance.examples",
	);
	requirePattern(
		examples.lockfileSha256,
		SHA256_HEX_PATTERN,
		"provenance.examples.lockfileSha256",
	);
};

const validateRequestedUploadKnobs = (result, invocation) => {
	for (const [resultKey, invocationKey] of [
		["postUploadMonitorMs", "postUploadMonitorMs"],
		["postTransferSoakMs", "postTransferSoakMs"],
		["pollMs", "pollMs"],
		["uploadTimeoutMs", "uploadTimeoutMs"],
		["downloadTimeoutMs", "downloadTimeoutMs"],
		["minReadySeeders", "minReadySeeders"],
		["readyTimeoutMs", "readyTimeoutMs"],
	]) {
		if (result[resultKey] !== invocation[invocationKey]) {
			throw new Error(
				`Benchmark result ${resultKey} does not match the requested invocation`,
			);
		}
	}
	if (result.readerLocalChunkTarget !== invocation.readerLocalChunkTarget) {
		throw new Error(
			"Benchmark result readerLocalChunkTarget does not match the requested invocation",
		);
	}
	if (
		result.readerLocalChunkMaxOvershoot !==
		invocation.readerLocalChunkMaxOvershoot
	) {
		throw new Error(
			"Benchmark result readerLocalChunkMaxOvershoot does not match the requested invocation",
		);
	}
	if (result.readerTerminalTopology !== invocation.readerTerminalTopology) {
		throw new Error(
			"Benchmark result readerTerminalTopology does not match the requested invocation",
		);
	}
	if (result.browserStorageMode !== invocation.browserStorageMode) {
		throw new Error(
			"Benchmark result browserStorageMode does not match the requested invocation",
		);
	}
};

const validateStorageUsageDetails = (value, label) => {
	if (value === null) {
		return null;
	}
	const details = requireRecord(value, label);
	const entries = Object.entries(details);
	if (entries.length > RESOURCE_MAX_STORAGE_DETAIL_KEYS) {
		throw new Error(`Benchmark ${label} contains too many storage categories`);
	}
	for (const [key, bytes] of entries) {
		if (
			key.length === 0 ||
			key.length > RESOURCE_MAX_STORAGE_DETAIL_KEY_LENGTH ||
			!/^\S(?:.*\S)?$/.test(key)
		) {
			throw new Error(
				`Benchmark ${label} contains an invalid storage category`,
			);
		}
		requireNonNegativeSafeInteger(bytes, `${label}.${key}`);
	}
	return details;
};

const validateStorageResourceSnapshot = (
	value,
	label,
	{ setStartedAt, pageCapturedAt, browserStorageMode },
) => {
	const storage = requireExactRecordKeys(
		requireRecord(value, label),
		["capturedAt", "origin", "backend", "peerbitLog", "backingStorage"],
		label,
	);
	const capturedAt = requirePositiveSafeInteger(
		storage.capturedAt,
		`${label}.capturedAt`,
	);
	if (capturedAt < setStartedAt || capturedAt > pageCapturedAt) {
		throw new Error(
			`Benchmark ${label} is outside its resource capture window`,
		);
	}
	const origin = requireString(storage.origin, `${label}.origin`);
	let parsedOrigin;
	try {
		parsedOrigin = new URL(origin).origin;
	} catch {
		throw new Error(`Benchmark result has invalid ${label}.origin`);
	}
	if (parsedOrigin !== origin) {
		throw new Error(`Benchmark result has invalid ${label}.origin`);
	}
	const backend = requireExactRecordKeys(
		requireRecord(storage.backend, `${label}.backend`),
		[
			"requestedMode",
			"directoryConfigured",
			"directoryConfigurationError",
			"persistence",
		],
		`${label}.backend`,
	);
	if (
		!["memory", "opfs"].includes(browserStorageMode) ||
		backend.requestedMode !== browserStorageMode ||
		backend.directoryConfigured !== (browserStorageMode === "opfs") ||
		backend.directoryConfigurationError !== null
	) {
		throw new Error(`Benchmark ${label}.backend contract is invalid`);
	}
	const persistence = requireExactRecordKeys(
		requireRecord(backend.persistence, `${label}.backend.persistence`),
		["navigatorStorage", "peerStorage", "peerBlocks", "peerIndexer"],
		`${label}.backend.persistence`,
	);
	const validatePersistedProbe = (
		value,
		probeLabel,
		expectedApi,
		expectedPersisted,
	) => {
		const probe = requireExactRecordKeys(
			requireRecord(value, probeLabel),
			["api", "available", "persisted", "error"],
			probeLabel,
		);
		if (
			probe.api !== expectedApi ||
			probe.available !== true ||
			typeof probe.persisted !== "boolean" ||
			probe.error !== null ||
			(expectedPersisted !== null && probe.persisted !== expectedPersisted)
		) {
			throw new Error(`Benchmark ${probeLabel} contract is invalid`);
		}
		return probe;
	};
	validatePersistedProbe(
		persistence.navigatorStorage,
		`${label}.backend.persistence.navigatorStorage`,
		"navigator.storage.persisted",
		null,
	);
	const expectedPeerPersistence = browserStorageMode === "opfs";
	validatePersistedProbe(
		persistence.peerStorage,
		`${label}.backend.persistence.peerStorage`,
		"peer.storage.persisted",
		expectedPeerPersistence,
	);
	validatePersistedProbe(
		persistence.peerBlocks,
		`${label}.backend.persistence.peerBlocks`,
		"peer.services.blocks.persisted",
		expectedPeerPersistence,
	);
	validatePersistedProbe(
		persistence.peerIndexer,
		`${label}.backend.persistence.peerIndexer`,
		"peer.indexer.persisted",
		expectedPeerPersistence,
	);
	const peerbitLog = requireExactRecordKeys(
		requireRecord(storage.peerbitLog, `${label}.peerbitLog`),
		["api", "scope", "available", "usageBytes", "error"],
		`${label}.peerbitLog`,
	);
	if (
		peerbitLog.api !== "SharedLog.getMemoryUsage" ||
		peerbitLog.scope !== "file-share-log-logical-usage" ||
		peerbitLog.available !== true ||
		peerbitLog.error !== null
	) {
		throw new Error(`Benchmark ${label}.peerbitLog contract is invalid`);
	}
	requireNonNegativeSafeInteger(
		peerbitLog.usageBytes,
		`${label}.peerbitLog.usageBytes`,
	);
	const backingStorage = requireExactRecordKeys(
		requireRecord(storage.backingStorage, `${label}.backingStorage`),
		[
			"api",
			"scope",
			"available",
			"usageBytes",
			"quotaBytes",
			"usageDetails",
			"error",
		],
		`${label}.backingStorage`,
	);
	if (
		backingStorage.api !== "navigator.storage.estimate" ||
		backingStorage.scope !== "browser-origin-aggregate" ||
		backingStorage.available !== true ||
		backingStorage.error !== null
	) {
		throw new Error(`Benchmark ${label}.backingStorage contract is invalid`);
	}
	const usageBytes = requireNonNegativeSafeInteger(
		backingStorage.usageBytes,
		`${label}.backingStorage.usageBytes`,
	);
	const quotaBytes = requireNonNegativeSafeInteger(
		backingStorage.quotaBytes,
		`${label}.backingStorage.quotaBytes`,
	);
	if (quotaBytes < usageBytes) {
		throw new Error(`Benchmark ${label}.backingStorage exceeds its quota`);
	}
	validateStorageUsageDetails(
		backingStorage.usageDetails,
		`${label}.backingStorage.usageDetails`,
	);
	return storage;
};

const validateEagerTelemetry = (value, label) => {
	const telemetry = requireExactRecordKeys(
		requireRecord(value, label),
		EAGER_TELEMETRY_KEYS,
		label,
	);
	for (const key of EAGER_TELEMETRY_KEYS.filter((key) => key !== "limits")) {
		requireNonNegativeSafeInteger(telemetry[key], `${label}.${key}`);
	}
	const limits = requireExactRecordKeys(
		requireRecord(telemetry.limits, `${label}.limits`),
		EAGER_LIMIT_KEYS,
		`${label}.limits`,
	);
	for (const key of EAGER_LIMIT_KEYS) {
		requirePositiveSafeInteger(limits[key], `${label}.limits.${key}`);
	}
	if (
		telemetry.entries > telemetry.peakEntries ||
		telemetry.peakEntries > limits.maxEntries ||
		telemetry.bytes > telemetry.peakBytes ||
		telemetry.peakBytes > limits.maxBytes ||
		telemetry.pendingEntries > telemetry.peakPendingEntries ||
		telemetry.peakPendingEntries > limits.maxPendingEntries ||
		telemetry.pendingBytes > telemetry.peakPendingBytes ||
		telemetry.peakPendingBytes > limits.maxPendingBytes
	) {
		throw new Error(
			`Benchmark ${label} eager-cache gauges exceed their bounds`,
		);
	}
	return telemetry;
};

const RUNTIME_IDENTITY_KEYS = Object.freeze([
	"programAddress",
	"peerId",
	"peerHash",
	"sessionId",
]);

const validateRuntimeIdentity = (value, label) => {
	const identity = requireExactRecordKeys(
		requireRecord(value, label),
		RUNTIME_IDENTITY_KEYS,
		label,
	);
	for (const key of RUNTIME_IDENTITY_KEYS) {
		requireString(identity[key], `${label}.${key}`);
	}
	return identity;
};

const validateRuntimeResourceSnapshot = (
	value,
	label,
	{ setStartedAt, pageCapturedAt },
) => {
	const runtime = requireExactRecordKeys(
		requireRecord(value, label),
		[
			"capturedAt",
			"programReady",
			"identity",
			"nativeGraph",
			"eagerBlocks",
			"pubsub",
		],
		label,
	);
	const capturedAt = requirePositiveSafeInteger(
		runtime.capturedAt,
		`${label}.capturedAt`,
	);
	if (capturedAt < setStartedAt || capturedAt > pageCapturedAt) {
		throw new Error(
			`Benchmark ${label} is outside its resource capture window`,
		);
	}
	if (runtime.programReady !== true) {
		throw new Error(`Benchmark ${label} did not capture a ready program`);
	}
	validateRuntimeIdentity(runtime.identity, `${label}.identity`);
	const nativeGraph = requireExactRecordKeys(
		requireRecord(runtime.nativeGraph, `${label}.nativeGraph`),
		["active", "useHeads"],
		`${label}.nativeGraph`,
	);
	if (
		typeof nativeGraph.active !== "boolean" ||
		typeof nativeGraph.useHeads !== "boolean" ||
		(nativeGraph.active === false && nativeGraph.useHeads !== false)
	) {
		throw new Error(`Benchmark ${label}.nativeGraph contract is invalid`);
	}
	const eagerBlocks = requireExactRecordKeys(
		requireRecord(runtime.eagerBlocks, `${label}.eagerBlocks`),
		["telemetryAvailable", "enabled", "telemetry"],
		`${label}.eagerBlocks`,
	);
	if (
		eagerBlocks.telemetryAvailable !== true ||
		typeof eagerBlocks.enabled !== "boolean"
	) {
		throw new Error(`Benchmark ${label}.eagerBlocks contract is invalid`);
	}
	if (eagerBlocks.enabled) {
		validateEagerTelemetry(
			eagerBlocks.telemetry,
			`${label}.eagerBlocks.telemetry`,
		);
	} else if (eagerBlocks.telemetry !== null) {
		throw new Error(
			`Benchmark ${label}.eagerBlocks disabled state contains telemetry`,
		);
	}
	const pubsub = requireExactRecordKeys(
		requireRecord(runtime.pubsub, `${label}.pubsub`),
		["runtimeSnapshotAvailable", "snapshot", "error"],
		`${label}.pubsub`,
	);
	if (pubsub.runtimeSnapshotAvailable !== true || pubsub.error !== null) {
		throw new Error(
			`Benchmark ${label}.pubsub runtime evidence is unavailable`,
		);
	}
	const pubsubSnapshot = requireExactRecordKeys(
		requireRecord(pubsub.snapshot, `${label}.pubsub.snapshot`),
		["fanout"],
		`${label}.pubsub.snapshot`,
	);
	const fanout = requireExactRecordKeys(
		requireRecord(pubsubSnapshot.fanout, `${label}.pubsub.snapshot.fanout`),
		["root", "node"],
		`${label}.pubsub.snapshot.fanout`,
	);
	for (const tier of ["root", "node"]) {
		const tierSnapshot = requireExactRecordKeys(
			requireRecord(fanout[tier], `${label}.pubsub.snapshot.fanout.${tier}`),
			["uploadLimitBps"],
			`${label}.pubsub.snapshot.fanout.${tier}`,
		);
		requirePositiveSafeInteger(
			tierSnapshot.uploadLimitBps,
			`${label}.pubsub.snapshot.fanout.${tier}.uploadLimitBps`,
		);
	}
	return runtime;
};

const validatePageResourceSnapshot = (
	value,
	role,
	{ setStartedAt, setFinishedAt, setLabel, browserStorageMode },
) => {
	const label = `resourceEvidence.snapshots.${setLabel}.${role}`;
	const page = requireExactRecordKeys(
		requireRecord(value, label),
		["role", "capturedAt", "storage", "runtime"],
		label,
	);
	const capturedAt = requirePositiveSafeInteger(
		page.capturedAt,
		`${label}.capturedAt`,
	);
	if (
		page.role !== role ||
		capturedAt < setStartedAt ||
		capturedAt > setFinishedAt
	) {
		throw new Error(`Benchmark ${label} capture ordering is invalid`);
	}
	validateStorageResourceSnapshot(page.storage, `${label}.storage`, {
		setStartedAt,
		pageCapturedAt: capturedAt,
		browserStorageMode,
	});
	validateRuntimeResourceSnapshot(page.runtime, `${label}.runtime`, {
		setStartedAt,
		pageCapturedAt: capturedAt,
	});
	return page;
};

const validateResourceSnapshotSet = (value, expectedLabel, invocation) => {
	const label = `resourceEvidence.snapshots.${expectedLabel}`;
	const snapshot = requireExactRecordKeys(
		requireRecord(value, label),
		["label", "startedAt", "finishedAt", "writer", "reader"],
		label,
	);
	const startedAt = requirePositiveSafeInteger(
		snapshot.startedAt,
		`${label}.startedAt`,
	);
	const finishedAt = requirePositiveSafeInteger(
		snapshot.finishedAt,
		`${label}.finishedAt`,
	);
	const schedulingToleranceMs = Math.max(250, invocation.pollMs + 250);
	if (
		snapshot.label !== expectedLabel ||
		finishedAt < startedAt ||
		finishedAt - startedAt >
			RESOURCE_SNAPSHOT_TIMEOUT_MS + schedulingToleranceMs
	) {
		throw new Error(
			`Benchmark ${label} capture window is invalid or unbounded`,
		);
	}
	validatePageResourceSnapshot(snapshot.writer, "writer", {
		setStartedAt: startedAt,
		setFinishedAt: finishedAt,
		setLabel: expectedLabel,
		browserStorageMode: invocation.browserStorageMode,
	});
	validatePageResourceSnapshot(snapshot.reader, "reader", {
		setStartedAt: startedAt,
		setFinishedAt: finishedAt,
		setLabel: expectedLabel,
		browserStorageMode: invocation.browserStorageMode,
	});
	return snapshot;
};

const validateResourceRoleSequence = (snapshots, role) => {
	const pages = [
		snapshots.beforeTimedRead[role],
		snapshots.afterSink[role],
		snapshots.beforeSoak[role],
		snapshots.afterSoak[role],
	];
	const firstRuntime = pages[0].runtime;
	const firstOrigin = pages[0].storage.origin;
	for (const page of pages.slice(1)) {
		if (
			page.storage.origin !== firstOrigin ||
			!isDeepStrictEqual(page.runtime.identity, firstRuntime.identity) ||
			!isDeepStrictEqual(page.runtime.nativeGraph, firstRuntime.nativeGraph) ||
			page.runtime.eagerBlocks.telemetryAvailable !==
				firstRuntime.eagerBlocks.telemetryAvailable ||
			page.runtime.eagerBlocks.enabled !== firstRuntime.eagerBlocks.enabled ||
			!isDeepStrictEqual(
				page.runtime.pubsub.snapshot,
				firstRuntime.pubsub.snapshot,
			) ||
			(page.runtime.eagerBlocks.enabled &&
				!isDeepStrictEqual(
					page.runtime.eagerBlocks.telemetry.limits,
					firstRuntime.eagerBlocks.telemetry.limits,
				))
		) {
			throw new Error(
				`Benchmark ${role} runtime provenance changed across resource snapshots`,
			);
		}
	}
	if (!firstRuntime.eagerBlocks.enabled) {
		return;
	}
	for (let index = 1; index < pages.length; index += 1) {
		const before = pages[index - 1].runtime.eagerBlocks.telemetry;
		const after = pages[index].runtime.eagerBlocks.telemetry;
		for (const key of [
			...EAGER_MONOTONIC_COUNTERS,
			"peakEntries",
			"peakBytes",
			"peakPendingEntries",
			"peakPendingBytes",
		]) {
			if (after[key] < before[key]) {
				throw new Error(
					`Benchmark ${role} eager-cache ${key} regressed across resource snapshots`,
				);
			}
		}
	}
};

const buildStorageResourceDelta = (before, after, role) => ({
	role,
	peerbitLogUsageDeltaBytes:
		after.storage.peerbitLog.usageBytes - before.storage.peerbitLog.usageBytes,
	backingStorageUsageDeltaBytes:
		after.storage.backingStorage.usageBytes -
		before.storage.backingStorage.usageBytes,
});

const buildEagerResourceDelta = (before, after) => {
	const beforeTelemetry = before.runtime.eagerBlocks.telemetry;
	const afterTelemetry = after.runtime.eagerBlocks.telemetry;
	if (beforeTelemetry === null && afterTelemetry === null) {
		return null;
	}
	if (beforeTelemetry === null || afterTelemetry === null) {
		throw new Error(
			"Benchmark eager-cache availability changed across resource snapshots",
		);
	}
	return Object.fromEntries(
		EAGER_MONOTONIC_COUNTERS.map((key) => {
			const delta = afterTelemetry[key] - beforeTelemetry[key];
			requireNonNegativeSafeInteger(delta, `resource eager delta ${key}`);
			return [key, delta];
		}),
	);
};

const buildResourceInterval = (before, after) => ({
	from: before.label,
	to: after.label,
	writerStorage: buildStorageResourceDelta(
		before.writer,
		after.writer,
		"writer",
	),
	readerStorage: buildStorageResourceDelta(
		before.reader,
		after.reader,
		"reader",
	),
	writerEager: buildEagerResourceDelta(before.writer, after.writer),
	readerEager: buildEagerResourceDelta(before.reader, after.reader),
});

const validateShutdownOutcome = (
	value,
	role,
	{ terminalSeederCapturedAt, schedulingToleranceMs },
) => {
	const label = `shutdownOutcomes.${role}`;
	const outcome = requireExactRecordKeys(
		requireRecord(value, label),
		[
			"role",
			"status",
			"startedAt",
			"finishedAt",
			"durationMs",
			"programClosed",
			"peerStopped",
			"identity",
			"error",
		],
		label,
	);
	const startedAt = requirePositiveSafeInteger(
		outcome.startedAt,
		`${label}.startedAt`,
	);
	const finishedAt = requirePositiveSafeInteger(
		outcome.finishedAt,
		`${label}.finishedAt`,
	);
	const durationMs = requireNonNegativeSafeInteger(
		outcome.durationMs,
		`${label}.durationMs`,
	);
	const identity = validateRuntimeIdentity(
		outcome.identity,
		`${label}.identity`,
	);
	if (
		outcome.role !== role ||
		outcome.status !== "fulfilled" ||
		outcome.programClosed !== true ||
		outcome.peerStopped !== true ||
		outcome.error !== null ||
		startedAt < terminalSeederCapturedAt ||
		finishedAt < startedAt ||
		durationMs !== finishedAt - startedAt ||
		durationMs > PAGE_SHUTDOWN_TIMEOUT_MS + schedulingToleranceMs
	) {
		throw new Error(`Benchmark ${label} is not a successful bounded shutdown`);
	}
	return { ...outcome, identity };
};

const validateResourceAndShutdownEvidence = (
	result,
	invocation,
	readWindow,
	{ integrityVerifiedAt, terminalTopologyFinishedAt },
) => {
	const evidence = requireExactRecordKeys(
		requireRecord(result.resourceEvidence, "resourceEvidence"),
		[
			"schemaVersion",
			"storageDefinition",
			"eagerDefinition",
			"snapshots",
			"intervals",
		],
		"resourceEvidence",
	);
	if (
		evidence.schemaVersion !== 2 ||
		evidence.storageDefinition !== RESOURCE_STORAGE_DEFINITION ||
		evidence.eagerDefinition !== RESOURCE_EAGER_DEFINITION
	) {
		throw new Error("Benchmark resource evidence contract is invalid");
	}
	const snapshotValues = requireExactRecordKeys(
		requireRecord(evidence.snapshots, "resourceEvidence.snapshots"),
		["beforeTimedRead", "afterSink", "beforeSoak", "afterSoak"],
		"resourceEvidence.snapshots",
	);
	const snapshots = {
		beforeTimedRead: validateResourceSnapshotSet(
			snapshotValues.beforeTimedRead,
			"beforeTimedRead",
			invocation,
		),
		afterSink: validateResourceSnapshotSet(
			snapshotValues.afterSink,
			"afterSink",
			invocation,
		),
		beforeSoak: validateResourceSnapshotSet(
			snapshotValues.beforeSoak,
			"beforeSoak",
			invocation,
		),
		afterSoak: validateResourceSnapshotSet(
			snapshotValues.afterSoak,
			"afterSoak",
			invocation,
		),
	};
	const terminalSnapshot = Array.isArray(result.snapshots)
		? result.snapshots.at(-1)
		: null;
	const terminalSeederCapturedAt = requirePositiveSafeInteger(
		terminalSnapshot?.at,
		"terminal seeder snapshot timestamp",
	);
	const downloadMemoryStartedAt = requirePositiveSafeInteger(
		requireRecord(result.downloadMemoryTelemetry, "downloadMemoryTelemetry")
			.startedAt,
		"downloadMemoryTelemetry.startedAt",
	);
	if (
		downloadMemoryStartedAt > snapshots.beforeTimedRead.startedAt ||
		snapshots.beforeTimedRead.finishedAt > readWindow.downloadStartedAt ||
		snapshots.afterSink.startedAt < readWindow.downloadCompletionObservedAt ||
		snapshots.afterSink.finishedAt > integrityVerifiedAt ||
		snapshots.afterSink.finishedAt > snapshots.beforeSoak.startedAt ||
		snapshots.beforeSoak.startedAt < integrityVerifiedAt ||
		(terminalTopologyFinishedAt !== null &&
			snapshots.beforeSoak.startedAt < terminalTopologyFinishedAt) ||
		snapshots.beforeSoak.finishedAt > readWindow.postTransferSoakStartedAt ||
		readWindow.postTransferSoakFinishedAt > snapshots.afterSoak.startedAt ||
		snapshots.afterSoak.finishedAt > terminalSeederCapturedAt
	) {
		throw new Error(
			"Benchmark resource snapshots, soak, integrity, and terminal evidence are out of order",
		);
	}
	validateResourceRoleSequence(snapshots, "writer");
	validateResourceRoleSequence(snapshots, "reader");
	const writerIdentity = snapshots.beforeTimedRead.writer.runtime.identity;
	const readerIdentity = snapshots.beforeTimedRead.reader.runtime.identity;
	if (
		writerIdentity.programAddress !== readerIdentity.programAddress ||
		writerIdentity.peerId === readerIdentity.peerId ||
		writerIdentity.peerHash === readerIdentity.peerHash ||
		writerIdentity.sessionId === readerIdentity.sessionId
	) {
		throw new Error(
			"Benchmark resource evidence does not identify two distinct peers in one program",
		);
	}
	const expectedIntervals = {
		timedReadEnvelope: buildResourceInterval(
			snapshots.beforeTimedRead,
			snapshots.afterSink,
		),
		postTransferWork: buildResourceInterval(
			snapshots.afterSink,
			snapshots.beforeSoak,
		),
		soak: buildResourceInterval(snapshots.beforeSoak, snapshots.afterSoak),
		total: buildResourceInterval(
			snapshots.beforeTimedRead,
			snapshots.afterSoak,
		),
	};
	if (!isDeepStrictEqual(evidence.intervals, expectedIntervals)) {
		throw new Error(
			"Benchmark resource interval deltas contradict their snapshots",
		);
	}
	for (const interval of Object.values(expectedIntervals)) {
		for (const role of ["writerStorage", "readerStorage"]) {
			requireSafeInteger(
				interval[role].peerbitLogUsageDeltaBytes,
				`resourceEvidence.intervals.${interval.from}-${interval.to}.${role}.peerbitLogUsageDeltaBytes`,
			);
			requireSafeInteger(
				interval[role].backingStorageUsageDeltaBytes,
				`resourceEvidence.intervals.${interval.from}-${interval.to}.${role}.backingStorageUsageDeltaBytes`,
			);
		}
	}
	const shutdownOutcomes = requireExactRecordKeys(
		requireRecord(result.shutdownOutcomes, "shutdownOutcomes"),
		["writer", "reader"],
		"shutdownOutcomes",
	);
	const schedulingToleranceMs = Math.max(250, invocation.pollMs + 250);
	const writer = validateShutdownOutcome(shutdownOutcomes.writer, "writer", {
		terminalSeederCapturedAt,
		schedulingToleranceMs,
	});
	const reader = validateShutdownOutcome(shutdownOutcomes.reader, "reader", {
		terminalSeederCapturedAt,
		schedulingToleranceMs,
	});
	if (
		!isDeepStrictEqual(writer.identity, writerIdentity) ||
		!isDeepStrictEqual(reader.identity, readerIdentity)
	) {
		throw new Error(
			"Benchmark shutdown identities do not match their resource snapshot sessions",
		);
	}
	return {
		afterSoakStartedAt: snapshots.afterSoak.startedAt,
		shutdownStartedAt: Math.min(writer.startedAt, reader.startedAt),
		shutdownFinishedAt: Math.max(writer.finishedAt, reader.finishedAt),
	};
};

const validateIdleDownloadScheduler = (value, label) => {
	const scheduler = requireRecord(value, label);
	for (const key of ["activeCount", "activeBytes", "queuedCount"]) {
		if (
			requireNonNegativeSafeInteger(scheduler[key], `${label}.${key}`) !== 0
		) {
			throw new Error(`${label} is not idle`);
		}
	}
};

const validateExactChunkIndexSet = (value, chunkCount, label) => {
	if (!Array.isArray(value)) {
		throw new Error(`${label} must be an array`);
	}
	let previous = -1;
	for (const [offset, index] of value.entries()) {
		if (
			!Number.isSafeInteger(index) ||
			index < 0 ||
			index >= chunkCount ||
			index <= previous
		) {
			throw new Error(`${label}[${offset}] is not a sorted unique chunk index`);
		}
		previous = index;
	}
	return value;
};

const validateLocalChunkPrefixObservation = (value, label) => {
	const observation = requireRecord(value, label);
	const capturedAt = requirePositiveSafeInteger(
		observation.capturedAt,
		`${label}.capturedAt`,
	);
	const fileId = requireString(observation.fileId, `${label}.fileId`);
	const chunkCount = requirePositiveSafeInteger(
		observation.chunkCount,
		`${label}.chunkCount`,
	);
	const indexRowCount = requireNonNegativeSafeInteger(
		observation.indexRowCount,
		`${label}.indexRowCount`,
	);
	const blockCount = requireNonNegativeSafeInteger(
		observation.blockCount,
		`${label}.blockCount`,
	);
	const indexedChunkIndices = validateExactChunkIndexSet(
		observation.indexedChunkIndices,
		chunkCount,
		`${label}.indexedChunkIndices`,
	);
	const blockChunkIndices = validateExactChunkIndexSet(
		observation.blockChunkIndices,
		chunkCount,
		`${label}.blockChunkIndices`,
	);
	if (
		indexRowCount !== indexedChunkIndices.length ||
		blockCount !== blockChunkIndices.length ||
		typeof observation.persistChunkReads !== "boolean" ||
		!Array.isArray(observation.activeTransfers) ||
		observation.activeTransfers.length !== 0
	) {
		throw new Error(`${label} has inconsistent or non-idle locality evidence`);
	}
	validateIdleDownloadScheduler(
		observation.downloadScheduler,
		`${label}.downloadScheduler`,
	);
	return {
		observation,
		capturedAt,
		fileId,
		chunkCount,
		indexRowCount,
		blockCount,
		indexedChunkIndices,
		blockChunkIndices,
		persistChunkReads: observation.persistChunkReads,
	};
};

const validateTopologyEvidence = (value, label) => {
	const topology = requireRecord(value, label);
	const capturedAt = requirePositiveSafeInteger(
		topology.capturedAt,
		`${label}.capturedAt`,
	);
	const peerHash = requireString(topology.peerHash, `${label}.peerHash`);
	const peerId = requireString(topology.peerId, `${label}.peerId`);
	const replicatorCount = requireNonNegativeSafeInteger(
		topology.replicatorCount,
		`${label}.replicatorCount`,
	);
	if (
		!Array.isArray(topology.replicatorHashes) ||
		topology.replicatorHashes.length !== replicatorCount
	) {
		throw new Error(
			`${label} does not contain the exact replicator identities`,
		);
	}
	const replicatorHashes = topology.replicatorHashes.map((hash, index) =>
		requireString(hash, `${label}.replicatorHashes[${index}]`),
	);
	if (
		typeof topology.selfInReplicatorSet !== "boolean" ||
		!isDeepStrictEqual(
			replicatorHashes,
			[...replicatorHashes].toSorted((left, right) =>
				left.localeCompare(right),
			),
		) ||
		new Set(replicatorHashes).size !== replicatorHashes.length
	) {
		throw new Error(`${label} contains inconsistent topology evidence`);
	}
	return {
		observation: topology,
		capturedAt,
		peerHash,
		peerId,
		replicatorCount,
		replicatorHashes,
		selfInReplicatorSet: topology.selfInReplicatorSet,
	};
};

const summarizeCounterpartPubsubTransport = (
	topology,
	{ direction, expectedPeerHash, expectedRemotePeerId, label },
) => {
	if (!Array.isArray(topology.observation.transportStreams)) {
		throw new Error(
			`Benchmark ${label} is missing transport stream diagnostics`,
		);
	}
	if (
		topology.observation.transportStreams.some((stream) => !isRecord(stream))
	) {
		throw new Error(
			`Benchmark ${label} contains malformed transport stream diagnostics`,
		);
	}
	const streams = topology.observation.transportStreams.filter(
		(stream) =>
			stream.service === "pubsub" &&
			stream.direction === direction &&
			(stream.remotePeerHash === expectedPeerHash ||
				stream.remotePeer === expectedRemotePeerId),
	);
	if (streams.length === 0) {
		throw new Error(
			`Benchmark ${label} has no relevant counterpart pubsub stream`,
		);
	}
	const counters = new Map();
	let totalBytes = 0;
	for (const [index, stream] of streams.entries()) {
		if (
			stream.remotePeerHash !== expectedPeerHash ||
			stream.remotePeer !== expectedRemotePeerId ||
			stream.peerHashIdentityMatch !== true ||
			stream.serviceProtocol !== PUBSUB_PROTOCOL ||
			stream.expectedProtocol !== PUBSUB_PROTOCOL ||
			stream.protocol !== PUBSUB_PROTOCOL ||
			stream.protocolIdentityMatch !== true ||
			stream.counterStreamIdentityMatch !== true ||
			stream.connectionIdentityMatchCount !== 1 ||
			typeof stream.connectionId !== "string" ||
			stream.connectionId.length === 0 ||
			typeof stream.multiplexer !== "string" ||
			stream.multiplexer.length === 0 ||
			typeof stream.id !== "string" ||
			stream.id.length === 0 ||
			!Number.isSafeInteger(stream.bytes) ||
			stream.bytes < 0 ||
			(direction === "outbound"
				? stream.aborted !== false
				: stream.aborted !== null)
		) {
			throw new Error(
				`Benchmark ${label} relevant pubsub stream ${index} is not authoritative`,
			);
		}
		const key = JSON.stringify([
			stream.service,
			stream.remotePeerHash,
			stream.remotePeer,
			stream.direction,
			stream.connectionId,
			stream.id,
			stream.multiplexer,
			stream.protocol,
		]);
		if (counters.has(key)) {
			throw new Error(
				`Benchmark ${label} contains a duplicate pubsub counter key`,
			);
		}
		counters.set(key, stream.bytes);
		totalBytes += stream.bytes;
	}
	if (!Number.isSafeInteger(totalBytes)) {
		throw new Error(`Benchmark ${label} has invalid pubsub counter totals`);
	}
	return {
		counters: [...counters.entries()]
			.map(([key, bytes]) => ({ key, bytes }))
			.toSorted((left, right) => left.key.localeCompare(right.key)),
		streamCount: streams.length,
		totalBytes,
	};
};

const requireMonotonicTransportCounterDelta = (before, after, label) => {
	const beforeKeys = before.counters.map(({ key }) => key);
	const afterKeys = after.counters.map(({ key }) => key);
	if (!isDeepStrictEqual(beforeKeys, afterKeys)) {
		throw new Error(
			`Benchmark ${label} pubsub counter key set changed during timed read`,
		);
	}
	for (const [index, afterCounter] of after.counters.entries()) {
		if (afterCounter.bytes < before.counters[index].bytes) {
			throw new Error(
				`Benchmark ${label} pubsub counter decreased during timed read`,
			);
		}
	}
	return after.totalBytes - before.totalBytes;
};

const validateTransportTopologyStability = ({
	control,
	phase,
	writerTopology,
	readerTopology,
	expectedWriterPeerHash,
	expectedReaderPeerHash,
	expectedWriterPeerId,
	expectedReaderPeerId,
	latestFinishedAt = null,
}) => {
	const phaseLabel = `${phase}TimedReadTopology`;
	const startedAt = requirePositiveSafeInteger(
		control[`${phaseLabel}StartedAt`],
		`readerLocalityControl.${phaseLabel}StartedAt`,
	);
	const deadlineAt = requirePositiveSafeInteger(
		control[`${phaseLabel}DeadlineAt`],
		`readerLocalityControl.${phaseLabel}DeadlineAt`,
	);
	const finishedAt = requirePositiveSafeInteger(
		control[`${phaseLabel}FinishedAt`],
		`readerLocalityControl.${phaseLabel}FinishedAt`,
	);
	const observations = control[`${phaseLabel}Observations`];
	if (latestFinishedAt !== null && finishedAt > latestFinishedAt) {
		throw new Error(
			`Benchmark ${phaseLabel} exceeded its bounded post-read capture delay`,
		);
	}
	const expectedDeadlineAt =
		latestFinishedAt === null
			? startedAt + TRANSPORT_COUNTER_STABILITY_TIMEOUT_MS
			: Math.min(
					startedAt + TRANSPORT_COUNTER_STABILITY_TIMEOUT_MS,
					latestFinishedAt,
				);
	const maxObservations =
		Math.floor(
			Math.max(0, deadlineAt - startedAt) /
				(TRANSPORT_COUNTER_STABILITY_POLL_MS -
					TRANSPORT_COUNTER_SAMPLE_CLOCK_TOLERANCE_MS),
		) + 1;
	if (
		deadlineAt !== expectedDeadlineAt ||
		deadlineAt <= startedAt ||
		finishedAt < startedAt ||
		finishedAt > deadlineAt ||
		!Array.isArray(observations) ||
		observations.length < TRANSPORT_COUNTER_STABLE_SAMPLE_COUNT ||
		observations.length > maxObservations
	) {
		throw new Error(
			`Benchmark ${phaseLabel} stability window is invalid or unbounded`,
		);
	}
	let previous = null;
	const validated = observations.map((value, index) => {
		const label = `readerLocalityControl.${phaseLabel}Observations[${index}]`;
		const observation = requireRecord(value, label);
		const capturedAt = requirePositiveSafeInteger(
			observation.capturedAt,
			`${label}.capturedAt`,
		);
		const writer = validateTopologyEvidence(
			observation.writerTopology,
			`${label}.writerTopology`,
		);
		const reader = validateTopologyEvidence(
			observation.readerTopology,
			`${label}.readerTopology`,
		);
		if (
			writer.peerHash !== expectedWriterPeerHash ||
			reader.peerHash !== expectedReaderPeerHash ||
			writer.peerId !== expectedWriterPeerId ||
			reader.peerId !== expectedReaderPeerId ||
			writer.capturedAt < startedAt ||
			reader.capturedAt < startedAt ||
			writer.capturedAt > capturedAt ||
			reader.capturedAt > capturedAt ||
			capturedAt > finishedAt ||
			(index > 0 &&
				capturedAt - observations[index - 1].capturedAt <
					TRANSPORT_COUNTER_STABILITY_POLL_MS -
						TRANSPORT_COUNTER_SAMPLE_CLOCK_TOLERANCE_MS)
		) {
			throw new Error(
				`Benchmark ${phaseLabel} observations have inconsistent identities or timestamps`,
			);
		}
		const writerSummary = summarizeCounterpartPubsubTransport(writer, {
			direction: "outbound",
			expectedPeerHash: expectedReaderPeerHash,
			expectedRemotePeerId: expectedReaderPeerId,
			label: `${label}.writerTopology`,
		});
		const readerSummary = summarizeCounterpartPubsubTransport(reader, {
			direction: "inbound",
			expectedPeerHash: expectedWriterPeerHash,
			expectedRemotePeerId: expectedWriterPeerId,
			label: `${label}.readerTopology`,
		});
		if (previous) {
			for (const side of ["writer", "reader"]) {
				const current = side === "writer" ? writerSummary : readerSummary;
				const previousKeys = previous[side].counters.map(({ key }) => key);
				const currentKeys = current.counters.map(({ key }) => key);
				if (isDeepStrictEqual(previousKeys, currentKeys)) {
					for (const [counterIndex, counter] of current.counters.entries()) {
						if (counter.bytes < previous[side].counters[counterIndex].bytes) {
							throw new Error(
								`Benchmark ${phaseLabel} counters decrease for an unchanged key set`,
							);
						}
					}
				}
			}
		}
		previous = { writer: writerSummary, reader: readerSummary };
		return {
			observation,
			capturedAt,
			writer,
			reader,
			writerSummary,
			readerSummary,
		};
	});
	const stable = validated.slice(-TRANSPORT_COUNTER_STABLE_SAMPLE_COUNT);
	const stableSignature = (entry) => ({
		writer: entry.writerSummary,
		reader: entry.readerSummary,
	});
	if (
		stable.some(
			(entry) =>
				Math.abs(
					entry.writerSummary.totalBytes - entry.readerSummary.totalBytes,
				) > TRANSPORT_COUNTER_MAX_COUNTERPART_BYTE_SKEW,
		) ||
		stable
			.slice(1)
			.some(
				(entry) =>
					!isDeepStrictEqual(
						stableSignature(entry),
						stableSignature(stable[0]),
					),
			) ||
		!isDeepStrictEqual(
			validated.at(-1).observation.writerTopology,
			writerTopology.observation,
		) ||
		!isDeepStrictEqual(
			validated.at(-1).observation.readerTopology,
			readerTopology.observation,
		)
	) {
		throw new Error(
			`Benchmark ${phaseLabel} observations do not prove stable counterpart pubsub counters`,
		);
	}
	return {
		startedAt,
		deadlineAt,
		finishedAt,
		observations: validated,
		writerSummary: stable.at(-1).writerSummary,
		readerSummary: stable.at(-1).readerSummary,
	};
};

const validateTopology = (
	value,
	label,
	expectedSelfInReplicatorSet,
	expectedReplicatorCount = 1,
) => {
	const topology = validateTopologyEvidence(value, label);
	if (
		topology.replicatorCount !== expectedReplicatorCount ||
		topology.selfInReplicatorSet !== expectedSelfInReplicatorSet
	) {
		throw new Error(`${label} does not prove the requested topology role`);
	}
	return topology;
};

const validateReaderLocalityControl = (result, invocation) => {
	const target = invocation.readerLocalChunkTarget;
	const maxOvershoot = invocation.readerLocalChunkMaxOvershoot;
	const expectedTerminalTopology = invocation.readerTerminalTopology;
	const persistChunkReads = invocation.readerPersistChunkReads;
	if (target == null) {
		if (
			maxOvershoot !== null ||
			expectedTerminalTopology !== null ||
			persistChunkReads !== null ||
			result.readerLocalityControl !== null ||
			result.readerLocalChunkBlockCount !== null ||
			result.readerLocalChunkIndexRowCount !== null ||
			result.readerPersistChunkReads !== null ||
			result.readerLocalityCohortKey !== null
		) {
			throw new Error(
				"Benchmark result contains unrequested reader locality control evidence",
			);
		}
		return null;
	}
	if (
		!Number.isSafeInteger(maxOvershoot) ||
		maxOvershoot < 0 ||
		!["observer", "replicator"].includes(expectedTerminalTopology) ||
		typeof persistChunkReads !== "boolean" ||
		invocation.mode !== "fixed1" ||
		invocation.minReadySeeders !== 1
	) {
		throw new Error("Benchmark reader locality invocation is invalid");
	}
	if (
		persistChunkReads === false &&
		(target !== 0 || expectedTerminalTopology !== "observer")
	) {
		throw new Error(
			"Benchmark transient reader invocation requires a zero prefix and observer topology",
		);
	}
	const control = requireRecord(
		result.readerLocalityControl,
		"readerLocalityControl",
	);
	if (
		control.profile !== "observer-topology-exact-manifest-prefix" ||
		control.provisioningMethod !== "exact-manifest-head-import" ||
		control.requestedLocalChunkBlockCount !== target ||
		control.maxSpeculativeOvershootChunkCount !== maxOvershoot ||
		control.countMetric !==
			"exact local Documents index rows and manifest entry blocks" ||
		control.writerUploadRole !== "fixed1" ||
		control.readerUploadRole !== "observer" ||
		control.readerTimedReadPolicy !==
			(persistChunkReads ? "persist-chunk-reads" : "transient-chunk-reads") ||
		control.expectedTerminalTopology !== expectedTerminalTopology ||
		control.stabilityPollIntervalMs !== Math.min(invocation.pollMs, 100) ||
		control.requiredStableObservationCount !== 3 ||
		control.transportCounterStabilityPollIntervalMs !==
			TRANSPORT_COUNTER_STABILITY_POLL_MS ||
		control.transportCounterStabilityTimeoutMs !==
			TRANSPORT_COUNTER_STABILITY_TIMEOUT_MS ||
		control.transportCounterRequiredStableObservationCount !==
			TRANSPORT_COUNTER_STABLE_SAMPLE_COUNT ||
		control.transportCounterMaxCounterpartByteSkew !==
			TRANSPORT_COUNTER_MAX_COUNTERPART_BYTE_SKEW ||
		control.transportCounterSampleClockToleranceMs !==
			TRANSPORT_COUNTER_SAMPLE_CLOCK_TOLERANCE_MS ||
		control.transportCounterPreReadStartToleranceMs !==
			TRANSPORT_COUNTER_PRE_READ_START_TOLERANCE_MS ||
		control.transportCounterPostReadCaptureMaxDelayMs !==
			TRANSPORT_COUNTER_POST_READ_CAPTURE_MAX_DELAY_MS ||
		control.status !== "complete" ||
		control.failure !== null
	) {
		throw new Error("Benchmark reader locality control contract is invalid");
	}
	const timestamps = requireRecord(result.timestamps, "timestamps");
	const uploadStartedAt = requirePositiveSafeInteger(
		timestamps.uploadStartedAt,
		"timestamps.uploadStartedAt",
	);
	const postMonitorFinishedAt = requirePositiveSafeInteger(
		timestamps.postMonitorFinishedAt,
		"timestamps.postMonitorFinishedAt",
	);
	const downloadStartedAt = requirePositiveSafeInteger(
		timestamps.downloadStartedAt,
		"timestamps.downloadStartedAt",
	);
	const downloadFinishedAt = requirePositiveSafeInteger(
		timestamps.downloadFinishedAt,
		"timestamps.downloadFinishedAt",
	);
	const downloadCompletionObservedAt = requirePositiveSafeInteger(
		timestamps.downloadCompletionObservedAt,
		"timestamps.downloadCompletionObservedAt",
	);
	const initialRoleEvidence = requireRecord(
		control.readerInitialRoleEvidence,
		"readerLocalityControl.readerInitialRoleEvidence",
	);
	const initialRoleCapturedAt = requirePositiveSafeInteger(
		initialRoleEvidence.capturedAt,
		"readerLocalityControl.readerInitialRoleEvidence.capturedAt",
	);
	const initialProgramAddress = requireString(
		initialRoleEvidence.programAddress,
		"readerLocalityControl.readerInitialRoleEvidence.programAddress",
	);
	if (
		initialRoleEvidence.persistChunkReads !== false ||
		initialRoleEvidence.initialRole !== "observer" ||
		initialRoleEvidence.updateRoleCount !== 0 ||
		initialRoleEvidence.lastAppliedRole !== null
	) {
		throw new Error(
			"Benchmark reader locality did not initialize the reader as an observer",
		);
	}
	const writerTopologyBeforeUpload = validateTopology(
		control.writerTopologyBeforeUpload,
		"readerLocalityControl.writerTopologyBeforeUpload",
		true,
	);
	const readerTopologyBeforeUpload = validateTopology(
		control.readerTopologyBeforeUpload,
		"readerLocalityControl.readerTopologyBeforeUpload",
		false,
	);
	const writerTopologyBeforeTimedRead = validateTopology(
		control.writerTopologyBeforeTimedRead,
		"readerLocalityControl.writerTopologyBeforeTimedRead",
		true,
	);
	const readerTopologyBeforeTimedRead = validateTopology(
		control.readerTopologyBeforeTimedRead,
		"readerLocalityControl.readerTopologyBeforeTimedRead",
		false,
	);
	const writerTopologyAfterTimedRead = validateTopologyEvidence(
		control.writerTopologyAfterTimedRead,
		"readerLocalityControl.writerTopologyAfterTimedRead",
	);
	const readerTopologyAfterTimedRead = validateTopologyEvidence(
		control.readerTopologyAfterTimedRead,
		"readerLocalityControl.readerTopologyAfterTimedRead",
	);
	if (
		writerTopologyBeforeUpload.replicatorCount !==
			readerTopologyBeforeUpload.replicatorCount ||
		writerTopologyBeforeTimedRead.replicatorCount !==
			readerTopologyBeforeTimedRead.replicatorCount ||
		writerTopologyBeforeUpload.peerHash ===
			readerTopologyBeforeUpload.peerHash ||
		writerTopologyBeforeTimedRead.peerHash ===
			readerTopologyBeforeTimedRead.peerHash ||
		writerTopologyBeforeUpload.peerHash !==
			writerTopologyBeforeTimedRead.peerHash ||
		readerTopologyBeforeUpload.peerHash !==
			readerTopologyBeforeTimedRead.peerHash ||
		writerTopologyBeforeUpload.peerHash !==
			writerTopologyAfterTimedRead.peerHash ||
		readerTopologyBeforeUpload.peerHash !==
			readerTopologyAfterTimedRead.peerHash ||
		writerTopologyBeforeUpload.peerId === readerTopologyBeforeUpload.peerId ||
		writerTopologyBeforeUpload.peerId !==
			writerTopologyBeforeTimedRead.peerId ||
		readerTopologyBeforeUpload.peerId !==
			readerTopologyBeforeTimedRead.peerId ||
		writerTopologyBeforeUpload.peerId !== writerTopologyAfterTimedRead.peerId ||
		readerTopologyBeforeUpload.peerId !== readerTopologyAfterTimedRead.peerId
	) {
		throw new Error(
			"Benchmark topology evidence does not preserve two distinct peer identities",
		);
	}
	const expectedReplicatorHashes = [writerTopologyBeforeUpload.peerHash];
	if (
		[
			writerTopologyBeforeUpload,
			readerTopologyBeforeUpload,
			writerTopologyBeforeTimedRead,
			readerTopologyBeforeTimedRead,
		].some(
			(topology) =>
				!isDeepStrictEqual(topology.replicatorHashes, expectedReplicatorHashes),
		)
	) {
		throw new Error(
			"Benchmark topology evidence does not agree on the writer as the exact singleton replicator",
		);
	}
	const postTimedReadSingleton = [writerTopologyBeforeUpload.peerHash];
	const postTimedReadPair = [
		writerTopologyBeforeUpload.peerHash,
		readerTopologyBeforeUpload.peerHash,
	].toSorted((left, right) => left.localeCompare(right));
	if (
		!isDeepStrictEqual(
			writerTopologyAfterTimedRead.replicatorHashes,
			readerTopologyAfterTimedRead.replicatorHashes,
		) ||
		writerTopologyAfterTimedRead.selfInReplicatorSet !== true ||
		readerTopologyAfterTimedRead.selfInReplicatorSet !==
			readerTopologyAfterTimedRead.replicatorHashes.includes(
				readerTopologyAfterTimedRead.peerHash,
			) ||
		![postTimedReadSingleton, postTimedReadPair].some((expectedHashes) =>
			isDeepStrictEqual(
				writerTopologyAfterTimedRead.replicatorHashes,
				expectedHashes,
			),
		)
	) {
		throw new Error(
			"Benchmark immediate post-read topology evidence is inconsistent",
		);
	}
	const preTimedReadTransportStability = validateTransportTopologyStability({
		control,
		phase: "pre",
		writerTopology: writerTopologyBeforeTimedRead,
		readerTopology: readerTopologyBeforeTimedRead,
		expectedWriterPeerHash: writerTopologyBeforeUpload.peerHash,
		expectedReaderPeerHash: readerTopologyBeforeUpload.peerHash,
		expectedWriterPeerId: writerTopologyBeforeUpload.peerId,
		expectedReaderPeerId: readerTopologyBeforeUpload.peerId,
	});
	const postTimedReadTransportStability = validateTransportTopologyStability({
		control,
		phase: "post",
		writerTopology: writerTopologyAfterTimedRead,
		readerTopology: readerTopologyAfterTimedRead,
		expectedWriterPeerHash: writerTopologyBeforeUpload.peerHash,
		expectedReaderPeerHash: readerTopologyBeforeUpload.peerHash,
		expectedWriterPeerId: writerTopologyBeforeUpload.peerId,
		expectedReaderPeerId: readerTopologyBeforeUpload.peerId,
		latestFinishedAt:
			downloadCompletionObservedAt +
			TRANSPORT_COUNTER_POST_READ_CAPTURE_MAX_DELAY_MS,
	});
	const postTimedReadTopologyCaptureDelayMs = requireNonNegativeSafeInteger(
		control.postTimedReadTopologyCaptureDelayMs,
		"readerLocalityControl.postTimedReadTopologyCaptureDelayMs",
	);
	if (
		postTimedReadTopologyCaptureDelayMs !==
			postTimedReadTransportStability.finishedAt -
				downloadCompletionObservedAt ||
		postTimedReadTopologyCaptureDelayMs >
			TRANSPORT_COUNTER_POST_READ_CAPTURE_MAX_DELAY_MS
	) {
		throw new Error(
			"Benchmark post-timed-read topology capture delay is inconsistent or unbounded",
		);
	}
	const writerCounterDelta = requireMonotonicTransportCounterDelta(
		preTimedReadTransportStability.writerSummary,
		postTimedReadTransportStability.writerSummary,
		"writer",
	);
	const readerCounterDelta = requireMonotonicTransportCounterDelta(
		preTimedReadTransportStability.readerSummary,
		postTimedReadTransportStability.readerSummary,
		"reader",
	);
	if (
		Math.abs(writerCounterDelta - readerCounterDelta) >
		TRANSPORT_COUNTER_MAX_COUNTERPART_BYTE_SKEW
	) {
		throw new Error(
			"Benchmark writer outbound and reader inbound pubsub counter deltas exceed the byte-skew bound",
		);
	}
	const beforePreload = validateLocalChunkPrefixObservation(
		control.beforePreloadObservation,
		"readerLocalityControl.beforePreloadObservation",
	);
	const readerManifest = requireRecord(
		result.readerManifestEvidence,
		"readerManifestEvidence",
	);
	const manifestFileId = requireString(
		readerManifest.fileId,
		"readerManifestEvidence.fileId",
	);
	const canonicalChunkCount = requirePositiveSafeInteger(
		requireRecord(result.readTransfer, "readTransfer").chunkCount,
		"readTransfer.chunkCount",
	);
	if (beforePreload.chunkCount !== canonicalChunkCount) {
		throw new Error(
			"Benchmark reader locality manifest chunk count contradicts the canonical completed read",
		);
	}
	if (target >= canonicalChunkCount) {
		throw new Error(
			"Benchmark reader locality target must be a partial prefix of the canonical completed read",
		);
	}
	if (
		beforePreload.fileId !== manifestFileId ||
		beforePreload.indexRowCount !== 0 ||
		beforePreload.blockCount !== 0 ||
		beforePreload.indexedChunkIndices.length !== 0 ||
		beforePreload.blockChunkIndices.length !== 0 ||
		beforePreload.persistChunkReads !== false
	) {
		throw new Error(
			"Benchmark reader locality did not begin as an empty observer cache",
		);
	}
	const preload = requireRecord(
		control.preloadEvidence,
		"readerLocalityControl.preloadEvidence",
	);
	const preloadStartedAt = requirePositiveSafeInteger(
		preload.startedAt,
		"readerLocalityControl.preloadEvidence.startedAt",
	);
	const preloadFinishedAt = requirePositiveSafeInteger(
		preload.finishedAt,
		"readerLocalityControl.preloadEvidence.finishedAt",
	);
	const rawFetchedByteCount = requireNonNegativeSafeInteger(
		preload.rawFetchedByteCount,
		"readerLocalityControl.preloadEvidence.rawFetchedByteCount",
	);
	const expectedImportedIndices = Array.from(
		{ length: target },
		(_, index) => index,
	);
	if (
		preload.fileId !== manifestFileId ||
		preload.provisioningMethod !== "exact-manifest-head-import" ||
		preload.transferId !== null ||
		preload.readDiagnostics !== null ||
		preload.requestedManifestEntryCount !== target ||
		preload.importedManifestEntryCount !== target ||
		!isDeepStrictEqual(
			preload.importedManifestEntryIndices,
			expectedImportedIndices,
		) ||
		!isDeepStrictEqual(
			preload.localManifestEntryIndicesAfter,
			expectedImportedIndices,
		) ||
		preload.maxConcurrentImports !== 8 ||
		preload.persistChunkReads !== persistChunkReads ||
		!Array.isArray(preload.activeTransfersAfterClose) ||
		preload.activeTransfersAfterClose.length !== 0 ||
		preloadFinishedAt < preloadStartedAt
	) {
		throw new Error("Benchmark reader locality preload evidence is invalid");
	}
	validateIdleDownloadScheduler(
		preload.downloadSchedulerAfterClose,
		"readerLocalityControl.preloadEvidence.downloadSchedulerAfterClose",
	);
	if (preload.aggregateTimedOut !== false) {
		throw new Error(
			"Benchmark reader locality preload aggregate deadline evidence is invalid",
		);
	}
	if (target === 0) {
		if (
			preload.aggregateTimeoutMs !== null ||
			preload.aggregateDeadlineAt !== null ||
			rawFetchedByteCount !== 0
		) {
			throw new Error("Zero-prefix locality preload imported a manifest entry");
		}
	} else {
		const aggregateTimeoutMs = requirePositiveSafeInteger(
			preload.aggregateTimeoutMs,
			"readerLocalityControl.preloadEvidence.aggregateTimeoutMs",
		);
		const aggregateDeadlineAt = requirePositiveSafeInteger(
			preload.aggregateDeadlineAt,
			"readerLocalityControl.preloadEvidence.aggregateDeadlineAt",
		);
		const aggregateSchedulingToleranceMs = Math.max(
			5_000,
			invocation.pollMs + 1_000,
		);
		if (
			aggregateTimeoutMs !== invocation.downloadTimeoutMs ||
			aggregateDeadlineAt !== preloadStartedAt + aggregateTimeoutMs ||
			preloadFinishedAt - preloadStartedAt >
				aggregateTimeoutMs + aggregateSchedulingToleranceMs ||
			rawFetchedByteCount === 0
		) {
			throw new Error(
				"Benchmark reader locality preload aggregate deadline evidence is invalid",
			);
		}
	}
	if (
		!Array.isArray(control.stabilityObservations) ||
		control.stabilityObservations.length !== 3
	) {
		throw new Error(
			"Benchmark reader locality requires exactly three stable observations",
		);
	}
	const stable = control.stabilityObservations.map((observation, index) =>
		validateLocalChunkPrefixObservation(
			observation,
			`readerLocalityControl.stabilityObservations[${index}]`,
		),
	);
	const actualLocalChunkBlockCount = requireNonNegativeSafeInteger(
		control.actualLocalChunkBlockCount,
		"readerLocalityControl.actualLocalChunkBlockCount",
	);
	const actualLocalChunkIndexRowCount = requireNonNegativeSafeInteger(
		control.actualLocalChunkIndexRowCount,
		"readerLocalityControl.actualLocalChunkIndexRowCount",
	);
	const expectedBlockPrefix = Array.from(
		{ length: actualLocalChunkBlockCount },
		(_, index) => index,
	);
	const expectedIndexPrefix = Array.from(
		{ length: actualLocalChunkIndexRowCount },
		(_, index) => index,
	);
	for (const [index, observation] of stable.entries()) {
		if (
			observation.fileId !== manifestFileId ||
			observation.chunkCount !== canonicalChunkCount ||
			observation.indexRowCount !== actualLocalChunkIndexRowCount ||
			observation.blockCount !== actualLocalChunkBlockCount ||
			observation.persistChunkReads !== persistChunkReads ||
			!isDeepStrictEqual(
				observation.indexedChunkIndices,
				expectedIndexPrefix,
			) ||
			!isDeepStrictEqual(observation.blockChunkIndices, expectedBlockPrefix) ||
			(index > 0 &&
				observation.capturedAt - stable[index - 1].capturedAt <
					control.stabilityPollIntervalMs)
		) {
			throw new Error(
				"Benchmark stable locality observations do not prove one exact prefix",
			);
		}
	}
	const preDownload = validateLocalChunkPrefixObservation(
		control.preDownloadObservation,
		"readerLocalityControl.preDownloadObservation",
	);
	const expectedCohortKey = `observer-${persistChunkReads ? "persistent" : "transient"}-${invocation.browserStorageMode}-prefix-b${actualLocalChunkBlockCount}-i${actualLocalChunkIndexRowCount}`;
	if (
		actualLocalChunkBlockCount < target ||
		actualLocalChunkBlockCount > target + maxOvershoot ||
		actualLocalChunkBlockCount >= canonicalChunkCount ||
		actualLocalChunkIndexRowCount > actualLocalChunkBlockCount ||
		control.speculativeOvershootChunkCount !==
			actualLocalChunkBlockCount - target ||
		control.cohortKey !== expectedCohortKey ||
		result.readerLocalChunkBlockCount !== actualLocalChunkBlockCount ||
		result.readerLocalChunkIndexRowCount !== actualLocalChunkIndexRowCount ||
		result.readerPersistChunkReads !== persistChunkReads ||
		result.readerLocalityCohortKey !== expectedCohortKey ||
		preDownload.chunkCount !== canonicalChunkCount ||
		!isDeepStrictEqual(preDownload.observation, stable.at(-1).observation)
	) {
		throw new Error(
			"Benchmark reader locality cohort count or key is inconsistent",
		);
	}
	const readerDiagnostics = requireRecord(
		result.readerDiagnostics,
		"readerDiagnostics",
	);
	const readerTimings = requireRecord(
		readerDiagnostics.timings,
		"readerDiagnostics.timings",
	);
	const timedRead = requireRecord(
		readerDiagnostics.lastReadDiagnostics,
		"readerDiagnostics.lastReadDiagnostics",
	);
	if (
		readerDiagnostics.persistChunkReads !== persistChunkReads ||
		readerDiagnostics.programAddress !== initialProgramAddress ||
		readerTimings.initialRole !== initialRoleEvidence.initialRole ||
		readerTimings.updateRoleCount !== initialRoleEvidence.updateRoleCount ||
		readerTimings.lastAppliedRole !== initialRoleEvidence.lastAppliedRole
	) {
		throw new Error(
			"Benchmark reader diagnostics contradict its initial observer-role evidence",
		);
	}
	const expectedInitialDiagnosticIndexRowCount = persistChunkReads
		? actualLocalChunkIndexRowCount
		: null;
	const expectedInitialDiagnosticBlockCount = persistChunkReads
		? actualLocalChunkBlockCount
		: null;
	if (
		timedRead.persistChunkReads !== persistChunkReads ||
		timedRead.programPersistChunkReads !== persistChunkReads ||
		timedRead.initialLocalChunkIndexRowCount !==
			expectedInitialDiagnosticIndexRowCount ||
		timedRead.initialLocalChunkCount !==
			expectedInitialDiagnosticIndexRowCount ||
		timedRead.initialLocalChunkBlockCount !==
			expectedInitialDiagnosticBlockCount
	) {
		throw new Error(
			"Benchmark timed read diagnostics do not match its exact locality cohort",
		);
	}
	const integrityVerifiedAt = requirePositiveSafeInteger(
		control.integrityVerifiedAt,
		"readerLocalityControl.integrityVerifiedAt",
	);
	const downloadMemoryTelemetry = requireRecord(
		result.downloadMemoryTelemetry,
		"downloadMemoryTelemetry",
	);
	const downloadMemoryStartedAt = requirePositiveSafeInteger(
		downloadMemoryTelemetry.startedAt,
		"downloadMemoryTelemetry.startedAt",
	);
	const terminalIdle = validateLocalChunkPrefixObservation(
		control.terminalIdleObservation,
		"readerLocalityControl.terminalIdleObservation",
	);
	const expectedTerminalIndexRowCount =
		expectedTerminalTopology === "replicator" ? canonicalChunkCount : 0;
	const expectedTerminalBlockCount = persistChunkReads
		? canonicalChunkCount
		: 0;
	if (
		terminalIdle.fileId !== manifestFileId ||
		terminalIdle.chunkCount !== canonicalChunkCount ||
		terminalIdle.blockCount !== expectedTerminalBlockCount ||
		terminalIdle.indexRowCount !== expectedTerminalIndexRowCount ||
		terminalIdle.indexedChunkIndices.length !== expectedTerminalIndexRowCount ||
		terminalIdle.persistChunkReads !== persistChunkReads ||
		control.terminalTopologyRole !== expectedTerminalTopology ||
		control.terminalTopologyExpectationSatisfied !== true
	) {
		throw new Error(
			"Benchmark terminal reader evidence does not match the requested persistence and topology policy",
		);
	}
	const terminalTopologyStartedAt = requirePositiveSafeInteger(
		control.terminalTopologyStartedAt,
		"readerLocalityControl.terminalTopologyStartedAt",
	);
	const terminalTopologyDeadlineAt = requirePositiveSafeInteger(
		control.terminalTopologyDeadlineAt,
		"readerLocalityControl.terminalTopologyDeadlineAt",
	);
	const terminalTopologyFinishedAt = requirePositiveSafeInteger(
		control.terminalTopologyFinishedAt,
		"readerLocalityControl.terminalTopologyFinishedAt",
	);
	if (
		!Array.isArray(control.terminalTopologyObservations) ||
		control.terminalTopologyObservations.length !== 3
	) {
		throw new Error(
			"Benchmark reader locality requires exactly three stable terminal topology observations",
		);
	}
	const expectedTerminalReplicatorCount =
		expectedTerminalTopology === "replicator" ? 2 : 1;
	const expectedReaderSelfInReplicatorSet =
		expectedTerminalTopology === "replicator";
	const expectedTerminalReplicatorHashes = (
		expectedTerminalTopology === "replicator"
			? [
					writerTopologyBeforeUpload.peerHash,
					readerTopologyBeforeUpload.peerHash,
				]
			: [writerTopologyBeforeUpload.peerHash]
	).toSorted((left, right) => left.localeCompare(right));
	const terminalTopologyObservations = control.terminalTopologyObservations.map(
		(value, index) => {
			const label = `readerLocalityControl.terminalTopologyObservations[${index}]`;
			const observation = requireRecord(value, label);
			const capturedAt = requirePositiveSafeInteger(
				observation.capturedAt,
				`${label}.capturedAt`,
			);
			const writerTopology = validateTopology(
				observation.writerTopology,
				`${label}.writerTopology`,
				true,
				expectedTerminalReplicatorCount,
			);
			const readerTopology = validateTopology(
				observation.readerTopology,
				`${label}.readerTopology`,
				expectedReaderSelfInReplicatorSet,
				expectedTerminalReplicatorCount,
			);
			if (
				writerTopology.peerHash !== writerTopologyBeforeUpload.peerHash ||
				readerTopology.peerHash !== readerTopologyBeforeUpload.peerHash ||
				!isDeepStrictEqual(
					writerTopology.replicatorHashes,
					expectedTerminalReplicatorHashes,
				) ||
				!isDeepStrictEqual(
					readerTopology.replicatorHashes,
					expectedTerminalReplicatorHashes,
				) ||
				writerTopology.capturedAt > capturedAt ||
				readerTopology.capturedAt > capturedAt ||
				writerTopology.capturedAt < terminalTopologyStartedAt ||
				readerTopology.capturedAt < terminalTopologyStartedAt ||
				capturedAt > terminalTopologyFinishedAt ||
				(index > 0 &&
					capturedAt -
						control.terminalTopologyObservations[index - 1].capturedAt <
						control.stabilityPollIntervalMs)
			) {
				throw new Error(
					"Benchmark terminal topology observations do not prove the requested stable topology",
				);
			}
			return { capturedAt, writerTopology, readerTopology };
		},
	);
	const writerDiagnostics = requireRecord(
		result.writerDiagnostics,
		"writerDiagnostics",
	);
	if (
		writerDiagnostics.peerHash !== writerTopologyBeforeUpload.peerHash ||
		readerDiagnostics.peerHash !== readerTopologyBeforeUpload.peerHash ||
		writerDiagnostics.replicatorCount !== expectedTerminalReplicatorCount ||
		readerDiagnostics.replicatorCount !== expectedTerminalReplicatorCount ||
		writerDiagnostics.replicationSetSize !== 1 ||
		readerDiagnostics.replicationSetSize !== (persistChunkReads ? 1 : 0)
	) {
		throw new Error(
			"Benchmark final peer diagnostics contradict the terminal topology evidence",
		);
	}
	if (
		initialRoleCapturedAt > uploadStartedAt ||
		writerTopologyBeforeUpload.capturedAt > uploadStartedAt ||
		readerTopologyBeforeUpload.capturedAt > uploadStartedAt ||
		beforePreload.capturedAt < postMonitorFinishedAt ||
		preloadStartedAt < beforePreload.capturedAt ||
		preloadFinishedAt < preloadStartedAt ||
		stable[0].capturedAt < preloadFinishedAt ||
		preTimedReadTransportStability.startedAt < preDownload.capturedAt ||
		preTimedReadTransportStability.startedAt < downloadMemoryStartedAt ||
		preTimedReadTransportStability.finishedAt > downloadStartedAt ||
		downloadStartedAt - preTimedReadTransportStability.finishedAt >
			TRANSPORT_COUNTER_PRE_READ_START_TOLERANCE_MS ||
		preDownload.capturedAt > writerTopologyBeforeTimedRead.capturedAt ||
		preDownload.capturedAt > readerTopologyBeforeTimedRead.capturedAt ||
		writerTopologyBeforeTimedRead.capturedAt > downloadStartedAt ||
		readerTopologyBeforeTimedRead.capturedAt > downloadStartedAt ||
		downloadFinishedAt > downloadCompletionObservedAt ||
		postTimedReadTransportStability.startedAt < downloadCompletionObservedAt ||
		postTimedReadTransportStability.finishedAt > integrityVerifiedAt ||
		writerTopologyAfterTimedRead.capturedAt < downloadCompletionObservedAt ||
		readerTopologyAfterTimedRead.capturedAt < downloadCompletionObservedAt ||
		writerTopologyAfterTimedRead.capturedAt > integrityVerifiedAt ||
		readerTopologyAfterTimedRead.capturedAt > integrityVerifiedAt ||
		integrityVerifiedAt !== result.integrityVerifiedAt ||
		integrityVerifiedAt < downloadCompletionObservedAt ||
		terminalIdle.capturedAt < integrityVerifiedAt ||
		terminalTopologyStartedAt < terminalIdle.capturedAt ||
		terminalTopologyDeadlineAt !==
			terminalTopologyStartedAt + invocation.readyTimeoutMs ||
		terminalTopologyFinishedAt < terminalTopologyStartedAt ||
		terminalTopologyFinishedAt > terminalTopologyDeadlineAt ||
		terminalTopologyObservations[0].capturedAt < terminalTopologyStartedAt ||
		terminalTopologyObservations.at(-1).capturedAt > terminalTopologyFinishedAt
	) {
		throw new Error(
			"Benchmark reader locality control timestamps are inconsistent",
		);
	}
	return {
		actualLocalChunkBlockCount,
		actualLocalChunkIndexRowCount,
		terminalTopologyFinishedAt,
	};
};

const validateCollectedStringEvidence = ({
	result,
	completeKey,
	countKey,
	itemsKey,
	label,
	inconsistentLabel = label,
	allowIncomplete,
}) => {
	if (result[completeKey] === false && allowIncomplete) {
		if (result[countKey] !== null || result[itemsKey] !== null) {
			throw new Error(
				`Incomplete ${label} must use null count and evidence fields`,
			);
		}
		return;
	}
	if (result[completeKey] !== true) {
		throw new Error(`Benchmark result has an incomplete ${label}`);
	}
	const count = requireNonNegativeSafeInteger(result[countKey], countKey);
	if (
		!Array.isArray(result[itemsKey]) ||
		result[itemsKey].length !== count ||
		result[itemsKey].some(
			(entry) => typeof entry !== "string" || entry.length === 0,
		)
	) {
		throw new Error(`Benchmark result has inconsistent ${inconsistentLabel}`);
	}
};

const validateErrorCollection = (result, { allowIncomplete = false } = {}) => {
	if (result.errorCollectionDefinition !== ERROR_COLLECTION_DEFINITION) {
		throw new Error(
			"Benchmark result has an invalid error collection definition",
		);
	}
	if (
		!isDeepStrictEqual(
			result.knownPeerbitFailureSignatures,
			KNOWN_PEERBIT_FAILURE_SIGNATURES,
		)
	) {
		throw new Error(
			"Benchmark result has invalid known Peerbit failure signatures",
		);
	}
	validateCollectedStringEvidence({
		result,
		completeKey: "errorCollectionComplete",
		countKey: "errorCount",
		itemsKey: "errors",
		label: "error collection",
		inconsistentLabel: "recorded errors",
		allowIncomplete,
	});
	if (
		result.requestFailureCollectionDefinition !==
		REQUEST_FAILURE_COLLECTION_DEFINITION
	) {
		throw new Error(
			"Benchmark result has an invalid request-failure collection definition",
		);
	}
	validateCollectedStringEvidence({
		result,
		completeKey: "requestFailureCollectionComplete",
		countKey: "requestFailureCount",
		itemsKey: "requestFailures",
		label: "request-failure collection",
		inconsistentLabel: "request-failure diagnostics",
		allowIncomplete,
	});
};

const validateZeroErrors = (result, label) => {
	validateErrorCollection(result);
	if (result.errorCount !== 0 || result.errors.length !== 0) {
		throw new Error(`Passed ${label} contains recorded errors`);
	}
};

const validateUploadSnapshots = (
	result,
	invocation,
	{ integrityVerifiedAt, terminalTopologyFinishedAt },
) => {
	if (!Array.isArray(result.snapshots) || result.snapshots.length === 0) {
		throw new Error("Passed upload result is missing seeder snapshots");
	}
	const timestamps = requireRecord(result.timestamps, "timestamps");
	const postMonitorStartedAt = requirePositiveNumber(
		timestamps.postMonitorStartedAt,
		"timestamps.postMonitorStartedAt",
	);
	const postMonitorFinishedAt = requirePositiveNumber(
		timestamps.postMonitorFinishedAt,
		"timestamps.postMonitorFinishedAt",
	);
	const downloadCompletionObservedAt = requirePositiveNumber(
		timestamps.downloadCompletionObservedAt,
		"timestamps.downloadCompletionObservedAt",
	);
	let previousAt = -1;
	let hasPostMonitorSample = false;
	const labels = new Set();
	const parsedSnapshots = [];
	for (const [index, value] of result.snapshots.entries()) {
		const snapshot = requireRecord(value, `snapshots[${index}]`);
		const label = requireString(snapshot.label, `snapshots[${index}].label`);
		if (
			labels.has(label) ||
			!/^(?:seeders-ready|during-\d+|after-\d+|terminal)$/.test(label)
		) {
			throw new Error(
				"Upload seeder snapshot labels are invalid or duplicated",
			);
		}
		labels.add(label);
		const writerSeeders = requireNonNegativeSafeInteger(
			snapshot.writerSeeders,
			`snapshots[${index}].writerSeeders`,
		);
		const readerSeeders = requireNonNegativeSafeInteger(
			snapshot.readerSeeders,
			`snapshots[${index}].readerSeeders`,
		);
		const capturedAt = requirePositiveNumber(
			snapshot.at,
			`snapshots[${index}].at`,
		);
		if (capturedAt < previousAt) {
			throw new Error("Upload seeder snapshots are not monotonic");
		}
		if (label.startsWith("after-")) {
			if (
				capturedAt < postMonitorStartedAt ||
				capturedAt > postMonitorFinishedAt
			) {
				throw new Error(
					"Upload post-monitor snapshot is outside the monitor window",
				);
			}
			hasPostMonitorSample = true;
		}
		parsedSnapshots.push({
			label,
			writerSeeders,
			readerSeeders,
			capturedAt,
		});
		previousAt = capturedAt;
	}
	if (invocation.postUploadMonitorMs > 0 && !hasPostMonitorSample) {
		throw new Error(
			"Passed upload result is missing a numeric post-monitor snapshot",
		);
	}
	const readySnapshots = parsedSnapshots.filter(
		(snapshot) => snapshot.label === "seeders-ready",
	);
	if (
		readySnapshots.length !== 1 ||
		parsedSnapshots[0]?.label !== "seeders-ready"
	) {
		throw new Error(
			"Passed upload result must begin with exactly one ready-seeder baseline snapshot",
		);
	}
	const terminalSnapshots = parsedSnapshots.filter(
		(snapshot) => snapshot.label === SEEDER_DROP_POLICY.terminalSnapshotLabel,
	);
	if (
		terminalSnapshots.length !== 1 ||
		parsedSnapshots.at(-1)?.label !== SEEDER_DROP_POLICY.terminalSnapshotLabel
	) {
		throw new Error(
			"Passed upload result must end with exactly one terminal seeder snapshot",
		);
	}
	const terminalSnapshot = terminalSnapshots[0];
	if (terminalSnapshot.capturedAt < downloadCompletionObservedAt) {
		throw new Error(
			"Upload terminal seeder snapshot precedes download completion",
		);
	}
	if (terminalSnapshot.capturedAt < integrityVerifiedAt) {
		throw new Error(
			"Upload terminal seeder snapshot precedes aggregate integrity verification",
		);
	}
	if (
		terminalTopologyFinishedAt !== null &&
		terminalSnapshot.capturedAt < terminalTopologyFinishedAt
	) {
		throw new Error(
			"Upload terminal seeder snapshot precedes terminal topology completion",
		);
	}
	const baselineWriterSeeders = requireNonNegativeSafeInteger(
		result.baselineWriterSeeders,
		"baselineWriterSeeders",
	);
	const baselineReaderSeeders = requireNonNegativeSafeInteger(
		result.baselineReaderSeeders,
		"baselineReaderSeeders",
	);
	const readySnapshot = readySnapshots[0];
	if (
		baselineWriterSeeders !== readySnapshot.writerSeeders ||
		baselineReaderSeeders !== readySnapshot.readerSeeders
	) {
		throw new Error(
			"Upload seeder baseline fields contradict the ready-seeder snapshot",
		);
	}
	if (
		baselineWriterSeeders < invocation.minReadySeeders ||
		baselineReaderSeeders < invocation.minReadySeeders
	) {
		throw new Error(
			"Upload ready-seeder baseline is below the requested minimum",
		);
	}
	let recomputedDroppedSeeders = false;
	let recomputedUnexpectedSeederDrop = false;
	let consecutiveBelowBaselineSnapshots = 0;
	for (const snapshot of parsedSnapshots.slice(1)) {
		const belowBaseline =
			snapshot.writerSeeders < baselineWriterSeeders ||
			snapshot.readerSeeders < baselineReaderSeeders;
		if (belowBaseline) {
			recomputedDroppedSeeders = true;
			consecutiveBelowBaselineSnapshots += 1;
			if (
				consecutiveBelowBaselineSnapshots >=
					SEEDER_DROP_POLICY.consecutiveBelowBaselineSnapshotThreshold ||
				(SEEDER_DROP_POLICY.terminalBelowBaselineIsUnexpected &&
					snapshot.label === SEEDER_DROP_POLICY.terminalSnapshotLabel)
			) {
				recomputedUnexpectedSeederDrop = true;
			}
		} else {
			consecutiveBelowBaselineSnapshots = 0;
		}
	}
	if (result.droppedSeeders !== recomputedDroppedSeeders) {
		throw new Error(
			"Upload droppedSeeders claim contradicts its numeric snapshot evidence",
		);
	}
	if (result.unexpectedSeederDrop !== recomputedUnexpectedSeederDrop) {
		throw new Error(
			"Upload unexpectedSeederDrop claim contradicts its numeric snapshot evidence",
		);
	}
};

const validateSeederProbe = (result, invocation) => {
	if (result.reachedTarget !== true) {
		throw new Error("Passed seeder probe did not reach a clean target state");
	}
	validateZeroErrors(result, "seeder probe");
	for (const key of [
		"readyTimeoutMs",
		"sampleMs",
		"sampleCount",
		"targetSeeders",
	]) {
		if (result[key] !== invocation[key]) {
			throw new Error(
				`Seeder probe ${key} does not match the requested invocation`,
			);
		}
	}
	if (!Array.isArray(result.samples) || result.samples.length === 0) {
		throw new Error("Passed seeder probe is missing convergence samples");
	}
	if (result.sampleCountDefinition !== SAMPLE_COUNT_DEFINITION) {
		throw new Error("Seeder probe has an invalid sampleCount definition");
	}
	const effectiveSampleIntervalMs = Math.max(
		1,
		Math.min(
			invocation.sampleMs,
			Math.max(
				1,
				Math.floor(invocation.readyTimeoutMs / invocation.sampleCount),
			),
		),
	);
	if (result.effectiveSampleIntervalMs !== effectiveSampleIntervalMs) {
		throw new Error("Seeder probe has an invalid effective sample interval");
	}
	const maxSamples =
		Math.floor(invocation.readyTimeoutMs / effectiveSampleIntervalMs) + 1;
	if (result.samples.length > maxSamples) {
		throw new Error(
			"Seeder probe contains more samples than its deadline allows",
		);
	}
	const probeStartedAt = requirePositiveNumber(
		result.probeStartedAt,
		"probeStartedAt",
	);
	const readyDeadlineAt = requirePositiveNumber(
		result.readyDeadlineAt,
		"readyDeadlineAt",
	);
	const probeFinishedAt = requirePositiveNumber(
		result.probeFinishedAt,
		"probeFinishedAt",
	);
	const probeDurationMs = requireNonNegativeNumber(
		result.probeDurationMs,
		"probeDurationMs",
	);
	const timeToTargetMs = requireNonNegativeNumber(
		result.timeToTargetMs,
		"timeToTargetMs",
	);
	if (
		readyDeadlineAt !== probeStartedAt + invocation.readyTimeoutMs ||
		probeFinishedAt !== probeStartedAt + probeDurationMs ||
		probeDurationMs !== timeToTargetMs ||
		probeFinishedAt > readyDeadlineAt ||
		timeToTargetMs > invocation.readyTimeoutMs
	) {
		throw new Error(
			"Seeder probe deadline/duration arithmetic is inconsistent",
		);
	}

	let previousCapturedAt = -1;
	let targetSample;
	let firstTargetSampleLabel;
	for (const [offset, value] of result.samples.entries()) {
		const index = offset + 1;
		const sample = requireRecord(value, `samples[${offset}]`);
		if (sample.index !== index || sample.label !== `sample-${index}`) {
			throw new Error("Seeder probe sample labels or indices are invalid");
		}
		const capturedAt = requirePositiveNumber(
			sample.capturedAt,
			`samples[${offset}].capturedAt`,
		);
		const elapsedMs = requireNonNegativeNumber(
			sample.elapsedMs,
			`samples[${offset}].elapsedMs`,
		);
		const writerSeeders = requireNonNegativeSafeInteger(
			sample.writerSeeders,
			`samples[${offset}].writerSeeders`,
		);
		const readerSeeders = requireNonNegativeSafeInteger(
			sample.readerSeeders,
			`samples[${offset}].readerSeeders`,
		);
		if (
			capturedAt < previousCapturedAt ||
			capturedAt !== probeStartedAt + elapsedMs ||
			capturedAt > readyDeadlineAt
		) {
			throw new Error("Seeder probe samples are not monotonic and bounded");
		}
		previousCapturedAt = capturedAt;
		if (
			firstTargetSampleLabel == null &&
			writerSeeders >= invocation.targetSeeders &&
			readerSeeders >= invocation.targetSeeders
		) {
			firstTargetSampleLabel = sample.label;
		}
		if (sample.label === result.targetSampleLabel) {
			targetSample = {
				sample,
				capturedAt,
				elapsedMs,
				writerSeeders,
				readerSeeders,
			};
		}
	}
	if (
		!targetSample ||
		firstTargetSampleLabel !== result.targetSampleLabel ||
		targetSample.sample !== result.samples.at(-1) ||
		targetSample.writerSeeders < invocation.targetSeeders ||
		targetSample.readerSeeders < invocation.targetSeeders ||
		targetSample.elapsedMs !== timeToTargetMs ||
		targetSample.capturedAt !== probeFinishedAt
	) {
		throw new Error(
			"Seeder probe target evidence is invalid or is not the first observed target",
		);
	}
};

export const validateBenchmarkResultEnvelope = (
	result,
	{
		expectedMode,
		expectedFileMb,
		expectedNetwork,
		expectedRunNonce,
		expectedProvenance,
		expectedInvocation,
	},
) => {
	if (!isRecord(result)) {
		throw new Error("Benchmark result must be an object");
	}
	validateEnvelope(result, {
		expectedRunNonce,
		expectedProvenance,
		expectedNetwork,
		expectedInvocation,
	});
	if (!["passed", "failed"].includes(result.status)) {
		throw new Error("Benchmark result has an unsupported status");
	}
	if (result.mode !== expectedMode) {
		throw new Error(
			`Benchmark result mode mismatch (expected ${expectedMode}, got ${String(result.mode)})`,
		);
	}
	if (
		expectedInvocation.scenario === "upload" &&
		result.fileSizeMb !== expectedFileMb
	) {
		throw new Error(
			`Benchmark result size mismatch (expected ${expectedFileMb} MiB, got ${String(result.fileSizeMb)})`,
		);
	}
	validateErrorCollection(result, {
		allowIncomplete: result.status === "failed",
	});
	if (expectedInvocation.scenario === "upload" && result.status === "failed") {
		validateFailedUploadIntegrityEvidence(
			result,
			expectedFileMb,
			expectedInvocation,
		);
	}
	return result;
};

export const validateBenchmarkResult = (
	result,
	{
		scenario,
		expectedMode,
		expectedFileMb,
		expectedNetwork,
		expectedFixtureSeed,
		expectedRunNonce,
		expectedProvenance,
		expectedInvocation,
	},
) => {
	validateBenchmarkResultEnvelope(result, {
		expectedMode,
		expectedFileMb,
		expectedNetwork,
		expectedRunNonce,
		expectedProvenance,
		expectedInvocation,
	});
	if (result.status !== "passed") {
		const detail =
			typeof result.failure?.message === "string"
				? `: ${result.failure.message}`
				: "";
		throw new Error(`Benchmark result status is not passed${detail}`);
	}
	if (scenario === "upload") {
		const integrityVerifiedAt = validateUploadIntegrity(
			result,
			expectedFileMb,
			expectedFixtureSeed,
			expectedInvocation,
		);
		const readWindow = validateUploadTimings(
			result,
			expectedInvocation,
			integrityVerifiedAt,
		);
		if (integrityVerifiedAt < readWindow.downloadCompletionObservedAt) {
			throw new Error(
				"Benchmark aggregate integrity gate precedes download completion",
			);
		}
		validateUploadProgressTelemetry(result, expectedInvocation);
		validateRequestedUploadKnobs(result, expectedInvocation);
		validateZeroErrors(result, "upload result");
		const readerLocalityControl = validateReaderLocalityControl(
			result,
			expectedInvocation,
		);
		validateUploadSnapshots(result, expectedInvocation, {
			integrityVerifiedAt,
			terminalTopologyFinishedAt:
				readerLocalityControl?.terminalTopologyFinishedAt ?? null,
		});
		const shutdownWindow = validateResourceAndShutdownEvidence(
			result,
			expectedInvocation,
			readWindow,
			{
				integrityVerifiedAt,
				terminalTopologyFinishedAt:
					readerLocalityControl?.terminalTopologyFinishedAt ?? null,
			},
		);
		validateDownloadMemoryTelemetry(result, expectedInvocation, {
			...readWindow,
			...shutdownWindow,
		});
		if (result.unexpectedSeederDrop !== false) {
			throw new Error(
				"Passed upload result contains an unexpected seeder drop",
			);
		}
	} else if (scenario === "seeder-probe") {
		validateSeederProbe(result, expectedInvocation);
	} else {
		throw new Error(`Unsupported benchmark scenario ${String(scenario)}`);
	}
	return result;
};

export const loadAndValidateBenchmarkResult = async ({
	resultFile,
	exitCode,
	scenario,
	expectedMode,
	expectedFileMb,
	expectedNetwork,
	expectedFixtureSeed,
	expectedRunNonce,
	expectedProvenance,
	expectedInvocation,
}) => {
	assertPlaywrightSucceeded(exitCode);
	let contents;
	try {
		contents = await fsp.readFile(resultFile, "utf8");
	} catch (error) {
		throw new Error(`Benchmark run did not produce ${resultFile}`, {
			cause: error,
		});
	}
	const result = parseBenchmarkResult(contents, resultFile);
	return validateBenchmarkResult(result, {
		scenario,
		expectedMode,
		expectedFileMb,
		expectedNetwork,
		expectedFixtureSeed,
		expectedRunNonce,
		expectedProvenance,
		expectedInvocation,
	});
};
