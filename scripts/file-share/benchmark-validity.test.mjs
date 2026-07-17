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
} from "./benchmark-orchestration.mjs";
import {
	assertPlaywrightSucceeded,
	calculateSinkAwaitSubtractedDiagnosticMs,
	loadAndValidateBenchmarkResult,
	parseBenchmarkResult,
	validateBenchmarkResult,
	validateBenchmarkResultEnvelope,
} from "./benchmark-validity.mjs";

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

const validResult = () => ({
	schema: { ...BENCHMARK_RESULT_SCHEMA },
	runNonce: RUN_NONCE,
	invocation: structuredClone(INVOCATION),
	provenance: structuredClone(PROVENANCE),
	status: "passed",
	mode: "adaptive",
	networkMode: "local",
	fileName: FILE_NAME,
	fileSizeMb: FILE_MB,
	integrityVerified: true,
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
	postUploadMonitorMs: 50,
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
	},
	baselineWriterSeeders: 2,
	baselineReaderSeeders: 2,
	droppedSeeders: false,
	errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
	knownPeerbitFailureSignatures: [...KNOWN_PEERBIT_FAILURE_SIGNATURES],
	errorCollectionComplete: true,
	errorCount: 0,
	errors: [],
	requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
	requestFailureCollectionComplete: true,
	requestFailureCount: 0,
	requestFailures: [],
	readerDiagnostics: {
		lastReadDiagnostics: {
			transferId: "benchmark-read-transfer-id",
			fileId: FILE_ID,
			fileName: FILE_NAME,
			startedAt: 1_170,
			finishedAt: 1_200,
			computedFinalHash: SHA256,
			chunkResolved: { 0: "remote", 1: "remote" },
			chunkByteLength: { 0: 3 * 1024 * 1024, 1: 2 * 1024 * 1024 },
			chunkDemandWaitMs: { 0: 8, 1: 4 },
			chunkWriteStartedAt: { 0: 1_180, 1: 1_192 },
			chunkWriteFinishedAt: { 0: 1_185, 1: 1_196 },
			chunkMaterializeStartedAt: { 0: 1_171, 1: 1_186 },
			chunkMaterializeFinishedAt: { 0: 1_173, 1: 1_188 },
			chunkHashStartedAt: { 0: 1_173, 1: 1_188 },
			chunkHashFinishedAt: { 0: 1_174, 1: 1_190 },
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
	],
});

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
	return {
		result,
		options: { ...options, expectedInvocation: invocation },
	};
};

test("accepts a complete deterministic transfer result", () => {
	assert.equal(
		validateBenchmarkResult(validResult(), options).status,
		"passed",
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

test("rejects a status-only passed payload at the v4 evidence envelope", () => {
	assert.throws(
		() => validateBenchmarkResultEnvelope({ status: "passed" }, options),
		/missing schema/,
	);
});

test("requires explicit error evidence on failed v4 envelopes", () => {
	const completeFailure = validResult();
	completeFailure.status = "failed";
	completeFailure.failure = { message: "synthetic browser failure" };
	assert.equal(
		validateBenchmarkResultEnvelope(completeFailure, options).status,
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
			result.snapshots.reverse();
			result.snapshots[0].at = 1130;
			result.snapshots[1].at = 1140;
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
			result.snapshots[1].readerSeeders = 1;
			result.droppedSeeders = true;
		},
		/contains seeder drops/,
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
