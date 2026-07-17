import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createBenchmarkInvocation } from "./benchmark-invocation.mjs";
import {
	assertPlaywrightSucceeded,
	loadAndValidateBenchmarkResult,
	parseBenchmarkResult,
	validateBenchmarkResult,
} from "./benchmark-validity.mjs";

const RUN_NONCE = "123e4567-e89b-42d3-a456-426614174000";
const SHA256 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const FILE_MB = 5;
const FILE_SIZE_BYTES = FILE_MB * 1024 * 1024;
const SAMPLE_COUNT_DEFINITION =
	"observation-density divisor: planned interval is min(sampleMs, floor(readyTimeoutMs/sampleCount)) clamped to 1ms; convergence may finish early";
const POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION =
	"max(250ms, pollMs + 250ms) for the final poll and event-loop scheduling";
const TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION =
	"max(5000ms, pollMs + 1000ms) for browser actions and event-loop scheduling";
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
	schema: { id: "peerbit-file-share-benchmark", version: 2 },
	runNonce: RUN_NONCE,
	invocation: structuredClone(INVOCATION),
	provenance: structuredClone(PROVENANCE),
	status: "passed",
	mode: "adaptive",
	networkMode: "local",
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
		downloadedSha256Base64: SHA256,
		sourceCrc32Hex: "00000000",
		downloadedCrc32Hex: "00000000",
		sizeVerified: true,
		manifestVerified: true,
		sha256Verified: true,
		crc32Verified: true,
		verified: true,
	},
	uploadDurationMs: 101,
	listingDurationMs: 19,
	postUploadMonitorDurationMs: 50,
	postUploadMonitorSchedulingToleranceMs: 1_250,
	postUploadMonitorSchedulingToleranceDefinition:
		POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION,
	postUploadMonitorMs: 50,
	pollMs: 1_000,
	uploadTimeoutMs: 600_000,
	downloadTimeoutMs: 600_000,
	minReadySeeders: 2,
	downloadDurationMs: 30,
	transferTimeoutSchedulingToleranceMs: 5_000,
	transferTimeoutSchedulingToleranceDefinition:
		TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION,
	downloadSink: "node-file",
	phaseDurationsMs: {
		timeToUploadSettled: 101,
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
	},
	baselineWriterSeeders: 2,
	baselineReaderSeeders: 2,
	droppedSeeders: false,
	errorCount: 0,
	errors: [],
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

test("accepts a complete deterministic transfer result", () => {
	assert.equal(
		validateBenchmarkResult(validResult(), options).status,
		"passed",
	);
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
		schema: { id: "peerbit-file-share-benchmark", version: 2 },
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
		errorCount: 0,
		errors: [],
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
