import fsp from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import {
	BENCHMARK_RESULT_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
} from "./benchmark-orchestration.mjs";

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
const TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION =
	"max(5000ms, pollMs + 1000ms) for browser actions and event-loop scheduling";
const TIME_TO_WRITER_READY_DEFINITION =
	"upload-input-set-to-writer-ready-manifest-listed";
const TIME_TO_READER_READY_DEFINITION =
	"upload-input-set-to-reader-ready-manifest-listed";
const LISTING_DURATION_DEFINITION =
	"post-upload-settlement-to-both-writer-and-reader-ready-manifests-listed; excludes upload time";

const isRecord = (value) =>
	value != null && typeof value === "object" && !Array.isArray(value);

const requireRecord = (value, label) => {
	if (!isRecord(value)) {
		throw new Error(`Benchmark result is missing ${label}`);
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

const requireNonNegativeSafeInteger = (value, label) => {
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new Error(`Benchmark result has invalid ${label}`);
	}
	return value;
};

const requirePattern = (value, pattern, label) => {
	const string = requireString(value, label);
	if (!pattern.test(string)) {
		throw new Error(`Benchmark result has malformed ${label}`);
	}
	return string;
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
	const downloadedSha256 = requirePattern(
		integrity.downloadedSha256Base64,
		SHA256_BASE64_PATTERN,
		"integrity.downloadedSha256Base64",
	);
	if (
		integrity.sha256Verified !== true ||
		sourceSha256 !== manifestSha256 ||
		sourceSha256 !== downloadedSha256
	) {
		throw new Error("Benchmark result failed its SHA-256 integrity gate");
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
};

const validateUploadTimings = (result, invocation) => {
	if (result.downloadSink !== "node-file") {
		throw new Error(
			"Benchmark result did not use the persisted Node file sink",
		);
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
	const downloadStartedAt = requirePositiveNumber(
		timestamps.downloadStartedAt,
		"timestamps.downloadStartedAt",
	);
	const downloadFinishedAt = requirePositiveNumber(
		timestamps.downloadFinishedAt,
		"timestamps.downloadFinishedAt",
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
		downloadFinishedAt <= downloadStartedAt
	) {
		throw new Error("Benchmark phase timestamps are not monotonic");
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
		invocationSchema.version !== 1
	) {
		throw new Error("Benchmark result has an unsupported invocation schema");
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

const validateUploadSnapshots = (result, invocation) => {
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
	let previousAt = -1;
	let hasPostMonitorSample = false;
	const labels = new Set();
	const parsedSnapshots = [];
	for (const [index, value] of result.snapshots.entries()) {
		const snapshot = requireRecord(value, `snapshots[${index}]`);
		const label = requireString(snapshot.label, `snapshots[${index}].label`);
		if (
			labels.has(label) ||
			!/^(?:seeders-ready|during-\d+|after-\d+)$/.test(label)
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
		parsedSnapshots.push({ label, writerSeeders, readerSeeders });
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
	const recomputedDroppedSeeders = parsedSnapshots
		.slice(1)
		.some(
			(snapshot) =>
				snapshot.writerSeeders < baselineWriterSeeders ||
				snapshot.readerSeeders < baselineReaderSeeders,
		);
	if (result.droppedSeeders !== recomputedDroppedSeeders) {
		throw new Error(
			"Upload droppedSeeders claim contradicts its numeric snapshot evidence",
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
		validateUploadIntegrity(result, expectedFileMb, expectedFixtureSeed);
		validateUploadTimings(result, expectedInvocation);
		validateRequestedUploadKnobs(result, expectedInvocation);
		validateZeroErrors(result, "upload result");
		validateUploadSnapshots(result, expectedInvocation);
		if (result.droppedSeeders !== false) {
			throw new Error("Passed upload result contains seeder drops");
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
