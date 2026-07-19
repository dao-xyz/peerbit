import fsp from "node:fs/promises";

export const BENCHMARK_RESULT_SCHEMA = Object.freeze({
	id: "peerbit-file-share-benchmark",
	version: 10,
});

export const SEEDER_DROP_POLICY = Object.freeze({
	id: "peerbit-file-share-seeder-drop-policy",
	version: 1,
	belowBaselineDefinition:
		"writerSeeders < baselineWriterSeeders || readerSeeders < baselineReaderSeeders",
	evaluatedSnapshotDefinition:
		"all chronological snapshots after the unique seeders-ready baseline snapshot",
	consecutiveBelowBaselineSnapshotThreshold: 2,
	terminalSnapshotLabel: "terminal",
	terminalBelowBaselineIsUnexpected: true,
});

export const BENCHMARK_SUMMARY_SCHEMA = Object.freeze({
	id: "peerbit-file-share-benchmark-summary",
	version: 6,
});

export const MATRIX_SUMMARY_SCHEMA = Object.freeze({
	id: "peerbit-file-share-matrix-summary",
	version: 6,
});

export const KNOWN_PEERBIT_FAILURE_SIGNATURES = Object.freeze([
	"Failed to resolve block",
	"DeliveryError",
	"Failed to get message",
	"delivery acknowledges",
	"Failed to bootstrap",
	"failed to open",
	"BorshError",
	"Failed to create space",
]);

export const ERROR_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every uncaught pageerror; every console.error; every console message at any level containing a known Peerbit failure signature; plus scenario-recorded operation failures";

export const REQUEST_FAILURE_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every Playwright requestfailed event, retained as non-fatal diagnostics and excluded from errorCount";

const isRecord = (value) =>
	value != null && typeof value === "object" && !Array.isArray(value);

const SHA256_BASE64_PATTERN = /^[A-Za-z0-9+/]{43}=$/;
const CRC32_HEX_PATTERN = /^[0-9a-f]{8}$/;

const hasCompleteVerifiedUploadIntegrityEvidence = (
	integrity,
	{ fileMb, fixtureSeed, downloadSink },
) => {
	const expectedSizeBytes = fileMb * 1024 * 1024;
	if (
		!isRecord(integrity) ||
		!Number.isSafeInteger(expectedSizeBytes) ||
		expectedSizeBytes <= 0 ||
		!["hash-only", "opfs", "node-file"].includes(downloadSink) ||
		integrity.fixtureMode !== "deterministic" ||
		integrity.fixtureFormat !== "aes-256-ctr-v1" ||
		integrity.fixtureSeed !== fixtureSeed ||
		integrity.expectedSizeBytes !== expectedSizeBytes ||
		integrity.sourceSizeBytes !== expectedSizeBytes ||
		integrity.manifestSizeBytes !== expectedSizeBytes ||
		integrity.downloadedSizeBytes !== expectedSizeBytes ||
		integrity.sizeVerified !== true ||
		integrity.manifestVerified !== true ||
		integrity.downloadSink !== downloadSink ||
		integrity.verified !== true
	) {
		return false;
	}
	const sourceSha256 = integrity.sourceSha256Base64;
	const sourceCrc32 = integrity.sourceCrc32Hex;
	if (
		typeof sourceSha256 !== "string" ||
		!SHA256_BASE64_PATTERN.test(sourceSha256) ||
		integrity.manifestSha256Base64 !== sourceSha256 ||
		integrity.libraryComputedSha256Base64 !== sourceSha256 ||
		integrity.sha256Verified !== true ||
		integrity.librarySha256Verified !== true ||
		typeof sourceCrc32 !== "string" ||
		!CRC32_HEX_PATTERN.test(sourceCrc32) ||
		integrity.downloadedCrc32Hex !== sourceCrc32 ||
		integrity.crc32Verified !== true
	) {
		return false;
	}
	if (downloadSink === "hash-only") {
		return (
			integrity.downloadedSha256Base64 === null &&
			integrity.persistedSinkSha256Verified === null &&
			integrity.sinkPersistence === "none" &&
			integrity.sinkPersistenceVerified === null
		);
	}
	return (
		integrity.downloadedSha256Base64 === sourceSha256 &&
		integrity.persistedSinkSha256Verified === true &&
		integrity.sinkPersistence === downloadSink &&
		integrity.sinkPersistenceVerified === true
	);
};

export const projectUploadIntegrityEvidence = (
	browserResult,
	{ fileMb, fixtureSeed, downloadSink } = {},
) => {
	if (!isRecord(browserResult)) {
		return {
			integrity: null,
			integrityVerified: false,
			integrityVerifiedAt: null,
		};
	}
	const integrity = browserResult.integrity ?? null;
	const integrityVerified = browserResult.integrityVerified ?? false;
	const integrityVerifiedAt = browserResult.integrityVerifiedAt ?? null;
	const validUnverifiedEvidence =
		integrityVerified === false &&
		integrityVerifiedAt === null &&
		(integrity === null ||
			(isRecord(integrity) && integrity.verified === false));
	const validVerifiedEvidence =
		integrityVerified === true &&
		Number.isSafeInteger(integrityVerifiedAt) &&
		integrityVerifiedAt > 0 &&
		hasCompleteVerifiedUploadIntegrityEvidence(integrity, {
			fileMb,
			fixtureSeed,
			downloadSink,
		});
	if (!validUnverifiedEvidence && !validVerifiedEvidence) {
		return {
			integrity: null,
			integrityVerified: false,
			integrityVerifiedAt: null,
		};
	}
	return { integrity, integrityVerified, integrityVerifiedAt };
};

export const serializeError = (error) => {
	if (error instanceof Error) {
		return {
			name: error.name,
			message: error.message,
			...(typeof error.stack === "string" ? { stack: error.stack } : {}),
		};
	}
	return {
		name: "Error",
		message: String(error),
	};
};

export const readJsonEvidence = async (
	filePath,
	{ readFile = fsp.readFile } = {},
) => {
	let contents;
	try {
		contents = await readFile(filePath, "utf8");
	} catch (error) {
		return {
			kind: "missing",
			filePath,
			failure: serializeError(error),
		};
	}
	try {
		const value = JSON.parse(contents);
		if (!isRecord(value)) {
			throw new Error("JSON evidence must contain an object");
		}
		return { kind: "parsed", filePath, value };
	} catch (error) {
		return {
			kind: "malformed",
			filePath,
			failure: serializeError(error),
		};
	}
};

export const processOutcomeFailure = (outcome) => {
	if (outcome == null) {
		return null;
	}
	const { exitCode, signal, spawnError } = outcome;
	if (spawnError) {
		return {
			kind: "spawn-error",
			message: `Could not start benchmark process: ${spawnError.message}`,
		};
	}
	if (signal) {
		return {
			kind: "signal",
			message: `Benchmark process terminated by signal ${signal}`,
		};
	}
	if (!Number.isInteger(exitCode) || exitCode !== 0) {
		return {
			kind: "nonzero-exit",
			message: `Benchmark process exited unsuccessfully (exitCode=${String(exitCode)})`,
		};
	}
	return null;
};

const hasCompleteErrorCollection = (result) =>
	isRecord(result) &&
	result.errorCollectionComplete === true &&
	result.errorCollectionDefinition === ERROR_COLLECTION_DEFINITION &&
	Array.isArray(result.knownPeerbitFailureSignatures) &&
	JSON.stringify(result.knownPeerbitFailureSignatures) ===
		JSON.stringify(KNOWN_PEERBIT_FAILURE_SIGNATURES) &&
	Number.isSafeInteger(result.errorCount) &&
	result.errorCount >= 0 &&
	Array.isArray(result.errors) &&
	result.errorCount === result.errors.length &&
	result.errors.every((entry) => typeof entry === "string" && entry.length > 0);

const hasCompleteRequestFailureCollection = (result) =>
	isRecord(result) &&
	result.requestFailureCollectionComplete === true &&
	result.requestFailureCollectionDefinition ===
		REQUEST_FAILURE_COLLECTION_DEFINITION &&
	Number.isSafeInteger(result.requestFailureCount) &&
	result.requestFailureCount >= 0 &&
	Array.isArray(result.requestFailures) &&
	result.requestFailureCount === result.requestFailures.length &&
	result.requestFailures.every(
		(entry) => typeof entry === "string" && entry.length > 0,
	);

export const extractCollectedErrorEvidence = (result) => {
	const errorCollectionComplete = hasCompleteErrorCollection(result);
	const requestFailureCollectionComplete =
		hasCompleteRequestFailureCollection(result);
	return {
		errorCollectionComplete,
		errorCount: errorCollectionComplete ? result.errorCount : null,
		errors: errorCollectionComplete ? result.errors : null,
		requestFailureCollectionComplete,
		requestFailureCount: requestFailureCollectionComplete
			? result.requestFailureCount
			: null,
		requestFailures: requestFailureCollectionComplete
			? result.requestFailures
			: null,
	};
};

export const createInvocationFailureEvidence = ({
	scenario,
	mode,
	network,
	fileMb,
	runNonce,
	invocation,
	provenance,
	resultFile,
	processOutcome,
	resultEvidence,
	failure,
}) => {
	const browserResult =
		resultEvidence?.kind === "parsed" ? resultEvidence.value : null;
	const collectedErrorEvidence = extractCollectedErrorEvidence(browserResult);
	const processFailure = processOutcomeFailure(processOutcome);
	const evidenceFailure =
		resultEvidence?.kind === "missing"
			? {
					kind: "missing-result",
					message: `Benchmark run did not produce ${resultFile}`,
				}
			: resultEvidence?.kind === "malformed"
				? {
						kind: "malformed-result",
						message: `Benchmark run produced malformed JSON in ${resultFile}`,
					}
				: null;
	const primaryFailure =
		processFailure ??
		evidenceFailure ??
		(failure == null
			? {
					kind: "invalid-result",
					message: "Benchmark result did not satisfy the validity contract",
				}
			: {
					kind: "result-validation",
					...serializeError(failure),
				});
	const uploadEvidence =
		scenario === "upload"
			? {
					seederDropPolicy: SEEDER_DROP_POLICY,
					...projectUploadIntegrityEvidence(browserResult, {
						fileMb,
						fixtureSeed: invocation?.fixtureSeed,
						downloadSink: invocation?.downloadSink,
					}),
				}
			: {};
	return {
		schema: BENCHMARK_RESULT_SCHEMA,
		runNonce,
		invocation,
		provenance,
		status: "failed",
		scenario,
		mode,
		networkMode: network,
		fileSizeMb: fileMb,
		...uploadEvidence,
		stage: "runner-evidence",
		browserStatus: browserResult?.status ?? null,
		browserStage: browserResult?.stage ?? null,
		browserFailure: browserResult?.failure ?? null,
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		errorCollectionComplete: collectedErrorEvidence.errorCollectionComplete,
		errorCount: collectedErrorEvidence.errorCount,
		errors: collectedErrorEvidence.errors,
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete:
			collectedErrorEvidence.requestFailureCollectionComplete,
		requestFailureCount: collectedErrorEvidence.requestFailureCount,
		requestFailures: collectedErrorEvidence.requestFailures,
		resultFile,
		playwrightExitCode: processOutcome?.exitCode ?? null,
		playwrightSignal: processOutcome?.signal ?? null,
		playwrightSpawnError: processOutcome?.spawnError
			? serializeError(processOutcome.spawnError)
			: null,
		failure: primaryFailure,
		runnerFailure: failure == null ? null : serializeError(failure),
		resultEvidence:
			resultEvidence?.kind === "parsed" ? { kind: "parsed" } : resultEvidence,
		browserResult,
	};
};

export const executePlanContinuing = async ({
	plan,
	execute,
	shouldStop = () => false,
}) => {
	const outcomes = [];
	for (const entry of plan) {
		try {
			outcomes.push({
				entry,
				status: "fulfilled",
				value: await execute(entry),
			});
		} catch (error) {
			outcomes.push({
				entry,
				status: "rejected",
				error,
				failure: serializeError(error),
			});
			if (shouldStop(error, entry)) {
				break;
			}
		}
	}
	return outcomes;
};

export const countBenchmarkOutcomes = (results, planned = results.length) => {
	const passed = results.filter((result) => result.status === "passed").length;
	const failed = results.length - passed;
	return {
		planned,
		completed: results.length,
		passed,
		failed,
	};
};

export const classifySubprocessSummary = ({
	processOutcome,
	summaryEvidence,
	expectedSchema = BENCHMARK_SUMMARY_SCHEMA,
}) => {
	const failures = [];
	const processFailure = processOutcomeFailure(processOutcome);
	if (processFailure) {
		failures.push(processFailure);
	}
	let summary = null;
	if (summaryEvidence.kind !== "parsed") {
		failures.push({
			kind: `${summaryEvidence.kind}-summary`,
			message:
				summaryEvidence.kind === "missing"
					? `Benchmark subprocess did not write ${summaryEvidence.filePath}`
					: `Benchmark subprocess wrote malformed JSON in ${summaryEvidence.filePath}`,
		});
	} else {
		summary = summaryEvidence.value;
		if (
			summary.schema?.id !== expectedSchema.id ||
			summary.schema?.version !== expectedSchema.version
		) {
			failures.push({
				kind: "unsupported-summary",
				message: "Benchmark subprocess summary has an unsupported schema",
			});
		}
		if (!Array.isArray(summary.results)) {
			failures.push({
				kind: "invalid-summary",
				message: "Benchmark subprocess summary is missing its results array",
			});
		}
		if (
			summary.errorCollectionDefinition !== ERROR_COLLECTION_DEFINITION ||
			JSON.stringify(summary.knownPeerbitFailureSignatures) !==
				JSON.stringify(KNOWN_PEERBIT_FAILURE_SIGNATURES) ||
			summary.requestFailureCollectionDefinition !==
				REQUEST_FAILURE_COLLECTION_DEFINITION
		) {
			failures.push({
				kind: "invalid-summary-error-contract",
				message:
					"Benchmark subprocess summary has an invalid error evidence contract",
			});
		}
	}
	return {
		ok: failures.length === 0,
		processSucceeded: processFailure == null,
		summary,
		failures,
	};
};

export const inspectSingleInvocationSummary = (summary) => {
	if (!isRecord(summary)) {
		return {
			result: null,
			resultEvidence: null,
			failures: [
				{
					kind: "invalid-summary",
					message: "Benchmark subprocess summary is not an object",
				},
			],
		};
	}
	const results = Array.isArray(summary.results) ? summary.results : [];
	const [resultEvidence] = results;
	if (results.length !== 1 || !isRecord(resultEvidence)) {
		return {
			result: null,
			resultEvidence: resultEvidence ?? null,
			failures: [
				{
					kind: "result-cardinality",
					message:
						"Benchmark subprocess summary did not contain exactly one object result",
				},
			],
		};
	}
	const failures = [];
	if (!["passed", "failed"].includes(resultEvidence.status)) {
		failures.push({
			kind: "invalid-result-status",
			message: "Benchmark subprocess result has an unsupported status",
		});
	}
	if (summary.status !== resultEvidence.status) {
		failures.push({
			kind: "summary-status",
			message:
				"Benchmark subprocess summary status contradicts its result status",
		});
	}
	const expectedPassed = resultEvidence.status === "passed" ? 1 : 0;
	const expectedFailed = resultEvidence.status === "passed" ? 0 : 1;
	const counts = summary.outcomeCounts;
	if (
		!isRecord(counts) ||
		counts.planned !== 1 ||
		counts.completed !== 1 ||
		counts.passed !== expectedPassed ||
		counts.failed !== expectedFailed
	) {
		failures.push({
			kind: "summary-outcome-counts",
			message:
				"Benchmark subprocess summary outcome counts contradict its result",
		});
	}
	return { result: resultEvidence, resultEvidence, failures };
};
