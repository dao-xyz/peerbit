import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createBenchmarkInvocation } from "./benchmark-invocation.mjs";
import {
	BENCHMARK_RESULT_SCHEMA,
	BENCHMARK_SUMMARY_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
	SEEDER_DROP_POLICY,
	classifySubprocessSummary,
	countBenchmarkOutcomes,
	createInvocationFailureEvidence,
	executePlanContinuing,
	extractCollectedErrorEvidence,
	inspectSingleInvocationSummary,
	readJsonEvidence,
} from "./benchmark-orchestration.mjs";
import { validateBenchmarkResultEnvelope } from "./benchmark-validity.mjs";
import { createMatrixInvocationFailure } from "./run-file-share-benchmark-matrix.mjs";

const RUN_NONCE = "123e4567-e89b-42d3-a456-426614174000";
const FILE_MB = 5;
const FILE_SIZE_BYTES = FILE_MB * 1024 * 1024;
const SHA256 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
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
		requestedRef: "examples",
		resolvedCommit: "b".repeat(40),
		lockfileSha256: "c".repeat(64),
		dirty: false,
		worktreeDigest: null,
	},
};
const UPLOAD_INVOCATION = createBenchmarkInvocation({
	scenario: "upload",
	mode: "adaptive",
	network: "local",
	integrationMode: "link",
	fileMb: FILE_MB,
	fixtureSeed: "fixture-seed",
});
const uploadEnvelopeOptions = {
	expectedMode: "adaptive",
	expectedFileMb: FILE_MB,
	expectedNetwork: "local",
	expectedRunNonce: RUN_NONCE,
	expectedProvenance: PROVENANCE,
	expectedInvocation: UPLOAD_INVOCATION,
};

test("reads parsed, missing, and malformed JSON evidence without throwing", async () => {
	const root = await mkdtemp(path.join(os.tmpdir(), "peerbit-evidence-test-"));
	try {
		const parsedFile = path.join(root, "parsed.json");
		const malformedFile = path.join(root, "malformed.json");
		await writeFile(parsedFile, '{"status":"failed"}\n');
		await writeFile(malformedFile, "{\n");
		assert.deepEqual(await readJsonEvidence(parsedFile), {
			kind: "parsed",
			filePath: parsedFile,
			value: { status: "failed" },
		});
		assert.equal((await readJsonEvidence(malformedFile)).kind, "malformed");
		assert.equal(
			(await readJsonEvidence(path.join(root, "missing.json"))).kind,
			"missing",
		);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});

test("continues a counterbalanced plan after an invocation rejects", async () => {
	const visited = [];
	const outcomes = await executePlanContinuing({
		plan: [1, 2, 3],
		execute: async (entry) => {
			visited.push(entry);
			if (entry === 2) {
				throw new Error("synthetic failure");
			}
			return { status: "passed", entry };
		},
	});
	assert.deepEqual(visited, [1, 2, 3]);
	assert.deepEqual(
		outcomes.map((outcome) => outcome.status),
		["fulfilled", "rejected", "fulfilled"],
	);
	assert.match(outcomes[1].failure.message, /synthetic failure/);
});

test("stops a plan after an explicitly unsafe failure", async () => {
	const visited = [];
	const outcomes = await executePlanContinuing({
		plan: [1, 2, 3],
		shouldStop: (error) => error.stop === true,
		execute: async (entry) => {
			visited.push(entry);
			if (entry === 2) {
				const error = new Error("provenance drift");
				error.stop = true;
				throw error;
			}
			return { status: "passed" };
		},
	});
	assert.deepEqual(visited, [1, 2]);
	assert.equal(outcomes.length, 2);
	assert.equal(outcomes.at(-1).status, "rejected");
});

test("constructs honest standalone and matrix failures from browser evidence", () => {
	const browserResult = {
		runNonce: RUN_NONCE,
		status: "failed",
		stage: "verify-terminal-reader-topology",
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
		integrityVerified: true,
		integrityVerifiedAt: 1_205,
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		errorCollectionComplete: true,
		errorCount: 1,
		errors: ["writer:pageerror:boom"],
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete: true,
		requestFailureCount: 1,
		requestFailures: ["writer:requestfailed:{}"],
	};
	const result = createInvocationFailureEvidence({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		fileMb: FILE_MB,
		runNonce: RUN_NONCE,
		invocation: UPLOAD_INVOCATION,
		provenance: PROVENANCE,
		resultFile: "/tmp/result.json",
		processOutcome: {
			exitCode: 1,
			signal: null,
			spawnError: undefined,
		},
		resultEvidence: { kind: "parsed", value: browserResult },
		failure: new Error("Playwright failed"),
	});
	assert.deepEqual(result.schema, BENCHMARK_RESULT_SCHEMA);
	assert.equal(result.status, "failed");
	assert.equal(result.errorCollectionComplete, true);
	assert.equal(result.errorCount, 1);
	assert.equal(result.requestFailureCount, 1);
	assert.equal(result.browserResult, browserResult);
	assert.deepEqual(result.seederDropPolicy, SEEDER_DROP_POLICY);
	assert.equal(result.integrity, browserResult.integrity);
	assert.equal(result.integrityVerified, true);
	assert.equal(result.integrityVerifiedAt, 1_205);
	assert.equal(result.failure.kind, "nonzero-exit");
	assert.equal(
		validateBenchmarkResultEnvelope(result, uploadEnvelopeOptions).status,
		"failed",
	);

	for (const key of ["integrity", "integrityVerified", "integrityVerifiedAt"]) {
		const falsified = structuredClone(result);
		if (key === "integrity") {
			falsified.integrity = {
				...falsified.integrity,
				sourceCrc32Hex: "ffffffff",
			};
		} else if (key === "integrityVerified") {
			falsified.integrityVerified = false;
		} else {
			falsified.integrityVerifiedAt += 1;
		}
		assert.throws(
			() => validateBenchmarkResultEnvelope(falsified, uploadEnvelopeOptions),
			/projection does not exactly match/,
		);
	}
	for (const kind of ["missing", "malformed"]) {
		const mislabeled = structuredClone(result);
		mislabeled.resultEvidence = { kind };
		assert.throws(
			() => validateBenchmarkResultEnvelope(mislabeled, uploadEnvelopeOptions),
			/without parsed browser evidence must use a null integrity projection/,
		);
	}

	const contradictoryBrowserResult = structuredClone(browserResult);
	contradictoryBrowserResult.integrity.sourceCrc32Hex = "ffffffff";
	const sanitized = createInvocationFailureEvidence({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		fileMb: FILE_MB,
		runNonce: RUN_NONCE,
		invocation: UPLOAD_INVOCATION,
		provenance: PROVENANCE,
		resultFile: "/tmp/result.json",
		processOutcome: { exitCode: 1, signal: null },
		resultEvidence: { kind: "parsed", value: contradictoryBrowserResult },
		failure: new Error("contradictory integrity evidence"),
	});
	assert.equal(sanitized.integrity, null);
	assert.equal(sanitized.integrityVerified, false);
	assert.equal(sanitized.integrityVerifiedAt, null);
	assert.equal(
		validateBenchmarkResultEnvelope(sanitized, uploadEnvelopeOptions).status,
		"failed",
	);

	const matrixResult = createMatrixInvocationFailure({
		plan: { mode: "adaptive", variant: "candidate", run: 1, sequence: 0 },
		scenario: "upload",
		network: "local",
		fileMb: FILE_MB,
		summaryFile: "/tmp/summary.json",
		processOutcome: { exitCode: 1, signal: null },
		expectedInvocation: UPLOAD_INVOCATION,
		expectedProvenance: PROVENANCE,
		failures: [{ kind: "nonzero-exit", message: "sub-run failed" }],
		browserResult,
	});
	assert.deepEqual(matrixResult.seederDropPolicy, SEEDER_DROP_POLICY);
	assert.equal(matrixResult.integrity, browserResult.integrity);
	assert.equal(matrixResult.integrityVerified, true);
	assert.equal(matrixResult.integrityVerifiedAt, 1_205);
	assert.equal(
		validateBenchmarkResultEnvelope(matrixResult, uploadEnvelopeOptions).status,
		"failed",
	);
	const falsifiedMatrixResult = structuredClone(matrixResult);
	falsifiedMatrixResult.integrityVerifiedAt += 1;
	assert.throws(
		() =>
			validateBenchmarkResultEnvelope(
				falsifiedMatrixResult,
				uploadEnvelopeOptions,
			),
		/projection does not exactly match/,
	);
});

test("rejects malformed error entries instead of treating collection as complete", () => {
	const evidence = extractCollectedErrorEvidence({
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		errorCollectionComplete: true,
		errorCount: 1,
		errors: [null],
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete: true,
		requestFailureCount: 1,
		requestFailures: [null],
	});
	assert.equal(evidence.errorCollectionComplete, false);
	assert.equal(evidence.errorCount, null);
	assert.equal(evidence.requestFailureCollectionComplete, false);
	assert.equal(evidence.requestFailureCount, null);
});

test("uses null rather than claiming zero errors when result evidence is absent", () => {
	const result = createInvocationFailureEvidence({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		fileMb: FILE_MB,
		runNonce: RUN_NONCE,
		invocation: UPLOAD_INVOCATION,
		provenance: PROVENANCE,
		resultFile: "/tmp/missing.json",
		processOutcome: null,
		resultEvidence: {
			kind: "missing",
			filePath: "/tmp/missing.json",
			failure: { message: "ENOENT" },
		},
		failure: new Error("missing"),
	});
	assert.equal(result.errorCollectionComplete, false);
	assert.equal(result.errorCount, null);
	assert.equal(result.errors, null);
	assert.equal(result.failure.kind, "missing-result");
	assert.deepEqual(result.seederDropPolicy, SEEDER_DROP_POLICY);
	assert.equal(result.integrity, null);
	assert.equal(result.integrityVerified, false);
	assert.equal(result.integrityVerifiedAt, null);
	assert.equal(
		validateBenchmarkResultEnvelope(result, uploadEnvelopeOptions).status,
		"failed",
	);

	const malformed = createInvocationFailureEvidence({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		fileMb: FILE_MB,
		runNonce: RUN_NONCE,
		invocation: UPLOAD_INVOCATION,
		provenance: PROVENANCE,
		resultFile: "/tmp/malformed.json",
		processOutcome: null,
		resultEvidence: {
			kind: "malformed",
			filePath: "/tmp/malformed.json",
			failure: { message: "Unexpected end of JSON input" },
		},
		failure: new Error("malformed"),
	});
	assert.equal(malformed.integrity, null);
	assert.equal(malformed.integrityVerified, false);
	assert.equal(malformed.integrityVerifiedAt, null);
	assert.equal(
		validateBenchmarkResultEnvelope(malformed, uploadEnvelopeOptions).status,
		"failed",
	);
});

test("sanitizes malformed browser integrity evidence and rejects forged projections", () => {
	const browserResult = {
		status: "failed",
		integrity: { verified: true, sourceCrc32Hex: "forged" },
		integrityVerified: true,
		integrityVerifiedAt: "now",
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		errorCollectionComplete: true,
		errorCount: 0,
		errors: [],
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete: true,
		requestFailureCount: 0,
		requestFailures: [],
	};
	const result = createInvocationFailureEvidence({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		fileMb: FILE_MB,
		runNonce: RUN_NONCE,
		invocation: UPLOAD_INVOCATION,
		provenance: PROVENANCE,
		resultFile: "/tmp/result.json",
		processOutcome: { exitCode: 1, signal: null },
		resultEvidence: { kind: "parsed", value: browserResult },
		failure: new Error("malformed integrity evidence"),
	});
	assert.equal(result.integrity, null);
	assert.equal(result.integrityVerified, false);
	assert.equal(result.integrityVerifiedAt, null);
	assert.equal(
		validateBenchmarkResultEnvelope(result, uploadEnvelopeOptions).status,
		"failed",
	);

	const forged = structuredClone(result);
	forged.integrity = structuredClone(browserResult.integrity);
	forged.integrityVerified = true;
	forged.integrityVerifiedAt = 1_205;
	assert.throws(
		() => validateBenchmarkResultEnvelope(forged, uploadEnvelopeOptions),
		/projection does not exactly match/,
	);
});

test("does not add upload-only evidence to seeder-probe failures", () => {
	const invocation = createBenchmarkInvocation({
		scenario: "seeder-probe",
		mode: "adaptive",
		network: "local",
		integrationMode: "link",
		fileMb: 1,
		fixtureSeed: "fixture-seed",
	});
	const result = createInvocationFailureEvidence({
		scenario: "seeder-probe",
		mode: "adaptive",
		network: "local",
		fileMb: 1,
		runNonce: RUN_NONCE,
		invocation,
		provenance: PROVENANCE,
		resultFile: "/tmp/missing.json",
		processOutcome: null,
		resultEvidence: {
			kind: "missing",
			filePath: "/tmp/missing.json",
			failure: { message: "ENOENT" },
		},
		failure: new Error("missing"),
	});
	for (const key of [
		"seederDropPolicy",
		"integrity",
		"integrityVerified",
		"integrityVerifiedAt",
	]) {
		assert.equal(
			Object.hasOwn(result, key),
			false,
			`${key} must stay upload-only`,
		);
	}
	assert.equal(
		validateBenchmarkResultEnvelope(result, {
			expectedMode: "adaptive",
			expectedFileMb: 1,
			expectedNetwork: "local",
			expectedRunNonce: RUN_NONCE,
			expectedProvenance: PROVENANCE,
			expectedInvocation: invocation,
		}).status,
		"failed",
	);
});

test("preserves a parsed sub-run summary while classifying nonzero exit", () => {
	const summary = {
		schema: BENCHMARK_SUMMARY_SCHEMA,
		status: "failed",
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		results: [{ status: "failed" }],
	};
	const classification = classifySubprocessSummary({
		processOutcome: { exitCode: 1, signal: null, spawnError: undefined },
		summaryEvidence: {
			kind: "parsed",
			filePath: "/tmp/summary.json",
			value: summary,
		},
	});
	assert.equal(classification.ok, false);
	assert.equal(classification.processSucceeded, false);
	assert.equal(classification.summary, summary);
	assert.deepEqual(
		classification.failures.map((failure) => failure.kind),
		["nonzero-exit"],
	);
});

test("inspects one sub-run result and rejects contradictory aggregate claims", () => {
	const passed = { status: "passed" };
	assert.deepEqual(
		inspectSingleInvocationSummary({
			status: "passed",
			outcomeCounts: { planned: 1, completed: 1, passed: 1, failed: 0 },
			results: [passed],
		}),
		{ result: passed, resultEvidence: passed, failures: [] },
	);
	assert.deepEqual(
		inspectSingleInvocationSummary({
			status: "passed",
			outcomeCounts: { planned: 1, completed: 1, passed: 1, failed: 0 },
			results: { status: "passed" },
		}).failures.map((failure) => failure.kind),
		["result-cardinality"],
	);
	assert.deepEqual(
		inspectSingleInvocationSummary({
			status: "failed",
			outcomeCounts: { planned: 1, completed: 1, passed: 1, failed: 0 },
			results: [passed],
		}).failures.map((failure) => failure.kind),
		["summary-status"],
	);
	assert.deepEqual(
		inspectSingleInvocationSummary({
			status: "unknown",
			outcomeCounts: { planned: 1, completed: 1, passed: 0, failed: 1 },
			results: [{ status: "unknown" }],
		}).failures.map((failure) => failure.kind),
		["invalid-result-status"],
	);
});

test("counts failed and passed aggregate outcomes", () => {
	assert.deepEqual(
		countBenchmarkOutcomes([{ status: "passed" }, { status: "failed" }], 3),
		{ planned: 3, completed: 2, passed: 1, failed: 1 },
	);
});
