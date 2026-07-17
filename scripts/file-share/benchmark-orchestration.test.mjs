import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	BENCHMARK_RESULT_SCHEMA,
	BENCHMARK_SUMMARY_SCHEMA,
	classifySubprocessSummary,
	countBenchmarkOutcomes,
	createInvocationFailureEvidence,
	ERROR_COLLECTION_DEFINITION,
	executePlanContinuing,
	extractCollectedErrorEvidence,
	inspectSingleInvocationSummary,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	readJsonEvidence,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
} from "./benchmark-orchestration.mjs";

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

test("constructs honest failure evidence from a failed browser result", () => {
	const browserResult = {
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
		fileMb: 5,
		runNonce: "nonce",
		invocation: { mode: "adaptive" },
		provenance: { peerbit: { resolvedCommit: "a" } },
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
	assert.equal(result.failure.kind, "nonzero-exit");
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
		fileMb: 5,
		runNonce: "nonce",
		invocation: {},
		provenance: {},
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
