import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import {
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
} from "./benchmark-orchestration.mjs";
import { repoRoot } from "./common.mjs";

const templates = [
	"upload-benchmark.local.e2e.spec.ts",
	"seeder-probe.e2e.spec.ts",
];

for (const name of templates) {
	test(`${name} emits the atomic v4 invocation envelope`, async () => {
		const contents = await readFile(
			path.join(repoRoot, "scripts", "file-share", "templates", name),
			"utf8",
		);
		for (const required of [
			'id: "peerbit-file-share-benchmark"',
			"version: 4",
			"PW_BENCHMARK_RUN_NONCE",
			"PW_BENCHMARK_INVOCATION",
			"PW_BENCHMARK_PROVENANCE",
			'process.env.PW_BENCH !== "1"',
			'serverMode: "production-preview"',
			"schema: RESULT_SCHEMA",
			"runNonce: RUN_NONCE",
			"invocation: INVOCATION",
			"provenance: PROVENANCE",
			"errorCollectionDefinition: ERROR_COLLECTION_DEFINITION",
			"knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES",
			"errorCollectionComplete: true",
			"requestFailureCollectionDefinition:",
			"requestFailureCollectionComplete: true",
			"requestFailureCount:",
			"requestFailures:",
			'page.on("requestfailed"',
			"await rename(temporaryPath, RESULT_FILE)",
			"await rm(temporaryPath, { force: true })",
		]) {
			assert.ok(contents.includes(required), `missing ${required}`);
		}
		for (const required of [
			"getLightweightSnapshot",
			"diagnostics?.programAddress",
			"diagnostics.programClosed === false",
			"await hooks.setReplicationRole(role)",
			"timeout: READY_TIMEOUT_MS",
			name === "upload-benchmark.local.e2e.spec.ts"
				? "3 * READY_TIMEOUT_MS"
				: "2 * READY_TIMEOUT_MS",
		]) {
			assert.ok(
				contents.includes(required),
				`${name} must wait for a live program before applying a role`,
			);
		}
		assert.ok(
			contents.indexOf("await writeFile(temporaryPath") <
				contents.indexOf("await rename(temporaryPath, RESULT_FILE)"),
			"result must be fully written before its atomic rename",
		);
		assert.ok(
			contents.indexOf("await rename(temporaryPath, RESULT_FILE)") <
				contents.indexOf("await rm(temporaryPath, { force: true })"),
			"a failed atomic rename must clean its temporary result",
		);
		assert.ok(
			!contents.includes("`error:${error.message}`"),
			"seeder-count failures must reject instead of becoming sample strings",
		);
		assert.ok(contents.includes(ERROR_COLLECTION_DEFINITION));
		assert.ok(contents.includes(REQUEST_FAILURE_COLLECTION_DEFINITION));
		for (const signature of KNOWN_PEERBIT_FAILURE_SIGNATURES) {
			assert.ok(contents.includes(`"${signature}"`));
		}
		assert.match(
			contents,
			/page\.on\("pageerror", \(error\) => \{[\s\S]*?errors\.push\(`\$\{label\}:pageerror:/,
			"every uncaught page error must be recorded without signature filtering",
		);
		assert.match(
			contents,
			/message\.type\(\) === "error" \|\|[\s\S]*?KNOWN_PEERBIT_FAILURE_SIGNATURES\.some/,
			"every console.error and matched Peerbit signature must be recorded",
		);
	});
}

test("seeder probe records enforceable convergence timing evidence", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"seeder-probe.e2e.spec.ts",
		),
		"utf8",
	);
	for (const required of [
		"readyDeadlineAt = probeStartedAt + READY_TIMEOUT_MS",
		"current.writerSeeders >= TARGET_SEEDERS",
		"current.readerSeeders >= TARGET_SEEDERS",
		"probeDurationMs",
		"timeToTargetMs",
		"targetSampleLabel",
		"effectiveSampleIntervalMs",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
});

test("upload probe fails closed and records bounded scheduling tolerances", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"upload-benchmark.local.e2e.spec.ts",
		),
		"utf8",
	);
	for (const required of [
		"readSeederCount(writer",
		"readSeederCount(reader",
		"timeToWriterReadyMs",
		"timeToReaderReadyMs",
		"writerListedAt - uploadStartedAt",
		"readerListedAt - uploadStartedAt",
		"readyTimeoutMs: READY_TIMEOUT_MS",
		"3 * READY_TIMEOUT_MS +",
		"POST_MONITOR_SCHEDULING_TOLERANCE_MS",
		"TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS",
		"Measured transfer duration exceeded",
		"PW_DOWNLOAD_SINK",
		"installHashOnlyMockSaveFilePicker",
		"installMockSaveFilePicker",
		"installNodeBackedMockSaveFilePicker",
		"summarizeReadTransferDiagnostics",
		"libraryComputedSha256Base64",
		'import { withDeadline } from "./generated.promise-deadline.mjs"',
		"UPLOAD_TIMEOUT_MS +",
		"READY_TIMEOUT_MS +",
		"const boundedReaderListedPromise =",
		"const readerManifest = await boundedReaderListedPromise",
		"readerListedPromise,",
		"readerReadyRemainingMs,",
		"Reader ready manifest was not listed within ${READY_TIMEOUT_MS}ms after writer readiness",
		"sinkAwaitSubtractedDiagnosticMs",
		"primaryDownloadMetric: PRIMARY_DOWNLOAD_METRIC",
		'primaryDownloadAuthoritative: DOWNLOAD_SINK === "hash-only"',
		"sinkWriteAwaitMs",
		"file-share-benchmark-${MODE}-${RUN_NONCE}.bin",
		"fileId: file.id",
		"sha256AndCrc32OpfsSavedViaPicker",
		'import { SHA256 } from "@stablelib/sha256"',
		"() => new SHA256()",
		"downloadCompletionObservedAt = Date.now()",
		"downloadFinishedAt = download.sinkCompletedAt",
		"download.sinkCompletedAt < downloadClickStartedAt",
		"download.sinkCompletedAt > downloadCompletionObservedAt",
		"SINK_WRITE_QUANTIZATION_ALLOWANCE_MS_PER_CHUNK",
		"SINK_SERVER_CLOCK_TOLERANCE_MS_PER_CHUNK",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
	assert.ok(
		contents.includes(
			"const MIN_READY_SEEDERS = Number(process.env.PW_MIN_READY_SEEDERS)",
		),
		"the template must consume the resolved invocation value",
	);
	assert.ok(
		!contents.includes('MODE === "adaptive" ? "2" : "0"'),
		"the template must not redefine mode-specific ready-seeder defaults",
	);
	assert.ok(
		!contents.includes("MIN_READY_SEEDERS, 180_000"),
		"the upload probe must honor the invocation readiness timeout",
	);
	assert.ok(
		!contents.includes("downloadFinishedAt = Date.now()"),
		"download duration must end at the primary sink's completion timestamp",
	);
	assert.ok(
		!contents.includes("createHash"),
		"the browser Playwright template must use a browser-compatible incremental SHA-256 implementation",
	);
	assert.ok(
		!contents.includes('crypto.subtle.digest("SHA-256"'),
		"OPFS SHA-256 readback must remain bounded rather than buffering the file",
	);
	assert.match(
		contents,
		/const readerListedPromise = waitForReadyManifest\(\s*reader,\s*fileName,\s*expectedSizeBytes,\s*preparedFile\.fixture\.sha256Base64,\s*UPLOAD_TIMEOUT_MS \+\s*READY_TIMEOUT_MS \+\s*TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,\s*\)/,
		"the early reader observation must remain alive through upload plus the post-writer readiness window",
	);
	assert.match(
		contents,
		/uploadSettledAt = writerReady\.readyAt;[\s\S]*?const boundedReaderListedPromise =[\s\S]*?withDeadline\(\s*readerListedPromise,\s*readerReadyRemainingMs,[\s\S]*?if \(ENABLE_VISIBILITY_PROBE\)/,
		"reader listing must arm its Node-side deadline before optional diagnostics",
	);
	const opfsReadback = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"opfs-readback.mjs",
		),
		"utf8",
	);
	for (const required of [
		"OPFS_READBACK_CHUNK_BYTES",
		"file.slice(start, finish).arrayBuffer()",
		"totalBytes !== expectedSizeBytes",
		"sha256.update(chunk)",
		"updateCrc32State(crc32State, chunk)",
	]) {
		assert.ok(
			opfsReadback.includes(required),
			`OPFS helper missing ${required}`,
		);
	}
	assert.ok(
		!opfsReadback.includes("createHash") &&
			!opfsReadback.includes("crypto.subtle.digest"),
		"the OPFS helper must use its browser-compatible incremental SHA factory",
	);
	for (const peer of ["writer", "reader"]) {
		assert.match(
			contents,
			new RegExp(
				`expectSeedersAtLeast\\(\\s*${peer},\\s*MIN_READY_SEEDERS,\\s*READY_TIMEOUT_MS,?\\s*\\)`,
			),
			`${peer} readiness must use the invocation timeout`,
		);
	}
});

test("aggregate comparisons require every planned invocation to pass", async () => {
	const standalone = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"run-file-share-benchmark.mjs",
		),
		"utf8",
	);
	for (const required of [
		"compareUploadPerformanceModesForCompletePlan(results, outcomeCounts)",
		"if (comparison)",
		'generatedPath: path.join("tests", "generated.promise-deadline.mjs")',
		'generatedPath: path.join("tests", "generated.opfs-readback.mjs")',
		"scenarioConfig.supportFiles ?? []",
	]) {
		assert.ok(standalone.includes(required), `standalone missing ${required}`);
	}
	assert.ok(
		standalone.indexOf("const outcomeCounts = countBenchmarkOutcomes") <
			standalone.indexOf("const comparison ="),
		"standalone comparison must be gated by complete outcome counts",
	);

	const matrix = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"run-file-share-benchmark-matrix.mjs",
		),
		"utf8",
	);
	for (const required of [
		"compareUploadPerformanceModesForCompletePlan(",
		"variantOutcomeCounts",
		"const matrixPlanPassed = isCompletePassedBenchmarkPlan(",
		"adaptiveComparison: matrixPlanPassed",
		"if (matrixSummary.adaptiveComparison)",
		"adaptiveLibraryStreamWallMsAvg",
		"libraryStreamWallDeltaMs",
		"primaryDownloadAuthoritative",
	]) {
		assert.ok(matrix.includes(required), `matrix missing ${required}`);
	}
});

test("matrix fail-closes every result envelope and fully validates passes", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"run-file-share-benchmark-matrix.mjs",
		),
		"utf8",
	);
	for (const required of [
		"validateBenchmarkResultEnvelope(rawResult",
		"validateBenchmarkResult(rawResult",
		"expectedInvocation = createBenchmarkInvocation",
		"expectedProvenance",
		"invocation: expectedInvocation ?? null",
		"provenance: expectedProvenance",
		"peerbitProvenance: prepared.peerbitProvenance",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
	assert.ok(
		!contents.includes("results[0]?.provenance?.peerbit"),
		"rejected result provenance must not become canonical variant provenance",
	);
});

test("standalone runner rechecks provenance even after result failure", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"run-file-share-benchmark.mjs",
		),
		"utf8",
	);
	assert.ok(contents.includes("readAndAssertSafeInvocationUnchanged"));
	assert.ok(contents.includes("unsafeProvenanceFailure ??= error"));
	assert.ok(
		contents.indexOf("if (unsafeProvenanceFailure)") <
			contents.indexOf("if (invocationFailure)"),
		"unsafe provenance drift must supersede an ordinary result failure",
	);
});
