import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
} from "./benchmark-orchestration.mjs";
import { repoRoot } from "./common.mjs";
import { instrumentFileShareFrontend } from "./run-file-share-benchmark.mjs";

const templates = [
	"upload-benchmark.local.e2e.spec.ts",
	"seeder-probe.e2e.spec.ts",
];

for (const name of templates) {
	test(`${name} emits the atomic v8 result envelope`, async () => {
		const contents = await readFile(
			path.join(repoRoot, "scripts", "file-share", "templates", name),
			"utf8",
		);
		for (const required of [
			'id: "peerbit-file-share-benchmark"',
			"version: 8",
			"PW_BENCHMARK_RUN_NONCE",
			"PW_BENCHMARK_INVOCATION",
			"PW_BENCHMARK_PROVENANCE",
			"PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT",
			"PW_READER_TERMINAL_TOPOLOGY",
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
		"const LOCALITY_CONTROL_OUTER_TIMEOUT_BUDGET_MS =",
		"READER_LOCAL_CHUNK_TARGET > 0",
		"DOWNLOAD_TIMEOUT_MS + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS",
		"LOCALITY_CONTROL_OUTER_TIMEOUT_BUDGET_MS +",
		"test.setTimeout(TEST_OUTER_TIMEOUT_MS)",
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
		'import { startDownloadMemoryTelemetry } from "./generated.download-memory-telemetry.mjs"',
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
		"PW_READER_LOCAL_CHUNK_TARGET",
		"PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT",
		"PW_READER_TERMINAL_TOPOLOGY",
		"LOCALITY_CONTROL_POLL_MS",
		"seedReplicationRole",
		"openWithSeededReplicationRole",
		"readerInitialRoleEvidence",
		"preloadLocalChunkPrefix",
		"Reader locality preload did not settle within ${timeoutMs}ms plus scheduling tolerance",
		"aggregateTimeoutMs !== DOWNLOAD_TIMEOUT_MS",
		"aggregateTimedOut !== false",
		"readLocalChunkPrefixObservation",
		"getTopologySnapshot",
		"topologyHasExactWriterSingleton",
		"writerTopology.replicatorHashes[0] === writerPeerHash",
		"readerTopology.replicatorHashes[0] === writerPeerHash",
		"writerTopologyBeforeUpload",
		"readerTopologyBeforeTimedRead",
		"requestedLocalChunkBlockCount",
		'provisioningMethod: "exact-manifest-head-import"',
		"stabilityObservations",
		"preDownloadObservation",
		"waitForTerminalReaderIdle",
		"collectStableTerminalTopology",
		"terminalIdleObservation",
		"terminalTopologyObservations",
		"expectedTerminalTopology: READER_TERMINAL_TOPOLOGY",
		"topologyMatchesTerminalExpectation",
		"terminalTopologyExpectationSatisfied = true",
		'readerLocalityControl.status = "complete"',
		"exact contiguous prefix",
		"SEEDER_DROP_POLICY",
		"consecutiveBelowBaselineSeederSnapshots",
		"seederDropPolicy: SEEDER_DROP_POLICY",
		"terminalSeederSnapshot",
		"unexpectedSeederDrop",
		"downloadMemoryTelemetryController =",
		"downloadMemoryTelemetryController.snapshot()",
		"await downloadMemoryTelemetryController.stopSampling()",
		"await downloadMemoryTelemetryController.cleanup()",
		"downloadMemoryTelemetry.complete !== true",
		"series.samplingErrors.length > 0",
		"downloadMemoryTelemetry,",
		"let integrityVerifiedAt: number | null = null",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
	assert.match(
		contents,
		/const terminalSeederSnapshot = await snapshot\([\s\S]*?noteSeederDrop\(terminalSeederSnapshot\);[\s\S]*?if \(unexpectedSeederDrop\)/,
		"the final numeric seeder snapshot must participate in the v8 drop policy before acceptance",
	);
	const lateFailureCatch = contents.slice(
		contents.lastIndexOf("\t\t} catch (error: any) {"),
	);
	assert.ok(
		lateFailureCatch.includes(
			"\n\t\t\t\tintegrity,\n\t\t\t\tintegrityVerified,\n\t\t\t\tintegrityVerifiedAt,",
		),
		"late failures must preserve completed integrity evidence and its gate timestamp",
	);
	assert.ok(
		!lateFailureCatch.includes("integrityVerified: false"),
		"the catch path must not overwrite completed integrity state",
	);
	assert.match(
		contents,
		/const LOCALITY_CONTROL_OUTER_TIMEOUT_BUDGET_MS =\s*READER_LOCAL_CHUNK_TARGET === null\s*\? 0\s*:\s*3 \* READY_TIMEOUT_MS \+\s*\(READER_LOCAL_CHUNK_TARGET > 0\s*\? DOWNLOAD_TIMEOUT_MS \+ TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS\s*: 0\);/,
		"controlled locality must reserve three readiness phases and a nonzero-prefix aggregate preload deadline plus cleanup tolerance",
	);
	assert.match(
		contents,
		/hooks\.preloadLocalChunkPrefix\(\s*expectedFileName,\s*requestedTarget,\s*timeout,\s*\)/,
		"the nonzero locality preload must pass the requested download timeout in the hook's third argument",
	);
	assert.match(
		contents,
		/const preloadLocalChunkPrefix = async \(\s*page: Page,\s*fileName: string,\s*target: number,\s*timeoutMs: number,\s*\)/,
		"the Playwright locality helper must keep page, file, target, and timeout in a fixed order",
	);
	assert.match(
		contents,
		/preloadLocalChunkPrefix\(\s*reader,\s*fileName,\s*READER_LOCAL_CHUNK_TARGET,\s*DOWNLOAD_TIMEOUT_MS,\s*\)/,
		"the nonzero locality call site must pass the configured download timeout after its target",
	);
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
		contents.indexOf("downloadMemoryTelemetryController =") <
			contents.indexOf("const initialMemorySeries =") &&
			contents.indexOf("const initialMemorySeries =") <
				contents.indexOf("const downloadCompletion = armSavedViaPicker(") &&
			contents.indexOf("const downloadCompletion = armSavedViaPicker(") <
				contents.indexOf("const downloadClickStartedAt = Date.now()"),
		"memory telemetry must collect clean initial samples before the sink waiter is armed and the timed click begins",
	);
	assert.ok(
		contents.indexOf("const download = await downloadCompletion") <
			contents.indexOf(
				"await downloadMemoryTelemetryController.stopSampling()",
			) &&
			contents.indexOf(
				"await downloadMemoryTelemetryController.stopSampling()",
			) < contents.indexOf('stage = "verify-integrity"'),
		"memory telemetry sampling must stop at sink completion before integrity and topology work",
	);
	assert.ok(
		contents.indexOf("integrityVerifiedAt = Date.now()") <
			contents.indexOf("await downloadMemoryTelemetryController.cleanup()"),
		"CDP cleanup must start only after transfer integrity evidence is finalized",
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
	assert.match(
		contents,
		/await seedReplicationRole\(reader, shareAddress, false\);[\s\S]*?readerLocalityControl\s*\? openWithSeededReplicationRole\(reader, shareUrl\)\s*: applyRole\(reader, shareUrl\)[\s\S]*?readInitialReaderRoleEvidence\(reader\)[\s\S]*?writerTopologyBeforeUpload[\s\S]*?uploadStartedAt = Date\.now\(\);[\s\S]*?setInputFiles/,
		"the reader observer role must be seeded before navigation and proven before upload",
	);
	assert.match(
		contents,
		/const openWithSeededReplicationRole =[\s\S]*?page\.goto\(shareUrl,[\s\S]*?await waitForTestHooks\(page\);\s*};/,
		"the controlled reader must not invoke the post-open role setter",
	);
	assert.ok(
		contents.includes("/[/?#]/.test(shareAddress)"),
		"the seeded role key must come from exactly one non-empty share-address segment",
	);
	assert.match(
		contents,
		/postMonitorFinishedAt = Date\.now\(\);[\s\S]*?beforePreloadObservation[\s\S]*?preloadLocalChunkPrefix\([\s\S]*?collectStableReaderLocality\(\)[\s\S]*?writerTopologyBeforeTimedRead[\s\S]*?stage = "download-to-selected-sink"/,
		"preload closure, stable exact-prefix evidence, and topology must precede the timed read",
	);
	assert.match(
		contents,
		/if \(!integrityVerified\)[\s\S]*?integrityVerifiedAt = Date\.now\(\);[\s\S]*?readerLocalityControl\.integrityVerifiedAt = integrityVerifiedAt;[\s\S]*?waitForTerminalReaderIdle\(\)[\s\S]*?collectStableTerminalTopology\(\s*terminalTopologyStartedAt,\s*terminalTopologyDeadlineAt,\s*\)[\s\S]*?terminalTopologyExpectationSatisfied = true[\s\S]*?const terminalSeederSnapshot = await snapshot\([\s\S]*?stage = "complete"/,
		"terminal topology and the final seeder snapshot must follow the aggregate integrity gate outside the measured download",
	);
	assert.ok(
		!contents.includes("monitorAndFreezeReaderLocality"),
		"controlled locality must not race a role switch against upload replication",
	);
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
		"peerbit-benchmark-upload-diagnostics",
		"lastUploadDiagnostics:",
		"(program as any)?.lastUploadDiagnostics ?? null",
		'requireUploadDiagnostics: scenario === "upload"',
		"peerbit-benchmark-locality-prefix-preload",
		"peerbit-benchmark-locality-preload-aggregate-deadline",
		"peerbit-benchmark-locality-exact-manifest-import",
		"peerbit-benchmark-locality-replicator-hashes",
		"peerbit-benchmark-locality-indexed-search",
		"replicatorHashes: replicatorHashes",
		"preloadLocalChunkPrefix",
		"getLocalChunkPrefixObservation",
		"const localityPreloadHookImplementation =",
		"const aggregateController = new AbortController()",
		"signal: aggregateController.signal",
		"aggregateController.abort(aggregateTimeoutError)",
		"const manifestEntryHeads = (file as any).chunkEntryHeads",
		'const documentId = \\`\\${file.id}:\\${index}\\`',
		"program.retainChunkRead(documentId, file.id)",
		"program.retainChunkEntryHead(\n\t\t\t\t\t\t\t\t\t\t\thead,\n\t\t\t\t\t\t\t\t\t\t\tfile.id,\n\t\t\t\t\t\t\t\t\t\t\tdocumentId",
		"const rawEntry = await blocks.get(head",
		"localManifestEntryIndicesAfter",
		"window.clearTimeout(aggregateTimeoutHandle)",
		"aggregateDeadlineAt",
		"aggregateTimedOut",
		"SearchRequestIndexed",
		"new SearchRequestIndexed({ fetch: 0xffffffff })",
		"resolve: false",
		"program.countLocalChunks(file)",
		"requireLocalityHook: readerLocalChunkTarget != null",
	]) {
		assert.ok(
			standalone.includes(required),
			`standalone locality instrumentation missing ${required}`,
		);
	}
	assert.equal(
		[
			...standalone.matchAll(
				/benchmarkStats: testWindow\.__peerbitFileShareBenchmarkStats \?\? null,/g,
			),
		].length,
		1,
		"the fallback benchmark hook must emit exactly one benchmarkStats field",
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

test("locality instrumentation migrates a reused legacy query idempotently", async (t) => {
	const frontendRoot = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-locality-instrumentation-"),
	);
	t.after(async () => rm(frontendRoot, { recursive: true, force: true }));
	const src = path.join(frontendRoot, "src");
	await mkdir(src);
	const dropPath = path.join(src, "Drop.tsx");
	await writeFile(
		dropPath,
		`import { IsNull, SearchRequest } from "@peerbit/document";
const __peerbitFileShareTestHooks = {
    setReplicationRole,
    getDiagnostics,
            preloadLocalChunkPrefix: async (fileName, target, timeoutMs) => {
                /* peerbit-benchmark-locality-prefix-preload */
                const startedAt = Date.now();
                const transferId = "legacy-locality-preload";
                let yieldedChunkCount = 0;
                let yieldedByteCount = 0;
                const iterator = file
                    .streamFile(program, { timeout: timeoutMs, transferId })
                    [Symbol.asyncIterator]();
                try {
                    while (yieldedChunkCount < target) {
                        const next = await iterator.next();
                        yieldedChunkCount += 1;
                        yieldedByteCount += next.value.byteLength;
                    }
                } finally {
                    await iterator.return?.();
                }
                return {
                    startedAt,
                    finishedAt: Date.now(),
                    transferId,
                    yieldedChunkCount,
                    yieldedByteCount,
                };
            },
            getLocalChunkPrefixObservation: async (fileName) => {
                const local = await program.files.index.search(
                    new SearchRequest({ fetch: 0xffffffff }),
                    { local: true, remote: false, resolve: false }
                );
                return { fileName, local };
            },
};
const __peerbitFileShareBenchmarkStats = { updateListStats };
updateListCalls.push(updateListStats);
/* peerbit-benchmark-locality-replicator-hashes */
`,
	);
	await instrumentFileShareFrontend(frontendRoot, {
		requireLocalityHook: true,
	});
	const migrated = await readFile(dropPath, "utf8");
	assert.ok(
		migrated.includes(
			'import { IsNull, SearchRequest, SearchRequestIndexed } from "@peerbit/document";',
		),
	);
	assert.ok(
		migrated.includes("/* peerbit-benchmark-locality-indexed-search */"),
	);
	assert.ok(
		migrated.includes("new SearchRequestIndexed({ fetch: 0xffffffff })"),
	);
	assert.ok(!migrated.includes("new SearchRequest({ fetch: 0xffffffff })"));
	for (const required of [
		"/* peerbit-benchmark-locality-preload-aggregate-deadline */",
		"/* peerbit-benchmark-locality-exact-manifest-import */",
		"const aggregateController = new AbortController()",
		"signal: aggregateController.signal",
		"const manifestEntryHeads = (file as any).chunkEntryHeads",
		"const documentId = `${file.id}:${index}`",
		"program.retainChunkRead(documentId, file.id)",
		"program.retainChunkEntryHead(",
		"const rawEntry = await blocks.get(head",
		"localManifestEntryIndicesAfter",
		"aggregateTimeoutMs",
		"aggregateDeadlineAt",
		"aggregateTimedOut",
	]) {
		assert.ok(migrated.includes(required), `missing migrated ${required}`);
	}
	assert.ok(
		!migrated.includes(".streamFile(program"),
		"exact locality provisioning must not start the product stream read-ahead path",
	);
	assert.equal(
		[...migrated.matchAll(/preloadLocalChunkPrefix: async/g)].length,
		1,
	);
	assert.equal(
		[...migrated.matchAll(/getLocalChunkPrefixObservation: async/g)].length,
		1,
	);
	assert.equal(
		[
			...migrated.matchAll(
				/import \{ IsNull, SearchRequest, SearchRequestIndexed \} from "@peerbit\/document";/g,
			),
		].length,
		1,
	);
	await instrumentFileShareFrontend(frontendRoot, {
		requireLocalityHook: true,
	});
	assert.equal(await readFile(dropPath, "utf8"), migrated);
});

test("upload diagnostics instrumentation migrates a reused v5 fallback idempotently", async (t) => {
	const frontendRoot = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-upload-diagnostics-instrumentation-"),
	);
	t.after(async () => rm(frontendRoot, { recursive: true, force: true }));
	const src = path.join(frontendRoot, "src");
	await mkdir(src);
	const dropPath = path.join(src, "Drop.tsx");
	await writeFile(
		dropPath,
		`/* peerbit-benchmark-hook */
const __peerbitFileShareTestHooks = {
    setReplicationRole,
    getDiagnostics: async () => {
        const program = files.program;
        const connections = [];
        return {
                    programAddress: program?.address ?? null,
                    programClosed: program?.closed ?? null,
                    connectionCount: connections.length,
                    connectionPeers: connections,
                    replicatorCount: null,
        };
    },
};
/* peerbit-benchmark-update-list */
const __peerbitFileShareBenchmarkStats = { updateListStats };
const updateListStats = {};
updateListCalls.push(updateListStats);
`,
	);
	await instrumentFileShareFrontend(frontendRoot, {
		requireUploadDiagnostics: true,
	});
	const migrated = await readFile(dropPath, "utf8");
	assert.ok(migrated.includes("/* peerbit-benchmark-upload-diagnostics */"));
	assert.ok(
		migrated.includes("(program as any)?.lastUploadDiagnostics ?? null"),
	);
	assert.equal([...migrated.matchAll(/lastUploadDiagnostics:/g)].length, 1);
	await instrumentFileShareFrontend(frontendRoot, {
		requireUploadDiagnostics: true,
	});
	assert.equal(await readFile(dropPath, "utf8"), migrated);
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
		"groupUploadResultsByLocalityCohort(results)",
		"readerLocalityCohorts",
		"if (comparison)",
		'generatedPath: path.join("tests", "generated.promise-deadline.mjs")',
		'generatedPath: path.join("tests", "generated.opfs-readback.mjs")',
		'"generated.download-memory-telemetry.mjs"',
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
		"groupUploadResultsByLocalityCohort(results)",
		"readerLocalityCohortKey",
		"readerLocalityCohorts",
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

test("download memory support is bounded, serial, and cleanup-safe", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"download-memory-telemetry.mjs",
		),
		"utf8",
	);
	for (const required of [
		'DOWNLOAD_MEMORY_PROFILE = "download-memory-v2"',
		"DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS = 5_000",
		"DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS = 4_000",
		"DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS = 9_000",
		"DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS = 30_000",
		"DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS = 30_000",
		"DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES = 4_096",
		"DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES = 256",
		"DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS = 16",
		"DOWNLOAD_MEMORY_MAX_CLEANUP_WARNINGS = 16",
		"DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH = 512",
		"startBoundedSerialSampler",
		"withDownloadMemoryOperationDeadline",
		"onLateResolution",
		"terminalSampleFailure = true",
		"samples.length >= maxSamples - 1",
		"await activeSample",
		"await takeSample(true)",
		"newCDPSession(page)",
		'"Page JS heap CDP session creation"',
		"Performance.getMetrics",
		'metric.name === "JSHeapUsedSize"',
		"newBrowserCDPSession()",
		'"Browser RSS CDP session creation"',
		"SystemInfo.getProcessInfo",
		'"ps"',
		"process.memoryUsage().rss",
		"playwright-worker-node-including-in-process-local-bootstrap",
		"samplingErrorOverflowCount",
		"cleanupWarningOverflowCount",
		"stopSampling",
	]) {
		assert.ok(
			contents.includes(required),
			`memory support missing ${required}`,
		);
	}
	assert.ok(
		contents.indexOf("await activeSample") <
			contents.indexOf("await takeSample(true)"),
		"stop must drain an active sample before the forced terminal sample",
	);
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
		"projectUploadIntegrityEvidence(browserResult, {",
		"seederDropPolicy: SEEDER_DROP_POLICY",
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
