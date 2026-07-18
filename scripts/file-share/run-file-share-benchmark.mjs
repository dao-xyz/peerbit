import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
	MAX_READER_LOCAL_CHUNK_OVERSHOOT,
	assertBenchmarkFileSize,
	assertCoreBenchmarkUsesLocalApp,
	assertInvocationUnchanged,
	createBenchmarkInvocation,
	createNonceIsolatedResultPath,
	createPlaywrightBenchmarkEnvironment,
	resolveBenchmarkDownloadSink,
	resolveBenchmarkPreviewOptions,
} from "./benchmark-invocation.mjs";
import {
	BENCHMARK_SUMMARY_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
	countBenchmarkOutcomes,
	createInvocationFailureEvidence,
	executePlanContinuing,
	readJsonEvidence,
} from "./benchmark-orchestration.mjs";
import {
	getExamplesProvenance,
	getPeerbitProvenance,
	resolveGitCommitAt,
} from "./benchmark-provenance.mjs";
import {
	compareUploadPerformanceModesForCompletePlan,
	groupUploadResultsByLocalityCohort,
	isCompletePassedBenchmarkPlan,
	summarizeUploadPerformance,
	uploadTimingTableColumns,
} from "./benchmark-summary.mjs";
import { loadAndValidateBenchmarkResult } from "./benchmark-validity.mjs";
import {
	buildPeerbitPackages,
	cleanPeerbitBuildArtifacts,
	collectLocalPeerbitPackages,
	copyTemplate,
	defaultExamplesDest,
	defaultExamplesSource,
	defaultFileShareLocalPackages,
	ensureExamplesAssetPackageLinks,
	getFileShareConsumerRoots,
	installPinnedExamplesDependencies,
	parseArgs,
	prepareExamplesRepo,
	repoRoot,
	run,
} from "./common.mjs";
import { createCounterbalancedModePlan } from "./matrix-variants.mjs";
import { instrumentFileShareViteConfigs } from "./vite-instrumentation.mjs";

const SCENARIOS = {
	upload: {
		templatePath: path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"upload-benchmark.local.e2e.spec.ts",
		),
		generatedSpecPath: path.join(
			"tests",
			"generated.upload-benchmark.e2e.spec.ts",
		),
		supportFiles: [
			{
				templatePath: path.join(
					repoRoot,
					"scripts",
					"file-share",
					"templates",
					"download-memory-telemetry.mjs",
				),
				generatedPath: path.join(
					"tests",
					"generated.download-memory-telemetry.mjs",
				),
			},
			{
				templatePath: path.join(
					repoRoot,
					"scripts",
					"file-share",
					"templates",
					"promise-deadline.mjs",
				),
				generatedPath: path.join("tests", "generated.promise-deadline.mjs"),
			},
			{
				templatePath: path.join(
					repoRoot,
					"scripts",
					"file-share",
					"templates",
					"opfs-readback.mjs",
				),
				generatedPath: path.join("tests", "generated.opfs-readback.mjs"),
			},
		],
	},
	"seeder-probe": {
		templatePath: path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"seeder-probe.e2e.spec.ts",
		),
		generatedSpecPath: path.join("tests", "generated.seeder-probe.e2e.spec.ts"),
	},
};
const DROP_HOOK_MARKER = "/* peerbit-benchmark-hook */";
const DROP_UPDATE_LIST_MARKER = "/* peerbit-benchmark-update-list */";
const DROP_UPLOAD_DIAGNOSTICS_MARKER =
	"/* peerbit-benchmark-upload-diagnostics */";
const DROP_LOCALITY_HOOK_MARKER =
	"/* peerbit-benchmark-locality-prefix-preload */";
const DROP_LOCALITY_TOPOLOGY_MARKER =
	"/* peerbit-benchmark-locality-replicator-hashes */";
const DROP_LOCALITY_INDEXED_SEARCH_MARKER =
	"/* peerbit-benchmark-locality-indexed-search */";
const DROP_LOCALITY_PRELOAD_DEADLINE_MARKER =
	"/* peerbit-benchmark-locality-preload-aggregate-deadline */";
const localityPreloadHookImplementation = `            preloadLocalChunkPrefix: async (fileName, target, timeoutMs) => {
                ${DROP_LOCALITY_HOOK_MARKER}
                ${DROP_LOCALITY_PRELOAD_DEADLINE_MARKER}
                if (!program || program.closed) {
                    throw new Error("Program is not ready");
                }
                const file = list.find((candidate) => candidate.name === fileName);
                if (!file) {
                    throw new Error("Locality preload file is not listed");
                }
                if (!isLargeFileLike(file)) {
                    throw new Error("Locality control target is not a large file");
                }
                if (
                    !Number.isSafeInteger(target) ||
                    target < 0 ||
                    target >= file.chunkCount
                ) {
                    throw new Error(
                        "Locality preload target must be a non-negative partial chunk prefix"
                    );
                }
                if (
                    !Number.isSafeInteger(timeoutMs) ||
                    timeoutMs <= 0
                ) {
                    throw new Error(
                        "Locality preload timeout must be a positive safe integer"
                    );
                }
                program.persistChunkReads = true;
                const startedAt = Date.now();
                const transferId =
                    target > 0
                        ? "benchmark-locality-preload-" + startedAt + "-" + target
                        : null;
                const aggregateTimeoutMs = transferId ? timeoutMs : null;
                const aggregateDeadlineAt = transferId
                    ? startedAt + timeoutMs
                    : null;
                if (
                    aggregateDeadlineAt !== null &&
                    !Number.isSafeInteger(aggregateDeadlineAt)
                ) {
                    throw new Error("Locality preload deadline is not a safe integer");
                }
                let aggregateTimedOut = false;
                let yieldedChunkCount = 0;
                let yieldedByteCount = 0;
                if (transferId) {
                    const aggregateController = new AbortController();
                    const aggregateTimeoutError = new Error(
                        "Locality prefix preload exceeded its aggregate deadline"
                    );
                    let aggregateTimeoutHandle: number | undefined;
                    let iterator: AsyncIterator<Uint8Array> | undefined;
                    aggregateTimeoutHandle = window.setTimeout(() => {
                        aggregateTimedOut = true;
                        if (!aggregateController.signal.aborted) {
                            aggregateController.abort(aggregateTimeoutError);
                        }
                    }, Math.max(0, aggregateDeadlineAt! - Date.now()));
                    try {
                        iterator = file
                            .streamFile(program, {
                                timeout: timeoutMs,
                                transferId,
                                signal: aggregateController.signal,
                            })
                            [Symbol.asyncIterator]();
                        while (yieldedChunkCount < target) {
                            const next = await iterator.next();
                            if (
                                Date.now() > aggregateDeadlineAt! &&
                                !aggregateController.signal.aborted
                            ) {
                                aggregateTimedOut = true;
                                aggregateController.abort(aggregateTimeoutError);
                            }
                            if (aggregateController.signal.aborted) {
                                throw (
                                    aggregateController.signal.reason ??
                                    aggregateTimeoutError
                                );
                            }
                            if (next.done) {
                                throw new Error(
                                    "Locality preload stream ended before the requested prefix"
                                );
                            }
                            yieldedChunkCount += 1;
                            yieldedByteCount += next.value.byteLength;
                        }
                    } finally {
                        try {
                            await iterator?.return?.();
                        } finally {
                            if (aggregateTimeoutHandle !== undefined) {
                                window.clearTimeout(aggregateTimeoutHandle);
                            }
                        }
                    }
                    if (aggregateController.signal.aborted) {
                        throw (
                            aggregateController.signal.reason ??
                            aggregateTimeoutError
                        );
                    }
                }
                const finishedAt = Date.now();
                return {
                    startedAt,
                    finishedAt,
                    fileId: file.id,
                    transferId,
                    aggregateTimeoutMs,
                    aggregateDeadlineAt,
                    aggregateTimedOut,
                    yieldedChunkCount,
                    yieldedByteCount,
                    persistChunkReads: program.persistChunkReads,
                    activeTransfersAfterClose: program.getActiveTransfers(),
                    downloadSchedulerAfterClose:
                        program.getTransferSchedulerDiagnostics().download,
                    readDiagnostics: transferId
                        ? program.getReadDiagnostics(transferId) ?? null
                        : null,
                };
            },`;
const getScenarioConfig = (scenario) => {
	const config = SCENARIOS[scenario];
	if (!config) {
		throw new Error(
			`Unsupported --scenario "${scenario}". Expected one of ${Object.keys(SCENARIOS).join(", ")}`,
		);
	}
	return config;
};

const maybeCopyFrontendCerts = async ({
	frontendRoot,
	sourceRoot,
	templateRoot,
}) => {
	const destCertDir = path.join(frontendRoot, ".cert");
	if (fs.existsSync(path.join(destCertDir, "key.pem"))) {
		return;
	}
	const candidateRoots = [templateRoot, sourceRoot].filter(
		(value) =>
			typeof value === "string" &&
			value.length > 0 &&
			!/^https?:\/\//.test(value),
	);
	for (const root of candidateRoots) {
		const sourceCertDir = path.join(
			root,
			"packages",
			"file-share",
			"frontend",
			".cert",
		);
		if (!fs.existsSync(path.join(sourceCertDir, "key.pem"))) {
			continue;
		}
		await fsp.mkdir(destCertDir, { recursive: true });
		await fsp.cp(sourceCertDir, destCertDir, { recursive: true });
		return;
	}
};

export const instrumentFileShareFrontend = async (
	frontendRoot,
	{ requireLocalityHook = false, requireUploadDiagnostics = false } = {},
) => {
	const dropPath = path.join(frontendRoot, "src", "Drop.tsx");
	let contents = await fsp.readFile(dropPath, "utf8");
	const hasExistingBenchmarkHook =
		contents.includes("__peerbitFileShareTestHooks") &&
		contents.includes("setReplicationRole") &&
		contents.includes("getDiagnostics");
	const hasExistingUpdateListStats =
		contents.includes("__peerbitFileShareBenchmarkStats") &&
		contents.includes("updateListStats") &&
		contents.includes("updateListCalls.push(updateListStats)");
	const hasUploadDiagnosticsHook = (source) =>
		source.includes("lastUploadDiagnostics:") &&
		source.includes(".lastUploadDiagnostics ?? null");
	if (requireLocalityHook && !hasExistingBenchmarkHook) {
		throw new Error(
			`Controlled-locality benchmarks require the modern rich test-hook contract in ${dropPath}`,
		);
	}
	if (
		(contents.includes(DROP_HOOK_MARKER) || hasExistingBenchmarkHook) &&
		(contents.includes(DROP_UPDATE_LIST_MARKER) ||
			hasExistingUpdateListStats) &&
		(!requireUploadDiagnostics || hasUploadDiagnosticsHook(contents)) &&
		(!requireLocalityHook ||
			(contents.includes(DROP_LOCALITY_HOOK_MARKER) &&
				contents.includes(DROP_LOCALITY_PRELOAD_DEADLINE_MARKER) &&
				contents.includes(DROP_LOCALITY_TOPOLOGY_MARKER) &&
				contents.includes(DROP_LOCALITY_INDEXED_SEARCH_MARKER)))
	) {
		return;
	}

	const anchor = "    useDebouncedEffect(";
	const hook = `    ${DROP_HOOK_MARKER}
    useEffect(() => {
        const testWindow = window as Window & {
            __peerbitFileShareTestHooks?: {
                setReplicationRole: (roleOptions: ReplicationOptions) => Promise<void>;
                getDiagnostics: () => Promise<Record<string, unknown>>;
            };
            __peerbitFileShareBenchmarkStats?: {
                updateListCalls?: Array<Record<string, unknown>>;
            };
        };
        if (!files.program || files.program.closed) {
            delete testWindow.__peerbitFileShareTestHooks;
            return;
        }
        testWindow.__peerbitFileShareTestHooks = {
            setReplicationRole: async (roleOptions) => {
                saveRoleLocalStorage(files.program, JSON.stringify(roleOptions));
                await updateRole(roleOptions);
            },
            getDiagnostics: async () => {
                const program = files.program;
                const log = program?.files.log;
                const replicators = log ? await log.getReplicators().catch(() => undefined) : undefined;
                const connections =
                    ((peer as any)?.libp2p?.getConnections?.() ?? []).map((connection) =>
                        connection?.remotePeer?.toString?.() ?? "unknown",
                    );
                return {
                    programAddress: program?.address ?? null,
                    programClosed: program?.closed ?? null,
                    peerHash: peer?.identity?.publicKey?.hashcode?.() ?? null,
					connectionCount: connections.length,
					connectionPeers: connections,
					${DROP_UPLOAD_DIAGNOSTICS_MARKER}
					lastUploadDiagnostics:
						(program as any)?.lastUploadDiagnostics ?? null,
					replicatorCount:
                        replicators && typeof replicators.size === "number"
                            ? replicators.size
                            : null,
                    listCount: list.length,
                    replicationSetSize: replicationSet.size,
                    benchmarkStats: testWindow.__peerbitFileShareBenchmarkStats ?? null,
                    isHost: isHost ?? null,
                    left,
                };
            },
        };
        return () => {
            delete testWindow.__peerbitFileShareTestHooks;
        };
    }, [files.program?.address, files.program?.closed]);

`;
	if (!contents.includes(DROP_HOOK_MARKER) && !hasExistingBenchmarkHook) {
		if (!contents.includes(anchor)) {
			throw new Error(`Could not find benchmark hook anchor in ${dropPath}`);
		}
		contents = contents.replace(anchor, `${hook}${anchor}`);
	}
	if (requireUploadDiagnostics && !hasUploadDiagnosticsHook(contents)) {
		const uploadDiagnosticsAnchor =
			"                    connectionPeers: connections,\n";
		if (!contents.includes(uploadDiagnosticsAnchor)) {
			throw new Error(
				`File-share upload benchmarks require the upload-diagnostics hook in ${dropPath}`,
			);
		}
		contents = contents.replace(
			uploadDiagnosticsAnchor,
			`${uploadDiagnosticsAnchor}                    ${DROP_UPLOAD_DIAGNOSTICS_MARKER}\n                    lastUploadDiagnostics:\n                        (program as any)?.lastUploadDiagnostics ?? null,\n`,
		);
	}

	const updateListAnchor = `    const updateList = async () => {\n        if (files.program.files.log.closed) {\n            return;\n        }\n\n        // TODO don't reload the whole list, just add the new elements..\n        try {\n`;
	const updateListInstrumented = `    const updateList = async () => {
        if (files.program.files.log.closed) {
            return;
        }

        const benchmarkWindow = window as Window & {
            __peerbitFileShareBenchmarkStats?: {
                updateListCalls?: Array<Record<string, unknown>>;
            };
        };
        const updateListStartedAt = performance.now();
        const updateListStats: Record<string, unknown> = {
            ${DROP_UPDATE_LIST_MARKER}
            startedAt: Date.now(),
        };

        // TODO don't reload the whole list, just add the new elements..
        try {
`;
	if (
		!contents.includes(DROP_UPDATE_LIST_MARKER) &&
		!hasExistingUpdateListStats
	) {
		if (!contents.includes(updateListAnchor)) {
			throw new Error(`Could not find updateList anchor in ${dropPath}`);
		}
		contents = contents.replace(updateListAnchor, updateListInstrumented);
	}

	contents = contents.replace(
		`            const list = await files.program.list();\n`,
		`            const listStartedAt = performance.now();
            const list = await files.program.list();
            updateListStats.listMs = performance.now() - listStartedAt;
            updateListStats.listCount = list.length;
`,
	);
	contents = contents.replace(
		`            setReplicationSet(
                new Set(
                    (
                        await files.program.files.index.search(
                            new SearchRequest({})
                        )
                    ).map((x) => x.id)
                )
            );
`,
		`            const replicationSetStartedAt = performance.now();
            const replicationSetResults = await files.program.files.index.search(
                new SearchRequest({})
            );
            updateListStats.replicationSetSearchMs =
                performance.now() - replicationSetStartedAt;
            updateListStats.replicationSetSize = replicationSetResults.length;
            setReplicationSet(new Set(replicationSetResults.map((x) => x.id)));
`,
	);
	contents = contents.replace(
		`            setReplicatorCount(
                (await files.program.files.log.getReplicators()).size
            );
            forceUpdate();
`,
		`            const replicatorCountStartedAt = performance.now();
            const replicators = await files.program.files.log.getReplicators();
            updateListStats.replicatorCountMs =
                performance.now() - replicatorCountStartedAt;
            updateListStats.replicatorCount = replicators.size;
            setReplicatorCount(replicators.size);
            updateListStats.totalMs = performance.now() - updateListStartedAt;
            const updateListCalls =
                benchmarkWindow.__peerbitFileShareBenchmarkStats?.updateListCalls ?? [];
            updateListCalls.push(updateListStats);
            benchmarkWindow.__peerbitFileShareBenchmarkStats = {
                updateListCalls,
            };
            forceUpdate();
`,
	);
	if (
		requireLocalityHook &&
		contents.includes(DROP_LOCALITY_HOOK_MARKER) &&
		!contents.includes(DROP_LOCALITY_PRELOAD_DEADLINE_MARKER)
	) {
		const preloadHookAnchor =
			"            preloadLocalChunkPrefix: async (fileName, target, timeoutMs) => {";
		const observationHookAnchor =
			"            getLocalChunkPrefixObservation: async (fileName) => {";
		const preloadHookStart = contents.indexOf(preloadHookAnchor);
		const observationHookStart = contents.indexOf(
			observationHookAnchor,
			preloadHookStart + preloadHookAnchor.length,
		);
		if (preloadHookStart < 0 || observationHookStart < 0) {
			throw new Error(
				`File-share benchmark locality migration requires the exact preload hook boundaries in ${dropPath}`,
			);
		}
		contents = `${contents.slice(0, preloadHookStart)}${localityPreloadHookImplementation}\n${contents.slice(observationHookStart)}`;
	}
	if (
		requireLocalityHook &&
		contents.includes(DROP_LOCALITY_HOOK_MARKER) &&
		!contents.includes(DROP_LOCALITY_INDEXED_SEARCH_MARKER)
	) {
		const searchImportAnchor =
			'import { IsNull, SearchRequest } from "@peerbit/document";';
		const indexedSearchImport =
			'import { IsNull, SearchRequest, SearchRequestIndexed } from "@peerbit/document";';
		if (!contents.includes(indexedSearchImport)) {
			if (!contents.includes(searchImportAnchor)) {
				throw new Error(
					`File-share benchmark locality migration requires the indexed-search import anchor in ${dropPath}`,
				);
			}
			contents = contents.replace(searchImportAnchor, indexedSearchImport);
		}
		const legacySearchAnchor = "new SearchRequest({ fetch: 0xffffffff }),";
		const indexedSearchAnchor =
			"new SearchRequestIndexed({ fetch: 0xffffffff }),";
		if (contents.includes(legacySearchAnchor)) {
			contents = contents.replace(legacySearchAnchor, indexedSearchAnchor);
		}
		if (!contents.includes(indexedSearchAnchor)) {
			throw new Error(
				`File-share benchmark locality migration requires the exact indexed-search query in ${dropPath}`,
			);
		}
		contents = contents.replace(
			indexedSearchAnchor,
			`${DROP_LOCALITY_INDEXED_SEARCH_MARKER}\n                    ${indexedSearchAnchor}`,
		);
	}
	if (
		requireLocalityHook &&
		!contents.includes(DROP_LOCALITY_TOPOLOGY_MARKER)
	) {
		const topologyAnchor = `                    selfInReplicatorSet:
                        peerHash && replicatorHashes
                            ? replicatorHashes.includes(peerHash)
                            : null,`;
		if (!contents.includes(topologyAnchor)) {
			throw new Error(
				`File-share benchmark locality control requires the exact-replicator topology anchor in ${dropPath}`,
			);
		}
		contents = contents.replace(
			topologyAnchor,
			`                    ${DROP_LOCALITY_TOPOLOGY_MARKER}
                    replicatorHashes: replicatorHashes
                        ? [...replicatorHashes].sort((left, right) =>
                              left.localeCompare(right)
                          )
                        : null,
${topologyAnchor}`,
		);
	}
	if (requireLocalityHook && !contents.includes(DROP_LOCALITY_HOOK_MARKER)) {
		const searchImportAnchor =
			'import { IsNull, SearchRequest } from "@peerbit/document";';
		if (!contents.includes(searchImportAnchor)) {
			throw new Error(
				`File-share benchmark locality control requires the indexed-search import anchor in ${dropPath}`,
			);
		}
		contents = contents.replace(
			searchImportAnchor,
			'import { IsNull, SearchRequest, SearchRequestIndexed } from "@peerbit/document";',
		);
		const typeAnchor =
			"                getLightweightSnapshot: () => Record<string, unknown>;";
		const implementationAnchor = "            getLightweightSnapshot: () => ({";
		if (
			!contents.includes(typeAnchor) ||
			!contents.includes(implementationAnchor)
		) {
			throw new Error(
				`File-share benchmark locality control requires the rich test-hook contract in ${dropPath}`,
			);
		}
		contents = contents.replace(
			typeAnchor,
			`${typeAnchor}\n                preloadLocalChunkPrefix: (fileName: string, target: number, timeoutMs: number) => Promise<Record<string, unknown>>;\n                getLocalChunkPrefixObservation: (fileName: string) => Promise<Record<string, unknown>>;`,
		);
		contents = contents.replace(
			implementationAnchor,
			`${localityPreloadHookImplementation}
            getLocalChunkPrefixObservation: async (fileName) => {
                const capturedAt = Date.now();
                if (!program || program.closed) {
                    throw new Error("Program is not ready");
                }
                const file = list.find((candidate) => candidate.name === fileName);
                if (!file) {
                    return {
                        capturedAt,
                        fileId: null,
                        chunkCount: null,
                        indexRowCount: null,
                        indexedChunkIndices: null,
                        blockCount: null,
                        blockChunkIndices: null,
                    };
                }
                if (!isLargeFileLike(file)) {
                    throw new Error("Locality control target is not a large file");
                }
				const local = await program.files.index.search(
					${DROP_LOCALITY_INDEXED_SEARCH_MARKER}
					new SearchRequestIndexed({ fetch: 0xffffffff }),
                    { local: true, remote: false, resolve: false } as any
                );
                const indexedChunkIndices = Array.from(
                    new Set<number>(
                        local
                            .filter((candidate) => candidate.parentId === file.id)
                            .map((candidate) => {
                                const prefix = file.name + "/";
                                if (
                                    typeof candidate.name !== "string" ||
                                    !candidate.name.startsWith(prefix)
                                ) {
                                    throw new Error(
                                        "Local chunk metadata has an invalid name"
                                    );
                                }
                                const suffix = candidate.name.slice(
                                    prefix.length
                                );
                                const index = Number(suffix);
                                if (
                                    !/^\\d+$/.test(suffix) ||
                                    !Number.isSafeInteger(index) ||
                                    index < 0 ||
                                    index >= file.chunkCount
                                ) {
                                    throw new Error(
                                        "Local chunk metadata has an invalid index suffix"
                                    );
                                }
                                return index;
                            })
                    )
                ).sort((left, right) => left - right);
                const candidateHeads = (
                    file as { chunkEntryHeads?: unknown }
                ).chunkEntryHeads;
                const heads =
                    Array.isArray(candidateHeads) &&
                    candidateHeads.length === file.chunkCount &&
                    candidateHeads.every((head) => typeof head === "string")
                        ? (candidateHeads as string[])
                        : null;
                const blockPresence = heads
                    ? typeof program.files.log.log.blocks.hasMany === "function"
                        ? await program.files.log.log.blocks.hasMany(heads)
                        : await Promise.all(
                              heads.map((head) =>
                                  program.files.log.log.blocks.has(head)
                              )
                          )
                    : null;
                const blockChunkIndices = blockPresence
                    ? blockPresence.flatMap((present, index) =>
                          present ? [index] : []
                      )
                    : null;
                const indexRowCount = await program.countLocalChunks(file);
                if (indexRowCount !== indexedChunkIndices.length) {
                    throw new Error(
                        "Local indexed chunk count disagrees with the exact index set"
                    );
                }
                return {
                    capturedAt,
                    fileId: file.id,
                    chunkCount: file.chunkCount,
                    indexRowCount,
                    indexedChunkIndices,
                    blockCount: blockChunkIndices?.length ?? null,
                    blockChunkIndices,
                    persistChunkReads: program.persistChunkReads,
                    activeTransfers: program.getActiveTransfers(),
                    downloadScheduler:
                        program.getTransferSchedulerDiagnostics().download,
                };
            },
${implementationAnchor}`,
		);
	}
	await fsp.writeFile(dropPath, contents);
};

const cleanupFrontendBenchmarkArtifacts = async (frontendRoot) => {
	const paths = [
		path.join(frontendRoot, "node_modules", ".vite"),
		path.join(frontendRoot, ".vite"),
		path.join(frontendRoot, "test-results"),
		path.join(frontendRoot, "playwright-report"),
	];
	await Promise.all(
		paths.map((targetPath) =>
			fsp.rm(targetPath, {
				recursive: true,
				force: true,
			}),
		),
	);
};

const average = (values) =>
	values.length > 0
		? Number(
				(values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(
					1,
				),
			)
		: null;

const summarizeUploadResults = (results) => {
	return groupUploadResultsByLocalityCohort(results).map(
		({ dimensions, results: items }) => ({
			...dimensions,
			runs: items.length,
			passed: items.filter((item) => item.status === "passed").length,
			failed: items.filter((item) => item.status !== "passed").length,
			...summarizeUploadPerformance(items),
			uploadSettledMsAvg: average(
				items
					.filter((item) => item.status === "passed")
					.map((item) => item.phaseDurationsMs?.timeToUploadSettled)
					.filter((value) => typeof value === "number"),
			),
			writerListingLagMsAvg: average(
				items
					.filter((item) => item.status === "passed")
					.map((item) => item.phaseDurationsMs?.writerListingLag)
					.filter((value) => typeof value === "number"),
			),
			readerListingLagMsAvg: average(
				items
					.filter((item) => item.status === "passed")
					.map((item) => item.phaseDurationsMs?.readerListingLag)
					.filter((value) => typeof value === "number"),
			),
			postUploadMonitorDurationMsAvg: average(
				items
					.map((item) => item.postUploadMonitorDurationMs)
					.filter((value) => typeof value === "number"),
			),
			errorCount: items.reduce(
				(sum, item) => sum + (Number(item.errorCount) || 0),
				0,
			),
			incompleteErrorCollections: items.filter(
				(item) => item.errorCollectionComplete !== true,
			).length,
			requestFailureCount: items.reduce(
				(sum, item) => sum + (Number(item.requestFailureCount) || 0),
				0,
			),
			seederDrops: items.filter((item) => item.droppedSeeders).length,
		}),
	);
};

const summarizeSeederProbeResults = (results) => {
	const grouped = new Map();
	for (const result of results) {
		const arr = grouped.get(result.mode) ?? [];
		arr.push(result);
		grouped.set(result.mode, arr);
	}
	const sampleMetric = (result, fn) =>
		(result.samples ?? []).map(fn).filter((value) => typeof value === "number");
	return [...grouped.entries()].map(([mode, items]) => ({
		mode,
		runs: items.length,
		passed: items.filter((item) => item.status === "passed").length,
		failed: items.filter((item) => item.status !== "passed").length,
		reachedTargetRuns: items.filter((item) => item.reachedTarget).length,
		writerSeedersLastAvg: average(
			items
				.map((item) => {
					const last = item.samples?.at(-1);
					return typeof last?.writerSeeders === "number"
						? last.writerSeeders
						: null;
				})
				.filter((value) => typeof value === "number"),
		),
		readerSeedersLastAvg: average(
			items
				.map((item) => {
					const last = item.samples?.at(-1);
					return typeof last?.readerSeeders === "number"
						? last.readerSeeders
						: null;
				})
				.filter((value) => typeof value === "number"),
		),
		writerSeedersMaxAvg: average(
			items
				.map((item) => {
					const values = sampleMetric(item, (sample) => sample.writerSeeders);
					return values.length > 0 ? Math.max(...values) : null;
				})
				.filter((value) => typeof value === "number"),
		),
		readerSeedersMaxAvg: average(
			items
				.map((item) => {
					const values = sampleMetric(item, (sample) => sample.readerSeeders);
					return values.length > 0 ? Math.max(...values) : null;
				})
				.filter((value) => typeof value === "number"),
		),
		errorCount: items.reduce(
			(sum, item) => sum + (Number(item.errorCount) || 0),
			0,
		),
		incompleteErrorCollections: items.filter(
			(item) => item.errorCollectionComplete !== true,
		).length,
		requestFailureCount: items.reduce(
			(sum, item) => sum + (Number(item.requestFailureCount) || 0),
			0,
		),
	}));
};

const summarizeResults = (results, scenario) =>
	scenario === "seeder-probe"
		? summarizeSeederProbeResults(results)
		: summarizeUploadResults(results);

const runPlaywright = ({
	frontendRoot,
	generatedSpecPath,
	resultFile,
	runNonce,
	provenance,
	invocation,
}) => {
	const startedAt = Date.now();
	const child = spawnSync(
		"pnpm",
		[
			"exec",
			"playwright",
			"test",
			generatedSpecPath,
			"-c",
			"playwright.config.ts",
			"--project",
			"chromium",
			"--reporter=line",
		],
		{
			cwd: frontendRoot,
			stdio: "inherit",
			env: createPlaywrightBenchmarkEnvironment({
				baseEnvironment: process.env,
				invocation,
				resultFile,
				runNonce,
				provenance,
			}),
		},
	);
	return {
		wallTimeMs: Date.now() - startedAt,
		exitCode: child.status,
		signal: child.signal,
		spawnError: child.error,
	};
};

const parsePositiveInteger = (value, label) => {
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed <= 0) {
		throw new Error(`${label} must be a positive safe integer`);
	}
	return parsed;
};

const parseNonNegativeNumber = (value, label) => {
	if (value == null) {
		return undefined;
	}
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed < 0) {
		throw new Error(`${label} must be a finite non-negative number`);
	}
	return parsed;
};

const parseNonNegativeInteger = (value, label) => {
	if (value == null) {
		return undefined;
	}
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed < 0) {
		throw new Error(`${label} must be a non-negative safe integer`);
	}
	return parsed;
};

const parseOptionalPositiveInteger = (value, label) =>
	value == null ? undefined : parsePositiveInteger(value, label);

const writeJsonAtomic = async (filePath, value) => {
	await fsp.mkdir(path.dirname(filePath), { recursive: true });
	const temporaryPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
	await fsp.writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`);
	await fsp.rename(temporaryPath, filePath);
};

const assertSafeInvocationUnchanged = (actual, expected, label) => {
	try {
		assertInvocationUnchanged(actual, expected, label);
	} catch (error) {
		const unsafeError = new Error(
			`${label} changed during the benchmark; remaining invocations are unsafe`,
			{ cause: error },
		);
		unsafeError.stopBenchmarkPlan = true;
		throw unsafeError;
	}
};

const readAndAssertSafeInvocationUnchanged = async (
	readActual,
	expected,
	label,
) => {
	try {
		assertInvocationUnchanged(await readActual(), expected, label);
	} catch (error) {
		const unsafeError = new Error(
			`${label} changed during the benchmark; remaining invocations are unsafe`,
			{ cause: error },
		);
		unsafeError.stopBenchmarkPlan = true;
		throw unsafeError;
	}
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const baseUrl = args["base-url"];
	assertCoreBenchmarkUsesLocalApp(baseUrl);
	const previewOptions = resolveBenchmarkPreviewOptions({
		protocol: args.protocol,
		viteMode: args["vite-mode"],
		viteConfig: args["vite-config"],
	});
	const requestedSummaryFile = args["summary-file"]
		? path.resolve(args["summary-file"])
		: undefined;
	const peerbitRoot = path.resolve(args["peerbit-root"] ?? repoRoot);
	const examplesRoot = path.resolve(
		args["examples-root"] ?? defaultExamplesDest(),
	);
	const freshEachRun = Boolean(args["fresh-each-run"]);
	const fileMb = parseNonNegativeNumber(args["file-mb"] ?? "25", "--file-mb");
	if (!Number.isSafeInteger(fileMb * 1024 * 1024)) {
		throw new Error("--file-mb must resolve to a safe integer byte count");
	}
	if (fileMb <= 0) {
		throw new Error("--file-mb must be greater than zero");
	}
	const runs = parsePositiveInteger(args.runs ?? "1", "--runs");
	const requestedMode = args.mode ?? "both";
	const modes =
		requestedMode === "both" ? ["adaptive", "fixed1"] : [requestedMode];
	const scenario = args.scenario ?? "upload";
	assertBenchmarkFileSize({ scenario, fileMb });
	const downloadSink = resolveBenchmarkDownloadSink(args["download-sink"], {
		scenario,
	});
	const network = args.network ?? "local";
	const uploadTimeoutMs = parseOptionalPositiveInteger(
		args["upload-timeout-ms"],
		"--upload-timeout-ms",
	);
	const downloadTimeoutMs = parseOptionalPositiveInteger(
		args["download-timeout-ms"],
		"--download-timeout-ms",
	);
	const postUploadMonitorMs = parseNonNegativeInteger(
		args["post-upload-monitor-ms"],
		"--post-upload-monitor-ms",
	);
	const pollMs = parseOptionalPositiveInteger(args["poll-ms"], "--poll-ms");
	const minReadySeeders = parseNonNegativeInteger(
		args["min-ready-seeders"],
		"--min-ready-seeders",
	);
	const readyTimeoutMs = parseOptionalPositiveInteger(
		args["ready-timeout-ms"],
		"--ready-timeout-ms",
	);
	const sampleMs = parseOptionalPositiveInteger(
		args["sample-ms"],
		"--sample-ms",
	);
	const sampleCount = args["sample-count"]
		? parsePositiveInteger(args["sample-count"], "--sample-count")
		: undefined;
	const targetSeeders = parseNonNegativeInteger(
		args["target-seeders"],
		"--target-seeders",
	);
	const readerLocalChunkTarget = parseNonNegativeInteger(
		args["reader-local-chunk-target"],
		"--reader-local-chunk-target",
	);
	const readerLocalChunkMaxOvershoot = parseNonNegativeInteger(
		args["reader-local-chunk-max-overshoot"],
		"--reader-local-chunk-max-overshoot",
	);
	const examplesSource = args.source ?? defaultExamplesSource();
	const fixtureSeed = args["fixture-seed"];
	const resolvedFixtureSeed = fixtureSeed ?? "peerbit-file-share-benchmark-v1";
	const peerbitRefLabel = args["peerbit-ref-label"] ?? "worktree";
	const expectedPeerbitCommit =
		args["expected-peerbit-commit"] ??
		resolveGitCommitAt(
			peerbitRoot,
			peerbitRefLabel === "worktree" ? "HEAD" : peerbitRefLabel,
		);
	const examplesRequestedRef = args["examples-ref"] ?? "HEAD";
	const expectedExamplesCommit = args["expected-examples-commit"];
	const expectedExamplesLockfileSha256 =
		args["expected-examples-lockfile-sha256"];
	const scenarioConfig = getScenarioConfig(scenario);
	const integrationMode = args["integration-mode"] ?? "link";
	const localPackagesArg = args["local-packages"];
	const localPackageNames =
		integrationMode === "none"
			? []
			: localPackagesArg === "all"
				? undefined
				: (localPackagesArg ?? defaultFileShareLocalPackages.join(","))
						.split(",")
						.map((value) => value.trim())
						.filter(Boolean);
	const effectiveLocalPackageNames = [
		...(
			await collectLocalPeerbitPackages(peerbitRoot, {
				names: localPackageNames,
			})
		).keys(),
	];

	if (!["local", "remote"].includes(network)) {
		throw new Error(
			`Unsupported --network "${network}". Expected "local" or "remote".`,
		);
	}
	if (
		modes.some((mode) => !["adaptive", "fixed1", "observer"].includes(mode))
	) {
		throw new Error(
			`Unsupported --mode "${requestedMode}". Expected adaptive, fixed1, observer, or both.`,
		);
	}
	if (
		readerLocalChunkTarget != null &&
		(scenario !== "upload" || modes.length !== 1 || modes[0] !== "fixed1")
	) {
		throw new Error(
			"--reader-local-chunk-target requires --scenario upload --mode fixed1",
		);
	}
	if (
		(readerLocalChunkTarget == null) !==
		(readerLocalChunkMaxOvershoot == null)
	) {
		throw new Error(
			"--reader-local-chunk-target and --reader-local-chunk-max-overshoot must be provided together",
		);
	}
	if (
		readerLocalChunkMaxOvershoot != null &&
		readerLocalChunkMaxOvershoot > MAX_READER_LOCAL_CHUNK_OVERSHOOT
	) {
		throw new Error(
			`--reader-local-chunk-max-overshoot must not exceed ${MAX_READER_LOCAL_CHUNK_OVERSHOOT}`,
		);
	}
	if (
		readerLocalChunkTarget != null &&
		minReadySeeders != null &&
		minReadySeeders !== 1
	) {
		throw new Error(
			"--reader-local-chunk-target requires --min-ready-seeders 1 because the reader starts as an observer",
		);
	}
	if (!["none", "link"].includes(integrationMode)) {
		throw new Error(
			`Unsupported --integration-mode "${integrationMode}". Evidence-producing benchmarks require "link" so package metadata and dependencies match the selected Peerbit checkout; "none" benchmarks only the pinned published dependencies.`,
		);
	}
	if (integrationMode === "link" && effectiveLocalPackageNames.length === 0) {
		throw new Error(
			"link integration requires at least one local Peerbit package",
		);
	}
	if (requestedSummaryFile) {
		await fsp.rm(requestedSummaryFile, { force: true });
	}

	const { protocol, viteMode, viteConfig } = previewOptions;
	if (integrationMode === "link") {
		run("pnpm", ["install", "--frozen-lockfile"], { cwd: peerbitRoot });
		await cleanPeerbitBuildArtifacts({
			peerbitRoot,
			packageNames: effectiveLocalPackageNames,
		});
		buildPeerbitPackages(peerbitRoot, effectiveLocalPackageNames);
	} else if (args["build-peerbit"]) {
		buildPeerbitPackages(peerbitRoot, defaultFileShareLocalPackages);
	}
	const harnessProvenance = await getPeerbitProvenance({
		root: repoRoot,
		requestedRef: "harness-worktree",
	});
	const peerbitProvenance = await getPeerbitProvenance({
		root: peerbitRoot,
		requestedRef: peerbitRefLabel,
		requireClean: Boolean(args["require-clean-peerbit"]),
		expectedResolvedCommit: expectedPeerbitCommit,
	});
	const templateProvenance = args.template
		? await getExamplesProvenance({
				root: path.resolve(args.template),
				requestedRef: examplesRequestedRef,
				fallbackResolvedCommit: expectedExamplesCommit,
				expectedResolvedCommit: expectedExamplesCommit,
				expectedLockfileSha256: expectedExamplesLockfileSha256,
			})
		: undefined;

	const prepareBenchmarkCheckout = async ({ fresh }) => {
		await prepareExamplesRepo({
			source: examplesSource,
			template: args.template,
			dest: examplesRoot,
			peerbitRoot,
			fresh,
			install: false,
			localPackageNames,
			applyOverrides: false,
			ref: examplesRequestedRef,
		});

		const frontendRoot = path.join(
			examplesRoot,
			"packages",
			"file-share",
			"frontend",
		);
		const examplesProvenance = await getExamplesProvenance({
			root: examplesRoot,
			requestedRef: examplesRequestedRef,
			fallbackProvenance: templateProvenance,
			fallbackResolvedCommit: expectedExamplesCommit,
			expectedResolvedCommit:
				expectedExamplesCommit ?? templateProvenance?.resolvedCommit,
			expectedLockfileSha256:
				expectedExamplesLockfileSha256 ?? templateProvenance?.lockfileSha256,
		});
		await installPinnedExamplesDependencies(examplesRoot);
		await maybeCopyFrontendCerts({
			frontendRoot,
			sourceRoot: examplesSource,
			templateRoot: args.template,
		});
		await instrumentFileShareFrontend(frontendRoot, {
			requireLocalityHook: readerLocalChunkTarget != null,
			requireUploadDiagnostics: scenario === "upload",
		});
		await instrumentFileShareViteConfigs(frontendRoot);
		const generatedSpec = path.join(
			frontendRoot,
			scenarioConfig.generatedSpecPath,
		);
		await copyTemplate({
			templatePath: scenarioConfig.templatePath,
			outputPath: generatedSpec,
		});
		for (const supportFile of scenarioConfig.supportFiles ?? []) {
			await copyTemplate({
				templatePath: supportFile.templatePath,
				outputPath: path.join(frontendRoot, supportFile.generatedPath),
			});
		}

		if (integrationMode === "link") {
			await ensureExamplesAssetPackageLinks({
				examplesRoot,
				peerbitRoot,
				packageNames: effectiveLocalPackageNames,
				consumerRoots: getFileShareConsumerRoots(examplesRoot),
			});
		}
		return { frontendRoot, examplesProvenance };
	};

	let { frontendRoot, examplesProvenance } = await prepareBenchmarkCheckout({
		fresh: Boolean(args.fresh),
	});
	const expectedExamplesProvenance = { ...examplesProvenance };

	const requestedResultsDir = args["results-dir"];
	const resultsRoot = requestedResultsDir
		? path.resolve(requestedResultsDir)
		: await fsp.mkdtemp(
				path.join(os.tmpdir(), "peerbit-file-share-benchmark-"),
			);
	if (requestedResultsDir) {
		await fsp.mkdir(resultsRoot, { recursive: true });
	}
	const sessionNonce = randomUUID();
	const resultsDir = path.join(resultsRoot, `session-${sessionNonce}`);
	await fsp.mkdir(resultsDir, { recursive: false });
	const aggregateSummaryFile =
		requestedSummaryFile ?? path.join(resultsDir, "summary.json");
	const results = [];
	let benchmarkInvocationCount = 0;
	const executionOrder = createCounterbalancedModePlan({ modes, runs });
	const invocationContexts = new Map();
	const outcomes = await executePlanContinuing({
		plan: executionOrder,
		shouldStop: (error) => error?.stopBenchmarkPlan === true,
		execute: async ({ sequence, mode, run: runIndex }) => {
			const runNonce = randomUUID();
			const invocation = createBenchmarkInvocation({
				scenario,
				mode,
				network,
				integrationMode,
				fileMb,
				fixtureSeed: resolvedFixtureSeed,
				downloadSink,
				uploadTimeoutMs,
				downloadTimeoutMs,
				postUploadMonitorMs,
				pollMs,
				minReadySeeders,
				readyTimeoutMs,
				sampleMs,
				sampleCount,
				targetSeeders,
				readerLocalChunkTarget,
				readerLocalChunkMaxOvershoot,
				baseUrl,
				protocol,
				viteMode,
				viteConfig,
				localPackages: effectiveLocalPackageNames,
				enableVisibilityProbe: Boolean(args["enable-visibility-probe"]),
				verbose: Boolean(args.verbose),
			});
			const resultFile = createNonceIsolatedResultPath({
				resultsDir,
				runNonce,
			});
			const context = {
				runNonce,
				invocation,
				resultFile,
				processOutcome: null,
				provenance: {
					harness: harnessProvenance,
					peerbit: peerbitProvenance,
					examples: examplesProvenance,
				},
			};
			invocationContexts.set(sequence, context);
			try {
				if (freshEachRun && benchmarkInvocationCount > 0) {
					({ frontendRoot, examplesProvenance } =
						await prepareBenchmarkCheckout({
							fresh: true,
						}));
					assertSafeInvocationUnchanged(
						examplesProvenance,
						expectedExamplesProvenance,
						"Pre-instrumentation examples provenance",
					);
					context.provenance = {
						harness: harnessProvenance,
						peerbit: peerbitProvenance,
						examples: examplesProvenance,
					};
				}
				console.log(
					`Running file-share benchmark sequence=${sequence}/${executionOrder.length} scenario=${scenario} network=${network} mode=${mode} run=${runIndex}/${runs} fileMb=${fileMb}`,
				);
				await cleanupFrontendBenchmarkArtifacts(frontendRoot);
				await fsp.rm(resultFile, { force: true });
				const processOutcome = runPlaywright({
					frontendRoot,
					generatedSpecPath: scenarioConfig.generatedSpecPath,
					resultFile,
					runNonce,
					provenance: context.provenance,
					invocation,
				});
				context.processOutcome = processOutcome;
				const { wallTimeMs, exitCode, signal, spawnError } = processOutcome;
				let result;
				let invocationFailure;
				try {
					if (spawnError) {
						throw new Error(
							`Could not start Playwright benchmark: ${spawnError.message}`,
							{ cause: spawnError },
						);
					}
					if (signal) {
						throw new Error(
							`Playwright benchmark terminated by signal ${signal}`,
						);
					}
					result = await loadAndValidateBenchmarkResult({
						resultFile,
						exitCode,
						scenario,
						expectedMode: mode,
						expectedFileMb: fileMb,
						expectedNetwork: network,
						expectedFixtureSeed: resolvedFixtureSeed,
						expectedRunNonce: runNonce,
						expectedProvenance: context.provenance,
						expectedInvocation: invocation,
					});
				} catch (error) {
					invocationFailure = error;
				}
				let unsafeProvenanceFailure;
				for (const [readActual, expected, label] of [
					[
						() =>
							getPeerbitProvenance({
								root: repoRoot,
								requestedRef: "harness-worktree",
							}),
						harnessProvenance,
						"Harness provenance",
					],
					[
						() =>
							getPeerbitProvenance({
								root: peerbitRoot,
								requestedRef: peerbitRefLabel,
								requireClean: Boolean(args["require-clean-peerbit"]),
								expectedResolvedCommit: expectedPeerbitCommit,
							}),
						peerbitProvenance,
						"Peerbit provenance",
					],
				]) {
					try {
						await readAndAssertSafeInvocationUnchanged(
							readActual,
							expected,
							label,
						);
					} catch (error) {
						unsafeProvenanceFailure ??= error;
					}
				}
				if (unsafeProvenanceFailure) {
					throw unsafeProvenanceFailure;
				}
				if (invocationFailure) {
					throw invocationFailure;
				}
				return {
					...result,
					run: runIndex,
					invocationSequence: sequence,
					resultFile,
					playwrightWallTimeMs: wallTimeMs,
					playwrightExitCode: exitCode,
				};
			} finally {
				benchmarkInvocationCount++;
			}
		},
	});
	for (const outcome of outcomes) {
		if (outcome.status === "fulfilled") {
			results.push(outcome.value);
			continue;
		}
		const { sequence, mode, run: runIndex } = outcome.entry;
		const context = invocationContexts.get(sequence);
		if (!context) {
			throw new Error(`Missing invocation context for sequence ${sequence}`);
		}
		const resultEvidence = await readJsonEvidence(context.resultFile);
		const failedResult = createInvocationFailureEvidence({
			scenario,
			mode,
			network,
			fileMb,
			runNonce: context.runNonce,
			invocation: context.invocation,
			provenance: context.provenance,
			resultFile: context.resultFile,
			processOutcome: context.processOutcome,
			resultEvidence,
			failure: outcome.error,
		});
		results.push({
			...failedResult,
			run: runIndex,
			invocationSequence: sequence,
			playwrightWallTimeMs: context.processOutcome?.wallTimeMs ?? null,
		});
	}

	console.log("\nPer-run results");
	console.table(
		results.map((result) => ({
			scenario,
			mode: result.mode,
			run: result.run,
			status: result.status,
			stage: result.stage ?? "complete",
			fileSizeMb: result.fileSizeMb,
			reachedTarget: result.reachedTarget ?? null,
			writerSeedersLast:
				typeof result.samples?.at(-1)?.writerSeeders === "number"
					? result.samples.at(-1).writerSeeders
					: null,
			readerSeedersLast:
				typeof result.samples?.at(-1)?.readerSeeders === "number"
					? result.samples.at(-1).readerSeeders
					: null,
			uploadDurationMs: result.uploadDurationMs,
			uploadSettledMs: result.phaseDurationsMs?.timeToUploadSettled ?? null,
			...uploadTimingTableColumns(result),
			writerListingLagMs: result.phaseDurationsMs?.writerListingLag ?? null,
			readerListingLagMs: result.phaseDurationsMs?.readerListingLag ?? null,
			postUploadMonitorDurationMs: result.postUploadMonitorDurationMs ?? null,
			readerLocalChunkTarget:
				result.readerLocalChunkTarget ??
				result.invocation?.readerLocalChunkTarget ??
				null,
			readerLocalChunkMaxOvershoot:
				result.readerLocalChunkMaxOvershoot ??
				result.invocation?.readerLocalChunkMaxOvershoot ??
				null,
			readerLocalChunkBlockCount: result.readerLocalChunkBlockCount ?? null,
			readerLocalChunkIndexRowCount:
				result.readerLocalChunkIndexRowCount ?? null,
			readerLocalityCohortKey: result.readerLocalityCohortKey ?? null,
			downloadSink: result.downloadSink ?? null,
			downloadDurationMs: result.downloadDurationMs ?? null,
			libraryStreamWallMs: result.libraryStreamWallMs ?? null,
			sinkWriteAwaitMs: result.sinkWriteAwaitMs ?? null,
			sinkAwaitSubtractedDiagnosticMs:
				result.sinkAwaitSubtractedDiagnosticMs ?? null,
			integrityVerified: result.integrityVerified ?? null,
			playwrightWallTimeMs: result.playwrightWallTimeMs,
			playwrightExitCode: result.playwrightExitCode,
			droppedSeeders: result.droppedSeeders,
			errorCollectionComplete: result.errorCollectionComplete === true,
			errorCount: result.errorCount,
			requestFailureCount: result.requestFailureCount ?? null,
			failure:
				result.browserFailure?.message ??
				result.runnerFailure?.message ??
				result.failure?.message ??
				"",
		})),
	);
	const aggregateRows = summarizeResults(results, scenario);
	console.log("\nSummary");
	console.table(aggregateRows);
	const outcomeCounts = countBenchmarkOutcomes(results, executionOrder.length);
	const planPassed = isCompletePassedBenchmarkPlan(results, outcomeCounts);
	const comparison =
		scenario === "upload"
			? compareUploadPerformanceModesForCompletePlan(results, outcomeCounts)
			: null;
	if (comparison) {
		console.log("\nAdaptive vs fixed1");
		console.table([comparison]);
	}
	console.log("\nOutcome counts");
	console.table([outcomeCounts]);
	const summary = {
		schema: BENCHMARK_SUMMARY_SCHEMA,
		status: planPassed ? "passed" : "failed",
		outcomeCounts,
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		sessionNonce,
		harnessProvenance,
		peerbitRoot,
		peerbitProvenance,
		examplesRoot,
		examplesProvenance: expectedExamplesProvenance,
		frontendRoot,
		scenario,
		downloadSink,
		integrationMode,
		localPackageNames: effectiveLocalPackageNames,
		network,
		fileMb,
		readerLocalChunkTarget: readerLocalChunkTarget ?? null,
		readerLocalChunkMaxOvershoot: readerLocalChunkMaxOvershoot ?? null,
		readerLocalityCohorts:
			scenario === "upload"
				? aggregateRows
						.map((row) => row.readerLocalityCohortKey)
						.filter((value) => typeof value === "string")
				: [],
		runs,
		modes,
		executionOrder,
		resultsRoot,
		resultsDir,
		aggregateSummaryFile,
		results,
		summary: aggregateRows,
		comparison,
	};
	await writeJsonAtomic(aggregateSummaryFile, summary);
	console.log(`\nRaw results: ${resultsDir}`);
	console.log(`Aggregate summary: ${aggregateSummaryFile}`);
	if (!planPassed) {
		process.exitCode = 1;
	}
};

if (
	process.argv[1] &&
	path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
	main().catch((error) => {
		console.error(error);
		process.exit(1);
	});
}
