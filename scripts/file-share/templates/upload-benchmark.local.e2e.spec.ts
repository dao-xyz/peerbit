import { type Page, expect, test } from "@playwright/test";
import { SHA256 } from "@stablelib/sha256";
import { randomUUID } from "node:crypto";
import { mkdir, rename, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { startBootstrapPeer } from "./bootstrapPeer";
import { startDownloadMemoryTelemetry } from "./generated.download-memory-telemetry.mjs";
import { sha256AndCrc32OpfsSavedViaPicker } from "./generated.opfs-readback.mjs";
import { withDeadline } from "./generated.promise-deadline.mjs";
import {
	armSavedViaPicker,
	crc32SavedViaPicker,
	createSpace,
	createSyntheticFileOnDisk,
	expectSeedersAtLeast,
	getSeederCount,
	installHashOnlyMockSaveFilePicker,
	installMockSaveFilePicker,
	installNodeBackedMockSaveFilePicker,
	rootUrl,
	sha256AndCrc32File,
	waitForUploadComplete,
	withBootstrap,
} from "./helpers";
import {
	resolveBenchmarkDownloadSink,
	summarizeReadTransferDiagnostics,
} from "./transfer-benchmark";

const FILE_SIZE_MB = Number(process.env.PW_FILE_MB || "100");
const FILE_SIZE_BYTES = FILE_SIZE_MB * 1024 * 1024;
const POLL_MS = Number(process.env.PW_POLL_MS || "1000");
const READER_LOCAL_CHUNK_TARGET = process.env.PW_READER_LOCAL_CHUNK_TARGET
	? Number(process.env.PW_READER_LOCAL_CHUNK_TARGET)
	: null;
const READER_LOCAL_CHUNK_MAX_OVERSHOOT = process.env
	.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT
	? Number(process.env.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT)
	: null;
const READER_TERMINAL_TOPOLOGY =
	process.env.PW_READER_TERMINAL_TOPOLOGY || null;
const LOCALITY_CONTROL_POLL_MS = Math.min(POLL_MS, 100);
const LOCALITY_CONTROL_STABLE_SAMPLE_COUNT = 3;
const POST_UPLOAD_MONITOR_MS = Number(
	process.env.PW_POST_UPLOAD_MONITOR_MS || "5000",
);
const UPLOAD_TIMEOUT_MS = Number(process.env.PW_UPLOAD_TIMEOUT_MS || "600000");
const DOWNLOAD_TIMEOUT_MS = Number(
	process.env.PW_DOWNLOAD_TIMEOUT_MS ||
		process.env.PW_UPLOAD_TIMEOUT_MS ||
		"600000",
);
const READY_TIMEOUT_MS = Number(process.env.PW_READY_TIMEOUT_MS);
const POST_MONITOR_SCHEDULING_TOLERANCE_MS = Math.max(250, POLL_MS + 250);
const POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION =
	"max(250ms, pollMs + 250ms) for the final poll and event-loop scheduling";
const TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS = Math.max(
	5_000,
	POLL_MS + 1_000,
);
const TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION =
	"max(5000ms, pollMs + 1000ms) for browser actions and event-loop scheduling";
// Controlled locality can spend a full download deadline preloading the prefix,
// then one readiness deadline each stabilizing that prefix, waiting for the
// completed transfer to become idle, and stabilizing terminal topology. The
// measured download retains its separate DOWNLOAD_TIMEOUT_MS budget below.
const LOCALITY_CONTROL_OUTER_TIMEOUT_BUDGET_MS =
	READER_LOCAL_CHUNK_TARGET === null
		? 0
		: 3 * READY_TIMEOUT_MS +
			(READER_LOCAL_CHUNK_TARGET > 0
				? DOWNLOAD_TIMEOUT_MS + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS
				: 0);
const TEST_OUTER_TIMEOUT_MS = Math.max(
	20 * 60 * 1000,
	3 * READY_TIMEOUT_MS +
		UPLOAD_TIMEOUT_MS +
		DOWNLOAD_TIMEOUT_MS +
		LOCALITY_CONTROL_OUTER_TIMEOUT_BUDGET_MS +
		POST_UPLOAD_MONITOR_MS +
		5 * 60 * 1000,
);
const TIME_TO_WRITER_READY_DEFINITION =
	"upload-input-set-to-writer-ready-manifest-listed";
const TIME_TO_READER_READY_DEFINITION =
	"upload-input-set-to-reader-ready-manifest-listed";
const LISTING_DURATION_DEFINITION =
	"post-upload-settlement-to-both-writer-and-reader-ready-manifests-listed; excludes upload time";
const ERROR_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every uncaught pageerror; every console.error; every console message at any level containing a known Peerbit failure signature; plus scenario-recorded operation failures";
const REQUEST_FAILURE_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every Playwright requestfailed event, retained as non-fatal diagnostics and excluded from errorCount";
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
// Date.now() endpoints can undercount the nested performance.now() helper
// interval by less than one millisecond for each chunk.
const SINK_WRITE_QUANTIZATION_ALLOWANCE_MS_PER_CHUNK = 1;
// Browser performance.now() and Node hrtime measure nested intervals on
// independent monotonic clocks; keep their aggregate drift finite per call.
const SINK_SERVER_CLOCK_TOLERANCE_MS_PER_CHUNK = 1;
const FIXTURE_SEED =
	process.env.PW_FIXTURE_SEED || "peerbit-file-share-benchmark-v1";
const RESULT_SCHEMA = {
	id: "peerbit-file-share-benchmark",
	version: 8,
} as const;
const SEEDER_DROP_POLICY = {
	id: "peerbit-file-share-seeder-drop-policy",
	version: 1,
	belowBaselineDefinition:
		"writerSeeders < baselineWriterSeeders || readerSeeders < baselineReaderSeeders",
	evaluatedSnapshotDefinition:
		"all chronological snapshots after the unique seeders-ready baseline snapshot",
	consecutiveBelowBaselineSnapshotThreshold: 2,
	terminalSnapshotLabel: "terminal",
	terminalBelowBaselineIsUnexpected: true,
} as const;
const RUN_NONCE = process.env.PW_BENCHMARK_RUN_NONCE;
const parseJsonEnvironment = (name: string) => {
	const encoded = process.env[name];
	if (!encoded) {
		throw new Error(`Missing ${name}`);
	}
	try {
		return JSON.parse(encoded) as Record<string, unknown>;
	} catch (error) {
		throw new Error(`Malformed ${name}`, { cause: error });
	}
};
const PROVENANCE = parseJsonEnvironment("PW_BENCHMARK_PROVENANCE");
const INVOCATION = parseJsonEnvironment("PW_BENCHMARK_INVOCATION");
const MODE = process.env.PW_REPLICATION_MODE || "adaptive";
const NETWORK_MODE = process.env.PW_NETWORK_MODE || "local";
const DOWNLOAD_SINK = resolveBenchmarkDownloadSink(
	process.env.PW_DOWNLOAD_SINK,
);
const RESULT_FILE = process.env.PW_RESULT_FILE;
const ENABLE_VISIBILITY_PROBE = process.env.PW_ENABLE_VISIBILITY_PROBE === "1";
const MIN_READY_SEEDERS = Number(process.env.PW_MIN_READY_SEEDERS);
const VERBOSE = process.env.PW_VERBOSE === "1";

if (!Number.isSafeInteger(FILE_SIZE_BYTES) || FILE_SIZE_BYTES < 0) {
	throw new Error(`Invalid PW_FILE_MB='${process.env.PW_FILE_MB}'`);
}
if (!RUN_NONCE) {
	throw new Error("Missing PW_BENCHMARK_RUN_NONCE");
}
if (
	READER_LOCAL_CHUNK_TARGET !== null &&
	(!Number.isSafeInteger(READER_LOCAL_CHUNK_TARGET) ||
		READER_LOCAL_CHUNK_TARGET < 0)
) {
	throw new Error(
		`Invalid PW_READER_LOCAL_CHUNK_TARGET='${process.env.PW_READER_LOCAL_CHUNK_TARGET}'`,
	);
}
if (
	(READER_LOCAL_CHUNK_TARGET === null) !==
	(READER_LOCAL_CHUNK_MAX_OVERSHOOT === null)
) {
	throw new Error(
		"PW_READER_LOCAL_CHUNK_TARGET and PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT must be provided together",
	);
}
if (
	(READER_LOCAL_CHUNK_TARGET === null) !==
	(READER_TERMINAL_TOPOLOGY === null)
) {
	throw new Error(
		"PW_READER_TERMINAL_TOPOLOGY must be provided exactly when PW_READER_LOCAL_CHUNK_TARGET is provided",
	);
}
if (
	READER_TERMINAL_TOPOLOGY !== null &&
	!(["observer", "replicator"] as const).includes(
		READER_TERMINAL_TOPOLOGY as "observer" | "replicator",
	)
) {
	throw new Error(
		`Invalid PW_READER_TERMINAL_TOPOLOGY='${process.env.PW_READER_TERMINAL_TOPOLOGY}'`,
	);
}
if (
	READER_LOCAL_CHUNK_MAX_OVERSHOOT !== null &&
	(!Number.isSafeInteger(READER_LOCAL_CHUNK_MAX_OVERSHOOT) ||
		READER_LOCAL_CHUNK_MAX_OVERSHOOT < 0 ||
		READER_LOCAL_CHUNK_MAX_OVERSHOOT > 8)
) {
	throw new Error(
		`Invalid PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT='${process.env.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT}'`,
	);
}
if (
	process.env.PW_MIN_READY_SEEDERS == null ||
	!Number.isSafeInteger(MIN_READY_SEEDERS) ||
	MIN_READY_SEEDERS < 0
) {
	throw new Error(
		`Invalid PW_MIN_READY_SEEDERS='${process.env.PW_MIN_READY_SEEDERS}'`,
	);
}
if (
	process.env.PW_READY_TIMEOUT_MS == null ||
	!Number.isSafeInteger(READY_TIMEOUT_MS) ||
	READY_TIMEOUT_MS <= 0
) {
	throw new Error(
		`Invalid PW_READY_TIMEOUT_MS='${process.env.PW_READY_TIMEOUT_MS}'`,
	);
}
if (process.env.PW_BENCH !== "1") {
	throw new Error("Upload benchmark must run against the production preview");
}
if (!["local", "remote"].includes(NETWORK_MODE)) {
	throw new Error(`Unsupported PW_NETWORK_MODE='${NETWORK_MODE}'`);
}
if (
	READER_LOCAL_CHUNK_TARGET !== null &&
	(MODE !== "fixed1" || MIN_READY_SEEDERS !== 1)
) {
	throw new Error(
		"PW_READER_LOCAL_CHUNK_TARGET requires fixed1 writer mode and exactly one ready seeder because the reader starts as an observer",
	);
}

const expectedInvocationValues: Record<string, unknown> = {
	scenario: "upload",
	mode: MODE,
	networkMode: NETWORK_MODE,
	fileSizeMb: FILE_SIZE_MB,
	fileSizeBytes: FILE_SIZE_BYTES,
	fixtureSeed: FIXTURE_SEED,
	downloadSink: DOWNLOAD_SINK,
	uploadTimeoutMs: UPLOAD_TIMEOUT_MS,
	downloadTimeoutMs: DOWNLOAD_TIMEOUT_MS,
	postUploadMonitorMs: POST_UPLOAD_MONITOR_MS,
	pollMs: POLL_MS,
	minReadySeeders: MIN_READY_SEEDERS,
	readyTimeoutMs: READY_TIMEOUT_MS,
	readerLocalChunkTarget: READER_LOCAL_CHUNK_TARGET,
	readerLocalChunkMaxOvershoot: READER_LOCAL_CHUNK_MAX_OVERSHOOT,
	readerTerminalTopology: READER_TERMINAL_TOPOLOGY,
	baseUrl: process.env.PW_BASE_URL || null,
	protocol: process.env.PW_PROTOCOL || null,
	viteMode: process.env.PW_VITE_MODE || null,
	viteConfig: process.env.PW_VITE_CONFIG || null,
	serverMode: "production-preview",
	serverHost: process.env.HOST || null,
	enableVisibilityProbe: ENABLE_VISIBILITY_PROBE,
	verbose: VERBOSE,
};
if (
	(INVOCATION.schema as Record<string, unknown> | undefined)?.id !==
		"peerbit-file-share-benchmark-invocation" ||
	(INVOCATION.schema as Record<string, unknown> | undefined)?.version !== 4
) {
	throw new Error("Unsupported PW_BENCHMARK_INVOCATION schema");
}
for (const [key, expected] of Object.entries(expectedInvocationValues)) {
	if (INVOCATION[key] !== expected) {
		throw new Error(
			`PW_BENCHMARK_INVOCATION.${key} does not match the effective environment`,
		);
	}
}

const ROLE_BY_MODE: Record<string, any> = {
	adaptive: {
		limits: {
			cpu: {
				max: 1,
			},
		},
	},
	fixed1: { factor: 1 },
	observer: false,
};

const KNOWN_PEERBIT_FAILURE_SIGNATURES = [
	"Failed to resolve block",
	"DeliveryError",
	"Failed to get message",
	"delivery acknowledges",
	"Failed to bootstrap",
	"failed to open",
	"BorshError",
	"Failed to create space",
];

const getRole = () => {
	const role = ROLE_BY_MODE[MODE];
	if (role === undefined) {
		throw new Error(`Unsupported PW_REPLICATION_MODE='${MODE}'`);
	}
	return role;
};

const attachTransferErrorCollector = (
	page: Page,
	label: string,
	errors: string[],
	requestFailures: string[],
) => {
	page.on("pageerror", (error) => {
		const text = String(error?.message || error);
		errors.push(`${label}:pageerror:${text}`);
	});
	page.on("console", (message) => {
		const text = message.text();
		if (
			message.type() === "error" ||
			KNOWN_PEERBIT_FAILURE_SIGNATURES.some((match) => text.includes(match))
		) {
			errors.push(`${label}:console.${message.type()}:${text}`);
		}
	});
	page.on("requestfailed", (request) => {
		requestFailures.push(
			`${label}:requestfailed:${JSON.stringify({
				method: request.method(),
				resourceType: request.resourceType(),
				url: request.url(),
				errorText: request.failure()?.errorText ?? null,
			})}`,
		);
	});
};

const waitForTestHooks = async (
	page: Page,
	options: {
		requireRoleSetter?: boolean;
		role?: ReturnType<typeof getRole>;
	} = {},
) => {
	await page.waitForFunction(
		async ({ requireRoleSetter, role }) => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			if (!hooks?.getDiagnostics) {
				return false;
			}
			if (!requireRoleSetter) {
				return true;
			}
			if (!hooks.setReplicationRole) {
				return false;
			}
			try {
				const diagnostics = hooks.getLightweightSnapshot
					? hooks.getLightweightSnapshot()
					: await hooks.getDiagnostics();
				const programReady =
					typeof diagnostics?.programAddress === "string" &&
					diagnostics.programClosed === false;
				if (!programReady) {
					return false;
				}
				await hooks.setReplicationRole(role);
				return true;
			} catch (error) {
				const message = error instanceof Error ? error.message : String(error);
				if (message.includes("Program is not ready")) {
					return false;
				}
				throw error;
			}
		},
		{
			requireRoleSetter: Boolean(options.requireRoleSetter),
			role: options.role,
		},
		{ timeout: READY_TIMEOUT_MS, polling: 100 },
	);
};

const applyRole = async (page: Page, shareUrl: string) => {
	await page.goto(shareUrl, { waitUntil: "domcontentloaded" });
	if (MODE === "adaptive") {
		await waitForTestHooks(page);
		return;
	}
	await waitForTestHooks(page, {
		requireRoleSetter: true,
		role: getRole(),
	});
};

const seedReplicationRole = async (
	page: Page,
	shareAddress: string,
	role: unknown,
) => {
	await page.addInitScript(
		({ address, roleOptions }) => {
			window.localStorage.setItem(
				`${address}-role`,
				JSON.stringify(roleOptions),
			);
		},
		{ address: shareAddress, roleOptions: role },
	);
};

const openWithSeededReplicationRole = async (page: Page, shareUrl: string) => {
	await page.goto(shareUrl, { waitUntil: "domcontentloaded" });
	await waitForTestHooks(page);
};

type InitialReaderRoleEvidence = {
	capturedAt: number;
	programAddress: unknown;
	persistChunkReads: unknown;
	initialRole: unknown;
	updateRoleCount: unknown;
	lastAppliedRole: unknown;
};

const readInitialReaderRoleEvidence = async (
	page: Page,
): Promise<InitialReaderRoleEvidence> =>
	page.evaluate(async () => {
		const hooks = (window as any).__peerbitFileShareTestHooks;
		if (!hooks?.getDiagnostics) {
			throw new Error("Benchmark diagnostics hook is unavailable");
		}
		const diagnostics = await hooks.getDiagnostics();
		return {
			capturedAt: Date.now(),
			programAddress: diagnostics?.programAddress,
			persistChunkReads: diagnostics?.persistChunkReads,
			initialRole: diagnostics?.timings?.initialRole,
			updateRoleCount: diagnostics?.timings?.updateRoleCount,
			lastAppliedRole: diagnostics?.timings?.lastAppliedRole,
		};
	});

type LocalitySchedulerObservation = {
	activeCount: number;
	activeBytes: number;
	queuedCount: number;
};

type LocalChunkPrefixObservation = {
	capturedAt: number;
	fileId: string | null;
	chunkCount: number | null;
	indexRowCount: number | null;
	indexedChunkIndices: number[] | null;
	blockCount: number | null;
	blockChunkIndices: number[] | null;
	persistChunkReads?: boolean;
	activeTransfers?: Array<Record<string, unknown>>;
	downloadScheduler?: LocalitySchedulerObservation;
};

const readLocalChunkPrefixObservation = async (
	page: Page,
	fileName: string,
): Promise<LocalChunkPrefixObservation> => {
	const observation = await page.evaluate(async (expectedFileName) => {
		const hooks = (window as any).__peerbitFileShareTestHooks;
		if (!hooks?.getLocalChunkPrefixObservation) {
			throw new Error(
				"Benchmark getLocalChunkPrefixObservation hook is unavailable",
			);
		}
		return await hooks.getLocalChunkPrefixObservation(expectedFileName);
	}, fileName);
	const validateIndices = (
		value: unknown,
		label: string,
		chunkCount: number,
	) => {
		if (
			!Array.isArray(value) ||
			value.some(
				(index, offset) =>
					!Number.isSafeInteger(index) ||
					index < 0 ||
					index >= chunkCount ||
					(offset > 0 && index <= value[offset - 1]),
			)
		) {
			throw new Error(`${label} is not a sorted unique chunk-index set`);
		}
	};
	if (
		!observation ||
		!Number.isSafeInteger(observation.capturedAt) ||
		observation.capturedAt <= 0 ||
		(observation.fileId !== null &&
			(typeof observation.fileId !== "string" ||
				observation.fileId.length === 0))
	) {
		throw new Error("Local chunk-prefix hook returned invalid evidence");
	}
	if (observation.fileId === null) {
		if (
			observation.chunkCount !== null ||
			observation.indexRowCount !== null ||
			observation.indexedChunkIndices !== null ||
			observation.blockCount !== null ||
			observation.blockChunkIndices !== null
		) {
			throw new Error(
				"Missing file locality must use null count and set fields",
			);
		}
		return observation;
	}
	if (
		!Number.isSafeInteger(observation.chunkCount) ||
		observation.chunkCount <= 0 ||
		!Number.isSafeInteger(observation.indexRowCount) ||
		observation.indexRowCount < 0 ||
		!Number.isSafeInteger(observation.blockCount) ||
		observation.blockCount < 0
	) {
		throw new Error("Local chunk-prefix hook returned invalid counts");
	}
	validateIndices(
		observation.indexedChunkIndices,
		"indexedChunkIndices",
		observation.chunkCount,
	);
	validateIndices(
		observation.blockChunkIndices,
		"blockChunkIndices",
		observation.chunkCount,
	);
	if (
		observation.indexRowCount !== observation.indexedChunkIndices.length ||
		observation.blockCount !== observation.blockChunkIndices.length
	) {
		throw new Error("Local chunk-prefix counts disagree with their exact sets");
	}
	return observation;
};

const preloadLocalChunkPrefix = async (
	page: Page,
	fileName: string,
	target: number,
	timeoutMs: number,
) => {
	const evaluation = page.evaluate(
		async ({ expectedFileName, requestedTarget, timeout }) => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			if (!hooks?.preloadLocalChunkPrefix) {
				throw new Error(
					"Benchmark preloadLocalChunkPrefix hook is unavailable",
				);
			}
			return await hooks.preloadLocalChunkPrefix(
				expectedFileName,
				requestedTarget,
				timeout,
			);
		},
		{ expectedFileName: fileName, requestedTarget: target, timeout: timeoutMs },
	);
	return target > 0
		? await withDeadline(
				evaluation,
				timeoutMs + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,
				`Reader locality preload did not settle within ${timeoutMs}ms plus scheduling tolerance`,
			)
		: await evaluation;
};

const getTopologySnapshot = async (page: Page) =>
	await page.evaluate(async () => {
		const hooks = (window as any).__peerbitFileShareTestHooks;
		if (!hooks?.getTopologySnapshot) {
			throw new Error("Benchmark topology hook is unavailable");
		}
		return await hooks.getTopologySnapshot();
	});

const topologyHasExactWriterSingleton = (
	writerTopology: Record<string, any>,
	readerTopology: Record<string, any>,
) => {
	const writerPeerHash = writerTopology?.peerHash;
	return (
		typeof writerPeerHash === "string" &&
		writerPeerHash.length > 0 &&
		typeof readerTopology?.peerHash === "string" &&
		readerTopology.peerHash.length > 0 &&
		readerTopology.peerHash !== writerPeerHash &&
		writerTopology.selfInReplicatorSet === true &&
		readerTopology.selfInReplicatorSet === false &&
		writerTopology.replicatorCount === 1 &&
		readerTopology.replicatorCount === 1 &&
		Array.isArray(writerTopology.replicatorHashes) &&
		writerTopology.replicatorHashes.length === 1 &&
		writerTopology.replicatorHashes[0] === writerPeerHash &&
		Array.isArray(readerTopology.replicatorHashes) &&
		readerTopology.replicatorHashes.length === 1 &&
		readerTopology.replicatorHashes[0] === writerPeerHash
	);
};

const topologyHasExactWriterReaderPair = (
	writerTopology: Record<string, any>,
	readerTopology: Record<string, any>,
	expectedWriterPeerHash: string,
	expectedReaderPeerHash: string,
) => {
	const expectedReplicatorHashes = [
		expectedWriterPeerHash,
		expectedReaderPeerHash,
	].sort((left, right) => left.localeCompare(right));
	return (
		writerTopology?.peerHash === expectedWriterPeerHash &&
		readerTopology?.peerHash === expectedReaderPeerHash &&
		writerTopology.selfInReplicatorSet === true &&
		readerTopology.selfInReplicatorSet === true &&
		writerTopology.replicatorCount === 2 &&
		readerTopology.replicatorCount === 2 &&
		Array.isArray(writerTopology.replicatorHashes) &&
		JSON.stringify(writerTopology.replicatorHashes) ===
			JSON.stringify(expectedReplicatorHashes) &&
		Array.isArray(readerTopology.replicatorHashes) &&
		JSON.stringify(readerTopology.replicatorHashes) ===
			JSON.stringify(expectedReplicatorHashes)
	);
};

const topologyMatchesTerminalExpectation = (
	writerTopology: Record<string, any>,
	readerTopology: Record<string, any>,
	expectedWriterPeerHash: string,
	expectedReaderPeerHash: string,
) =>
	READER_TERMINAL_TOPOLOGY === "observer"
		? topologyHasExactWriterSingleton(writerTopology, readerTopology)
		: topologyHasExactWriterReaderPair(
				writerTopology,
				readerTopology,
				expectedWriterPeerHash,
				expectedReaderPeerHash,
			);

const getDiagnostics = async (page: Page) => {
	try {
		return await page.evaluate(async () => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			const benchmarkStats = (window as any).__peerbitFileShareBenchmarkStats;
			if (!hooks?.getDiagnostics) {
				return benchmarkStats ? { benchmarkStats } : null;
			}
			return {
				...(await hooks.getDiagnostics()),
				benchmarkStats: benchmarkStats ?? null,
			};
		});
	} catch {
		return null;
	}
};

const probeVisibilityPath = async (page: Page) => {
	try {
		return await page.evaluate(async () => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			return hooks?.probeVisibilityPath
				? await hooks.probeVisibilityPath()
				: null;
		});
	} catch {
		return null;
	}
};

type ReadyManifestEvidence = {
	capturedAt: number;
	fileId: string;
	fileName: string;
	sizeBytes: number;
	finalHash: string;
};

const waitForReadyManifest = async (
	page: Page,
	fileName: string,
	expectedSizeBytes: number,
	expectedFinalHash: string,
	timeout: number,
) => {
	const handle = await page.waitForFunction(
		({ expectedName, expectedSize, expectedHash }) => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			if (!hooks?.getLightweightSnapshot) {
				return null;
			}
			const snapshot = hooks.getLightweightSnapshot();
			const file = snapshot?.listedFiles?.find(
				(candidate: Record<string, unknown>) => candidate.name === expectedName,
			);
			if (!file || file.ready !== true) {
				return null;
			}
			if (file.size !== String(expectedSize)) {
				throw new Error(
					`Ready manifest size ${String(file.size)} does not match ${expectedSize}`,
				);
			}
			if (file.finalHash !== expectedHash) {
				throw new Error("Ready manifest hash does not match fixture SHA-256");
			}
			if (typeof file.id !== "string" || file.id.length === 0) {
				throw new Error("Ready manifest is missing its stable file id");
			}
			const row = Array.from(document.querySelectorAll("li")).find(
				(candidate) =>
					Array.from(candidate.querySelectorAll("span")).some(
						(label) => label.textContent === expectedName,
					),
			);
			const button = row?.querySelector('[data-testid="download-file"]');
			if (
				!(button instanceof HTMLButtonElement) ||
				button.disabled ||
				button.textContent?.includes("pending")
			) {
				return null;
			}
			return {
				capturedAt: Date.now(),
				fileId: file.id,
				fileName: expectedName,
				sizeBytes: expectedSize,
				finalHash: file.finalHash,
			};
		},
		{
			expectedName: fileName,
			expectedSize: expectedSizeBytes,
			expectedHash: expectedFinalHash,
		},
		{ polling: 50, timeout },
	);
	try {
		return (await handle.jsonValue()) as ReadyManifestEvidence;
	} finally {
		await handle.dispose();
	}
};

const persistResult = async (result: Record<string, unknown>) => {
	if (!RESULT_FILE) {
		return;
	}
	await mkdir(path.dirname(RESULT_FILE), { recursive: true });
	const temporaryPath = `${RESULT_FILE}.${process.pid}.${randomUUID()}.tmp`;
	try {
		await writeFile(temporaryPath, `${JSON.stringify(result, null, 2)}\n`);
		await rename(temporaryPath, RESULT_FILE);
	} catch (error) {
		await rm(temporaryPath, { force: true }).catch(() => {});
		throw error;
	}
};

const logStage = (stage: string, details: Record<string, unknown> = {}) => {
	console.log(
		`FILE_SHARE_BENCHMARK_STAGE ${JSON.stringify({
			mode: MODE,
			stage,
			...details,
		})}`,
	);
};

const logSnapshot = (snapshot: Record<string, unknown>) => {
	if (VERBOSE) {
		console.log(
			`FILE_SHARE_BENCHMARK_SNAPSHOT ${JSON.stringify({
				mode: MODE,
				...snapshot,
			})}`,
		);
	}
};

test.describe("generated transfer-validity benchmark", () => {
	test("measures upload, discovery, monitoring, and persisted download", async ({
		browser,
		baseURL,
	}) => {
		test.setTimeout(TEST_OUTER_TIMEOUT_MS);
		if (!baseURL) {
			throw new Error("Missing baseURL");
		}

		const usesLocalBootstrap = NETWORK_MODE === "local";
		const bootstrap:
			| Awaited<ReturnType<typeof startBootstrapPeer>>
			| undefined = usesLocalBootstrap ? await startBootstrapPeer() : undefined;
		const fileName = `file-share-benchmark-${MODE}-${RUN_NONCE}.bin`;
		const writerContext = await browser.newContext({ acceptDownloads: true });
		const readerContext = await browser.newContext({ acceptDownloads: true });
		const writer = await writerContext.newPage();
		const reader = await readerContext.newPage();
		const errors: string[] = [];
		const requestFailures: string[] = [];
		const snapshots: Array<Record<string, unknown>> = [];
		let preparedFile:
			| Awaited<ReturnType<typeof createSyntheticFileOnDisk>>
			| undefined;
		let cleanupDownload: (() => Promise<void>) | undefined;
		let downloadMemoryTelemetryController:
			| Awaited<ReturnType<typeof startDownloadMemoryTelemetry>>
			| undefined;
		let downloadMemoryTelemetry: Record<string, any> | null = null;
		let nodeSinkController:
			| Awaited<ReturnType<typeof installNodeBackedMockSaveFilePicker>>
			| undefined;
		let stage = "setup";
		let uploadStartedAt: number | undefined;
		let uploadSettledAt: number | undefined;
		let progressSettledAt: number | undefined;
		let progressVisibleAt: number | undefined;
		let writerListedAt: number | undefined;
		let readerListedAt: number | undefined;
		let postMonitorStartedAt: number | undefined;
		let postMonitorFinishedAt: number | undefined;
		let downloadStartedAt: number | undefined;
		let downloadFinishedAt: number | undefined;
		let downloadCompletionObservedAt: number | undefined;
		let shareUrl: string | undefined;
		let writerVisibilityProbe: Record<string, unknown> | null = null;
		let readerVisibilityProbe: Record<string, unknown> | null = null;
		let writerManifestEvidence: ReadyManifestEvidence | undefined;
		let readerManifestEvidence: ReadyManifestEvidence | undefined;
		let integrity: Record<string, unknown> | null = null;
		let integrityVerified = false;
		let integrityVerifiedAt: number | null = null;
		let dropped = false;
		let unexpectedSeederDrop = false;
		let consecutiveBelowBaselineSeederSnapshots = 0;
		let baselineWriterSeeders = MIN_READY_SEEDERS;
		let baselineReaderSeeders = MIN_READY_SEEDERS;
		const readerLocalityControl =
			READER_LOCAL_CHUNK_TARGET === null
				? null
				: {
						profile: "observer-topology-exact-manifest-prefix",
						provisioningMethod: "exact-manifest-head-import",
						requestedLocalChunkBlockCount: READER_LOCAL_CHUNK_TARGET,
						maxSpeculativeOvershootChunkCount:
							READER_LOCAL_CHUNK_MAX_OVERSHOOT!,
						countMetric:
							"exact local Documents index rows and manifest entry blocks",
						writerUploadRole: "fixed1",
						readerUploadRole: "observer",
						readerTimedReadPolicy: "persist-chunk-reads",
						expectedTerminalTopology: READER_TERMINAL_TOPOLOGY,
						stabilityPollIntervalMs: LOCALITY_CONTROL_POLL_MS,
						requiredStableObservationCount:
							LOCALITY_CONTROL_STABLE_SAMPLE_COUNT,
						status: "pending",
						readerInitialRoleEvidence: null as InitialReaderRoleEvidence | null,
						writerTopologyBeforeUpload: null as Record<string, unknown> | null,
						readerTopologyBeforeUpload: null as Record<string, unknown> | null,
						writerTopologyBeforeTimedRead: null as Record<
							string,
							unknown
						> | null,
						readerTopologyBeforeTimedRead: null as Record<
							string,
							unknown
						> | null,
						beforePreloadObservation:
							null as LocalChunkPrefixObservation | null,
						preloadEvidence: null as Record<string, unknown> | null,
						stabilityObservations: [] as LocalChunkPrefixObservation[],
						preDownloadObservation: null as LocalChunkPrefixObservation | null,
						integrityVerifiedAt: null as number | null,
						terminalIdleObservation: null as LocalChunkPrefixObservation | null,
						terminalTopologyStartedAt: null as number | null,
						terminalTopologyDeadlineAt: null as number | null,
						terminalTopologyFinishedAt: null as number | null,
						terminalTopologyRole: null as string | null,
						terminalTopologyExpectationSatisfied: null as boolean | null,
						terminalTopologyObservations: [] as Array<{
							capturedAt: number;
							writerTopology: Record<string, unknown>;
							readerTopology: Record<string, unknown>;
						}>,
						actualLocalChunkBlockCount: null as number | null,
						actualLocalChunkIndexRowCount: null as number | null,
						speculativeOvershootChunkCount: null as number | null,
						cohortKey: null as string | null,
						failure: null as string | null,
					};
		attachTransferErrorCollector(writer, "writer", errors, requestFailures);
		attachTransferErrorCollector(reader, "reader", errors, requestFailures);

		const readSeederCount = async (page: Page, label: string) => {
			try {
				const count = await getSeederCount(page);
				if (!Number.isSafeInteger(count) || count < 0) {
					throw new Error(`returned invalid count ${String(count)}`);
				}
				return count;
			} catch (error: any) {
				const message = `${label}:seeder-count:${
					typeof error?.message === "string" ? error.message : String(error)
				}`;
				errors.push(message);
				throw new Error(message, { cause: error });
			}
		};

		const snapshot = async (label: string) => {
			let values: [number, number, boolean, boolean, boolean];
			try {
				values = await Promise.all([
					readSeederCount(writer, "writer"),
					readSeederCount(reader, "reader"),
					writer
						.locator('[data-testid="upload-progress"], .progress-root')
						.first()
						.isVisible(),
					writer.locator("li", { hasText: fileName }).first().isVisible(),
					reader.locator("li", { hasText: fileName }).first().isVisible(),
				]);
			} catch (error: any) {
				const message = `${label}:sample:${
					typeof error?.message === "string" ? error.message : String(error)
				}`;
				errors.push(message);
				throw new Error(message, { cause: error });
			}
			const [
				writerSeeders,
				readerSeeders,
				uploadVisible,
				writerRow,
				readerRow,
			] = values;
			const state = {
				label,
				writerSeeders,
				readerSeeders,
				uploadVisible,
				writerRow,
				readerRow,
				at: Date.now(),
			};
			snapshots.push(state);
			logSnapshot(state);
			return state;
		};

		const isContiguousPrefix = (indices: number[]) =>
			indices.every((index, offset) => index === offset);

		const localityObservationIsIdle = (
			observation: LocalChunkPrefixObservation,
		) =>
			Array.isArray(observation.activeTransfers) &&
			observation.activeTransfers.length === 0 &&
			observation.downloadScheduler?.activeCount === 0 &&
			observation.downloadScheduler.activeBytes === 0 &&
			observation.downloadScheduler.queuedCount === 0;

		const collectStableReaderLocality = async () => {
			if (
				!readerLocalityControl ||
				READER_LOCAL_CHUNK_TARGET === null ||
				READER_LOCAL_CHUNK_MAX_OVERSHOOT === null ||
				!readerManifestEvidence
			) {
				throw new Error("Reader locality control is not ready for sampling");
			}
			const deadline = Date.now() + READY_TIMEOUT_MS;
			let stable: LocalChunkPrefixObservation[] = [];
			let lastObservation: LocalChunkPrefixObservation | null = null;
			while (Date.now() <= deadline) {
				const observation = await readLocalChunkPrefixObservation(
					reader,
					fileName,
				);
				lastObservation = observation;
				const exactSetsAreValid =
					observation.fileId === readerManifestEvidence.fileId &&
					observation.indexRowCount !== null &&
					observation.blockCount !== null &&
					observation.indexedChunkIndices !== null &&
					observation.blockChunkIndices !== null &&
					observation.indexRowCount <= observation.blockCount &&
					isContiguousPrefix(observation.indexedChunkIndices) &&
					isContiguousPrefix(observation.blockChunkIndices) &&
					observation.blockCount >= READER_LOCAL_CHUNK_TARGET &&
					observation.blockCount <=
						READER_LOCAL_CHUNK_TARGET + READER_LOCAL_CHUNK_MAX_OVERSHOOT &&
					observation.blockCount < observation.chunkCount! &&
					observation.persistChunkReads === true &&
					localityObservationIsIdle(observation);
				if (!exactSetsAreValid) {
					stable = [];
				} else {
					const signature = JSON.stringify({
						fileId: observation.fileId,
						indexedChunkIndices: observation.indexedChunkIndices,
						blockChunkIndices: observation.blockChunkIndices,
					});
					const previous = stable.at(-1);
					const previousSignature = previous
						? JSON.stringify({
								fileId: previous.fileId,
								indexedChunkIndices: previous.indexedChunkIndices,
								blockChunkIndices: previous.blockChunkIndices,
							})
						: null;
					stable =
						previousSignature === signature
							? [...stable, observation]
							: [observation];
					if (stable.length >= LOCALITY_CONTROL_STABLE_SAMPLE_COUNT) {
						return stable;
					}
				}
				await reader.waitForTimeout(LOCALITY_CONTROL_POLL_MS);
			}
			throw new Error(
				`Reader locality did not reach a stable exact contiguous prefix: ${JSON.stringify(lastObservation)}`,
			);
		};

		const waitForTerminalReaderIdle = async () => {
			const deadline = Date.now() + READY_TIMEOUT_MS;
			let lastObservation: LocalChunkPrefixObservation | null = null;
			while (Date.now() <= deadline) {
				lastObservation = await readLocalChunkPrefixObservation(
					reader,
					fileName,
				);
				const expectedTerminalIndexRowCount =
					READER_TERMINAL_TOPOLOGY === "replicator"
						? lastObservation.chunkCount
						: 0;
				if (
					localityObservationIsIdle(lastObservation) &&
					lastObservation.fileId === readerManifestEvidence?.fileId &&
					lastObservation.chunkCount !== null &&
					lastObservation.blockCount !== null &&
					lastObservation.blockChunkIndices !== null &&
					lastObservation.blockCount === lastObservation.chunkCount &&
					lastObservation.indexRowCount === expectedTerminalIndexRowCount &&
					Array.isArray(lastObservation.indexedChunkIndices) &&
					lastObservation.indexedChunkIndices.length ===
						expectedTerminalIndexRowCount &&
					isContiguousPrefix(lastObservation.indexedChunkIndices) &&
					isContiguousPrefix(lastObservation.blockChunkIndices) &&
					lastObservation.persistChunkReads === true
				) {
					return lastObservation;
				}
				await reader.waitForTimeout(LOCALITY_CONTROL_POLL_MS);
			}
			throw new Error(
				`Timed reader did not become transfer-idle with every manifest chunk local: ${JSON.stringify(lastObservation)}`,
			);
		};

		const collectStableTerminalTopology = async (
			startedAt: number,
			deadline: number,
		) => {
			if (!readerLocalityControl) {
				throw new Error("Reader locality control is unavailable");
			}
			const writerBeforeUpload =
				readerLocalityControl.writerTopologyBeforeUpload;
			const readerBeforeUpload =
				readerLocalityControl.readerTopologyBeforeUpload;
			const expectedWriterPeerHash = writerBeforeUpload?.peerHash;
			const expectedReaderPeerHash = readerBeforeUpload?.peerHash;
			if (
				typeof expectedWriterPeerHash !== "string" ||
				typeof expectedReaderPeerHash !== "string"
			) {
				throw new Error("Pre-read topology peer identities are unavailable");
			}
			let stable: Array<{
				capturedAt: number;
				writerTopology: Record<string, unknown>;
				readerTopology: Record<string, unknown>;
			}> = [];
			let lastObservation: Record<string, unknown> | null = null;
			while (Date.now() <= deadline) {
				const [writerTopology, readerTopology] = await Promise.all([
					getTopologySnapshot(writer),
					getTopologySnapshot(reader),
				]);
				const observation = {
					capturedAt: Date.now(),
					writerTopology,
					readerTopology,
				};
				lastObservation = observation;
				if (
					observation.capturedAt < startedAt ||
					observation.capturedAt > deadline
				) {
					break;
				}
				if (
					writerTopology?.peerHash === expectedWriterPeerHash &&
					readerTopology?.peerHash === expectedReaderPeerHash &&
					topologyMatchesTerminalExpectation(
						writerTopology,
						readerTopology,
						expectedWriterPeerHash,
						expectedReaderPeerHash,
					)
				) {
					stable = [...stable, observation];
					if (stable.length >= LOCALITY_CONTROL_STABLE_SAMPLE_COUNT) {
						return stable;
					}
				} else {
					stable = [];
				}
				await reader.waitForTimeout(LOCALITY_CONTROL_POLL_MS);
			}
			throw new Error(
				`Timed reader did not converge to requested terminal topology ${READER_TERMINAL_TOPOLOGY}: ${JSON.stringify(lastObservation)}`,
			);
		};

		const noteSeederDrop = (current: Record<string, unknown>) => {
			const writerDropped =
				typeof current.writerSeeders === "number" &&
				current.writerSeeders < baselineWriterSeeders;
			const readerDropped =
				typeof current.readerSeeders === "number" &&
				current.readerSeeders < baselineReaderSeeders;
			const belowBaseline = writerDropped || readerDropped;
			if (!belowBaseline) {
				consecutiveBelowBaselineSeederSnapshots = 0;
				return;
			}
			dropped = true;
			consecutiveBelowBaselineSeederSnapshots += 1;
			if (
				consecutiveBelowBaselineSeederSnapshots >=
					SEEDER_DROP_POLICY.consecutiveBelowBaselineSnapshotThreshold ||
				(SEEDER_DROP_POLICY.terminalBelowBaselineIsUnexpected &&
					current.label === SEEDER_DROP_POLICY.terminalSnapshotLabel)
			) {
				unexpectedSeederDrop = true;
			}
		};

		const getPhaseDurations = () =>
			uploadStartedAt == null
				? undefined
				: {
						timeToProgressVisible:
							progressVisibleAt != null
								? progressVisibleAt - uploadStartedAt
								: undefined,
						activeUpload:
							progressVisibleAt != null && uploadSettledAt != null
								? uploadSettledAt - progressVisibleAt
								: undefined,
						timeToUploadSettled:
							uploadSettledAt != null
								? uploadSettledAt - uploadStartedAt
								: undefined,
						timeToWriterReady:
							writerListedAt != null
								? writerListedAt - uploadStartedAt
								: undefined,
						timeToReaderReady:
							readerListedAt != null
								? readerListedAt - uploadStartedAt
								: undefined,
						writerListingLag:
							uploadSettledAt != null && writerListedAt != null
								? Math.max(0, writerListedAt - uploadSettledAt)
								: undefined,
						readerListingLag:
							uploadSettledAt != null && readerListedAt != null
								? Math.max(0, readerListedAt - uploadSettledAt)
								: undefined,
						readerAfterWriter:
							writerListedAt != null && readerListedAt != null
								? readerListedAt - writerListedAt
								: undefined,
						postUploadMonitor:
							postMonitorStartedAt != null && postMonitorFinishedAt != null
								? postMonitorFinishedAt - postMonitorStartedAt
								: undefined,
						download:
							downloadStartedAt != null && downloadFinishedAt != null
								? downloadFinishedAt - downloadStartedAt
								: undefined,
					};

		try {
			stage = `install-${DOWNLOAD_SINK}-download-sink`;
			if (DOWNLOAD_SINK === "node-file") {
				nodeSinkController = await installNodeBackedMockSaveFilePicker(reader, {
					expectedName: fileName,
					expectedSizeBytes: FILE_SIZE_BYTES,
				});
			} else if (DOWNLOAD_SINK === "hash-only") {
				await installHashOnlyMockSaveFilePicker(reader, {
					expectedName: fileName,
					expectedSizeBytes: FILE_SIZE_BYTES,
				});
			} else {
				await installMockSaveFilePicker(reader);
			}
			stage = "create-space";
			logStage(stage, { networkMode: NETWORK_MODE });
			const entryUrl =
				usesLocalBootstrap && bootstrap
					? withBootstrap(rootUrl(baseURL), bootstrap.addrs)
					: rootUrl(baseURL);
			shareUrl = await createSpace(
				writer,
				entryUrl,
				`file-share-benchmark-${MODE}-${Date.now()}`,
			);

			stage = "open-share";
			logStage(stage, { shareUrl });
			if (readerLocalityControl) {
				const shareHash = new URL(shareUrl).hash;
				const shareAddress = shareHash.replace(/^#\/s\//, "");
				if (
					!shareAddress ||
					/[/?#]/.test(shareAddress) ||
					shareHash !== `#/s/${shareAddress}`
				) {
					throw new Error(`Failed to derive share address from ${shareUrl}`);
				}
				await seedReplicationRole(reader, shareAddress, false);
			}
			await Promise.all([
				applyRole(writer, shareUrl),
				readerLocalityControl
					? openWithSeededReplicationRole(reader, shareUrl)
					: applyRole(reader, shareUrl),
			]);

			stage = "wait-for-seeders";
			logStage(stage, {
				minReadySeeders: MIN_READY_SEEDERS,
				readyTimeoutMs: READY_TIMEOUT_MS,
			});
			if (MIN_READY_SEEDERS > 0) {
				await Promise.all([
					expectSeedersAtLeast(writer, MIN_READY_SEEDERS, READY_TIMEOUT_MS),
					expectSeedersAtLeast(reader, MIN_READY_SEEDERS, READY_TIMEOUT_MS),
				]);
			} else {
				await Promise.all([
					readSeederCount(writer, "writer"),
					readSeederCount(reader, "reader"),
				]);
			}
			const ready = await snapshot("seeders-ready");
			baselineWriterSeeders =
				typeof ready.writerSeeders === "number"
					? ready.writerSeeders
					: MIN_READY_SEEDERS;
			baselineReaderSeeders =
				typeof ready.readerSeeders === "number"
					? ready.readerSeeders
					: MIN_READY_SEEDERS;
			if (readerLocalityControl) {
				const initialRoleEvidence = await readInitialReaderRoleEvidence(reader);
				readerLocalityControl.readerInitialRoleEvidence = initialRoleEvidence;
				if (
					typeof initialRoleEvidence.programAddress !== "string" ||
					initialRoleEvidence.programAddress.length === 0 ||
					initialRoleEvidence.persistChunkReads !== false ||
					initialRoleEvidence.initialRole !== "observer" ||
					initialRoleEvidence.updateRoleCount !== 0 ||
					initialRoleEvidence.lastAppliedRole !== null
				) {
					throw new Error(
						"Controlled-locality reader was not initialized as an observer before upload",
					);
				}
				const [writerTopology, readerTopology] = await Promise.all([
					getTopologySnapshot(writer),
					getTopologySnapshot(reader),
				]);
				readerLocalityControl.writerTopologyBeforeUpload = writerTopology;
				readerLocalityControl.readerTopologyBeforeUpload = readerTopology;
				if (!topologyHasExactWriterSingleton(writerTopology, readerTopology)) {
					throw new Error(
						"Controlled-locality reader did not start in an observer topology",
					);
				}
			}

			stage = "prepare-deterministic-fixture";
			logStage(stage, { fixtureSeed: FIXTURE_SEED });
			preparedFile = await createSyntheticFileOnDisk(fileName, FILE_SIZE_MB, {
				mode: "deterministic",
				seed: FIXTURE_SEED,
			});
			if (
				!preparedFile.fixture.sha256Base64 ||
				!preparedFile.fixture.crc32Hex
			) {
				throw new Error("Deterministic fixture did not provide both digests");
			}
			const expectedSizeBytes = FILE_SIZE_BYTES;
			const sourceDetails = await stat(preparedFile.filePath);
			if (sourceDetails.size !== expectedSizeBytes) {
				throw new Error(
					`Fixture size ${sourceDetails.size} does not match ${expectedSizeBytes}`,
				);
			}

			stage = "wait-for-upload-input";
			await writer.locator("#imgupload").waitFor({
				state: "attached",
				timeout: 60_000,
			});
			stage = "upload";
			logStage(stage);
			uploadStartedAt = Date.now();
			await writer.locator("#imgupload").setInputFiles(preparedFile.filePath);
			await expect(writer.locator("#imgupload")).toHaveValue("");
			const writerListedPromise = waitForReadyManifest(
				writer,
				fileName,
				expectedSizeBytes,
				preparedFile.fixture.sha256Base64,
				UPLOAD_TIMEOUT_MS,
			).then((evidence) => ({ evidence, listedAt: evidence.capturedAt }));
			const readerListedPromise = waitForReadyManifest(
				reader,
				fileName,
				expectedSizeBytes,
				preparedFile.fixture.sha256Base64,
				UPLOAD_TIMEOUT_MS +
					READY_TIMEOUT_MS +
					TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,
			).then((evidence) => ({ evidence, listedAt: evidence.capturedAt }));
			void readerListedPromise.catch(() => {});
			const writerReadyPromise = Promise.all([
				writerListedPromise,
				waitForUploadComplete(writer, UPLOAD_TIMEOUT_MS).then(() => Date.now()),
			]).then(([manifest, progressDoneAt]) => ({
				manifest,
				progressDoneAt,
				readyAt: Date.now(),
			}));
			const progressWasVisible = await writer
				.locator('[data-testid="upload-progress"], .progress-root')
				.first()
				.waitFor({ state: "visible", timeout: 5000 })
				.then(
					() => true,
					() => false,
				);
			if (progressWasVisible) {
				progressVisibleAt = Date.now();
			}

			stage = "wait-for-writer-ready";
			let writerReady: Awaited<typeof writerReadyPromise> | undefined;
			while (!writerReady) {
				const outcome = await Promise.race([
					writerReadyPromise.then((value) => ({ ready: value })),
					writer.waitForTimeout(POLL_MS).then(() => ({ ready: undefined })),
				]);
				if (outcome.ready) {
					writerReady = outcome.ready;
					break;
				}
				const current = await snapshot(`during-${snapshots.length}`);
				noteSeederDrop(current);
			}
			writerManifestEvidence = writerReady.manifest.evidence;
			writerListedAt = writerReady.manifest.listedAt;
			progressSettledAt = writerReady.progressDoneAt;
			// Writer readiness is the primary endpoint. A hidden progress element is
			// only diagnostic because it can disappear before a ready manifest exists.
			uploadSettledAt = writerReady.readyAt;
			const readerReadyDeadlineAt = writerReady.readyAt + READY_TIMEOUT_MS;
			const readerReadyDeadlineMessage = `Reader ready manifest was not listed within ${READY_TIMEOUT_MS}ms after writer readiness`;
			const readerReadyRemainingMs = readerReadyDeadlineAt - Date.now();
			const boundedReaderListedPromise =
				readerReadyRemainingMs > 0
					? withDeadline(
							readerListedPromise,
							readerReadyRemainingMs,
							readerReadyDeadlineMessage,
						)
					: Promise.reject(new Error(readerReadyDeadlineMessage));
			void boundedReaderListedPromise.catch(() => {});

			if (ENABLE_VISIBILITY_PROBE) {
				stage = "probe-visibility-path";
				[writerVisibilityProbe, readerVisibilityProbe] = await Promise.all([
					probeVisibilityPath(writer),
					probeVisibilityPath(reader),
				]);
			}

			stage = "wait-for-reader-listing";
			logStage(stage);
			const readerManifest = await boundedReaderListedPromise;
			if (readerManifest.listedAt > readerReadyDeadlineAt) {
				throw new Error(
					`Reader ready manifest evidence was captured after its writer-readiness deadline (${readerManifest.listedAt} > ${readerReadyDeadlineAt})`,
				);
			}
			readerManifestEvidence = readerManifest.evidence;
			readerListedAt = readerManifest.listedAt;

			stage = "post-upload-monitor";
			logStage(stage);
			postMonitorStartedAt = Date.now();
			const deadline = postMonitorStartedAt + POST_UPLOAD_MONITOR_MS;
			while (Date.now() < deadline) {
				const current = await snapshot(`after-${snapshots.length}`);
				noteSeederDrop(current);
				await writer.waitForTimeout(
					Math.min(POLL_MS, Math.max(0, deadline - Date.now())),
				);
			}
			postMonitorFinishedAt = Date.now();
			const measuredPostMonitorMs =
				postMonitorFinishedAt - postMonitorStartedAt;
			if (
				measuredPostMonitorMs < POST_UPLOAD_MONITOR_MS ||
				measuredPostMonitorMs >
					POST_UPLOAD_MONITOR_MS + POST_MONITOR_SCHEDULING_TOLERANCE_MS
			) {
				throw new Error(
					`Post-upload monitor duration ${measuredPostMonitorMs}ms is outside ${POST_UPLOAD_MONITOR_MS}ms + ${POST_MONITOR_SCHEDULING_TOLERANCE_MS}ms scheduling tolerance`,
				);
			}
			if (readerLocalityControl) {
				stage = "prepare-reader-locality-cohort";
				readerLocalityControl.status = "preparing";
				const beforePreloadObservation = await readLocalChunkPrefixObservation(
					reader,
					fileName,
				);
				readerLocalityControl.beforePreloadObservation =
					beforePreloadObservation;
				if (
					beforePreloadObservation.fileId !== readerManifestEvidence.fileId ||
					beforePreloadObservation.chunkCount === null ||
					READER_LOCAL_CHUNK_TARGET >= beforePreloadObservation.chunkCount ||
					beforePreloadObservation.indexRowCount !== 0 ||
					beforePreloadObservation.blockCount !== 0 ||
					beforePreloadObservation.indexedChunkIndices?.length !== 0 ||
					beforePreloadObservation.blockChunkIndices?.length !== 0 ||
					beforePreloadObservation.persistChunkReads !== false ||
					!localityObservationIsIdle(beforePreloadObservation)
				) {
					throw new Error(
						"Controlled-locality reader was not an empty, idle observer before preload",
					);
				}
				logStage("reader-locality-preload", {
					requestedLocalChunkBlockCount: READER_LOCAL_CHUNK_TARGET,
					maxSpeculativeOvershootChunkCount: READER_LOCAL_CHUNK_MAX_OVERSHOOT,
				});
				const preloadEvidence = (await preloadLocalChunkPrefix(
					reader,
					fileName,
					READER_LOCAL_CHUNK_TARGET,
					DOWNLOAD_TIMEOUT_MS,
				)) as Record<string, unknown>;
				readerLocalityControl.preloadEvidence = preloadEvidence;
				const preloadScheduler = preloadEvidence.downloadSchedulerAfterClose as
					| LocalitySchedulerObservation
					| undefined;
				const preloadStartedAt = preloadEvidence.startedAt as number;
				const preloadFinishedAt = preloadEvidence.finishedAt as number;
				const expectedImportedIndices = Array.from(
					{ length: READER_LOCAL_CHUNK_TARGET },
					(_, index) => index,
				);
				if (
					preloadEvidence.fileId !== readerManifestEvidence.fileId ||
					preloadEvidence.provisioningMethod !== "exact-manifest-head-import" ||
					preloadEvidence.transferId !== null ||
					preloadEvidence.requestedManifestEntryCount !==
						READER_LOCAL_CHUNK_TARGET ||
					preloadEvidence.importedManifestEntryCount !==
						READER_LOCAL_CHUNK_TARGET ||
					JSON.stringify(preloadEvidence.importedManifestEntryIndices) !==
						JSON.stringify(expectedImportedIndices) ||
					JSON.stringify(preloadEvidence.localManifestEntryIndicesAfter) !==
						JSON.stringify(expectedImportedIndices) ||
					!Number.isSafeInteger(preloadEvidence.rawFetchedByteCount) ||
					(preloadEvidence.rawFetchedByteCount as number) < 0 ||
					preloadEvidence.maxConcurrentImports !== 8 ||
					preloadEvidence.persistChunkReads !== true ||
					!Array.isArray(preloadEvidence.activeTransfersAfterClose) ||
					preloadEvidence.activeTransfersAfterClose.length !== 0 ||
					preloadScheduler?.activeCount !== 0 ||
					preloadScheduler.activeBytes !== 0 ||
					preloadScheduler.queuedCount !== 0 ||
					!Number.isSafeInteger(preloadEvidence.startedAt) ||
					!Number.isSafeInteger(preloadEvidence.finishedAt) ||
					preloadFinishedAt < preloadStartedAt ||
					preloadEvidence.aggregateTimedOut !== false ||
					preloadEvidence.readDiagnostics !== null
				) {
					throw new Error(
						"Reader locality preload did not close with clean transfer resources",
					);
				}
				if (READER_LOCAL_CHUNK_TARGET === 0) {
					if (
						preloadEvidence.aggregateTimeoutMs !== null ||
						preloadEvidence.aggregateDeadlineAt !== null ||
						preloadEvidence.rawFetchedByteCount !== 0
					) {
						throw new Error(
							"Zero-prefix preload unexpectedly imported a manifest entry",
						);
					}
				} else if (
					preloadEvidence.aggregateTimeoutMs !== DOWNLOAD_TIMEOUT_MS ||
					preloadEvidence.aggregateDeadlineAt !==
						preloadStartedAt + DOWNLOAD_TIMEOUT_MS ||
					preloadFinishedAt - preloadStartedAt >
						DOWNLOAD_TIMEOUT_MS + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS ||
					(preloadEvidence.rawFetchedByteCount as number) <= 0
				) {
					throw new Error(
						"Exact manifest-prefix import did not prove a bounded clean preload",
					);
				}

				const stabilityObservations = await collectStableReaderLocality();
				readerLocalityControl.stabilityObservations = stabilityObservations;
				const preDownloadObservation = stabilityObservations.at(-1)!;
				readerLocalityControl.preDownloadObservation = preDownloadObservation;
				readerLocalityControl.actualLocalChunkBlockCount =
					preDownloadObservation.blockCount;
				readerLocalityControl.actualLocalChunkIndexRowCount =
					preDownloadObservation.indexRowCount;
				readerLocalityControl.speculativeOvershootChunkCount =
					preDownloadObservation.blockCount! - READER_LOCAL_CHUNK_TARGET;
				readerLocalityControl.cohortKey = `observer-persistent-prefix-b${preDownloadObservation.blockCount}-i${preDownloadObservation.indexRowCount}`;
				const [writerTopology, readerTopology] = await Promise.all([
					getTopologySnapshot(writer),
					getTopologySnapshot(reader),
				]);
				readerLocalityControl.writerTopologyBeforeTimedRead = writerTopology;
				readerLocalityControl.readerTopologyBeforeTimedRead = readerTopology;
				if (!topologyHasExactWriterSingleton(writerTopology, readerTopology)) {
					throw new Error(
						"Controlled-locality reader joined the replication set before the timed read",
					);
				}
				readerLocalityControl.status = "stable";
				logStage("reader-locality-stable", {
					actualLocalChunkBlockCount:
						readerLocalityControl.actualLocalChunkBlockCount,
					actualLocalChunkIndexRowCount:
						readerLocalityControl.actualLocalChunkIndexRowCount,
					cohortKey: readerLocalityControl.cohortKey,
				});
			}

			stage = "download-to-selected-sink";
			logStage(stage, { downloadSink: DOWNLOAD_SINK });
			const row = reader.locator("li", { hasText: fileName }).first();
			await expect(row).toBeVisible({ timeout: DOWNLOAD_TIMEOUT_MS });
			const byTestId = row.getByTestId("download-file");
			const downloadButton =
				(await byTestId.count()) > 0 ? byTestId : row.locator("button").first();
			await expect(downloadButton).toBeEnabled({
				timeout: DOWNLOAD_TIMEOUT_MS,
			});
			downloadMemoryTelemetryController = await startDownloadMemoryTelemetry({
				browser,
				reader,
				writer,
				downloadTimeoutMs: DOWNLOAD_TIMEOUT_MS,
				schedulingToleranceMs: TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,
			});
			downloadMemoryTelemetry = downloadMemoryTelemetryController.snapshot();
			const initialMemorySeries = [
				downloadMemoryTelemetry.readerJsHeap,
				downloadMemoryTelemetry.writerJsHeap,
				downloadMemoryTelemetry.hostRss,
			];
			if (
				initialMemorySeries.some(
					(series) =>
						series.sampleCount < 1 ||
						series.samplingErrors.length > 0 ||
						series.samplingErrorOverflowCount !== 0,
				)
			) {
				throw new Error(
					"Download memory telemetry did not collect clean initial samples",
				);
			}
			const downloadCompletion = armSavedViaPicker(
				reader,
				fileName,
				FILE_SIZE_MB,
				DOWNLOAD_TIMEOUT_MS,
			);
			const downloadClickStartedAt = Date.now();
			downloadStartedAt = downloadClickStartedAt;
			await downloadButton.click();
			const download = await downloadCompletion;
			downloadCompletionObservedAt = Date.now();
			downloadMemoryTelemetry =
				await downloadMemoryTelemetryController.stopSampling();
			const memorySeries = [
				downloadMemoryTelemetry.readerJsHeap,
				downloadMemoryTelemetry.writerJsHeap,
				downloadMemoryTelemetry.hostRss,
			];
			if (
				downloadMemoryTelemetry.complete !== true ||
				downloadMemoryTelemetry.finishedAt == null ||
				memorySeries.some(
					(series) =>
						series.sampleCount < 2 ||
						series.samplingErrors.length > 0 ||
						series.samplingErrorOverflowCount !== 0,
				)
			) {
				throw new Error(
					"Download memory telemetry did not complete without sampling errors",
				);
			}
			if (
				!Number.isSafeInteger(download.sinkCompletedAt) ||
				download.sinkCompletedAt < downloadClickStartedAt ||
				download.sinkCompletedAt > downloadCompletionObservedAt
			) {
				throw new Error(
					"Download sink completion timestamp is outside the click-to-observation window",
				);
			}
			downloadFinishedAt = download.sinkCompletedAt;
			cleanupDownload = download.cleanup;
			if (download.sink !== DOWNLOAD_SINK) {
				throw new Error(
					`Download completed through ${download.sink}, expected ${DOWNLOAD_SINK}`,
				);
			}
			if (DOWNLOAD_SINK === "node-file" && !download.downloadPath) {
				throw new Error("Node-file download did not expose its persisted path");
			}
			if (DOWNLOAD_SINK !== "node-file" && download.downloadPath != null) {
				throw new Error(
					`${DOWNLOAD_SINK} download unexpectedly exposed a Node file path`,
				);
			}

			stage = "verify-integrity";
			const readerDiagnostics = await getDiagnostics(reader);
			if (!readerDiagnostics) {
				throw new Error("Missing reader diagnostics after sink completion");
			}
			const rawReadDiagnosticsValue = (
				readerDiagnostics as Record<string, unknown>
			).lastReadDiagnostics;
			if (
				rawReadDiagnosticsValue == null ||
				typeof rawReadDiagnosticsValue !== "object" ||
				Array.isArray(rawReadDiagnosticsValue)
			) {
				throw new Error("Missing completed library read diagnostics");
			}
			const rawReadDiagnostics = rawReadDiagnosticsValue as Record<
				string,
				unknown
			>;
			const libraryReadStartedAt = rawReadDiagnostics.startedAt;
			const libraryReadFinishedAt = rawReadDiagnostics.finishedAt;
			if (
				!Number.isSafeInteger(libraryReadStartedAt) ||
				!Number.isSafeInteger(libraryReadFinishedAt) ||
				downloadStartedAt == null ||
				downloadFinishedAt == null ||
				(libraryReadStartedAt as number) < downloadStartedAt ||
				(libraryReadFinishedAt as number) < (libraryReadStartedAt as number) ||
				(libraryReadFinishedAt as number) > downloadFinishedAt
			) {
				throw new Error(
					"Library read diagnostics are outside the clicked download window",
				);
			}
			if (
				rawReadDiagnostics.fileName !== fileName ||
				rawReadDiagnostics.fileId !== readerManifestEvidence?.fileId ||
				writerManifestEvidence?.fileId !== readerManifestEvidence?.fileId ||
				typeof rawReadDiagnostics.transferId !== "string" ||
				rawReadDiagnostics.transferId.length === 0
			) {
				throw new Error(
					"Library read diagnostics do not match the clicked file manifest",
				);
			}
			if (readerLocalityControl) {
				const actualLocalChunkBlockCount =
					readerLocalityControl.actualLocalChunkBlockCount;
				const actualLocalChunkIndexRowCount =
					readerLocalityControl.actualLocalChunkIndexRowCount;
				if (
					!Number.isSafeInteger(actualLocalChunkBlockCount) ||
					!Number.isSafeInteger(actualLocalChunkIndexRowCount) ||
					rawReadDiagnostics.persistChunkReads !== true ||
					rawReadDiagnostics.programPersistChunkReads !== true ||
					rawReadDiagnostics.initialLocalChunkIndexRowCount !==
						actualLocalChunkIndexRowCount ||
					rawReadDiagnostics.initialLocalChunkCount !==
						actualLocalChunkIndexRowCount ||
					rawReadDiagnostics.initialLocalChunkBlockCount !==
						actualLocalChunkBlockCount
				) {
					throw new Error(
						"Timed read diagnostics do not match the controlled pre-download locality cohort",
					);
				}
			}
			const summarizedReadTransfer = summarizeReadTransferDiagnostics(
				rawReadDiagnostics,
				expectedSizeBytes,
			);
			const {
				streamReadExclusiveMs: sinkAwaitSubtractedDiagnosticMs,
				...readTransferStages
			} = summarizedReadTransfer.stages;
			const readTransfer = {
				...summarizedReadTransfer,
				stages: {
					...readTransferStages,
					sinkAwaitSubtractedDiagnosticMs,
				},
			};
			const libraryComputedSha256Base64 = rawReadDiagnostics.computedFinalHash;
			if (typeof libraryComputedSha256Base64 !== "string") {
				throw new Error("Missing library-computed download SHA-256");
			}
			if (
				!Number.isSafeInteger(download.sinkWriteCalls) ||
				(download.sinkWriteCalls ?? -1) <= 0 ||
				download.sinkWriteCalls !== readTransfer.chunkCount ||
				typeof download.sinkWriteDurationMs !== "number" ||
				!Number.isFinite(download.sinkWriteDurationMs) ||
				download.sinkWriteDurationMs < 0
			) {
				throw new Error("Download sink timing evidence is incomplete");
			}
			if (
				download.sinkWriteDurationMs >
				readTransfer.stages.sinkWriteAwaitMs +
					readTransfer.chunkCount *
						SINK_WRITE_QUANTIZATION_ALLOWANCE_MS_PER_CHUNK
			) {
				throw new Error(
					"Download sink helper duration exceeds canonical read evidence plus its bounded clock allowance",
				);
			}
			if (DOWNLOAD_SINK === "node-file") {
				if (
					!Number.isSafeInteger(download.serverWriteCalls) ||
					download.serverWriteCalls !== download.sinkWriteCalls ||
					typeof download.serverWriteDurationMs !== "number" ||
					!Number.isFinite(download.serverWriteDurationMs) ||
					download.serverWriteDurationMs < 0
				) {
					throw new Error("Node-file server timing evidence is incomplete");
				}
				if (
					download.serverWriteDurationMs >
					download.sinkWriteDurationMs +
						readTransfer.chunkCount * SINK_SERVER_CLOCK_TOLERANCE_MS_PER_CHUNK
				) {
					throw new Error(
						"Node-file server duration exceeds its browser sink duration plus the bounded clock tolerance",
					);
				}
			} else if (
				download.serverWriteCalls != null ||
				download.serverWriteDurationMs != null
			) {
				throw new Error(
					`${DOWNLOAD_SINK} download reported Node-only server timing evidence`,
				);
			}
			const nodePersistedSinkDigests = download.downloadPath
				? await sha256AndCrc32File(download.downloadPath)
				: null;
			const opfsPersistedSinkDigests =
				DOWNLOAD_SINK === "opfs"
					? await sha256AndCrc32OpfsSavedViaPicker(
							reader,
							fileName,
							expectedSizeBytes,
							() => new SHA256(),
						)
					: null;
			if (
				opfsPersistedSinkDigests &&
				opfsPersistedSinkDigests.sizeBytes !== expectedSizeBytes
			) {
				throw new Error("Persisted OPFS readback size does not match fixture");
			}
			const persistedSinkDigests =
				nodePersistedSinkDigests ?? opfsPersistedSinkDigests;
			const downloadedCrc32Hex = persistedSinkDigests
				? persistedSinkDigests.crc32Hex
				: await crc32SavedViaPicker(reader, fileName);
			const sizeVerified = download.size === expectedSizeBytes;
			const librarySha256Verified =
				libraryComputedSha256Base64 === preparedFile.fixture.sha256Base64;
			const persistedSinkSha256Verified = persistedSinkDigests
				? persistedSinkDigests.sha256Base64 ===
					preparedFile.fixture.sha256Base64
				: null;
			const sha256Verified =
				librarySha256Verified &&
				(DOWNLOAD_SINK === "hash-only" || persistedSinkSha256Verified === true);
			const crc32Verified =
				downloadedCrc32Hex === preparedFile.fixture.crc32Hex;
			const manifestVerified =
				writerManifestEvidence?.sizeBytes === expectedSizeBytes &&
				readerManifestEvidence?.sizeBytes === expectedSizeBytes &&
				writerManifestEvidence?.finalHash ===
					preparedFile.fixture.sha256Base64 &&
				readerManifestEvidence?.finalHash === preparedFile.fixture.sha256Base64;
			const sinkPersistence =
				DOWNLOAD_SINK === "hash-only" ? "none" : DOWNLOAD_SINK;
			const sinkPersistenceVerified =
				DOWNLOAD_SINK === "hash-only"
					? null
					: sizeVerified &&
						crc32Verified &&
						persistedSinkSha256Verified === true;
			integrityVerified =
				sizeVerified &&
				sha256Verified &&
				crc32Verified &&
				manifestVerified &&
				sinkPersistenceVerified !== false;
			integrity = {
				fixtureMode: "deterministic",
				fixtureFormat: preparedFile.fixture.mode,
				fixtureSeed: FIXTURE_SEED,
				expectedSizeBytes,
				sourceSizeBytes: sourceDetails.size,
				manifestSizeBytes: readerManifestEvidence?.sizeBytes,
				downloadedSizeBytes: download.size,
				sourceSha256Base64: preparedFile.fixture.sha256Base64,
				libraryComputedSha256Base64,
				downloadedSha256Base64: persistedSinkDigests?.sha256Base64 ?? null,
				manifestSha256Base64: readerManifestEvidence?.finalHash,
				sourceCrc32Hex: preparedFile.fixture.crc32Hex,
				downloadedCrc32Hex,
				downloadSink: DOWNLOAD_SINK,
				sinkPersistence,
				sinkPersistenceVerified,
				sizeVerified,
				sha256Verified,
				librarySha256Verified,
				persistedSinkSha256Verified,
				crc32Verified,
				manifestVerified,
				verified: integrityVerified,
			};
			if (!integrityVerified) {
				throw new Error(
					`Downloaded file failed integrity validation: ${JSON.stringify(integrity)}`,
				);
			}
			integrityVerifiedAt = Date.now();
			// Sampling is finalized at sink completion above, but CDP cleanup is
			// deliberately deferred until the transfer and its SHA-256/CRC-32
			// evidence are complete. A slow Performance.disable/detach must not
			// erase an otherwise valid transfer result.
			downloadMemoryTelemetry =
				await downloadMemoryTelemetryController.cleanup();
			const finalizedMemorySeries = [
				downloadMemoryTelemetry.readerJsHeap,
				downloadMemoryTelemetry.writerJsHeap,
				downloadMemoryTelemetry.hostRss,
			];
			if (
				downloadMemoryTelemetry.cleanupComplete !== true ||
				finalizedMemorySeries.some(
					(series) =>
						series.samplingErrors.length > 0 ||
						series.samplingErrorOverflowCount !== 0,
				)
			) {
				throw new Error(
					"Download memory telemetry cleanup reported a non-timeout failure",
				);
			}
			if (readerLocalityControl) {
				stage = "verify-terminal-reader-topology";
				readerLocalityControl.integrityVerifiedAt = integrityVerifiedAt;
				const terminalIdleObservation = await waitForTerminalReaderIdle();
				readerLocalityControl.terminalIdleObservation = terminalIdleObservation;
				const terminalTopologyStartedAt = Date.now();
				const terminalTopologyDeadlineAt =
					terminalTopologyStartedAt + READY_TIMEOUT_MS;
				readerLocalityControl.terminalTopologyStartedAt =
					terminalTopologyStartedAt;
				readerLocalityControl.terminalTopologyDeadlineAt =
					terminalTopologyDeadlineAt;
				const terminalTopologyObservations =
					await collectStableTerminalTopology(
						terminalTopologyStartedAt,
						terminalTopologyDeadlineAt,
					);
				const terminalTopologyFinishedAt = Date.now();
				if (terminalTopologyFinishedAt > terminalTopologyDeadlineAt) {
					throw new Error(
						"Terminal topology converged after its bounded readiness deadline",
					);
				}
				readerLocalityControl.terminalTopologyFinishedAt =
					terminalTopologyFinishedAt;
				readerLocalityControl.terminalTopologyObservations =
					terminalTopologyObservations;
				readerLocalityControl.terminalTopologyRole =
					READER_TERMINAL_TOPOLOGY;
				readerLocalityControl.terminalTopologyExpectationSatisfied = true;
				readerLocalityControl.status = "complete";
				logStage("reader-terminal-topology-stable", {
					terminalTopologyRole: readerLocalityControl.terminalTopologyRole,
					terminalTopologyExpectationSatisfied:
						readerLocalityControl.terminalTopologyExpectationSatisfied,
				});
			}
			const terminalSeederSnapshot = await snapshot(
				SEEDER_DROP_POLICY.terminalSnapshotLabel,
			);
			noteSeederDrop(terminalSeederSnapshot);
			if (unexpectedSeederDrop) {
				throw new Error(
					"Seeder counts dropped outside the benchmark topology contract",
				);
			}
			if (errors.length > 0) {
				throw new Error(
					`Observed transfer/delivery errors: ${JSON.stringify(errors)}`,
				);
			}

			stage = "complete";
			logStage(stage);
			const phaseDurationsMs = getPhaseDurations();
			if (
				!phaseDurationsMs ||
				uploadSettledAt == null ||
				writerListedAt == null ||
				readerListedAt == null ||
				postMonitorStartedAt == null ||
				postMonitorFinishedAt == null ||
				downloadStartedAt == null ||
				downloadFinishedAt == null ||
				downloadCompletionObservedAt == null
			) {
				throw new Error("Benchmark completed without all phase timestamps");
			}
			if (
				phaseDurationsMs.timeToUploadSettled == null ||
				phaseDurationsMs.timeToWriterReady == null ||
				phaseDurationsMs.timeToReaderReady == null ||
				phaseDurationsMs.download == null ||
				phaseDurationsMs.timeToUploadSettled >
					UPLOAD_TIMEOUT_MS + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS ||
				phaseDurationsMs.download >
					DOWNLOAD_TIMEOUT_MS + TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS
			) {
				throw new Error(
					"Measured transfer duration exceeded its requested timeout and scheduling tolerance",
				);
			}
			const [writerDiagnostics, finalReaderDiagnostics] = await Promise.all([
				getDiagnostics(writer),
				getDiagnostics(reader),
			]);
			if (
				readerLocalityControl &&
				(!writerDiagnostics || !finalReaderDiagnostics)
			) {
				throw new Error(
					"Controlled-locality benchmark is missing final peer diagnostics",
				);
			}
			if (errors.length > 0) {
				throw new Error(
					`Observed transfer/delivery errors while collecting diagnostics: ${JSON.stringify(errors)}`,
				);
			}
			const collectedErrors = [...errors];
			const collectedRequestFailures = [...requestFailures];
			const result = {
				schema: RESULT_SCHEMA,
				runNonce: RUN_NONCE,
				invocation: INVOCATION,
				provenance: PROVENANCE,
				status: "passed",
				mode: MODE,
				readerLocalChunkTarget: READER_LOCAL_CHUNK_TARGET,
				readerLocalChunkMaxOvershoot: READER_LOCAL_CHUNK_MAX_OVERSHOOT,
				readerTerminalTopology: READER_TERMINAL_TOPOLOGY,
				readerLocalChunkBlockCount:
					readerLocalityControl?.actualLocalChunkBlockCount ?? null,
				readerLocalChunkIndexRowCount:
					readerLocalityControl?.actualLocalChunkIndexRowCount ?? null,
				readerLocalityCohortKey: readerLocalityControl?.cohortKey ?? null,
				readerLocalityControl,
				networkMode: NETWORK_MODE,
				fileName,
				fileSizeMb: FILE_SIZE_MB,
				integrity,
				integrityVerified,
				integrityVerifiedAt,
				uploadDurationMs: phaseDurationsMs.timeToUploadSettled,
				uploadDurationDefinition:
					"input-set-to-writer-ready-manifest-and-upload-progress-settled; excludes reader discovery, post-monitor, and download",
				timeToWriterReadyMs: phaseDurationsMs.timeToWriterReady,
				timeToWriterReadyDefinition: TIME_TO_WRITER_READY_DEFINITION,
				timeToReaderReadyMs: phaseDurationsMs.timeToReaderReady,
				timeToReaderReadyDefinition: TIME_TO_READER_READY_DEFINITION,
				listingDurationMs: Math.max(
					0,
					Math.max(writerListedAt, readerListedAt) - uploadSettledAt,
				),
				listingDurationDefinition: LISTING_DURATION_DEFINITION,
				postUploadMonitorDurationMs:
					postMonitorFinishedAt - postMonitorStartedAt,
				postUploadMonitorDurationDefinition:
					"post-listing seeder/error observation only",
				postUploadMonitorSchedulingToleranceMs:
					POST_MONITOR_SCHEDULING_TOLERANCE_MS,
				postUploadMonitorSchedulingToleranceDefinition:
					POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION,
				downloadDurationMs: downloadFinishedAt - downloadStartedAt,
				downloadDurationDefinition: DOWNLOAD_DURATION_DEFINITION,
				transferTimeoutSchedulingToleranceMs:
					TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,
				transferTimeoutSchedulingToleranceDefinition:
					TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION,
				downloadSink: download.sink,
				requestedDownloadSink: DOWNLOAD_SINK,
				downloadMemoryTelemetry,
				sinkWriteCalls: download.sinkWriteCalls,
				sinkWriteDurationMs: download.sinkWriteDurationMs,
				sinkWriteDurationDefinition: SINK_WRITE_DURATION_DEFINITION,
				sinkServerWriteCalls: download.serverWriteCalls ?? null,
				sinkServerWriteDurationMs: download.serverWriteDurationMs ?? null,
				sinkServerWriteDurationDefinition:
					download.sink === "node-file"
						? SINK_SERVER_WRITE_DURATION_DEFINITION
						: null,
				readTransfer,
				libraryStreamWallMs: readTransfer.stages.libraryStreamWallMs,
				libraryStreamWallDefinition: LIBRARY_STREAM_WALL_DEFINITION,
				primaryDownloadMetric: PRIMARY_DOWNLOAD_METRIC,
				primaryDownloadAuthoritative: DOWNLOAD_SINK === "hash-only",
				primaryDownloadMetricDefinition: PRIMARY_DOWNLOAD_METRIC_DEFINITION,
				sinkWriteAwaitMs: readTransfer.stages.sinkWriteAwaitMs,
				sinkWriteAwaitDefinition: SINK_WRITE_AWAIT_DEFINITION,
				sinkAwaitSubtractedDiagnosticMs:
					readTransfer.stages.sinkAwaitSubtractedDiagnosticMs,
				sinkAwaitSubtractedDiagnosticDefinition:
					SINK_AWAIT_SUBTRACTED_DIAGNOSTIC_DEFINITION,
				phaseDurationsMs,
				timestamps: {
					uploadStartedAt,
					uploadSettledAt,
					progressVisibleAt,
					progressSettledAt,
					writerListedAt,
					readerListedAt,
					postMonitorStartedAt,
					postMonitorFinishedAt,
					downloadStartedAt,
					downloadFinishedAt,
					downloadCompletionObservedAt,
				},
				writerManifestEvidence,
				readerManifestEvidence,
				postUploadMonitorMs: POST_UPLOAD_MONITOR_MS,
				pollMs: POLL_MS,
				uploadTimeoutMs: UPLOAD_TIMEOUT_MS,
				downloadTimeoutMs: DOWNLOAD_TIMEOUT_MS,
				minReadySeeders: MIN_READY_SEEDERS,
				readyTimeoutMs: READY_TIMEOUT_MS,
				baselineWriterSeeders,
				baselineReaderSeeders,
				shareUrl,
				seederDropPolicy: SEEDER_DROP_POLICY,
				droppedSeeders: dropped,
				unexpectedSeederDrop,
				writerVisibilityProbe,
				readerVisibilityProbe,
				errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
				knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
				errorCollectionComplete: true,
				errorCount: collectedErrors.length,
				errors: collectedErrors,
				requestFailureCollectionDefinition:
					REQUEST_FAILURE_COLLECTION_DEFINITION,
				requestFailureCollectionComplete: true,
				requestFailureCount: collectedRequestFailures.length,
				requestFailures: collectedRequestFailures,
				snapshots,
				writerDiagnostics,
				readerDiagnostics: finalReaderDiagnostics,
			};
			await persistResult(result);
			console.log(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);
		} catch (error: any) {
			if (downloadMemoryTelemetryController) {
				downloadMemoryTelemetry =
					await downloadMemoryTelemetryController.cleanup();
			}
			if (readerLocalityControl) {
				readerLocalityControl.status = "failed";
				readerLocalityControl.failure =
					typeof error?.message === "string" ? error.message : String(error);
			}
			const failureSnapshot = await snapshot(`failure-${stage}`).catch(
				() => null,
			);
			if (failureSnapshot) {
				noteSeederDrop(failureSnapshot);
			}
			const writerDiagnostics = await getDiagnostics(writer);
			const readerDiagnostics = await getDiagnostics(reader);
			const collectedErrors = [...errors];
			const collectedRequestFailures = [...requestFailures];
			const result = {
				schema: RESULT_SCHEMA,
				runNonce: RUN_NONCE,
				invocation: INVOCATION,
				provenance: PROVENANCE,
				status: "failed",
				mode: MODE,
				readerLocalChunkTarget: READER_LOCAL_CHUNK_TARGET,
				readerLocalChunkMaxOvershoot: READER_LOCAL_CHUNK_MAX_OVERSHOOT,
				readerTerminalTopology: READER_TERMINAL_TOPOLOGY,
				readerLocalChunkBlockCount:
					readerLocalityControl?.actualLocalChunkBlockCount ?? null,
				readerLocalChunkIndexRowCount:
					readerLocalityControl?.actualLocalChunkIndexRowCount ?? null,
				readerLocalityCohortKey: readerLocalityControl?.cohortKey ?? null,
				readerLocalityControl,
				networkMode: NETWORK_MODE,
				fileSizeMb: FILE_SIZE_MB,
				stage,
				requestedDownloadSink: DOWNLOAD_SINK,
				downloadMemoryTelemetry,
				integrity,
				integrityVerified,
				integrityVerifiedAt,
				phaseDurationsMs: getPhaseDurations(),
				postUploadMonitorSchedulingToleranceMs:
					POST_MONITOR_SCHEDULING_TOLERANCE_MS,
				postUploadMonitorSchedulingToleranceDefinition:
					POST_MONITOR_SCHEDULING_TOLERANCE_DEFINITION,
				transferTimeoutSchedulingToleranceMs:
					TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS,
				transferTimeoutSchedulingToleranceDefinition:
					TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_DEFINITION,
				postUploadMonitorMs: POST_UPLOAD_MONITOR_MS,
				pollMs: POLL_MS,
				uploadTimeoutMs: UPLOAD_TIMEOUT_MS,
				downloadTimeoutMs: DOWNLOAD_TIMEOUT_MS,
				minReadySeeders: MIN_READY_SEEDERS,
				readyTimeoutMs: READY_TIMEOUT_MS,
				baselineWriterSeeders,
				baselineReaderSeeders,
				shareUrl,
				seederDropPolicy: SEEDER_DROP_POLICY,
				droppedSeeders: dropped,
				unexpectedSeederDrop,
				writerVisibilityProbe,
				readerVisibilityProbe,
				errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
				knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
				errorCollectionComplete: true,
				errorCount: collectedErrors.length,
				errors: collectedErrors,
				requestFailureCollectionDefinition:
					REQUEST_FAILURE_COLLECTION_DEFINITION,
				requestFailureCollectionComplete: true,
				requestFailureCount: collectedRequestFailures.length,
				requestFailures: collectedRequestFailures,
				snapshots,
				writerDiagnostics,
				readerDiagnostics,
				failure: {
					message:
						typeof error?.message === "string" ? error.message : String(error),
					stack: typeof error?.stack === "string" ? error.stack : undefined,
				},
			};
			await persistResult(result);
			console.error(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);
			throw error;
		} finally {
			await downloadMemoryTelemetryController?.cleanup().catch(() => {});
			await cleanupDownload?.().catch(() => {});
			await nodeSinkController?.cleanup().catch(() => {});
			if (preparedFile) {
				await rm(preparedFile.dir, { recursive: true, force: true }).catch(
					() => {},
				);
			}
			await writerContext.close().catch(() => {});
			await readerContext.close().catch(() => {});
			await bootstrap?.stop().catch(() => {});
		}
	});
});
