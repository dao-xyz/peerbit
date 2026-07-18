import { type Page, expect, test } from "@playwright/test";
import { SHA256 } from "@stablelib/sha256";
import { randomUUID } from "node:crypto";
import { mkdir, rename, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { startBootstrapPeer } from "./bootstrapPeer";
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
	version: 4,
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
	(INVOCATION.schema as Record<string, unknown> | undefined)?.version !== 2
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
		test.setTimeout(
			Math.max(
				20 * 60 * 1000,
				3 * READY_TIMEOUT_MS +
					UPLOAD_TIMEOUT_MS +
					DOWNLOAD_TIMEOUT_MS +
					POST_UPLOAD_MONITOR_MS +
					5 * 60 * 1000,
			),
		);
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
		let dropped = false;
		let baselineWriterSeeders = MIN_READY_SEEDERS;
		let baselineReaderSeeders = MIN_READY_SEEDERS;
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

		const noteSeederDrop = (current: Record<string, unknown>) => {
			if (
				(typeof current.writerSeeders === "number" &&
					current.writerSeeders < baselineWriterSeeders) ||
				(typeof current.readerSeeders === "number" &&
					current.readerSeeders < baselineReaderSeeders)
			) {
				dropped = true;
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
			await Promise.all([
				applyRole(writer, shareUrl),
				applyRole(reader, shareUrl),
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
			const integrityVerified =
				sizeVerified &&
				sha256Verified &&
				crc32Verified &&
				manifestVerified &&
				sinkPersistenceVerified !== false;
			const integrity = {
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
			if (dropped) {
				throw new Error("Seeder counts dropped during benchmark");
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
			const writerDiagnostics = await getDiagnostics(writer);
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
				networkMode: NETWORK_MODE,
				fileName,
				fileSizeMb: FILE_SIZE_MB,
				integrity,
				integrityVerified,
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
				droppedSeeders: dropped,
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
			};
			await persistResult(result);
			console.log(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);
		} catch (error: any) {
			await snapshot(`failure-${stage}`).catch(() => {});
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
				networkMode: NETWORK_MODE,
				fileSizeMb: FILE_SIZE_MB,
				stage,
				requestedDownloadSink: DOWNLOAD_SINK,
				integrityVerified: false,
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
				droppedSeeders: dropped,
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
