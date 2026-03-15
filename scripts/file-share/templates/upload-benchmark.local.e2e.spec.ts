import { test, type Page } from "@playwright/test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { startBootstrapPeer } from "./bootstrapPeer";
import {
	createSpace,
	createSyntheticFileOnDisk,
	expectSeedersAtLeast,
	getSeederCount,
	rootUrl,
	waitForFileListed,
	withBootstrap,
} from "./helpers";

const FILE_SIZE_MB = Number(process.env.PW_FILE_MB || "100");
const POLL_MS = Number(process.env.PW_POLL_MS || "1000");
const POST_UPLOAD_MONITOR_MS = Number(
	process.env.PW_POST_UPLOAD_MONITOR_MS || "5000",
);
const UPLOAD_TIMEOUT_MS = Number(process.env.PW_UPLOAD_TIMEOUT_MS || "600000");
const MODE = process.env.PW_REPLICATION_MODE || "adaptive";
const NETWORK_MODE = process.env.PW_NETWORK_MODE || "local";
const RESULT_FILE = process.env.PW_RESULT_FILE;
const ENABLE_VISIBILITY_PROBE = process.env.PW_ENABLE_VISIBILITY_PROBE === "1";
const MIN_READY_SEEDERS = Number(
	process.env.PW_MIN_READY_SEEDERS || (MODE === "adaptive" ? "2" : "0"),
);
const VERBOSE = process.env.PW_VERBOSE === "1";

if (!["local", "remote"].includes(NETWORK_MODE)) {
	throw new Error(`Unsupported PW_NETWORK_MODE='${NETWORK_MODE}'`);
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

const MATCHED_ERRORS = [
	"Failed to resolve block",
	"DeliveryError",
	"Failed to get message",
	"delivery acknowledges",
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
	output: string[],
) => {
	page.on("pageerror", (error) => {
		const text = String(error?.message || error);
		if (MATCHED_ERRORS.some((match) => text.includes(match))) {
			output.push(`${label}:pageerror:${text}`);
		}
	});
	page.on("console", (message) => {
		const text = message.text();
		if (MATCHED_ERRORS.some((match) => text.includes(match))) {
			output.push(`${label}:console.${message.type()}:${text}`);
		}
	});
};

const waitForTestHooks = async (
	page: Page,
	options: { requireRoleSetter?: boolean } = {},
) => {
	await page.waitForFunction(
		(requireRoleSetter) => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			return requireRoleSetter
				? Boolean(hooks?.setReplicationRole && hooks?.getDiagnostics)
				: Boolean(hooks?.getDiagnostics);
		},
		Boolean(options.requireRoleSetter),
		{ timeout: 60_000 },
	);
};

const applyRole = async (page: Page, shareUrl: string) => {
	await page.goto(shareUrl, { waitUntil: "domcontentloaded" });
	await waitForTestHooks(page, { requireRoleSetter: MODE !== "adaptive" });
	if (MODE === "adaptive") {
		return;
	}
	await page.evaluate(async (role) => {
		const hooks = (window as any).__peerbitFileShareTestHooks;
		if (!hooks?.setReplicationRole) {
			throw new Error("Missing __peerbitFileShareTestHooks.setReplicationRole");
		}
		await hooks.setReplicationRole(role);
	}, getRole());
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
			if (!hooks?.probeVisibilityPath) {
				return null;
			}
			return await hooks.probeVisibilityPath();
		});
	} catch {
		return null;
	}
};

const persistResult = async (result: Record<string, unknown>) => {
	if (!RESULT_FILE) {
		return;
	}
	await mkdir(path.dirname(RESULT_FILE), { recursive: true });
	await writeFile(RESULT_FILE, `${JSON.stringify(result, null, 2)}\n`);
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
	if (!VERBOSE) {
		return;
	}
	console.log(
		`FILE_SHARE_BENCHMARK_SNAPSHOT ${JSON.stringify({
			mode: MODE,
			...snapshot,
		})}`,
	);
};

test.describe("generated upload benchmark", () => {
	test("measures file-share upload duration", async ({ browser, baseURL }) => {
		test.setTimeout(
			Math.max(
				20 * 60 * 1000,
				UPLOAD_TIMEOUT_MS + POST_UPLOAD_MONITOR_MS + 5 * 60 * 1000,
			),
		);
		if (!baseURL) {
			throw new Error("Missing baseURL");
		}

		const usesLocalBootstrap = NETWORK_MODE === "local";
		const bootstrap: Awaited<ReturnType<typeof startBootstrapPeer>> | undefined =
			usesLocalBootstrap ? await startBootstrapPeer() : undefined;
		const file = await createSyntheticFileOnDisk(
			`file-share-benchmark-${MODE}-${Date.now()}.bin`,
			FILE_SIZE_MB,
		);
		const writerContext = await browser.newContext({ acceptDownloads: true });
		const readerContext = await browser.newContext({ acceptDownloads: true });
		const writer = await writerContext.newPage();
		const reader = await readerContext.newPage();
		const errors: string[] = [];
		const snapshots: Array<Record<string, unknown>> = [];
		let stage = "setup";
		let startedAt: number | undefined;
		let finishedAt: number | undefined;
		let shareUrl: string | undefined;
		let progressVisibleAt: number | undefined;
		let uploadSettledAt: number | undefined;
		let writerListedAt: number | undefined;
		let readerListedAt: number | undefined;
		let writerVisibilityProbe: Record<string, unknown> | null = null;
		let readerVisibilityProbe: Record<string, unknown> | null = null;
		let dropped = false;
		let baselineWriterSeeders = MIN_READY_SEEDERS;
		let baselineReaderSeeders = MIN_READY_SEEDERS;
		attachTransferErrorCollector(writer, "writer", errors);
		attachTransferErrorCollector(reader, "reader", errors);

		const snapshot = async (label: string) => {
			const [writerSeeders, readerSeeders, uploadVisible, writerRow, readerRow] =
				await Promise.all([
					getSeederCount(writer).catch((e) => `error:${e.message}`),
					getSeederCount(reader).catch((e) => `error:${e.message}`),
					writer
						.locator('[data-testid="upload-progress"], .progress-root')
						.first()
						.isVisible()
						.catch(() => false),
					writer
						.locator("li", { hasText: file.fileName })
						.first()
						.isVisible()
						.catch(() => false),
					reader
						.locator("li", { hasText: file.fileName })
						.first()
						.isVisible()
						.catch(() => false),
				]);

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

		const getPhaseDurations = () =>
			startedAt == null
				? undefined
				: {
						timeToProgressVisible:
							progressVisibleAt != null ? progressVisibleAt - startedAt : undefined,
						activeUpload:
							progressVisibleAt != null && uploadSettledAt != null
								? uploadSettledAt - progressVisibleAt
								: uploadSettledAt != null
									? uploadSettledAt - startedAt
									: undefined,
						timeToUploadSettled:
							uploadSettledAt != null ? uploadSettledAt - startedAt : undefined,
						writerListingLag:
							uploadSettledAt != null && writerListedAt != null
								? writerListedAt - uploadSettledAt
								: undefined,
						readerListingLag:
							uploadSettledAt != null && readerListedAt != null
								? readerListedAt - uploadSettledAt
								: undefined,
						readerAfterWriter:
							writerListedAt != null && readerListedAt != null
								? readerListedAt - writerListedAt
								: undefined,
						timeToWriterListed:
							writerListedAt != null ? writerListedAt - startedAt : undefined,
						timeToReaderListed:
							readerListedAt != null ? readerListedAt - startedAt : undefined,
					};

		try {
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
			logStage(stage, { minReadySeeders: MIN_READY_SEEDERS });
			if (MIN_READY_SEEDERS > 0) {
				await expectSeedersAtLeast(writer, MIN_READY_SEEDERS, 180_000);
				await expectSeedersAtLeast(reader, MIN_READY_SEEDERS, 180_000);
			} else {
				await getSeederCount(writer);
				await getSeederCount(reader);
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
			logStage(stage, {
				baselineWriterSeeders,
				baselineReaderSeeders,
			});

			stage = "upload";
			logStage(stage);
			startedAt = Date.now();
			stage = "wait-for-upload-input";
			logStage(stage);
			await writer.locator("#imgupload").waitFor({
				state: "attached",
				timeout: 60_000,
			});
			stage = "set-input-files";
			logStage(stage);
			await writer.locator("#imgupload").setInputFiles(file.filePath);
			stage = "wait-for-progress";
			logStage(stage);
			await writer
				.locator('[data-testid="upload-progress"], .progress-root')
				.first()
				.waitFor({ state: "visible", timeout: 5000 })
				.catch(() => {});
			progressVisibleAt = Date.now();
			stage = "monitor-upload";
			logStage(stage);

			let uploadCompleted = false;
			while (!uploadCompleted) {
				const current = await snapshot(`during-${snapshots.length}`);
				if (
					typeof current.writerSeeders === "number" &&
					current.writerSeeders < baselineWriterSeeders
				) {
					dropped = true;
				}
				if (
					typeof current.readerSeeders === "number" &&
					current.readerSeeders < baselineReaderSeeders
				) {
					dropped = true;
				}
				uploadCompleted = !(current.uploadVisible as boolean);
				if (uploadCompleted) {
					uploadSettledAt = current.at as number;
					break;
				}
				if (Date.now() - startedAt > UPLOAD_TIMEOUT_MS) {
					throw new Error(
						`Upload did not finish within ${UPLOAD_TIMEOUT_MS}ms: ${JSON.stringify(snapshots)}`,
					);
				}
				await writer.waitForTimeout(POLL_MS);
			}

			if (ENABLE_VISIBILITY_PROBE) {
				stage = "probe-visibility-path";
				logStage(stage);
				[writerVisibilityProbe, readerVisibilityProbe] = await Promise.all([
					probeVisibilityPath(writer),
					probeVisibilityPath(reader),
				]);
			}

			stage = "wait-for-listing";
			logStage(stage);
			[writerListedAt, readerListedAt] = await Promise.all([
				(async () => {
					await waitForFileListed(writer, file.fileName, 180_000);
					return Date.now();
				})(),
				(async () => {
					await waitForFileListed(reader, file.fileName, 180_000);
					return Date.now();
				})(),
			]);

			stage = "post-upload-monitor";
			logStage(stage);
			const deadline = Date.now() + POST_UPLOAD_MONITOR_MS;
			while (Date.now() < deadline) {
				const current = await snapshot(`after-${snapshots.length}`);
				if (
					typeof current.writerSeeders === "number" &&
					current.writerSeeders < baselineWriterSeeders
				) {
					dropped = true;
				}
				if (
					typeof current.readerSeeders === "number" &&
					current.readerSeeders < baselineReaderSeeders
				) {
					dropped = true;
				}
				await writer.waitForTimeout(POLL_MS);
			}

			stage = "complete";
			logStage(stage);
			finishedAt = Date.now();
			const result = {
				status: "passed",
				mode: MODE,
				networkMode: NETWORK_MODE,
				fileSizeMb: FILE_SIZE_MB,
				uploadDurationMs: finishedAt - startedAt,
				postUploadMonitorMs: POST_UPLOAD_MONITOR_MS,
				pollMs: POLL_MS,
				uploadTimeoutMs: UPLOAD_TIMEOUT_MS,
				minReadySeeders: MIN_READY_SEEDERS,
				baselineWriterSeeders,
				baselineReaderSeeders,
				shareUrl,
				droppedSeeders: dropped,
				phaseDurationsMs: getPhaseDurations(),
				writerVisibilityProbe,
				readerVisibilityProbe,
				errorCount: errors.length,
				errors,
				snapshots,
				writerDiagnostics: await getDiagnostics(writer),
				readerDiagnostics: await getDiagnostics(reader),
			};

			await persistResult(result);
			console.log(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);

			if (dropped) {
				throw new Error(
					`Seeder counts dropped during benchmark: ${JSON.stringify(snapshots)}`,
				);
			}
			if (errors.length > 0) {
				throw new Error(
					`Observed transfer/delivery errors: ${JSON.stringify(errors)}`,
				);
			}
		} catch (error: any) {
			await snapshot(`failure-${stage}`).catch(() => {});
			const result = {
				status: "failed",
				mode: MODE,
				networkMode: NETWORK_MODE,
				fileSizeMb: FILE_SIZE_MB,
				stage,
				uploadDurationMs:
					startedAt != null ? Date.now() - startedAt : undefined,
				postUploadMonitorMs: POST_UPLOAD_MONITOR_MS,
				pollMs: POLL_MS,
				uploadTimeoutMs: UPLOAD_TIMEOUT_MS,
				minReadySeeders: MIN_READY_SEEDERS,
				baselineWriterSeeders,
				baselineReaderSeeders,
				shareUrl,
				droppedSeeders: dropped,
				phaseDurationsMs: getPhaseDurations(),
				writerVisibilityProbe,
				readerVisibilityProbe,
				errorCount: errors.length,
				errors,
				snapshots,
				writerDiagnostics: await getDiagnostics(writer),
				readerDiagnostics: await getDiagnostics(reader),
				failure: {
					message:
						typeof error?.message === "string"
							? error.message
							: String(error),
					stack:
						typeof error?.stack === "string" ? error.stack : undefined,
				},
			};
			await persistResult(result);
			console.error(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);
			throw error;
		} finally {
			await writerContext.close().catch(() => {});
			await readerContext.close().catch(() => {});
			await rm(file.dir, { recursive: true, force: true }).catch(() => {});
			await bootstrap?.stop().catch(() => {});
		}
	});
});
