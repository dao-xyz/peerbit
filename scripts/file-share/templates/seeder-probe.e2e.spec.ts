import { type Page, test } from "@playwright/test";
import { randomUUID } from "node:crypto";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { startBootstrapPeer } from "./bootstrapPeer";
import { createSpace, getSeederCount, rootUrl, withBootstrap } from "./helpers";

const MODE = process.env.PW_REPLICATION_MODE || "adaptive";
const NETWORK_MODE = process.env.PW_NETWORK_MODE || "local";
const RESULT_FILE = process.env.PW_RESULT_FILE;
const READY_TIMEOUT_MS = Number(process.env.PW_READY_TIMEOUT_MS || "180000");
const SAMPLE_MS = Number(process.env.PW_SAMPLE_MS || "15000");
const SAMPLE_COUNT = Number(process.env.PW_SAMPLE_COUNT || "4");
const TARGET_SEEDERS = Number(process.env.PW_TARGET_SEEDERS || "2");
const SAMPLE_COUNT_DEFINITION =
	"observation-density divisor: planned interval is min(sampleMs, floor(readyTimeoutMs/sampleCount)) clamped to 1ms; convergence may finish early";
const ERROR_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every uncaught pageerror; every console.error; every console message at any level containing a known Peerbit failure signature; plus scenario-recorded operation failures";
const REQUEST_FAILURE_COLLECTION_DEFINITION =
	"from collector attachment through result snapshot: every Playwright requestfailed event, retained as non-fatal diagnostics and excluded from errorCount";
const EFFECTIVE_SAMPLE_INTERVAL_MS = Math.max(
	1,
	Math.min(SAMPLE_MS, Math.max(1, Math.floor(READY_TIMEOUT_MS / SAMPLE_COUNT))),
);
const RESULT_SCHEMA = {
	id: "peerbit-file-share-benchmark",
	version: 8,
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
const READER_LOCAL_CHUNK_TARGET = process.env.PW_READER_LOCAL_CHUNK_TARGET
	? Number(process.env.PW_READER_LOCAL_CHUNK_TARGET)
	: null;
const READER_LOCAL_CHUNK_MAX_OVERSHOOT = process.env
	.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT
	? Number(process.env.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT)
	: null;
const READER_TERMINAL_TOPOLOGY =
	process.env.PW_READER_TERMINAL_TOPOLOGY || null;

if (!["local", "remote"].includes(NETWORK_MODE)) {
	throw new Error(`Unsupported PW_NETWORK_MODE='${NETWORK_MODE}'`);
}
if (!RUN_NONCE) {
	throw new Error("Missing PW_BENCHMARK_RUN_NONCE");
}
if (process.env.PW_BENCH !== "1") {
	throw new Error("Seeder benchmark must run against the production preview");
}
if (!Number.isSafeInteger(READY_TIMEOUT_MS) || READY_TIMEOUT_MS <= 0) {
	throw new Error(
		`Invalid PW_READY_TIMEOUT_MS='${process.env.PW_READY_TIMEOUT_MS}'`,
	);
}
if (!Number.isSafeInteger(SAMPLE_MS) || SAMPLE_MS <= 0) {
	throw new Error(`Invalid PW_SAMPLE_MS='${process.env.PW_SAMPLE_MS}'`);
}
if (!Number.isSafeInteger(SAMPLE_COUNT) || SAMPLE_COUNT <= 0) {
	throw new Error(`Invalid PW_SAMPLE_COUNT='${process.env.PW_SAMPLE_COUNT}'`);
}
if (!Number.isSafeInteger(TARGET_SEEDERS) || TARGET_SEEDERS < 0) {
	throw new Error(
		`Invalid PW_TARGET_SEEDERS='${process.env.PW_TARGET_SEEDERS}'`,
	);
}
if (
	(INVOCATION.schema as Record<string, unknown> | undefined)?.id !==
		"peerbit-file-share-benchmark-invocation" ||
	(INVOCATION.schema as Record<string, unknown> | undefined)?.version !== 4
) {
	throw new Error("Unsupported PW_BENCHMARK_INVOCATION schema");
}
const expectedInvocationValues: Record<string, unknown> = {
	scenario: "seeder-probe",
	mode: MODE,
	networkMode: NETWORK_MODE,
	readyTimeoutMs: READY_TIMEOUT_MS,
	sampleMs: SAMPLE_MS,
	sampleCount: SAMPLE_COUNT,
	targetSeeders: TARGET_SEEDERS,
	readerLocalChunkTarget: READER_LOCAL_CHUNK_TARGET,
	readerLocalChunkMaxOvershoot: READER_LOCAL_CHUNK_MAX_OVERSHOOT,
	readerTerminalTopology: READER_TERMINAL_TOPOLOGY,
	baseUrl: process.env.PW_BASE_URL || null,
	protocol: process.env.PW_PROTOCOL || null,
	viteMode: process.env.PW_VITE_MODE || null,
	viteConfig: process.env.PW_VITE_CONFIG || null,
	serverMode: "production-preview",
	serverHost: process.env.HOST || null,
};
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

const attachErrorCollector = (
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

const getDiagnostics = async (page: Page) =>
	page
		.evaluate(async () => {
			const hooks = (window as any).__peerbitFileShareTestHooks;
			if (!hooks?.getDiagnostics) {
				return null;
			}
			return await hooks.getDiagnostics();
		})
		.catch(() => null);

const getBodyText = async (page: Page) =>
	page
		.locator("body")
		.innerText()
		.then((text) => text.slice(0, 500))
		.catch(() => null);

const withDeadline = async <T>(
	promise: Promise<T>,
	deadlineAt: number,
	label: string,
): Promise<T> => {
	const remainingMs = deadlineAt - Date.now();
	if (remainingMs <= 0) {
		throw new Error(`${label} exceeded the seeder convergence deadline`);
	}
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_resolve, reject) => {
				timeout = setTimeout(
					() =>
						reject(
							new Error(`${label} exceeded the seeder convergence deadline`),
						),
					remainingMs,
				);
			}),
		]);
	} finally {
		if (timeout) {
			clearTimeout(timeout);
		}
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

test.describe("generated seeder probe", () => {
	test("tracks seeder convergence before upload", async ({
		browser,
		baseURL,
	}) => {
		test.setTimeout(
			Math.max(
				15 * 60 * 1000,
				2 * READY_TIMEOUT_MS + SAMPLE_MS * (SAMPLE_COUNT + 2) + 60_000,
			),
		);
		if (!baseURL) {
			throw new Error("Missing baseURL");
		}

		const usesLocalBootstrap = NETWORK_MODE === "local";
		const bootstrap:
			| Awaited<ReturnType<typeof startBootstrapPeer>>
			| undefined = usesLocalBootstrap ? await startBootstrapPeer() : undefined;
		const writerContext = await browser.newContext({ acceptDownloads: true });
		const readerContext = await browser.newContext({ acceptDownloads: true });
		const writer = await writerContext.newPage();
		const reader = await readerContext.newPage();
		const errors: string[] = [];
		const requestFailures: string[] = [];
		const samples: Array<Record<string, unknown>> = [];
		let stage = "setup";
		let shareUrl: string | undefined;
		let reachedTarget = false;
		let timeToTargetMs: number | undefined;
		let targetSampleLabel: string | undefined;
		let probeStartedAt: number | undefined;
		let readyDeadlineAt: number | undefined;
		let probeFinishedAt: number | undefined;
		attachErrorCollector(writer, "writer", errors, requestFailures);
		attachErrorCollector(reader, "reader", errors, requestFailures);

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

		const sample = async (
			label: string,
			index: number,
			startedAt: number,
			deadlineAt: number,
		) => {
			const [
				writerSeeders,
				readerSeeders,
				writerDiagnostics,
				readerDiagnostics,
				writerBodyText,
				readerBodyText,
			] = await withDeadline(
				Promise.all([
					readSeederCount(writer, "writer"),
					readSeederCount(reader, "reader"),
					getDiagnostics(writer),
					getDiagnostics(reader),
					getBodyText(writer),
					getBodyText(reader),
				]),
				deadlineAt,
				label,
			);
			const capturedAt = Date.now();
			if (capturedAt > deadlineAt) {
				throw new Error(
					`${label} completed after the seeder convergence deadline`,
				);
			}
			const current = {
				index,
				label,
				capturedAt,
				elapsedMs: capturedAt - startedAt,
				writerSeeders,
				readerSeeders,
				writerDiagnostics,
				readerDiagnostics,
				writerBodyText,
				readerBodyText,
			};
			samples.push(current);
			return current;
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
				`file-share-probe-${MODE}-${Date.now()}`,
			);

			stage = "open-share";
			logStage(stage, { shareUrl });
			await Promise.all([
				applyRole(writer, shareUrl),
				applyRole(reader, shareUrl),
			]);

			stage = "sample-seeders";
			logStage(stage, {
				targetSeeders: TARGET_SEEDERS,
				sampleMs: SAMPLE_MS,
				sampleCount: SAMPLE_COUNT,
				effectiveSampleIntervalMs: EFFECTIVE_SAMPLE_INTERVAL_MS,
				readyTimeoutMs: READY_TIMEOUT_MS,
			});
			probeStartedAt = Date.now();
			readyDeadlineAt = probeStartedAt + READY_TIMEOUT_MS;
			let sampleIndex = 0;
			while (Date.now() <= readyDeadlineAt) {
				sampleIndex += 1;
				const current = await sample(
					`sample-${sampleIndex}`,
					sampleIndex,
					probeStartedAt,
					readyDeadlineAt,
				);
				if (
					!reachedTarget &&
					current.writerSeeders >= TARGET_SEEDERS &&
					current.readerSeeders >= TARGET_SEEDERS
				) {
					reachedTarget = true;
					timeToTargetMs = current.elapsedMs;
					targetSampleLabel = current.label;
					probeFinishedAt = current.capturedAt;
					break;
				}
				const remainingMs = readyDeadlineAt - Date.now();
				if (remainingMs <= 0) {
					break;
				}
				await writer.waitForTimeout(
					Math.min(EFFECTIVE_SAMPLE_INTERVAL_MS, remainingMs),
				);
			}
			probeFinishedAt ??= Date.now();
			if (errors.length > 0) {
				throw new Error(
					`Observed seeder probe errors: ${JSON.stringify(errors)}`,
				);
			}
			const collectedErrors = [...errors];
			const collectedRequestFailures = [...requestFailures];

			const result = {
				schema: RESULT_SCHEMA,
				runNonce: RUN_NONCE,
				invocation: INVOCATION,
				provenance: PROVENANCE,
				status: reachedTarget ? "passed" : "failed",
				mode: MODE,
				networkMode: NETWORK_MODE,
				stage: reachedTarget ? "complete" : stage,
				shareUrl,
				targetSeeders: TARGET_SEEDERS,
				sampleMs: SAMPLE_MS,
				sampleCount: SAMPLE_COUNT,
				readyTimeoutMs: READY_TIMEOUT_MS,
				effectiveSampleIntervalMs: EFFECTIVE_SAMPLE_INTERVAL_MS,
				sampleCountDefinition: SAMPLE_COUNT_DEFINITION,
				probeStartedAt,
				readyDeadlineAt,
				probeFinishedAt,
				probeDurationMs: probeFinishedAt - probeStartedAt,
				reachedTarget,
				timeToTargetMs,
				targetSampleLabel,
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
				samples,
			};

			await persistResult(result);
			console.log(`FILE_SHARE_BENCHMARK_RESULT ${JSON.stringify(result)}`);

			if (!reachedTarget) {
				throw new Error(
					`Seeder counts never reached ${TARGET_SEEDERS}: ${JSON.stringify(samples)}`,
				);
			}
		} catch (error: any) {
			probeFinishedAt ??= Date.now();
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
				stage,
				shareUrl,
				targetSeeders: TARGET_SEEDERS,
				sampleMs: SAMPLE_MS,
				sampleCount: SAMPLE_COUNT,
				readyTimeoutMs: READY_TIMEOUT_MS,
				effectiveSampleIntervalMs: EFFECTIVE_SAMPLE_INTERVAL_MS,
				sampleCountDefinition: SAMPLE_COUNT_DEFINITION,
				probeStartedAt,
				readyDeadlineAt,
				probeFinishedAt,
				probeDurationMs:
					probeStartedAt == null ? undefined : probeFinishedAt - probeStartedAt,
				reachedTarget,
				timeToTargetMs,
				targetSampleLabel,
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
				samples,
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
			await writerContext.close().catch(() => {});
			await readerContext.close().catch(() => {});
			await bootstrap?.stop().catch(() => {});
		}
	});
});
