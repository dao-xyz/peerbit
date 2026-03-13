import { test, type Page } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { startBootstrapPeer } from "./bootstrapPeer";
import {
	createSpace,
	getSeederCount,
	rootUrl,
	withBootstrap,
} from "./helpers";

const MODE = process.env.PW_REPLICATION_MODE || "adaptive";
const NETWORK_MODE = process.env.PW_NETWORK_MODE || "local";
const RESULT_FILE = process.env.PW_RESULT_FILE;
const READY_TIMEOUT_MS = Number(process.env.PW_READY_TIMEOUT_MS || "180000");
const SAMPLE_MS = Number(process.env.PW_SAMPLE_MS || "15000");
const SAMPLE_COUNT = Number(process.env.PW_SAMPLE_COUNT || "4");
const TARGET_SEEDERS = Number(
	process.env.PW_TARGET_SEEDERS || (MODE === "adaptive" ? "2" : "1"),
);

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

const attachErrorCollector = (page: Page, label: string, output: string[]) => {
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

test.describe("generated seeder probe", () => {
	test("tracks seeder convergence before upload", async ({ browser, baseURL }) => {
		test.setTimeout(
			Math.max(
				15 * 60 * 1000,
				READY_TIMEOUT_MS + SAMPLE_MS * (SAMPLE_COUNT + 2) + 60_000,
			),
		);
		if (!baseURL) {
			throw new Error("Missing baseURL");
		}

		const usesLocalBootstrap = NETWORK_MODE === "local";
		const bootstrap: Awaited<ReturnType<typeof startBootstrapPeer>> | undefined =
			usesLocalBootstrap ? await startBootstrapPeer() : undefined;
		const writerContext = await browser.newContext({ acceptDownloads: true });
		const readerContext = await browser.newContext({ acceptDownloads: true });
		const writer = await writerContext.newPage();
		const reader = await readerContext.newPage();
		const errors: string[] = [];
		const samples: Array<Record<string, unknown>> = [];
		let stage = "setup";
		let shareUrl: string | undefined;
		let reachedTarget = false;
		let timeToTargetMs: number | undefined;
		attachErrorCollector(writer, "writer", errors);
		attachErrorCollector(reader, "reader", errors);

		const sample = async (label: string, startedAt: number) => {
			const [
				writerSeeders,
				readerSeeders,
				writerDiagnostics,
				readerDiagnostics,
				writerBodyText,
				readerBodyText,
			] = await Promise.all([
				getSeederCount(writer).catch((error) => `error:${error.message}`),
				getSeederCount(reader).catch((error) => `error:${error.message}`),
				getDiagnostics(writer),
				getDiagnostics(reader),
				getBodyText(writer),
				getBodyText(reader),
			]);
			const current = {
				label,
				atMs: Date.now() - startedAt,
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
			});
			const startedAt = Date.now();
			for (let sampleIndex = 1; sampleIndex <= SAMPLE_COUNT; sampleIndex++) {
				await writer.waitForTimeout(SAMPLE_MS);
				const current = await sample(`sample-${sampleIndex}`, startedAt);
				if (
					!reachedTarget &&
					current.writerSeeders === TARGET_SEEDERS &&
					current.readerSeeders === TARGET_SEEDERS
				) {
					reachedTarget = true;
					timeToTargetMs = current.atMs as number;
					break;
				}
			}

			const result = {
				status: reachedTarget ? "passed" : "failed",
				mode: MODE,
				networkMode: NETWORK_MODE,
				stage: reachedTarget ? "complete" : stage,
				shareUrl,
				targetSeeders: TARGET_SEEDERS,
				sampleMs: SAMPLE_MS,
				sampleCount: SAMPLE_COUNT,
				readyTimeoutMs: READY_TIMEOUT_MS,
				reachedTarget,
				timeToTargetMs,
				errorCount: errors.length,
				errors,
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
			const result = {
				status: "failed",
				mode: MODE,
				networkMode: NETWORK_MODE,
				stage,
				shareUrl,
				targetSeeders: TARGET_SEEDERS,
				sampleMs: SAMPLE_MS,
				sampleCount: SAMPLE_COUNT,
				readyTimeoutMs: READY_TIMEOUT_MS,
				reachedTarget,
				timeToTargetMs,
				errorCount: errors.length,
				errors,
				samples,
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
			await bootstrap?.stop().catch(() => {});
		}
	});
});
