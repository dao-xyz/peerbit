import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..");
const browserBenchRoot = path.join(
	repoRoot,
	"packages",
	"clients",
	"peerbit-react",
	"e2e",
	"browser",
);

const defaultProfiles = [
	{
		name: "inmemory-full",
		persistent: false,
		params: {},
	},
	{
		name: "persisted-clone-full",
		persistent: true,
		params: {
			sqliteprotocol: "clone",
		},
	},
	{
		name: "persisted-clone-meta",
		persistent: true,
		params: {
			sqliteprotocol: "clone",
			docindex: "meta",
		},
	},
];

const parseArgs = (argv) => {
	const out = {};
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		if (arg === "--") {
			continue;
		}
		if (!arg.startsWith("--")) {
			continue;
		}
		const key = arg.slice(2);
		if (key === "skip-build") {
			out[key] = true;
			continue;
		}
		const value = argv[i + 1];
		if (value == null || value.startsWith("--")) {
			throw new Error(`Missing value for ${arg}`);
		}
		out[key] = value;
		i++;
	}
	return out;
};

const run = (command, args, options = {}) =>
	new Promise((resolve, reject) => {
		const child = spawn(command, args, {
			cwd: options.cwd ?? repoRoot,
			stdio: "inherit",
			env: {
				...process.env,
				...(options.env ?? {}),
			},
		});
		child.on("exit", (code) => {
			if (code === 0) {
				resolve();
				return;
			}
			reject(
				new Error(
					`${command} ${args.join(" ")} exited with code ${code ?? "unknown"}`,
				),
			);
		});
		child.on("error", reject);
	});

const startServer = ({ port }) => {
	const child = spawn(
		"pnpm",
		["exec", "vite", "preview", "--host", "--strictPort", "--port", String(port)],
		{
			cwd: browserBenchRoot,
			stdio: "inherit",
			env: process.env,
		},
	);
	return child;
};

const waitForServer = async (baseURL, timeoutMs = 120_000) => {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			const response = await fetch(baseURL);
			if (response.ok) {
				return;
			}
		} catch {}
		await new Promise((resolve) => setTimeout(resolve, 1000));
	}
	throw new Error(`Timed out waiting for ${baseURL}`);
};

const bytesToMiB = (bytes) => bytes / (1024 * 1024);

const getDefaultCount = (payloadBytes) => {
	const targetPayloadBytes = 4 * 1024 * 1024;
	const targetOps = Math.round(targetPayloadBytes / payloadBytes);
	return Math.max(16, Math.min(256, targetOps));
};

const withPersistedStorage = async (context, origin) => {
	await context.addInitScript(() => {
		Object.defineProperty(navigator, "storage", {
			value: {
				...navigator.storage,
				persist: async () => true,
				persisted: async () => true,
			},
			configurable: true,
		});
	});
	await context.grantPermissions(["storage-access"], { origin });
};

const runCase = async ({
	baseURL,
	profile,
	payloadBytes,
	count,
	sqliteSynchronous,
	sqliteLockingMode,
	sqliteTempStore,
}) => {
	const origin = new URL(baseURL).origin;
	let context;
	let userDataDir;
	try {
		if (profile.persistent) {
			userDataDir = await fs.mkdtemp(
				path.join(os.tmpdir(), `peerbit-docbench-${profile.name}-`),
			);
			context = await chromium.launchPersistentContext(userDataDir, {
				headless: true,
				viewport: { width: 1280, height: 800 },
				args: ["--enable-features=FileSystemAccessAPI"],
			});
			await withPersistedStorage(context, origin);
		} else {
			const browser = await chromium.launch({
				headless: true,
				args: ["--enable-features=FileSystemAccessAPI"],
			});
			context = await browser.newContext({
				viewport: { width: 1280, height: 800 },
			});
		}

		const page = await context.newPage();
		const params = new URLSearchParams({
			docbench: "1",
			bytes: String(payloadBytes),
			count: String(count),
			...profile.params,
			...(sqliteSynchronous ? { sqlitesynchronous: sqliteSynchronous } : {}),
			...(sqliteLockingMode
				? { sqlitelockingmode: sqliteLockingMode }
				: {}),
			...(sqliteTempStore ? { sqlitetempstore: sqliteTempStore } : {}),
		});
		if (!profile.persistent) {
			params.set("inmemory", "1");
		}
		await page.goto(`${baseURL}/?${params.toString()}`, {
			waitUntil: "domcontentloaded",
		});
		await page.waitForFunction(
			() => {
				const el = document.querySelector(
					'[data-testid="document-benchmark-status"]',
				);
				return el?.textContent === "ready";
			},
			{ timeout: 120_000 },
		);

		const text = await page.getByTestId("document-benchmark-results").textContent();
		const result = JSON.parse(text || "{}");
		const totalMiB = bytesToMiB(payloadBytes * count);
		const documentSeconds = result.documentPutMs / 1000;
		return {
			profile: profile.name,
			payloadBytes,
			count,
			inMemory: result.inMemory,
			persisted: result.persisted,
			docIndexMode: result.docIndexMode,
			sqliteProtocol: result.sqliteProtocol,
			sqliteSynchronous:
				result.sqliteSynchronous ?? sqliteSynchronous ?? "default",
			sqliteLockingMode:
				result.sqliteLockingMode ?? sqliteLockingMode ?? "default",
			sqliteTempStore:
				result.sqliteTempStore ?? sqliteTempStore ?? "default",
			serializeMs: Number(result.serializeMs.toFixed(1)),
			blockPutMs: Number(result.blockPutMs.toFixed(1)),
			documentPutMs: Number(result.documentPutMs.toFixed(1)),
			tps: Number((count / documentSeconds).toFixed(1)),
			mbps: Number((totalMiB / documentSeconds).toFixed(2)),
			sqliteRequests: result.sqliteProfile?.totalRequests ?? 0,
			sqliteWorkerExecMs: result.sqliteProfile
				? Number(result.sqliteProfile.totalWorkerExecMs.toFixed(1))
				: 0,
		};
	} finally {
		await context?.close();
		if (userDataDir) {
			await fs.rm(userDataDir, { recursive: true, force: true });
		}
	}
};

const percentile = (values, p) => {
	if (values.length === 0) {
		return null;
	}
	const sorted = [...values].sort((a, b) => a - b);
	const index = Math.min(
		sorted.length - 1,
		Math.max(0, Math.ceil(sorted.length * p) - 1),
	);
	return sorted[index];
};

const summarize = (runs) => {
	const groups = new Map();
	for (const run of runs) {
		const key = `${run.profile}|${run.payloadBytes}|${run.count}`;
		const bucket = groups.get(key) ?? [];
		bucket.push(run);
		groups.set(key, bucket);
	}
	return [...groups.values()]
		.map((bucket) => {
			const first = bucket[0];
			const documentPutValues = bucket.map((entry) => entry.documentPutMs);
			const tpsValues = bucket.map((entry) => entry.tps);
			const mbpsValues = bucket.map((entry) => entry.mbps);
			const average = (values) =>
				values.reduce((sum, value) => sum + value, 0) / values.length;
			return {
				profile: first.profile,
				payloadBytes: first.payloadBytes,
				count: first.count,
				runs: bucket.length,
				documentPutMsAvg: Number(average(documentPutValues).toFixed(1)),
				documentPutMsP95: Number((percentile(documentPutValues, 0.95) ?? 0).toFixed(1)),
				tpsAvg: Number(average(tpsValues).toFixed(1)),
				tpsP95: Number((percentile(tpsValues, 0.95) ?? 0).toFixed(1)),
				mbpsAvg: Number(average(mbpsValues).toFixed(2)),
				mbpsP95: Number((percentile(mbpsValues, 0.95) ?? 0).toFixed(2)),
			};
		})
		.sort((a, b) => {
			if (a.profile !== b.profile) {
				return a.profile.localeCompare(b.profile);
			}
			return a.payloadBytes - b.payloadBytes;
		});
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const port = Number(args.port ?? "4183");
	const baseURL = `http://localhost:${port}`;
	const payloadSizes = (args.sizes ?? "256,1024,4096,32768,262144")
		.split(",")
		.map((value) => Number(value.trim()))
		.filter((value) => Number.isFinite(value) && value > 0);
	const repeats = Math.max(1, Number(args.repeats ?? "3"));
	const profiles = (args.profiles
		? args.profiles.split(",").map((value) => value.trim())
		: defaultProfiles.map((profile) => profile.name)
	)
		.map((name) => defaultProfiles.find((profile) => profile.name === name))
		.filter(Boolean);
	if (profiles.length === 0) {
		throw new Error("No valid profiles selected");
	}
	const sqliteSynchronous = args["sqlite-synchronous"];
	const sqliteLockingMode = args["sqlite-locking-mode"];
	const sqliteTempStore = args["sqlite-temp-store"];
	const outputPath = args.output ? path.resolve(args.output) : undefined;
	const countsOverride = args.count ? Number(args.count) : undefined;

	if (!args["skip-build"]) {
		await run("pnpm", ["--filter", "@peerbit/react-e2e-browser", "build"]);
	}

	const server = startServer({ port });
	try {
		await waitForServer(baseURL);
		const runs = [];
		for (const profile of profiles) {
			for (const payloadBytes of payloadSizes) {
				const count = countsOverride ?? getDefaultCount(payloadBytes);
				for (let repeat = 1; repeat <= repeats; repeat++) {
					console.log(
						`Running document-put benchmark profile=${profile.name} payloadBytes=${payloadBytes} count=${count} repeat=${repeat}/${repeats}`,
					);
					runs.push(
						await runCase({
							baseURL,
							profile,
							payloadBytes,
							count,
							sqliteSynchronous,
							sqliteLockingMode,
							sqliteTempStore,
						}),
					);
				}
			}
		}
		const summary = summarize(runs);
		const output = {
			baseURL,
			repeats,
			payloadSizes,
			sqliteSynchronous: sqliteSynchronous ?? "default",
			sqliteLockingMode: sqliteLockingMode ?? "default",
			sqliteTempStore: sqliteTempStore ?? "default",
			runs,
			summary,
		};
		if (outputPath) {
			await fs.mkdir(path.dirname(outputPath), { recursive: true });
			await fs.writeFile(outputPath, `${JSON.stringify(output, null, 2)}\n`);
		}
		console.log("\nSummary");
		console.table(summary);
		console.log("\nRaw JSON");
		console.log(JSON.stringify(output, null, 2));
	} finally {
		server.kill("SIGINT");
	}
};

main().catch((error) => {
	console.error(error);
	process.exitCode = 1;
});
