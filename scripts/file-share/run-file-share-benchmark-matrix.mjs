import { spawnSync } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
	defaultExamplesDest,
	defaultExamplesSource,
	parseArgs,
	repoRoot,
	run,
} from "./common.mjs";

const RUNNER_PATH = path.join(
	repoRoot,
	"scripts",
	"file-share",
	"run-file-share-benchmark.mjs",
);

const VARIANT_ORDER = ["current", "head", "downstream"];

const shellQuote = (value) => `'${String(value).replace(/'/g, `'\\''`)}'`;

const runShell = (command, { cwd = repoRoot } = {}) => {
	console.log(`$ ${command}`);
	const result = spawnSync("bash", ["-lc", command], {
		cwd,
		stdio: "inherit",
		env: process.env,
	});
	if (result.status !== 0) {
		throw new Error(`Command failed with exit code ${result.status}: ${command}`);
	}
};

const createHeadClone = async (dest) => {
	await fsp.rm(dest, { recursive: true, force: true });
	runShell(`git clone --quiet ${shellQuote(repoRoot)} ${shellQuote(dest)}`);
	runShell(`git -C ${shellQuote(dest)} checkout --quiet --detach HEAD`);
};

const replaceOnce = (contents, search, replacement, label) => {
	if (!contents.includes(search)) {
		throw new Error(`Could not find expected snippet for ${label}`);
	}
	return contents.replace(search, replacement);
};

const applyDownstreamAdaptivePatch = async (variantRoot) => {
	const filePath = path.join(
		variantRoot,
		"packages",
		"programs",
		"data",
		"shared-log",
		"src",
		"index.ts",
	);
	let contents = await fsp.readFile(filePath, "utf8");

	contents = replaceOnce(
		contents,
		`const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_MEMORY_LIMIT = 0.001;
const RECALCULATE_PARTICIPATION_RELATIVE_DENOMINATOR_FLOOR = 1e-3;

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;`,
		`const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_MEMORY_LIMIT = 0.001;
const RECALCULATE_PARTICIPATION_RELATIVE_DENOMINATOR_FLOOR = 1e-3;
const ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER = 5;
const ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS = 10_000;

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;`,
		"constants",
	);

	contents = replaceOnce(
		contents,
		`	private cpuUsage?: CPUUsage;

	timeUntilRoleMaturity!: number;`,
		`	private cpuUsage?: CPUUsage;
	private _lastLocalAppendAt!: number;
	private adaptiveRebalanceIdleMs!: number;

	timeUntilRoleMaturity!: number;`,
		"fields",
	);

	contents = replaceOnce(
		contents,
		`	private setupRebalanceDebounceFunction(
		interval = RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL,
	) {
		this.rebalanceParticipationDebounced = undefined;

		this.rebalanceParticipationDebounced = debounceFixedInterval(
			() => this.rebalanceParticipation(),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			interval, // TODO make this dynamic on the number of replicators
		);
	}

	private async _replicate(`,
		`	private setupRebalanceDebounceFunction(
		interval = RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL,
	) {
		this.rebalanceParticipationDebounced = undefined;

		this.rebalanceParticipationDebounced = debounceFixedInterval(
			() => this.rebalanceParticipation(),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			interval, // TODO make this dynamic on the number of replicators
		);
	}

	private markLocalAppendActivity(timestamp = Date.now()) {
		this._lastLocalAppendAt = Math.max(this._lastLocalAppendAt ?? 0, timestamp);
	}

	private shouldDelayAdaptiveRebalance(now = Date.now()) {
		return (
			this._isAdaptiveReplicating &&
			this._lastLocalAppendAt > 0 &&
			now - this._lastLocalAppendAt < this.adaptiveRebalanceIdleMs
		);
	}

	private async _replicate(`,
		"methods",
	);

	contents = replaceOnce(
		contents,
		`	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		const appendOptions: AppendOptions<T> = { ...options };`,
		`	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const appendOptions: AppendOptions<T> = { ...options };`,
		"append-activity",
	);

	contents = replaceOnce(
		contents,
		`		if (!isLeader) {
			this.pruneDebouncedFnAddIfNotKeeping({
				key: result.entry.hash,
				value: { entry: result.entry, leaders },
			});
		}
		this.rebalanceParticipationDebounced?.call();`,
		`		if (!isLeader && !this.shouldDelayAdaptiveRebalance()) {
			this.pruneDebouncedFnAddIfNotKeeping({
				key: result.entry.hash,
				value: { entry: result.entry, leaders },
			});
		}
		if (!this._isAdaptiveReplicating) {
			this.rebalanceParticipationDebounced?.call();
		}`,
		"append-prune-rebalance",
	);

	contents = replaceOnce(
		contents,
		`		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();

		this.openTime = +new Date();`,
		`		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();
		this._lastLocalAppendAt = 0;
		const adaptiveReplicateOptions =
			options?.replicate && isAdaptiveReplicatorOption(options.replicate)
				? options.replicate
				: undefined;
		this.adaptiveRebalanceIdleMs = Math.max(
			ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS,
			(adaptiveReplicateOptions?.limits?.interval ??
				RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL) *
				ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER,
		);

		this.openTime = +new Date();`,
		"open-init",
	);

	contents = replaceOnce(
		contents,
		`	async rebalanceParticipation() {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu

		const fn = async () => {`,
		`	async rebalanceParticipation() {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu

		const isClosedStoreRace = (error: any) => {
			const message =
				typeof error?.message === "string" ? error.message : String(error);
			return (
				this.closed ||
				message.includes("Iterator is not open") ||
				message.includes("cannot read after close()") ||
				message.includes("Database is not open")
			);
		};

		const fn = async () => {`,
		"rebalance-helper",
	);

	contents = replaceOnce(
		contents,
		`			if (this._isAdaptiveReplicating) {
				const peers = this.replicationIndex;
				const usedMemory = await this.getMemoryUsage();`,
		`			if (this._isAdaptiveReplicating) {
				if (this.shouldDelayAdaptiveRebalance()) {
					this.rebalanceParticipationDebounced?.call();
					return false;
				}

				const peers = this.replicationIndex;
				const usedMemory = await this.getMemoryUsage();`,
		"rebalance-delay",
	);

	contents = replaceOnce(
		contents,
		`		const resp = await fn();
`,
		`		const resp = await fn().catch((error: any) => {
			if (isNotStartedError(error) || isClosedStoreRace(error)) {
				return false;
			}
			throw error;
		});
`,
		"rebalance-catch",
	);

	await fsp.writeFile(filePath, contents);
};

const ensureDependenciesInstalled = async (variantRoot) => {
	if (fs.existsSync(path.join(variantRoot, "node_modules"))) {
		return;
	}
	run("pnpm", ["install"], { cwd: variantRoot });
};

const buildVariant = async (variantRoot) => {
	run(
		"pnpm",
		[
			"--filter", "@peerbit/build-assets...",
			"--filter", "@peerbit/any-store-opfs...",
			"--filter", "peerbit...",
			"--filter", "@peerbit/react...",
			"--filter", "@peerbit/document...",
			"--filter", "@peerbit/shared-log...",
			"--filter", "@peerbit/stream...",
			"--filter", "@peerbit/crypto...",
			"--filter", "@peerbit/trusted-network...",
			"--filter", "@peerbit/vite...",
			"--filter", "@peerbit/test-utils...",
			"build",
		],
		{ cwd: variantRoot },
	);
};

const defaultMatrixRoot = () => {
	const preferredParent = path.resolve(repoRoot, "..", "tmp");
	const parent = fs.existsSync(preferredParent) ? preferredParent : os.tmpdir();
	return path.join(
		parent,
		`peerbit-file-share-matrix-${new Date()
			.toISOString()
			.replaceAll(":", "-")
			.replace(/\..+$/, "")}`,
	);
};

const normalizeVariants = (value) => {
	const variants = (value ?? VARIANT_ORDER.join(","))
		.split(",")
		.map((item) => item.trim())
		.filter(Boolean);
	for (const variant of variants) {
		if (!VARIANT_ORDER.includes(variant)) {
			throw new Error(
				`Unsupported variant "${variant}". Expected one of ${VARIANT_ORDER.join(", ")}`,
			);
		}
	}
	return variants;
};

const summarizeUploadMatrix = (variantSummaries) => {
	return variantSummaries.map((summary) => ({
		variant: summary.variant,
		adaptiveAvgMs: summary.comparison?.adaptiveAvgMs ?? null,
		fixed1AvgMs: summary.comparison?.fixed1AvgMs ?? null,
		adaptiveVsFixed1Pct: summary.comparison?.adaptiveVsFixed1Pct ?? null,
		adaptiveStatus:
			summary.summary.find((entry) => entry.mode === "adaptive")?.failed === 0
				? "passed"
				: "failed",
		fixed1Status:
			summary.summary.find((entry) => entry.mode === "fixed1")?.failed === 0
				? "passed"
				: "failed",
	}));
};

const summarizeSeederProbeMatrix = (variantSummaries) => {
	return variantSummaries.map((summary) => {
		const adaptive = summary.summary.find((entry) => entry.mode === "adaptive");
		return {
			variant: summary.variant,
			adaptiveStatus: adaptive?.failed === 0 ? "passed" : "failed",
			reachedTargetRuns: adaptive?.reachedTargetRuns ?? null,
			writerSeedersLastAvg: adaptive?.writerSeedersLastAvg ?? null,
			readerSeedersLastAvg: adaptive?.readerSeedersLastAvg ?? null,
			writerSeedersMaxAvg: adaptive?.writerSeedersMaxAvg ?? null,
			readerSeedersMaxAvg: adaptive?.readerSeedersMaxAvg ?? null,
			errorCount: adaptive?.errorCount ?? null,
		};
	});
};

const summarizeMatrix = (variantSummaries, scenario) =>
	scenario === "seeder-probe"
		? summarizeSeederProbeMatrix(variantSummaries)
		: summarizeUploadMatrix(variantSummaries);

const compareAdaptiveAcrossVariants = (variantSummaries, scenario) => {
	return variantSummaries
		.map((summary) => {
			const adaptive = summary.summary.find((entry) => entry.mode === "adaptive");
			if (!adaptive) {
				return null;
			}
			return scenario === "seeder-probe"
				? {
						variant: summary.variant,
						reachedTargetRuns: adaptive.reachedTargetRuns ?? null,
						writerSeedersLastAvg: adaptive.writerSeedersLastAvg ?? null,
						readerSeedersLastAvg: adaptive.readerSeedersLastAvg ?? null,
						errorCount: adaptive.errorCount ?? null,
				  }
				: adaptive.uploadDurationMsAvg != null
					? {
							variant: summary.variant,
							adaptiveAvgMs: adaptive.uploadDurationMsAvg,
					  }
					: null;
		})
		.filter(Boolean);
};

const prepareVariant = async ({ variant, variantRoot }) => {
	if (variant === "current") {
		return { peerbitRoot: repoRoot, materialized: false };
	}
	if (variant === "head") {
		await createHeadClone(variantRoot);
		await ensureDependenciesInstalled(variantRoot);
		return { peerbitRoot: variantRoot, materialized: true };
	}
	if (variant === "downstream") {
		await createHeadClone(variantRoot);
		await applyDownstreamAdaptivePatch(variantRoot);
		await ensureDependenciesInstalled(variantRoot);
		return { peerbitRoot: variantRoot, materialized: true };
	}
	throw new Error(`Unhandled variant ${variant}`);
};

const runVariantBenchmark = async ({
	variant,
	peerbitRoot,
	examplesSource,
	examplesTemplate,
	examplesRoot,
	resultsDir,
	summaryFile,
	fileMb,
	runs,
	mode,
	network,
	freshExamples,
	freshExamplesEachRun,
	installExamples,
	uploadTimeoutMs,
	postUploadMonitorMs,
	pollMs,
	scenario,
	integrationMode,
	localPackages,
	readyTimeoutMs,
	sampleMs,
	sampleCount,
	targetSeeders,
	baseUrl,
	protocol,
	viteMode,
	viteConfig,
}) => {
	buildVariant(peerbitRoot);
	run("node", [
		RUNNER_PATH,
		"--scenario",
		scenario,
		"--integration-mode",
		integrationMode,
		"--peerbit-root",
		peerbitRoot,
		"--examples-root",
		examplesRoot,
		"--source",
		examplesSource,
		...(localPackages ? ["--local-packages", localPackages] : []),
		...(examplesTemplate ? ["--template", examplesTemplate] : []),
		"--results-dir",
		resultsDir,
		"--summary-file",
		summaryFile,
		"--file-mb",
		String(fileMb),
		"--runs",
		String(runs),
		"--mode",
		mode,
		"--network",
		network,
		...(freshExamples ? ["--fresh"] : []),
		...(freshExamplesEachRun ? ["--fresh-each-run"] : []),
		...(installExamples ? ["--install"] : []),
		...(uploadTimeoutMs
			? ["--upload-timeout-ms", String(uploadTimeoutMs)]
			: []),
		...(postUploadMonitorMs
			? ["--post-upload-monitor-ms", String(postUploadMonitorMs)]
			: []),
		...(pollMs ? ["--poll-ms", String(pollMs)] : []),
		...(readyTimeoutMs
			? ["--ready-timeout-ms", String(readyTimeoutMs)]
			: []),
		...(sampleMs ? ["--sample-ms", String(sampleMs)] : []),
		...(sampleCount ? ["--sample-count", String(sampleCount)] : []),
		...(targetSeeders ? ["--target-seeders", String(targetSeeders)] : []),
		...(baseUrl ? ["--base-url", baseUrl] : []),
		...(protocol ? ["--protocol", protocol] : []),
		...(viteMode ? ["--vite-mode", viteMode] : []),
		...(viteConfig ? ["--vite-config", viteConfig] : []),
	], {
		cwd: repoRoot,
		env: process.env,
	});
	const summary = JSON.parse(await fsp.readFile(summaryFile, "utf8"));
	return {
		variant,
		...summary,
	};
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const matrixRoot = path.resolve(args["matrix-root"] ?? defaultMatrixRoot());
	const variants = normalizeVariants(args.variants);
	const fileMb = Number(args["file-mb"] ?? "5");
	const runs = Number(args.runs ?? "1");
	const mode = args.mode ?? "both";
	const scenario = args.scenario ?? "upload";
	const integrationMode = args["integration-mode"] ?? "overlay";
	const localPackages = args["local-packages"] ?? "@peerbit/shared-log";
	const network = args.network ?? "local";
	const uploadTimeoutMs = args["upload-timeout-ms"];
	const postUploadMonitorMs = args["post-upload-monitor-ms"];
	const pollMs = args["poll-ms"];
	const readyTimeoutMs = args["ready-timeout-ms"];
	const sampleMs = args["sample-ms"];
	const sampleCount = args["sample-count"];
	const targetSeeders = args["target-seeders"];
	const baseUrl = args["base-url"];
	const protocol = args.protocol;
	const viteMode = args["vite-mode"];
	const viteConfig = args["vite-config"];
	const examplesSource = args.source ?? defaultExamplesSource();
	const isolatedExamples = Boolean(args["isolated-examples"]);

	if (!["local", "remote"].includes(network)) {
		throw new Error(`Unsupported --network "${network}". Expected "local" or "remote".`);
	}

	const preparedExamplesRoot = fs.existsSync(path.join(defaultExamplesDest(), "node_modules"))
		? defaultExamplesDest()
		: undefined;
	const sharedExamplesRoot = isolatedExamples
		? undefined
		: args["examples-root"] ?? preparedExamplesRoot;
	const examplesTemplate = isolatedExamples
		? args.template ?? args["examples-root"] ?? preparedExamplesRoot
		: sharedExamplesRoot
			? undefined
			: args.template ?? preparedExamplesRoot;

	if (isolatedExamples && !examplesTemplate) {
		throw new Error(
			"Isolated examples mode requires a prepared template checkout. Pass --template or --examples-root.",
		);
	}

	await fsp.mkdir(matrixRoot, { recursive: true });
	const variantSummaries = [];

	for (const variant of variants) {
		console.log(`\n=== Variant: ${variant} ===`);
		const variantRoot = path.join(matrixRoot, "variants", variant);
		const { peerbitRoot } = await prepareVariant({ variant, variantRoot });
		const examplesRoot = sharedExamplesRoot
			? path.resolve(sharedExamplesRoot)
			: path.join(matrixRoot, "examples", variant);
		const resultsDir = path.join(matrixRoot, "results", variant, "runs");
		const summaryFile = path.join(matrixRoot, "results", variant, "summary.json");
			const summary = await runVariantBenchmark({
				variant,
				peerbitRoot,
				examplesSource,
				examplesTemplate,
				examplesRoot,
				resultsDir,
				summaryFile,
				fileMb,
				runs,
				mode,
				network,
				scenario,
				integrationMode,
				localPackages,
				freshExamples: !sharedExamplesRoot,
				freshExamplesEachRun: isolatedExamples,
				installExamples: isolatedExamples,
				uploadTimeoutMs,
				postUploadMonitorMs,
				pollMs,
				readyTimeoutMs,
				sampleMs,
				sampleCount,
				targetSeeders,
				baseUrl,
				protocol,
				viteMode,
				viteConfig,
			});
		variantSummaries.push(summary);
	}

		const matrixSummary = {
			matrixRoot,
			variants,
			network,
			fileMb,
			runs,
			mode,
			scenario,
			integrationMode,
			localPackages,
			examplesSource,
			isolatedExamples,
			sharedExamplesRoot,
			examplesTemplate,
			variantSummaries,
			summary: summarizeMatrix(variantSummaries, scenario),
			adaptiveComparison: compareAdaptiveAcrossVariants(variantSummaries, scenario),
		};
	const matrixSummaryFile = path.join(matrixRoot, "results", "matrix-summary.json");
	await fsp.mkdir(path.dirname(matrixSummaryFile), { recursive: true });
	await fsp.writeFile(
		matrixSummaryFile,
		`${JSON.stringify(matrixSummary, null, 2)}\n`,
	);

	console.log("\nVariant summary");
	console.table(matrixSummary.summary);
	console.log("\nAdaptive across variants");
	console.table(matrixSummary.adaptiveComparison);
	console.log(`\nMatrix summary: ${matrixSummaryFile}`);
};

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
