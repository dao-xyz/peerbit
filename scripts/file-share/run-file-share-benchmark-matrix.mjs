import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
	assertBenchmarkFileSize,
	assertCoreBenchmarkUsesLocalApp,
	resolveBenchmarkPreviewOptions,
} from "./benchmark-invocation.mjs";
import {
	getExamplesProvenance,
	getPeerbitProvenance,
	resolveGitCommitAt,
} from "./benchmark-provenance.mjs";
import {
	compareUploadPerformanceModes,
	summarizeUploadPerformance,
} from "./benchmark-summary.mjs";
import {
	collectLocalPeerbitPackages,
	defaultExamplesSource,
	defaultFileShareLocalPackages,
	parseArgs,
	repoRoot,
	run,
} from "./common.mjs";
import {
	assertMatrixIntegrationMode,
	assertMatrixPackageRequest,
	assertUniqueResolvedVariantCommits,
	createCounterbalancedInvocationPlan,
	createExclusiveMatrixSession,
	createVariantMaterializationPlan,
	normalizeVariantSpecs,
} from "./matrix-variants.mjs";

const RUNNER_PATH = path.join(
	repoRoot,
	"scripts",
	"file-share",
	"run-file-share-benchmark.mjs",
);

const runGit = (args, { cwd = repoRoot, capture = false } = {}) => {
	console.log(`$ git ${args.join(" ")}`);
	const result = spawnSync("git", args, {
		cwd,
		env: process.env,
		encoding: "utf8",
		stdio: capture ? ["ignore", "pipe", "pipe"] : "inherit",
	});
	if (result.error) {
		throw new Error(`Could not start git: ${result.error.message}`, {
			cause: result.error,
		});
	}
	if (result.status !== 0) {
		const detail = capture ? `: ${result.stderr.trim()}` : "";
		throw new Error(
			`git ${args.join(" ")} failed with exit code ${String(result.status)}${detail}`,
		);
	}
	return capture ? result.stdout.trim() : undefined;
};

const createRefClone = async ({ dest, commit }) => {
	await fsp.rm(dest, { recursive: true, force: true });
	runGit(["clone", "--quiet", "--no-checkout", repoRoot, dest]);
	runGit(["checkout", "--quiet", "--detach", commit], { cwd: dest });
};

const preparePinnedExamplesTemplate = async ({ source, ref, dest }) => {
	await fsp.rm(dest, { recursive: true, force: true });
	runGit(["clone", "--quiet", "--no-checkout", source, dest]);
	const resolvedCommit = resolveGitCommitAt(dest, ref);
	runGit(["checkout", "--quiet", "--detach", resolvedCommit], { cwd: dest });
	const provenance = await getExamplesProvenance({
		root: dest,
		requestedRef: ref,
	});
	if (provenance.dirty) {
		throw new Error("Pinned examples template must be clean");
	}
	run("pnpm", ["install", "--frozen-lockfile"], { cwd: dest });
	const postInstallStatus = runGit(["status", "--porcelain=v1"], {
		cwd: dest,
		capture: true,
	});
	if (postInstallStatus) {
		throw new Error(
			`Installing the pinned examples template changed tracked files:\n${postInstallStatus}`,
		);
	}
	return { root: dest, provenance };
};

const writeJsonAtomic = async (filePath, value) => {
	await fsp.mkdir(path.dirname(filePath), { recursive: true });
	const temporaryPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
	await fsp.writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`);
	await fsp.rename(temporaryPath, filePath);
};

const ensureDependenciesInstalled = async (variantRoot) => {
	run("pnpm", ["install", "--frozen-lockfile"], { cwd: variantRoot });
};

const defaultMatrixBase = () => {
	const preferredParent = path.resolve(repoRoot, "..", "tmp");
	const parent = fs.existsSync(preferredParent) ? preferredParent : os.tmpdir();
	return parent;
};

const resolveVariantSpecs = async (variantSpecs) => {
	const resolved = [];
	for (const variantSpec of variantSpecs) {
		const resolvedCommit =
			variantSpec.kind === "worktree"
				? (
						await getPeerbitProvenance({
							root: repoRoot,
							requestedRef: "worktree",
							requireClean: true,
						})
					).resolvedCommit
				: resolveGitCommitAt(repoRoot, variantSpec.ref);
		resolved.push({ ...variantSpec, resolvedCommit });
	}
	return assertUniqueResolvedVariantCommits(resolved);
};

const summarizeUploadMatrix = (variantSummaries) => {
	return variantSummaries.map((summary) => {
		const adaptive = summary.summary.find((entry) => entry.mode === "adaptive");
		const fixed1 = summary.summary.find((entry) => entry.mode === "fixed1");
		return {
			variant: summary.variant,
			adaptiveAvgMs: summary.comparison?.adaptiveAvgMs ?? null,
			fixed1AvgMs: summary.comparison?.fixed1AvgMs ?? null,
			adaptiveVsFixed1Pct: summary.comparison?.adaptiveVsFixed1Pct ?? null,
			adaptiveWriterReadyMsAvg: adaptive?.timeToWriterReadyMsAvg ?? null,
			fixed1WriterReadyMsAvg: fixed1?.timeToWriterReadyMsAvg ?? null,
			adaptiveWriterReadyMsMedian: adaptive?.timeToWriterReadyMsMedian ?? null,
			fixed1WriterReadyMsMedian: fixed1?.timeToWriterReadyMsMedian ?? null,
			adaptiveReaderReadyMsAvg: adaptive?.timeToReaderReadyMsAvg ?? null,
			fixed1ReaderReadyMsAvg: fixed1?.timeToReaderReadyMsAvg ?? null,
			adaptiveReaderReadyMsMedian: adaptive?.timeToReaderReadyMsMedian ?? null,
			fixed1ReaderReadyMsMedian: fixed1?.timeToReaderReadyMsMedian ?? null,
			adaptiveStatus: adaptive?.failed === 0 ? "passed" : "failed",
			fixed1Status: fixed1?.failed === 0 ? "passed" : "failed",
		};
	});
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
			const adaptive = summary.summary.find(
				(entry) => entry.mode === "adaptive",
			);
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
							adaptiveWriterReadyMsAvg: adaptive.timeToWriterReadyMsAvg,
							adaptiveWriterReadyMsMedian: adaptive.timeToWriterReadyMsMedian,
							adaptiveReaderReadyMsAvg: adaptive.timeToReaderReadyMsAvg,
							adaptiveReaderReadyMsMedian: adaptive.timeToReaderReadyMsMedian,
						}
					: null;
		})
		.filter(Boolean);
};

const average = (values) =>
	values.length === 0
		? null
		: Number(
				(
					values.reduce((total, value) => total + value, 0) / values.length
				).toFixed(1),
			);

const summarizeInvocationResults = (results, scenario) => {
	const grouped = new Map();
	for (const result of results) {
		const items = grouped.get(result.mode) ?? [];
		items.push(result);
		grouped.set(result.mode, items);
	}
	return [...grouped.entries()].map(([mode, items]) => {
		const base = {
			mode,
			runs: items.length,
			passed: items.filter((item) => item.status === "passed").length,
			failed: items.filter((item) => item.status !== "passed").length,
			errorCount: items.reduce(
				(total, item) => total + (Number(item.errorCount) || 0),
				0,
			),
		};
		if (scenario === "seeder-probe") {
			const lastNumbers = (key) =>
				items
					.map((item) => item.samples?.at(-1)?.[key])
					.filter((value) => typeof value === "number");
			const maxima = (key) =>
				items
					.map((item) => {
						const values = (item.samples ?? [])
							.map((sample) => sample[key])
							.filter((value) => typeof value === "number");
						return values.length > 0 ? Math.max(...values) : null;
					})
					.filter((value) => typeof value === "number");
			return {
				...base,
				reachedTargetRuns: items.filter((item) => item.reachedTarget).length,
				writerSeedersLastAvg: average(lastNumbers("writerSeeders")),
				readerSeedersLastAvg: average(lastNumbers("readerSeeders")),
				writerSeedersMaxAvg: average(maxima("writerSeeders")),
				readerSeedersMaxAvg: average(maxima("readerSeeders")),
			};
		}
		const numbers = (getter) =>
			items
				.filter((item) => item.status === "passed")
				.map(getter)
				.filter((value) => typeof value === "number");
		return {
			...base,
			...summarizeUploadPerformance(items),
			uploadSettledMsAvg: average(
				numbers((item) => item.phaseDurationsMs?.timeToUploadSettled),
			),
			writerListingLagMsAvg: average(
				numbers((item) => item.phaseDurationsMs?.writerListingLag),
			),
			readerListingLagMsAvg: average(
				numbers((item) => item.phaseDurationsMs?.readerListingLag),
			),
			postUploadMonitorDurationMsAvg: average(
				numbers((item) => item.postUploadMonitorDurationMs),
			),
			seederDrops: items.filter((item) => item.droppedSeeders).length,
		};
	});
};

const prepareVariant = async ({ variantSpec, variantRoot }) => {
	const materialization = createVariantMaterializationPlan({
		variantSpec,
		variantRoot,
	});
	if (materialization.sourceWorktreeMustBeClean) {
		await getPeerbitProvenance({
			root: repoRoot,
			requestedRef: "worktree",
			requireClean: true,
			expectedResolvedCommit: variantSpec.resolvedCommit,
		});
	}
	await createRefClone({
		dest: materialization.peerbitRoot,
		commit: materialization.cloneCommit,
	});
	await ensureDependenciesInstalled(materialization.peerbitRoot);
	await getPeerbitProvenance({
		root: materialization.peerbitRoot,
		requestedRef: materialization.requestedRef,
		requireClean: true,
		expectedResolvedCommit: materialization.cloneCommit,
	});
	return {
		peerbitRoot: materialization.peerbitRoot,
		resolvedCommit: materialization.cloneCommit,
	};
};

const runVariantBenchmark = async ({
	variant,
	peerbitRoot,
	examplesSource,
	examplesTemplate,
	pinnedExamplesProvenance,
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
	downloadTimeoutMs,
	postUploadMonitorMs,
	pollMs,
	minReadySeeders,
	scenario,
	integrationMode,
	localPackages,
	readyTimeoutMs,
	sampleMs,
	sampleCount,
	targetSeeders,
	protocol,
	fixtureSeed,
	enableVisibilityProbe,
	verbose,
	variantRef,
	resolvedCommit,
	examplesRef,
}) => {
	await fsp.rm(summaryFile, { force: true });
	run(
		"node",
		[
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
			"--examples-ref",
			examplesRef,
			"--peerbit-ref-label",
			variantRef,
			"--expected-peerbit-commit",
			resolvedCommit,
			"--require-clean-peerbit",
			"--expected-examples-commit",
			pinnedExamplesProvenance.resolvedCommit,
			"--expected-examples-lockfile-sha256",
			pinnedExamplesProvenance.lockfileSha256,
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
			...(uploadTimeoutMs != null
				? ["--upload-timeout-ms", String(uploadTimeoutMs)]
				: []),
			...(downloadTimeoutMs != null
				? ["--download-timeout-ms", String(downloadTimeoutMs)]
				: []),
			...(postUploadMonitorMs != null
				? ["--post-upload-monitor-ms", String(postUploadMonitorMs)]
				: []),
			...(pollMs != null ? ["--poll-ms", String(pollMs)] : []),
			...(minReadySeeders != null
				? ["--min-ready-seeders", String(minReadySeeders)]
				: []),
			...(readyTimeoutMs != null
				? ["--ready-timeout-ms", String(readyTimeoutMs)]
				: []),
			...(sampleMs != null ? ["--sample-ms", String(sampleMs)] : []),
			...(sampleCount != null ? ["--sample-count", String(sampleCount)] : []),
			...(targetSeeders != null
				? ["--target-seeders", String(targetSeeders)]
				: []),
			...(protocol ? ["--protocol", protocol] : []),
			...(fixtureSeed ? ["--fixture-seed", fixtureSeed] : []),
			...(enableVisibilityProbe ? ["--enable-visibility-probe"] : []),
			...(verbose ? ["--verbose"] : []),
		],
		{
			cwd: repoRoot,
			env: process.env,
		},
	);
	const summary = JSON.parse(await fsp.readFile(summaryFile, "utf8"));
	return {
		variant,
		variantRef,
		resolvedCommit,
		...summary,
	};
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	assertCoreBenchmarkUsesLocalApp(args["base-url"]);
	const previewOptions = resolveBenchmarkPreviewOptions({
		protocol: args.protocol,
		viteMode: args["vite-mode"],
		viteConfig: args["vite-config"],
	});
	const requestedMatrixBase = path.resolve(
		args["matrix-root"] ?? defaultMatrixBase(),
	);
	const requestedVariantSpecs = normalizeVariantSpecs(args.variants);
	const fileMb = Number(args["file-mb"] ?? "5");
	const runs = Number(args.runs ?? "1");
	const mode = args.mode ?? "both";
	const modes = mode === "both" ? ["adaptive", "fixed1"] : [mode];
	const scenario = args.scenario ?? "upload";
	const integrationMode = args["integration-mode"] ?? "link";
	const localPackages =
		args["local-packages"] ?? defaultFileShareLocalPackages.join(",");
	const requestedLocalPackageNames =
		localPackages === "all"
			? undefined
			: localPackages.split(",").map((value) => value.trim());
	const network = args.network ?? "local";
	const uploadTimeoutMs = args["upload-timeout-ms"];
	const downloadTimeoutMs = args["download-timeout-ms"];
	const postUploadMonitorMs = args["post-upload-monitor-ms"];
	const pollMs = args["poll-ms"];
	const minReadySeeders = args["min-ready-seeders"];
	const readyTimeoutMs = args["ready-timeout-ms"];
	const sampleMs = args["sample-ms"];
	const sampleCount = args["sample-count"];
	const targetSeeders = args["target-seeders"];
	const { protocol } = previewOptions;
	const fixtureSeed = args["fixture-seed"];
	const enableVisibilityProbe = Boolean(args["enable-visibility-probe"]);
	const verbose = Boolean(args.verbose);
	const examplesSource =
		args.source ??
		args.template ??
		args["examples-root"] ??
		defaultExamplesSource();
	const examplesRef = args["examples-ref"] ?? "HEAD";

	if (!["local", "remote"].includes(network)) {
		throw new Error(
			`Unsupported --network "${network}". Expected "local" or "remote".`,
		);
	}
	if (!Number.isSafeInteger(runs) || runs <= 0) {
		throw new Error("--runs must be a positive safe integer");
	}
	if (
		modes.some((entry) => !["adaptive", "fixed1", "observer"].includes(entry))
	) {
		throw new Error(
			`Unsupported --mode "${mode}". Expected adaptive, fixed1, observer, or both.`,
		);
	}
	if (!["upload", "seeder-probe"].includes(scenario)) {
		throw new Error(`Unsupported --scenario "${scenario}"`);
	}
	assertMatrixIntegrationMode(integrationMode);
	assertBenchmarkFileSize({ scenario, fileMb });
	assertMatrixPackageRequest({
		requestedNames: requestedLocalPackageNames,
		requiredNames: defaultFileShareLocalPackages,
	});
	const variantSpecs = await resolveVariantSpecs(requestedVariantSpecs);
	const matrixSession = await createExclusiveMatrixSession({
		baseDir: requestedMatrixBase,
		repoRoot,
	});
	const { matrixRoot } = matrixSession;
	const pinnedExamples = await preparePinnedExamplesTemplate({
		source: examplesSource,
		ref: examplesRef,
		dest: path.join(matrixRoot, "examples-template"),
	});
	const examplesTemplate = pinnedExamples.root;
	const preparedVariants = new Map();
	for (const variantSpec of variantSpecs) {
		const variant = variantSpec.name;
		console.log(
			`\n=== Variant: ${variant}${variantSpec.ref ? ` (${variantSpec.ref})` : " (worktree)"} ===`,
		);
		const variantRoot = path.join(matrixRoot, "variants", variant);
		const { peerbitRoot, resolvedCommit } = await prepareVariant({
			variantSpec,
			variantRoot,
		});
		const effectiveLocalPackageNames = [
			...(
				await collectLocalPeerbitPackages(peerbitRoot, {
					names: requestedLocalPackageNames,
				})
			).keys(),
		];
		const missingRequiredPackages = defaultFileShareLocalPackages.filter(
			(name) => !effectiveLocalPackageNames.includes(name),
		);
		if (
			effectiveLocalPackageNames.length === 0 ||
			missingRequiredPackages.length > 0
		) {
			throw new Error(
				`Variant ${variant} cannot run the file-share matrix; missing effective packages: ${missingRequiredPackages.join(", ") || "all packages"}`,
			);
		}
		preparedVariants.set(variant, {
			variant,
			variantRef:
				variantSpec.kind === "worktree" ? "worktree" : variantSpec.ref,
			resolvedCommit,
			peerbitRoot,
			effectiveLocalPackageNames,
		});
	}

	const executionOrder = createCounterbalancedInvocationPlan({
		variants: variantSpecs.map((spec) => spec.name),
		modes,
		runs,
	});
	const resultsByVariant = new Map(variantSpecs.map((spec) => [spec.name, []]));
	const invocationRecords = [];
	let benchmarkExamplesProvenance;
	let benchmarkHarnessProvenance;

	for (const plan of executionOrder) {
		const prepared = preparedVariants.get(plan.variant);
		if (!prepared) {
			throw new Error(`Missing prepared variant ${plan.variant}`);
		}
		console.log(
			`\n=== Invocation ${plan.sequence}/${executionOrder.length}: ${plan.variant} ${plan.mode}, repetition ${plan.run}/${runs} ===`,
		);
		const invocationSlug = `${String(plan.sequence).padStart(4, "0")}-${plan.variant}-${plan.mode}`;
		const examplesRoot = path.join(
			matrixRoot,
			"examples",
			"invocations",
			invocationSlug,
		);
		const resultsDir = path.join(
			matrixRoot,
			"results",
			"invocations",
			invocationSlug,
		);
		const summaryFile = path.join(resultsDir, "summary.json");
		const invocationSummary = await runVariantBenchmark({
			...prepared,
			examplesSource,
			examplesRef,
			examplesTemplate,
			pinnedExamplesProvenance: pinnedExamples.provenance,
			examplesRoot,
			resultsDir,
			summaryFile,
			fileMb,
			runs: 1,
			mode: plan.mode,
			network,
			scenario,
			integrationMode,
			localPackages,
			freshExamples: true,
			freshExamplesEachRun: false,
			installExamples: false,
			uploadTimeoutMs,
			downloadTimeoutMs,
			postUploadMonitorMs,
			pollMs,
			minReadySeeders,
			readyTimeoutMs,
			sampleMs,
			sampleCount,
			targetSeeders,
			protocol,
			fixtureSeed,
			enableVisibilityProbe,
			verbose,
		});
		if (
			invocationSummary.peerbitProvenance?.resolvedCommit !==
				prepared.resolvedCommit ||
			invocationSummary.peerbitProvenance?.dirty !== false
		) {
			throw new Error(
				`Variant ${plan.variant} did not report its exact clean resolved HEAD`,
			);
		}
		if (
			invocationSummary.examplesProvenance?.resolvedCommit !==
				pinnedExamples.provenance.resolvedCommit ||
			invocationSummary.examplesProvenance?.lockfileSha256 !==
				pinnedExamples.provenance.lockfileSha256
		) {
			throw new Error(
				`Variant ${plan.variant} did not use the pinned examples commit and lockfile`,
			);
		}
		const [rawResult] = invocationSummary.results ?? [];
		if (!rawResult || invocationSummary.results.length !== 1) {
			throw new Error("Matrix invocation did not produce exactly one result");
		}
		if (rawResult.invocation?.baseUrl !== null) {
			throw new Error(
				"Matrix invocation used an effective remote PW_BASE_URL instead of its local app",
			);
		}
		if (
			JSON.stringify(rawResult.invocation?.localPackages) !==
			JSON.stringify(prepared.effectiveLocalPackageNames)
		) {
			throw new Error(
				`Variant ${plan.variant} did not bind its exact effective local package set`,
			);
		}
		if (
			rawResult.invocation?.serverMode !== "production-preview" ||
			rawResult.invocation?.serverHost !== "127.0.0.1"
		) {
			throw new Error(
				"Matrix invocation did not use the required local production preview server",
			);
		}
		benchmarkExamplesProvenance ??= invocationSummary.examplesProvenance;
		benchmarkHarnessProvenance ??= invocationSummary.harnessProvenance;
		if (
			JSON.stringify(benchmarkExamplesProvenance) !==
				JSON.stringify(invocationSummary.examplesProvenance) ||
			JSON.stringify(benchmarkHarnessProvenance) !==
				JSON.stringify(invocationSummary.harnessProvenance)
		) {
			throw new Error(
				"Matrix invocations changed harness or examples provenance",
			);
		}
		const result = {
			...rawResult,
			variant: plan.variant,
			run: plan.run,
			matrixSequence: plan.sequence,
		};
		resultsByVariant.get(plan.variant).push(result);
		invocationRecords.push({
			...plan,
			runNonce: result.runNonce,
			resultFile: result.resultFile,
			peerbitResolvedCommit: prepared.resolvedCommit,
			effectiveLocalPackageNames: prepared.effectiveLocalPackageNames,
		});
	}

	const variantSummaries = variantSpecs.map((variantSpec) => {
		const prepared = preparedVariants.get(variantSpec.name);
		const results = resultsByVariant
			.get(variantSpec.name)
			.toSorted((left, right) => left.matrixSequence - right.matrixSequence);
		return {
			variant: variantSpec.name,
			variantRef: prepared.variantRef,
			resolvedCommit: prepared.resolvedCommit,
			effectiveLocalPackageNames: prepared.effectiveLocalPackageNames,
			harnessProvenance: benchmarkHarnessProvenance,
			peerbitProvenance: results[0]?.provenance?.peerbit,
			examplesProvenance: benchmarkExamplesProvenance,
			results,
			summary: summarizeInvocationResults(results, scenario),
			comparison:
				scenario === "upload" ? compareUploadPerformanceModes(results) : null,
		};
	});

	const matrixSummary = {
		schema: {
			id: "peerbit-file-share-matrix-summary",
			version: 2,
		},
		matrixBase: matrixSession.matrixBase,
		matrixRoot,
		matrixSessionNonce: matrixSession.nonce,
		matrixSessionMarker: matrixSession.markerFile,
		variants: variantSpecs,
		network,
		fileMb,
		runs,
		mode,
		modes,
		scenario,
		integrationMode,
		localPackages,
		requestedLocalPackageNames:
			requestedLocalPackageNames ?? "all-workspace-peerbit-packages",
		examplesSource,
		examplesRef,
		examplesTemplate,
		pinnedExamplesProvenance: pinnedExamples.provenance,
		benchmarkExamplesProvenance,
		benchmarkHarnessProvenance,
		isolatedExamples: true,
		executionOrder: invocationRecords,
		variantSummaries,
		summary: summarizeMatrix(variantSummaries, scenario),
		adaptiveComparison: compareAdaptiveAcrossVariants(
			variantSummaries,
			scenario,
		),
	};
	const matrixSummaryFile = path.join(
		matrixRoot,
		"results",
		"matrix-summary.json",
	);
	await writeJsonAtomic(matrixSummaryFile, matrixSummary);

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
