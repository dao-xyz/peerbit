import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import {
	MAX_READER_LOCAL_CHUNK_OVERSHOOT,
	READER_TERMINAL_TOPOLOGIES,
	assertBenchmarkFileSize,
	assertCoreBenchmarkUsesLocalApp,
	createBenchmarkInvocation,
	resolveBenchmarkBrowserStorageMode,
	resolveBenchmarkDownloadSink,
	resolveBenchmarkPreviewOptions,
} from "./benchmark-invocation.mjs";
import {
	BENCHMARK_RESULT_SCHEMA,
	BENCHMARK_SUMMARY_SCHEMA,
	ERROR_COLLECTION_DEFINITION,
	KNOWN_PEERBIT_FAILURE_SIGNATURES,
	MATRIX_SUMMARY_SCHEMA,
	REQUEST_FAILURE_COLLECTION_DEFINITION,
	SEEDER_DROP_POLICY,
	classifySubprocessSummary,
	countBenchmarkOutcomes,
	extractCollectedErrorEvidence,
	inspectSingleInvocationSummary,
	projectUploadIntegrityEvidence,
	readJsonEvidence,
	serializeError,
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
} from "./benchmark-summary.mjs";
import {
	validateBenchmarkResult,
	validateBenchmarkResultEnvelope,
} from "./benchmark-validity.mjs";
import {
	collectLocalPeerbitPackages,
	defaultExamplesSource,
	defaultFileShareLocalPackages,
	parseArgs,
	preflightBetterSqlite3Runtime,
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
const DEFAULT_FIXTURE_SEED = "peerbit-file-share-benchmark-v1";

const optionalNumber = (value) => (value == null ? undefined : Number(value));
const optionalBoolean = (value, label) => {
	if (value == null) {
		return undefined;
	}
	if (value === "true") {
		return true;
	}
	if (value === "false") {
		return false;
	}
	throw new Error(`${label} must be exactly "true" or "false"`);
};

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

const hasRuntimeConfigurationCohortKey = (entry) =>
	entry != null &&
	typeof entry === "object" &&
	Object.hasOwn(entry, "runtimeConfigurationCohortKey");

const runtimeConfigurationCohortKey = (entry) =>
	typeof entry?.runtimeConfigurationCohortKey === "string"
		? entry.runtimeConfigurationCohortKey
		: null;

const createUploadMatrixRow = ({
	summary,
	adaptive,
	fixed1,
	runtimeAware,
	runtimeKey,
}) => {
	const comparison = (() => {
		if (!summary.comparison || !adaptive || !fixed1) {
			return null;
		}
		if (!runtimeAware) {
			return summary.comparison;
		}
		return runtimeKey !== null &&
			summary.comparison.runtimeConfigurationCohortKey === runtimeKey
			? summary.comparison
			: null;
	})();
	return {
		variant: summary.variant,
		...(runtimeAware ? { runtimeConfigurationCohortKey: runtimeKey } : {}),
		readerLocalChunkTarget: fixed1?.readerLocalChunkTarget ?? null,
		readerLocalChunkMaxOvershoot: fixed1?.readerLocalChunkMaxOvershoot ?? null,
		readerLocalChunkBlockCount: fixed1?.readerLocalChunkBlockCount ?? null,
		readerLocalChunkIndexRowCount:
			fixed1?.readerLocalChunkIndexRowCount ?? null,
		readerLocalityCohortKey: fixed1?.readerLocalityCohortKey ?? null,
		cohortRuns: fixed1?.runs ?? null,
		cohortPassed: fixed1?.passed ?? null,
		cohortFailed: fixed1?.failed ?? null,
		downloadSink:
			comparison?.downloadSink ??
			adaptive?.downloadSink ??
			fixed1?.downloadSink ??
			null,
		primaryDownloadMetric:
			comparison?.primaryDownloadMetric ??
			adaptive?.primaryDownloadMetric ??
			fixed1?.primaryDownloadMetric ??
			null,
		primaryDownloadAuthoritative:
			comparison?.primaryDownloadAuthoritative ??
			adaptive?.primaryDownloadAuthoritative ??
			fixed1?.primaryDownloadAuthoritative ??
			null,
		adaptiveAvgMs:
			comparison?.adaptiveAvgMs ?? adaptive?.uploadDurationMsAvg ?? null,
		fixed1AvgMs: comparison?.fixed1AvgMs ?? fixed1?.uploadDurationMsAvg ?? null,
		adaptiveVsFixed1Pct: comparison?.adaptiveVsFixed1Pct ?? null,
		adaptiveWriterReadyMsAvg: adaptive?.timeToWriterReadyMsAvg ?? null,
		fixed1WriterReadyMsAvg: fixed1?.timeToWriterReadyMsAvg ?? null,
		adaptiveWriterReadyMsMedian: adaptive?.timeToWriterReadyMsMedian ?? null,
		fixed1WriterReadyMsMedian: fixed1?.timeToWriterReadyMsMedian ?? null,
		adaptiveReaderReadyMsAvg: adaptive?.timeToReaderReadyMsAvg ?? null,
		fixed1ReaderReadyMsAvg: fixed1?.timeToReaderReadyMsAvg ?? null,
		adaptiveReaderReadyMsMedian: adaptive?.timeToReaderReadyMsMedian ?? null,
		fixed1ReaderReadyMsMedian: fixed1?.timeToReaderReadyMsMedian ?? null,
		adaptiveLibraryStreamWallMsAvg: adaptive?.libraryStreamWallMsAvg ?? null,
		fixed1LibraryStreamWallMsAvg: fixed1?.libraryStreamWallMsAvg ?? null,
		adaptiveLibraryStreamWallMsMedian:
			adaptive?.libraryStreamWallMsMedian ?? null,
		fixed1LibraryStreamWallMsMedian: fixed1?.libraryStreamWallMsMedian ?? null,
		libraryStreamWallDeltaMs: comparison?.libraryStreamWallDeltaMs ?? null,
		adaptiveVsFixed1LibraryStreamWallPct:
			comparison?.adaptiveVsFixed1LibraryStreamWallPct ?? null,
		adaptiveStatus:
			adaptive == null ? null : adaptive.failed === 0 ? "passed" : "failed",
		fixed1Status:
			fixed1 == null ? null : fixed1.failed === 0 ? "passed" : "failed",
		adaptiveErrorCount: adaptive?.errorCount ?? null,
		fixed1ErrorCount: fixed1?.errorCount ?? null,
		adaptiveIncompleteErrorCollections:
			adaptive?.incompleteErrorCollections ?? null,
		fixed1IncompleteErrorCollections:
			fixed1?.incompleteErrorCollections ?? null,
		adaptiveRequestFailureCount: adaptive?.requestFailureCount ?? null,
		fixed1RequestFailureCount: fixed1?.requestFailureCount ?? null,
	};
};

export const summarizeUploadMatrix = (variantSummaries) => {
	return variantSummaries.flatMap((summary) => {
		const adaptiveEntries = summary.summary.filter(
			(entry) => entry.mode === "adaptive",
		);
		const fixed1Entries = summary.summary.filter(
			(entry) => entry.mode === "fixed1",
		);
		const controlledFixed1Entries = fixed1Entries.filter(
			(entry) => entry.readerLocalChunkTarget != null,
		);
		const fixed1Cohorts =
			controlledFixed1Entries.length > 0
				? controlledFixed1Entries
				: fixed1Entries;
		const runtimeAware = [...adaptiveEntries, ...fixed1Cohorts].some(
			hasRuntimeConfigurationCohortKey,
		);
		if (!runtimeAware) {
			const legacyFixed1Cohorts =
				fixed1Cohorts.length > 0 ? fixed1Cohorts : [undefined];
			return legacyFixed1Cohorts.map((fixed1) =>
				createUploadMatrixRow({
					summary,
					adaptive: adaptiveEntries[0],
					fixed1,
					runtimeAware: false,
					runtimeKey: null,
				}),
			);
		}

		const rows = [];
		const matchedAdaptiveEntries = new Set();
		for (const fixed1 of fixed1Cohorts) {
			const runtimeKey = runtimeConfigurationCohortKey(fixed1);
			const matches =
				runtimeKey === null
					? []
					: adaptiveEntries.filter(
							(entry) => runtimeConfigurationCohortKey(entry) === runtimeKey,
						);
			const adaptive = matches.length === 1 ? matches[0] : undefined;
			if (adaptive) {
				matchedAdaptiveEntries.add(adaptive);
			}
			rows.push(
				createUploadMatrixRow({
					summary,
					adaptive,
					fixed1,
					runtimeAware: true,
					runtimeKey,
				}),
			);
		}
		for (const adaptive of adaptiveEntries) {
			if (matchedAdaptiveEntries.has(adaptive)) {
				continue;
			}
			rows.push(
				createUploadMatrixRow({
					summary,
					adaptive,
					fixed1: undefined,
					runtimeAware: true,
					runtimeKey: runtimeConfigurationCohortKey(adaptive),
				}),
			);
		}
		return rows;
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
			incompleteErrorCollections: adaptive?.incompleteErrorCollections ?? null,
			requestFailureCount: adaptive?.requestFailureCount ?? null,
		};
	});
};

const summarizeMatrix = (variantSummaries, scenario) =>
	scenario === "seeder-probe"
		? summarizeSeederProbeMatrix(variantSummaries)
		: summarizeUploadMatrix(variantSummaries);

export const compareAdaptiveAcrossVariants = (variantSummaries, scenario) => {
	const adaptiveByVariant = variantSummaries.map((summary) => ({
		variant: summary.variant,
		entries: summary.summary.filter((entry) => entry.mode === "adaptive"),
	}));
	const runtimeAware = adaptiveByVariant.some(({ entries }) =>
		entries.some(hasRuntimeConfigurationCohortKey),
	);
	if (runtimeAware) {
		if (adaptiveByVariant.some(({ entries }) => entries.length !== 1)) {
			return null;
		}
		const runtimeKeys = adaptiveByVariant.map(({ entries }) =>
			runtimeConfigurationCohortKey(entries[0]),
		);
		if (
			runtimeKeys.some((runtimeKey) => runtimeKey === null) ||
			new Set(runtimeKeys).size !== 1
		) {
			return null;
		}
	}
	return adaptiveByVariant
		.map(({ variant, entries }) => {
			const adaptive = entries[0];
			if (!adaptive) {
				return null;
			}
			return scenario === "seeder-probe"
				? {
						variant,
						reachedTargetRuns: adaptive.reachedTargetRuns ?? null,
						writerSeedersLastAvg: adaptive.writerSeedersLastAvg ?? null,
						readerSeedersLastAvg: adaptive.readerSeedersLastAvg ?? null,
						errorCount: adaptive.errorCount ?? null,
					}
				: adaptive.uploadDurationMsAvg != null
					? {
							variant,
							...(runtimeAware
								? {
										runtimeConfigurationCohortKey:
											adaptive.runtimeConfigurationCohortKey,
									}
								: {}),
							downloadSink: adaptive.downloadSink ?? null,
							primaryDownloadMetric: adaptive.primaryDownloadMetric ?? null,
							primaryDownloadAuthoritative:
								adaptive.primaryDownloadAuthoritative ?? null,
							adaptiveAvgMs: adaptive.uploadDurationMsAvg,
							adaptiveWriterReadyMsAvg: adaptive.timeToWriterReadyMsAvg,
							adaptiveWriterReadyMsMedian: adaptive.timeToWriterReadyMsMedian,
							adaptiveReaderReadyMsAvg: adaptive.timeToReaderReadyMsAvg,
							adaptiveReaderReadyMsMedian: adaptive.timeToReaderReadyMsMedian,
							adaptiveLibraryStreamWallMsAvg: adaptive.libraryStreamWallMsAvg,
							adaptiveLibraryStreamWallMsMedian:
								adaptive.libraryStreamWallMsMedian,
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
	const groups =
		scenario === "seeder-probe"
			? (() => {
					const grouped = new Map();
					for (const result of results) {
						const items = grouped.get(result.mode) ?? [];
						items.push(result);
						grouped.set(result.mode, items);
					}
					return [...grouped.entries()].map(([mode, items]) => ({
						dimensions: { mode },
						results: items,
					}));
				})()
			: groupUploadResultsByLocalityCohort(results);
	return groups.map(({ dimensions, results: items }) => {
		const base = {
			...dimensions,
			runs: items.length,
			passed: items.filter((item) => item.status === "passed").length,
			failed: items.filter((item) => item.status !== "passed").length,
			errorCount: items.reduce(
				(total, item) => total + (Number(item.errorCount) || 0),
				0,
			),
			incompleteErrorCollections: items.filter(
				(item) => item.errorCollectionComplete !== true,
			).length,
			requestFailureCount: items.reduce(
				(total, item) => total + (Number(item.requestFailureCount) || 0),
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
	const peerbitProvenance = await getPeerbitProvenance({
		root: materialization.peerbitRoot,
		requestedRef: materialization.requestedRef,
		requireClean: true,
		expectedResolvedCommit: materialization.cloneCommit,
	});
	return {
		peerbitRoot: materialization.peerbitRoot,
		resolvedCommit: materialization.cloneCommit,
		peerbitProvenance,
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
	downloadSink,
	browserStorageMode,
	postTransferSoakMs,
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
	readerLocalChunkTarget,
	readerLocalChunkMaxOvershoot,
	readerTerminalTopology,
	readerPersistChunkReads,
	protocol,
	fixtureSeed,
	enableVisibilityProbe,
	verbose,
	variantRef,
	resolvedCommit,
	examplesRef,
}) => {
	await fsp.rm(summaryFile, { force: true });
	const runnerArgs = [
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
		...(downloadSink != null ? ["--download-sink", downloadSink] : []),
		...(browserStorageMode != null
			? ["--browser-storage", browserStorageMode]
			: []),
		...(postTransferSoakMs != null
			? ["--post-transfer-soak-ms", String(postTransferSoakMs)]
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
		...(readerLocalChunkTarget != null
			? ["--reader-local-chunk-target", String(readerLocalChunkTarget)]
			: []),
		...(readerLocalChunkMaxOvershoot != null
			? [
					"--reader-local-chunk-max-overshoot",
					String(readerLocalChunkMaxOvershoot),
				]
			: []),
		...(readerTerminalTopology != null
			? ["--reader-terminal-topology", readerTerminalTopology]
			: []),
		...(readerPersistChunkReads != null
			? ["--reader-persist-chunk-reads", readerPersistChunkReads]
			: []),
		...(protocol ? ["--protocol", protocol] : []),
		...(fixtureSeed ? ["--fixture-seed", fixtureSeed] : []),
		...(enableVisibilityProbe ? ["--enable-visibility-probe"] : []),
		...(verbose ? ["--verbose"] : []),
	];
	console.log(`$ node ${runnerArgs.join(" ")}`);
	const startedAt = Date.now();
	const child = spawnSync("node", runnerArgs, {
		cwd: repoRoot,
		env: process.env,
		stdio: "inherit",
	});
	const processOutcome = {
		wallTimeMs: Date.now() - startedAt,
		exitCode: child.status,
		signal: child.signal,
		spawnError: child.error ? serializeError(child.error) : null,
	};
	const summaryEvidence = await readJsonEvidence(summaryFile);
	const classification = classifySubprocessSummary({
		processOutcome,
		summaryEvidence,
		expectedSchema: BENCHMARK_SUMMARY_SCHEMA,
	});
	return {
		variant,
		variantRef,
		resolvedCommit,
		summaryFile,
		processOutcome,
		summaryEvidenceKind: summaryEvidence.kind,
		subprocessFailures: classification.failures,
		subprocessSucceeded: classification.processSucceeded,
		summary: classification.summary,
	};
};

export const createMatrixInvocationFailure = ({
	plan,
	scenario,
	network,
	fileMb,
	summaryFile,
	processOutcome,
	expectedInvocation,
	expectedProvenance,
	failures,
	browserResult,
}) => {
	const collectedErrorEvidence = extractCollectedErrorEvidence(browserResult);
	const uploadEvidence =
		scenario === "upload"
			? {
					seederDropPolicy: SEEDER_DROP_POLICY,
					...projectUploadIntegrityEvidence(browserResult, {
						fileMb,
						fixtureSeed: expectedInvocation?.fixtureSeed,
						downloadSink: expectedInvocation?.downloadSink,
					}),
				}
			: {};
	return {
		schema: BENCHMARK_RESULT_SCHEMA,
		runNonce: browserResult?.runNonce ?? null,
		invocation: expectedInvocation ?? null,
		provenance: expectedProvenance,
		status: "failed",
		scenario,
		mode: plan.mode,
		networkMode: network,
		fileSizeMb: fileMb,
		...uploadEvidence,
		stage: "matrix-orchestration",
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		errorCollectionComplete: collectedErrorEvidence.errorCollectionComplete,
		errorCount: collectedErrorEvidence.errorCount,
		errors: collectedErrorEvidence.errors,
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		requestFailureCollectionComplete:
			collectedErrorEvidence.requestFailureCollectionComplete,
		requestFailureCount: collectedErrorEvidence.requestFailureCount,
		requestFailures: collectedErrorEvidence.requestFailures,
		variant: plan.variant,
		run: plan.run,
		matrixSequence: plan.sequence,
		subRunSummaryFile: summaryFile,
		subprocess: processOutcome ?? null,
		orchestrationFailures: failures,
		failure: {
			kind: "matrix-subrun",
			message: failures.map((failure) => failure.message).join("; "),
		},
		browserResult: browserResult ?? null,
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
	const downloadSink = resolveBenchmarkDownloadSink(args["download-sink"], {
		scenario,
	});
	const browserStorageMode = resolveBenchmarkBrowserStorageMode(
		args["browser-storage"],
		{ scenario },
	);
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
	const postTransferSoakMs = args["post-transfer-soak-ms"];
	const postUploadMonitorMs = args["post-upload-monitor-ms"];
	const pollMs = args["poll-ms"];
	const minReadySeeders = args["min-ready-seeders"];
	const readyTimeoutMs = args["ready-timeout-ms"];
	const sampleMs = args["sample-ms"];
	const sampleCount = args["sample-count"];
	const targetSeeders = args["target-seeders"];
	const readerLocalChunkTarget = args["reader-local-chunk-target"];
	const readerLocalChunkMaxOvershoot = args["reader-local-chunk-max-overshoot"];
	const readerTerminalTopology = args["reader-terminal-topology"];
	const readerPersistChunkReads = args["reader-persist-chunk-reads"];
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
	if (
		readerLocalChunkTarget != null &&
		(scenario !== "upload" || modes.length !== 1 || modes[0] !== "fixed1")
	) {
		throw new Error(
			"--reader-local-chunk-target requires --scenario upload --mode fixed1",
		);
	}
	if (
		readerLocalChunkTarget != null &&
		(!Number.isSafeInteger(Number(readerLocalChunkTarget)) ||
			Number(readerLocalChunkTarget) < 0)
	) {
		throw new Error(
			"--reader-local-chunk-target must be a non-negative safe integer",
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
	if ((readerLocalChunkTarget == null) !== (readerTerminalTopology == null)) {
		throw new Error(
			"--reader-terminal-topology must be provided exactly when --reader-local-chunk-target is provided",
		);
	}
	const parsedReaderPersistChunkReads = optionalBoolean(
		readerPersistChunkReads,
		"--reader-persist-chunk-reads",
	);
	if (parsedReaderPersistChunkReads != null && readerLocalChunkTarget == null) {
		throw new Error(
			"--reader-persist-chunk-reads requires --reader-local-chunk-target",
		);
	}
	if (
		parsedReaderPersistChunkReads === false &&
		(Number(readerLocalChunkTarget) !== 0 ||
			readerTerminalTopology !== "observer")
	) {
		throw new Error(
			"--reader-persist-chunk-reads false requires --reader-local-chunk-target 0 and --reader-terminal-topology observer",
		);
	}
	if (
		readerTerminalTopology != null &&
		!READER_TERMINAL_TOPOLOGIES.includes(readerTerminalTopology)
	) {
		throw new Error(
			`Unsupported --reader-terminal-topology ${JSON.stringify(readerTerminalTopology)}. Expected one of ${READER_TERMINAL_TOPOLOGIES.join(", ")}`,
		);
	}
	if (
		readerLocalChunkMaxOvershoot != null &&
		(!Number.isSafeInteger(Number(readerLocalChunkMaxOvershoot)) ||
			Number(readerLocalChunkMaxOvershoot) < 0 ||
			Number(readerLocalChunkMaxOvershoot) > MAX_READER_LOCAL_CHUNK_OVERSHOOT)
	) {
		throw new Error(
			`--reader-local-chunk-max-overshoot must be a non-negative safe integer no greater than ${MAX_READER_LOCAL_CHUNK_OVERSHOOT}`,
		);
	}
	if (
		readerLocalChunkTarget != null &&
		minReadySeeders != null &&
		Number(minReadySeeders) !== 1
	) {
		throw new Error(
			"--reader-local-chunk-target requires --min-ready-seeders 1 because the reader starts as an observer",
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
	const expectedHarnessProvenance = await getPeerbitProvenance({
		root: repoRoot,
		requestedRef: "harness-worktree",
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
		const { peerbitRoot, resolvedCommit, peerbitProvenance } =
			await prepareVariant({
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
		await preflightBetterSqlite3Runtime(peerbitRoot, {
			packageNames: effectiveLocalPackageNames,
		});
		preparedVariants.set(variant, {
			variant,
			variantRef:
				variantSpec.kind === "worktree" ? "worktree" : variantSpec.ref,
			resolvedCommit,
			peerbitRoot,
			peerbitProvenance,
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
	const benchmarkExamplesProvenance = pinnedExamples.provenance;
	const benchmarkHarnessProvenance = expectedHarnessProvenance;

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
		let subRun;
		const structuralFailures = [];
		try {
			subRun = await runVariantBenchmark({
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
				downloadSink,
				browserStorageMode,
				postTransferSoakMs,
				postUploadMonitorMs,
				pollMs,
				minReadySeeders,
				readyTimeoutMs,
				sampleMs,
				sampleCount,
				targetSeeders,
				readerLocalChunkTarget,
				readerLocalChunkMaxOvershoot,
				readerTerminalTopology,
				readerPersistChunkReads,
				protocol,
				fixtureSeed,
				enableVisibilityProbe,
				verbose,
			});
		} catch (error) {
			structuralFailures.push({
				kind: "subrun-orchestration",
				...serializeError(error),
			});
		}
		const invocationSummary = subRun?.summary;
		const subprocessFailures = subRun?.subprocessFailures ?? [];
		const processFailures = [];
		for (const failure of subprocessFailures) {
			if (["spawn-error", "signal", "nonzero-exit"].includes(failure.kind)) {
				processFailures.push(failure);
			} else {
				structuralFailures.push(failure);
			}
		}
		const expectedProvenance = {
			harness: expectedHarnessProvenance,
			peerbit: prepared.peerbitProvenance,
			examples: pinnedExamples.provenance,
		};
		let expectedInvocation;
		try {
			expectedInvocation = createBenchmarkInvocation({
				scenario,
				mode: plan.mode,
				network,
				integrationMode,
				fileMb,
				fixtureSeed: fixtureSeed ?? DEFAULT_FIXTURE_SEED,
				downloadSink,
				browserStorageMode,
				uploadTimeoutMs: optionalNumber(uploadTimeoutMs),
				downloadTimeoutMs: optionalNumber(downloadTimeoutMs),
				postTransferSoakMs: optionalNumber(postTransferSoakMs),
				postUploadMonitorMs: optionalNumber(postUploadMonitorMs),
				pollMs: optionalNumber(pollMs),
				minReadySeeders: optionalNumber(minReadySeeders),
				readyTimeoutMs: optionalNumber(readyTimeoutMs),
				sampleMs: optionalNumber(sampleMs),
				sampleCount: optionalNumber(sampleCount),
				targetSeeders: optionalNumber(targetSeeders),
				readerLocalChunkTarget: optionalNumber(readerLocalChunkTarget),
				readerLocalChunkMaxOvershoot: optionalNumber(
					readerLocalChunkMaxOvershoot,
				),
				readerTerminalTopology,
				readerPersistChunkReads: parsedReaderPersistChunkReads,
				baseUrl: null,
				protocol,
				viteMode: null,
				viteConfig: null,
				localPackages: prepared.effectiveLocalPackageNames,
				enableVisibilityProbe,
				verbose,
			});
		} catch (error) {
			structuralFailures.push({
				kind: "expected-invocation",
				...serializeError(error),
			});
		}
		let rawResult;
		let rawResultEvidence;
		if (invocationSummary) {
			const inspection = inspectSingleInvocationSummary(invocationSummary);
			rawResult = inspection.result;
			rawResultEvidence = inspection.resultEvidence;
			structuralFailures.push(...inspection.failures);
			if (
				!isDeepStrictEqual(
					invocationSummary.harnessProvenance,
					expectedHarnessProvenance,
				)
			) {
				structuralFailures.push({
					kind: "harness-provenance",
					message:
						"Benchmark subprocess did not report the matrix harness's exact pinned provenance",
				});
			}
			if (
				!isDeepStrictEqual(
					invocationSummary.peerbitProvenance,
					prepared.peerbitProvenance,
				)
			) {
				structuralFailures.push({
					kind: "peerbit-provenance",
					message: `Variant ${plan.variant} did not report its exact clean pinned provenance`,
				});
			}
			if (
				!isDeepStrictEqual(
					invocationSummary.examplesProvenance,
					pinnedExamples.provenance,
				)
			) {
				structuralFailures.push({
					kind: "examples-provenance",
					message: `Variant ${plan.variant} did not report the exact clean pinned examples provenance`,
				});
			}
			if (
				rawResult &&
				(!isDeepStrictEqual(
					invocationSummary.harnessProvenance,
					rawResult.provenance?.harness,
				) ||
					!isDeepStrictEqual(
						invocationSummary.peerbitProvenance,
						rawResult.provenance?.peerbit,
					) ||
					!isDeepStrictEqual(
						invocationSummary.examplesProvenance,
						rawResult.provenance?.examples,
					))
			) {
				structuralFailures.push({
					kind: "summary-result-provenance",
					message:
						"Benchmark subprocess summary provenance contradicts its result envelope",
				});
			}
			if (rawResult && expectedInvocation) {
				let envelopeValid = false;
				try {
					validateBenchmarkResultEnvelope(rawResult, {
						expectedMode: plan.mode,
						expectedFileMb: fileMb,
						expectedNetwork: network,
						expectedRunNonce: rawResult.runNonce,
						expectedProvenance,
						expectedInvocation,
					});
					envelopeValid = true;
				} catch (error) {
					structuralFailures.push({
						kind: "invalid-result-envelope",
						...serializeError(error),
					});
				}
				if (envelopeValid && rawResult.status === "passed") {
					try {
						validateBenchmarkResult(rawResult, {
							scenario,
							expectedMode: plan.mode,
							expectedFileMb: fileMb,
							expectedNetwork: network,
							expectedFixtureSeed: fixtureSeed ?? DEFAULT_FIXTURE_SEED,
							expectedRunNonce: rawResult.runNonce,
							expectedProvenance,
							expectedInvocation,
						});
					} catch (error) {
						structuralFailures.push({
							kind: "invalid-passed-result",
							...serializeError(error),
						});
					}
				}
			}
		}
		if (
			rawResult?.status === "passed" &&
			subRun?.subprocessSucceeded === false
		) {
			structuralFailures.push({
				kind: "passed-result-nonzero-exit",
				message:
					"Benchmark subprocess returned a passed result with an unsuccessful process outcome",
			});
		}
		if (
			rawResult?.status !== "passed" &&
			subRun?.subprocessSucceeded === true
		) {
			structuralFailures.push({
				kind: "failed-result-zero-exit",
				message:
					"Benchmark subprocess returned a failed result with a successful process outcome",
			});
		}
		const allFailures = [...processFailures, ...structuralFailures];
		const result =
			rawResult && structuralFailures.length === 0
				? {
						...rawResult,
						variant: plan.variant,
						run: plan.run,
						matrixSequence: plan.sequence,
						subRunSummaryFile: summaryFile,
						subprocess: subRun.processOutcome,
						subprocessFailures,
					}
				: createMatrixInvocationFailure({
						plan,
						scenario,
						network,
						fileMb,
						summaryFile,
						processOutcome: subRun?.processOutcome,
						expectedInvocation,
						expectedProvenance,
						failures:
							allFailures.length > 0
								? allFailures
								: [
										{
											kind: "missing-summary",
											message:
												"Matrix invocation did not return usable summary evidence",
										},
									],
						browserResult: rawResultEvidence,
					});
		resultsByVariant.get(plan.variant).push(result);
		invocationRecords.push({
			...plan,
			status: result.status,
			readerLocalChunkTarget:
				result.readerLocalChunkTarget ??
				result.invocation?.readerLocalChunkTarget ??
				null,
			readerLocalChunkMaxOvershoot:
				result.readerLocalChunkMaxOvershoot ??
				result.invocation?.readerLocalChunkMaxOvershoot ??
				null,
			readerTerminalTopology:
				result.readerTerminalTopology ??
				result.invocation?.readerTerminalTopology ??
				null,
			readerLocalChunkBlockCount: result.readerLocalChunkBlockCount ?? null,
			readerLocalChunkIndexRowCount:
				result.readerLocalChunkIndexRowCount ?? null,
			readerLocalityCohortKey: result.readerLocalityCohortKey ?? null,
			runNonce: result.runNonce,
			resultFile: result.resultFile ?? null,
			subRunSummaryFile: summaryFile,
			subRunSummary: invocationSummary ?? null,
			subprocess: subRun?.processOutcome ?? null,
			subprocessFailures,
			orchestrationFailures: structuralFailures,
			peerbitResolvedCommit: prepared.resolvedCommit,
			effectiveLocalPackageNames: prepared.effectiveLocalPackageNames,
		});
	}

	const allResults = [...resultsByVariant.values()].flat();
	const outcomeCounts = countBenchmarkOutcomes(
		allResults,
		executionOrder.length,
	);
	const matrixPlanPassed = isCompletePassedBenchmarkPlan(
		allResults,
		outcomeCounts,
	);
	const variantSummaries = variantSpecs.map((variantSpec) => {
		const prepared = preparedVariants.get(variantSpec.name);
		const results = resultsByVariant
			.get(variantSpec.name)
			.toSorted((left, right) => left.matrixSequence - right.matrixSequence);
		const variantPlanned = executionOrder.filter(
			(plan) => plan.variant === variantSpec.name,
		).length;
		const variantOutcomeCounts = countBenchmarkOutcomes(
			results,
			variantPlanned,
		);
		const summary = summarizeInvocationResults(results, scenario);
		return {
			variant: variantSpec.name,
			variantRef: prepared.variantRef,
			resolvedCommit: prepared.resolvedCommit,
			effectiveLocalPackageNames: prepared.effectiveLocalPackageNames,
			harnessProvenance: benchmarkHarnessProvenance,
			peerbitProvenance: prepared.peerbitProvenance,
			examplesProvenance: benchmarkExamplesProvenance,
			results,
			outcomeCounts: variantOutcomeCounts,
			readerLocalityCohorts: summary
				.map((row) => row.readerLocalityCohortKey)
				.filter((value) => typeof value === "string"),
			summary,
			comparison:
				scenario === "upload"
					? compareUploadPerformanceModesForCompletePlan(
							results,
							variantOutcomeCounts,
						)
					: null,
		};
	});
	const matrixSummary = {
		schema: MATRIX_SUMMARY_SCHEMA,
		status: matrixPlanPassed ? "passed" : "failed",
		outcomeCounts,
		errorCollectionDefinition: ERROR_COLLECTION_DEFINITION,
		knownPeerbitFailureSignatures: KNOWN_PEERBIT_FAILURE_SIGNATURES,
		requestFailureCollectionDefinition: REQUEST_FAILURE_COLLECTION_DEFINITION,
		matrixBase: matrixSession.matrixBase,
		matrixRoot,
		matrixSessionNonce: matrixSession.nonce,
		matrixSessionMarker: matrixSession.markerFile,
		variants: variantSpecs,
		network,
		fileMb,
		readerLocalChunkTarget: optionalNumber(readerLocalChunkTarget) ?? null,
		readerLocalChunkMaxOvershoot:
			optionalNumber(readerLocalChunkMaxOvershoot) ?? null,
		readerTerminalTopology: readerTerminalTopology ?? null,
		readerPersistChunkReads:
			readerLocalChunkTarget == null
				? null
				: (parsedReaderPersistChunkReads ?? true),
		readerLocalityCohorts: [
			...new Set(
				allResults
					.map((result) => result.readerLocalityCohortKey)
					.filter((value) => typeof value === "string"),
			),
		],
		runs,
		mode,
		modes,
		scenario,
		downloadSink,
		browserStorageMode,
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
		adaptiveComparison: matrixPlanPassed
			? compareAdaptiveAcrossVariants(variantSummaries, scenario)
			: null,
	};
	const matrixSummaryFile = path.join(
		matrixRoot,
		"results",
		"matrix-summary.json",
	);
	await writeJsonAtomic(matrixSummaryFile, matrixSummary);

	console.log("\nVariant summary");
	console.table(matrixSummary.summary);
	console.log("\nOutcome counts");
	console.table([outcomeCounts]);
	if (matrixSummary.adaptiveComparison) {
		console.log("\nAdaptive across variants");
		console.table(matrixSummary.adaptiveComparison);
	}
	console.log(`\nMatrix summary: ${matrixSummaryFile}`);
	if (!matrixPlanPassed) {
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
