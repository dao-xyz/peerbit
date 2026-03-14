import { spawnSync } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
	copyTemplate,
	defaultExamplesDest,
	defaultExamplesSource,
	ensureExamplesAssetPackageLinks,
	overlayInstalledPackages,
	parseArgs,
	prepareExamplesRepo,
	repoRoot,
	run,
} from "./common.mjs";

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
	},
	"seeder-probe": {
		templatePath: path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"seeder-probe.e2e.spec.ts",
		),
		generatedSpecPath: path.join(
			"tests",
			"generated.seeder-probe.e2e.spec.ts",
		),
	},
};
const DROP_HOOK_MARKER = "/* peerbit-benchmark-hook */";
const DROP_UPDATE_LIST_MARKER = "/* peerbit-benchmark-update-list */";
const VITE_BENCHMARK_MARKER = "/* peerbit-benchmark-vite */";
const REMOTE_NETWORK_DEFAULTS = {
	viteMode: "staging",
	viteConfig: "vite.config.remote.ts",
};

const DEFAULT_LOCAL_PACKAGES = ["@peerbit/shared-log"];

const getScenarioConfig = (scenario) => {
	const config = SCENARIOS[scenario];
	if (!config) {
		throw new Error(
			`Unsupported --scenario "${scenario}". Expected one of ${Object.keys(SCENARIOS).join(", ")}`,
		);
	}
	return config;
};

const injectViteBenchmarkResolveGuards = (contents, filePath, frontendRoot) => {
	let next = contents;
	if (!next.includes("resolve: {")) {
		throw new Error(`Could not find resolve block in ${filePath}`);
	}
	const examplesNodeModules = path.resolve(frontendRoot, "..", "..", "..", "node_modules");
	const reactAliasBlock = `        ${VITE_BENCHMARK_MARKER}
        alias: {
            react: ${JSON.stringify(path.join(examplesNodeModules, "react"))},
            "react-dom": ${JSON.stringify(path.join(examplesNodeModules, "react-dom"))},
            "@dao-xyz/borsh": ${JSON.stringify(
				path.join(examplesNodeModules, "@dao-xyz", "borsh"),
			)},
        },\n`;
		next = next.replace(
			/ {8}\/\* peerbit-benchmark-vite \*\/\n {8}alias: \{\n[\s\S]*? {8}\},\n/,
			reactAliasBlock,
		);
	if (next.includes(`${VITE_BENCHMARK_MARKER}\n        preserveSymlinks: true,\n`)) {
		next = next.replace(
			`${VITE_BENCHMARK_MARKER}\n        preserveSymlinks: true,\n`,
			reactAliasBlock,
		);
	}
	if (!next.includes(VITE_BENCHMARK_MARKER)) {
		next = next.replace("    resolve: {\n", `    resolve: {\n${reactAliasBlock}`);
	}
	if (!next.includes('"react",')) {
		next = next.replace(
			"        dedupe: [\n",
			`        dedupe: [\n            "react",\n            "react-dom",\n`,
		);
	}
	if (!next.includes('"@dao-xyz/borsh",')) {
		next = next.replace(
			'            "react-dom",\n',
			'            "react-dom",\n            "@dao-xyz/borsh",\n',
		);
	}
	next = next.replace(
		'        include: [\n            "react",\n            "react-dom",\n',
		"        include: [\n",
	);
	return next;
};

const instrumentFileShareViteConfigs = async (frontendRoot) => {
	for (const configName of ["vite.config.ts", "vite.config.remote.ts"]) {
		const configPath = path.join(frontendRoot, configName);
		if (!fs.existsSync(configPath)) {
			continue;
		}
		const contents = await fsp.readFile(configPath, "utf8");
		const next = injectViteBenchmarkResolveGuards(
			contents,
			configPath,
			frontendRoot,
		);
		if (next !== contents) {
			await fsp.writeFile(configPath, next);
		}
	}
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

const instrumentFileShareFrontend = async (frontendRoot) => {
	const dropPath = path.join(frontendRoot, "src", "Drop.tsx");
	let contents = await fsp.readFile(dropPath, "utf8");
	if (
		contents.includes(DROP_HOOK_MARKER) &&
		contents.includes(DROP_UPDATE_LIST_MARKER)
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
	if (!contents.includes(DROP_HOOK_MARKER)) {
		if (!contents.includes(anchor)) {
			throw new Error(`Could not find benchmark hook anchor in ${dropPath}`);
		}
		contents = contents.replace(anchor, `${hook}${anchor}`);
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
	if (!contents.includes(DROP_UPDATE_LIST_MARKER)) {
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
				(
					values.reduce((sum, value) => sum + value, 0) / values.length
				).toFixed(1),
		  )
		: null;

const summarizeUploadResults = (results) => {
	const grouped = new Map();
	for (const result of results) {
		const arr = grouped.get(result.mode) ?? [];
		arr.push(result);
		grouped.set(result.mode, arr);
	}
	return [...grouped.entries()].map(([mode, items]) => ({
		mode,
		runs: items.length,
		passed: items.filter((item) => item.status === "passed").length,
		failed: items.filter((item) => item.status !== "passed").length,
		uploadDurationMsAvg: average(
			items
				.filter((item) => item.status === "passed")
				.map((item) => item.uploadDurationMs)
				.filter((value) => typeof value === "number"),
		),
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
		errorCount: items.reduce((sum, item) => sum + item.errorCount, 0),
		seederDrops: items.filter((item) => item.droppedSeeders).length,
	}));
};

const summarizeSeederProbeResults = (results) => {
	const grouped = new Map();
	for (const result of results) {
		const arr = grouped.get(result.mode) ?? [];
		arr.push(result);
		grouped.set(result.mode, arr);
	}
	const sampleMetric = (result, fn) =>
		(result.samples ?? [])
			.map(fn)
			.filter((value) => typeof value === "number");
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
		errorCount: items.reduce((sum, item) => sum + item.errorCount, 0),
	}));
};

const summarizeResults = (results, scenario) =>
	scenario === "seeder-probe"
		? summarizeSeederProbeResults(results)
		: summarizeUploadResults(results);

const compareUploadModes = (results) => {
	const byMode = new Map();
	for (const result of results) {
		if (result.status !== "passed") {
			continue;
		}
		const arr = byMode.get(result.mode) ?? [];
		arr.push(result);
		byMode.set(result.mode, arr);
	}
	const adaptive = byMode.get("adaptive");
	const fixed = byMode.get("fixed1");
	if (!adaptive?.length || !fixed?.length) {
		return null;
	}
	const adaptiveAvg =
		adaptive.reduce((sum, item) => sum + item.uploadDurationMs, 0) /
		adaptive.length;
	const fixedAvg =
		fixed.reduce((sum, item) => sum + item.uploadDurationMs, 0) / fixed.length;
	const deltaMs = adaptiveAvg - fixedAvg;
	return {
		adaptiveAvgMs: Number(adaptiveAvg.toFixed(1)),
		fixed1AvgMs: Number(fixedAvg.toFixed(1)),
		deltaMs: Number(deltaMs.toFixed(1)),
		adaptiveVsFixed1Pct: Number(((deltaMs / fixedAvg) * 100).toFixed(1)),
	};
};

const buildPeerbitForFileShare = (peerbitRoot) => {
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
		{ cwd: peerbitRoot },
	);
};

const runPlaywright = ({
	frontendRoot,
	scenario,
	generatedSpecPath,
	mode,
	fileMb,
	resultFile,
	network,
	uploadTimeoutMs,
	postUploadMonitorMs,
	pollMs,
	minReadySeeders,
	readyTimeoutMs,
	sampleMs,
	sampleCount,
	targetSeeders,
	baseUrl,
	protocol,
	viteMode,
	viteConfig,
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
			env: {
				...process.env,
				PW_FILE_MB: String(fileMb),
				PW_REPLICATION_MODE: mode,
				PW_RESULT_FILE: resultFile,
				PW_BENCHMARK_SCENARIO: scenario,
				PW_NETWORK_MODE: network,
				...(minReadySeeders != null
					? { PW_MIN_READY_SEEDERS: String(minReadySeeders) }
					: {}),
				...(uploadTimeoutMs
					? { PW_UPLOAD_TIMEOUT_MS: String(uploadTimeoutMs) }
					: {}),
				...(postUploadMonitorMs
					? { PW_POST_UPLOAD_MONITOR_MS: String(postUploadMonitorMs) }
					: {}),
				...(pollMs ? { PW_POLL_MS: String(pollMs) } : {}),
				...(readyTimeoutMs
					? { PW_READY_TIMEOUT_MS: String(readyTimeoutMs) }
					: {}),
				...(sampleMs ? { PW_SAMPLE_MS: String(sampleMs) } : {}),
				...(sampleCount ? { PW_SAMPLE_COUNT: String(sampleCount) } : {}),
				...(targetSeeders
					? { PW_TARGET_SEEDERS: String(targetSeeders) }
					: {}),
				...(baseUrl ? { PW_BASE_URL: baseUrl } : {}),
				...(protocol ? { PW_PROTOCOL: protocol } : {}),
				...(viteMode ? { PW_VITE_MODE: viteMode } : {}),
				...(viteConfig ? { PW_VITE_CONFIG: viteConfig } : {}),
			},
		},
	);
	return {
		wallTimeMs: Date.now() - startedAt,
		exitCode: child.status ?? (child.error ? 1 : 0),
	};
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const peerbitRoot = args["peerbit-root"] ?? repoRoot;
	const examplesRoot = args["examples-root"] ?? defaultExamplesDest();
	const freshEachRun = Boolean(args["fresh-each-run"]);
	const fileMb = Number(args["file-mb"] ?? "25");
	const runs = Number(args.runs ?? "1");
	const requestedMode = args.mode ?? "both";
	const modes = requestedMode === "both" ? ["adaptive", "fixed1"] : [requestedMode];
	const scenario = args.scenario ?? "upload";
	const network = args.network ?? "local";
	const uploadTimeoutMs = args["upload-timeout-ms"];
	const postUploadMonitorMs = args["post-upload-monitor-ms"];
	const pollMs = args["poll-ms"];
	const minReadySeeders = args["min-ready-seeders"];
	const readyTimeoutMs = args["ready-timeout-ms"];
	const sampleMs = args["sample-ms"];
	const sampleCount = args["sample-count"];
	const targetSeeders = args["target-seeders"];
	const baseUrl = args["base-url"];
	const examplesSource = args.source ?? defaultExamplesSource();
	const requestedProtocol = args.protocol;
	const scenarioConfig = getScenarioConfig(scenario);
	const integrationMode = args["integration-mode"] ?? "overlay";
	const localPackagesArg = args["local-packages"];
	const localPackageNames =
		integrationMode === "none"
			? []
			: localPackagesArg === "all"
				? undefined
				: (localPackagesArg ?? DEFAULT_LOCAL_PACKAGES.join(","))
						.split(",")
						.map((value) => value.trim())
						.filter(Boolean);

	if (!["local", "remote"].includes(network)) {
		throw new Error(`Unsupported --network "${network}". Expected "local" or "remote".`);
	}
	if (!["none", "link", "overlay"].includes(integrationMode)) {
		throw new Error(
			`Unsupported --integration-mode "${integrationMode}". Expected "none", "link", or "overlay".`,
		);
	}

	const viteMode =
		args["vite-mode"] ??
		(network === "remote" ? REMOTE_NETWORK_DEFAULTS.viteMode : undefined);
	const viteConfig =
		args["vite-config"] ??
		(network === "remote" ? REMOTE_NETWORK_DEFAULTS.viteConfig : undefined);

	if (args["build-peerbit"]) {
		buildPeerbitForFileShare(peerbitRoot);
	}

	const prepareBenchmarkCheckout = async ({ fresh }) => {
		await prepareExamplesRepo({
			source: examplesSource,
			template: args.template,
			dest: examplesRoot,
			peerbitRoot,
			fresh,
			install: Boolean(args.install),
			localPackageNames,
			applyOverrides: integrationMode === "link",
		});

		const frontendRoot = path.join(
			examplesRoot,
			"packages",
			"file-share",
			"frontend",
		);
		await maybeCopyFrontendCerts({
			frontendRoot,
			sourceRoot: examplesSource,
			templateRoot: args.template,
		});
		await instrumentFileShareFrontend(frontendRoot);
		await instrumentFileShareViteConfigs(frontendRoot);
		const generatedSpec = path.join(
			frontendRoot,
			scenarioConfig.generatedSpecPath,
		);
		await copyTemplate({
			templatePath: scenarioConfig.templatePath,
			outputPath: generatedSpec,
		});

		if (!fs.existsSync(path.join(examplesRoot, "node_modules"))) {
			run("pnpm", ["install"], { cwd: examplesRoot });
		}
		if (integrationMode === "link") {
			await ensureExamplesAssetPackageLinks({
				examplesRoot,
				peerbitRoot,
				packageNames: localPackageNames,
			});
		} else if (integrationMode === "overlay") {
			await overlayInstalledPackages({
				examplesRoot,
				peerbitRoot,
				packageNames: localPackageNames,
			});
		}
		return { frontendRoot };
	};

	let { frontendRoot } = await prepareBenchmarkCheckout({
		fresh: Boolean(args.fresh),
	});
	let protocol =
		requestedProtocol ??
		(network === "remote" && !baseUrl
			? fs.existsSync(path.join(frontendRoot, ".cert", "key.pem"))
				? "https"
				: "http"
			: undefined);

	const requestedResultsDir = args["results-dir"];
	const resultsDir = requestedResultsDir
		? path.resolve(requestedResultsDir)
		: await fsp.mkdtemp(path.join(os.tmpdir(), "peerbit-file-share-benchmark-"));
	if (requestedResultsDir) {
		await fsp.mkdir(resultsDir, { recursive: true });
	}
	const results = [];
	let benchmarkInvocationCount = 0;

	for (const mode of modes) {
		for (let runIndex = 1; runIndex <= runs; runIndex++) {
			if (freshEachRun && benchmarkInvocationCount > 0) {
				({ frontendRoot } = await prepareBenchmarkCheckout({ fresh: true }));
				protocol =
					requestedProtocol ??
					(network === "remote" && !baseUrl
						? fs.existsSync(path.join(frontendRoot, ".cert", "key.pem"))
							? "https"
							: "http"
						: undefined);
			}
			const resultFile = path.join(resultsDir, `${mode}-${runIndex}.json`);
			console.log(
				`Running file-share benchmark scenario=${scenario} network=${network} mode=${mode} run=${runIndex}/${runs} fileMb=${fileMb}`,
			);
			await cleanupFrontendBenchmarkArtifacts(frontendRoot);
			const { wallTimeMs, exitCode } = runPlaywright({
				frontendRoot,
				scenario,
				generatedSpecPath: scenarioConfig.generatedSpecPath,
				mode,
				fileMb,
				resultFile,
				network,
				uploadTimeoutMs,
				postUploadMonitorMs,
				pollMs,
				minReadySeeders,
				readyTimeoutMs,
				sampleMs,
				sampleCount,
				targetSeeders,
				baseUrl,
				protocol,
				viteMode,
				viteConfig,
			});
			if (!fs.existsSync(resultFile)) {
				throw new Error(
					`Benchmark run did not produce ${resultFile} (mode=${mode}, exitCode=${exitCode})`,
				);
			}
			const result = JSON.parse(await fsp.readFile(resultFile, "utf8"));
			results.push({
				...result,
				run: runIndex,
				playwrightWallTimeMs: wallTimeMs,
				playwrightExitCode: exitCode,
			});
			benchmarkInvocationCount++;
		}
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
			writerListingLagMs: result.phaseDurationsMs?.writerListingLag ?? null,
			readerListingLagMs: result.phaseDurationsMs?.readerListingLag ?? null,
			playwrightWallTimeMs: result.playwrightWallTimeMs,
			playwrightExitCode: result.playwrightExitCode,
			droppedSeeders: result.droppedSeeders,
			errorCount: result.errorCount,
			failure: result.failure?.message ?? "",
		})),
	);
	console.log("\nSummary");
	console.table(summarizeResults(results, scenario));
	const comparison = scenario === "upload" ? compareUploadModes(results) : null;
	if (comparison) {
		console.log("\nAdaptive vs fixed1");
		console.table([comparison]);
	}
	const summary = {
		peerbitRoot,
		examplesRoot,
		frontendRoot,
		scenario,
		integrationMode,
		localPackageNames: localPackageNames ?? "all",
		network,
		fileMb,
			runs,
			modes,
			resultsDir,
			results,
			summary: summarizeResults(results, scenario),
			comparison,
	};
	if (args["summary-file"]) {
		const summaryFile = path.resolve(args["summary-file"]);
		await fsp.mkdir(path.dirname(summaryFile), { recursive: true });
		await fsp.writeFile(summaryFile, `${JSON.stringify(summary, null, 2)}\n`);
	}
	console.log(`\nRaw results: ${resultsDir}`);
};

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
