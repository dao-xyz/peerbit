import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	lstat,
	mkdir,
	mkdtemp,
	readdir,
	rename,
	rm,
	writeFile,
} from "node:fs/promises";
import os from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";

const LIFECYCLE_FILE_CATEGORIES = [
	[
		/^coordinate-wal\/coordinates\.bin\.checkpoint-(?:state|a|b)$/,
		"coordinateCheckpoint",
	],
	[
		/^coordinate-wal\/coordinates\.(?:bin|wal(?:\.checkpoint-[ab])?)$/,
		"coordinateWal",
	],
	[
		/^coordinate-wal\/document-values\.(?:bin|wal(?:\.checkpoint-[ab])?)$/,
		"documentValueWal",
	],
	[
		/^coordinate-wal\/document-signers\.(?:bin|wal(?:\.checkpoint-[ab])?)$/,
		"documentSignerWal",
	],
];

export const classifyLifecycleCensusFile = (path) => {
	for (const [pattern, category] of LIFECYCLE_FILE_CATEGORIES) {
		if (pattern.test(path)) return category;
	}
	if (path.includes("/sublevels/blocks/")) return "entryBlockStore";
	if (path.includes("/log/heads/")) return "headIndex";
	if (path.includes("/replication/")) return "replicationIndex";
	if (path.startsWith("libp2p/")) return "libp2p";
	return "fixedAndOther";
};

export const parsePositiveInteger = (value, label) => {
	const normalized = String(value).replaceAll("_", "");
	if (!/^[1-9][0-9]*$/.test(normalized)) {
		throw new Error(`${label} must be a positive integer, got '${value}'`);
	}
	const parsed = Number(normalized);
	if (!Number.isSafeInteger(parsed)) {
		throw new Error(`${label} exceeds JavaScript's safe integer range`);
	}
	return parsed;
};

export const LIFECYCLE_PARSE_OPTIONS = Object.freeze({
	runs: { type: "string" },
	output: { type: "string" },
	json: { type: "boolean" },
	help: { type: "boolean" },
	worker: { type: "boolean" },
	scenario: { type: "string" },
	phase: { type: "string" },
	run: { type: "string" },
	directory: { type: "string" },
	"compact-max-journal-bytes": { type: "string" },
	"compact-max-journal-records": { type: "string" },
});

export const optionalEnvironmentValue = (value) =>
	typeof value === "string" && value.trim() === "" ? undefined : value;

export const parseLifecycleCompactionOptions = ({ bytes, records }) => {
	const options = {};
	if (bytes != null) {
		options.compactMaxJournalBytes = parsePositiveInteger(
			bytes,
			"compact-max-journal-bytes",
		);
	}
	if (records != null) {
		options.compactMaxJournalRecords = parsePositiveInteger(
			records,
			"compact-max-journal-records",
		);
	}
	return options;
};

export const parseLifecycleExecutionOptions = ({
	values,
	env,
	envPrefix,
	scenarios,
	scenarioLabel,
	workload,
	workerOnly = [],
	workerExtras = () => ({}),
}) => {
	const compaction = parseLifecycleCompactionOptions({
		bytes:
			values["compact-max-journal-bytes"] ??
			optionalEnvironmentValue(env[`${envPrefix}_COMPACT_MAX_JOURNAL_BYTES`]),
		records:
			values["compact-max-journal-records"] ??
			optionalEnvironmentValue(env[`${envPrefix}_COMPACT_MAX_JOURNAL_RECORDS`]),
	});
	if (values.worker) {
		if (values.output) throw new Error("worker mode does not accept --output");
		if (!values.scenario || !values.phase || !values.run || !values.directory) {
			throw new Error(
				"worker mode requires --scenario, --phase, --run, and --directory",
			);
		}
		if (!scenarios.includes(values.scenario)) {
			throw new Error(`Unknown ${scenarioLabel} scenario '${values.scenario}'`);
		}
		if (values.phase !== "seed" && values.phase !== "reopen") {
			throw new Error("worker phase must be seed or reopen");
		}
		return {
			mode: "worker",
			scenario: values.scenario,
			phase: values.phase,
			run: parsePositiveInteger(values.run, "run"),
			directory: values.directory,
			...workload,
			...compaction,
			...workerExtras(values),
		};
	}
	const workerKeys = ["scenario", "phase", "run", "directory", ...workerOnly];
	if (workerKeys.some((key) => values[key])) {
		throw new Error(`${workerKeys.join(", ")} are worker-only options`);
	}
	return {
		mode: "parent",
		...workload,
		...compaction,
		runs: parsePositiveInteger(
			values.runs ?? env[`${envPrefix}_RUNS`] ?? "1",
			"runs",
		),
		...((values.output ?? env[`${envPrefix}_OUTPUT`])
			? { output: values.output ?? env[`${envPrefix}_OUTPUT`] }
			: {}),
		json: values.json === true || env.BENCH_JSON === "1",
	};
};

export const reportProgress = ({
	expectedRows,
	rows,
	activeRow = null,
	failure = null,
}) => ({
	expectedRows,
	completedRows: rows.length,
	complete:
		rows.length === expectedRows && activeRow === null && failure === null,
	activeRow,
	...(failure ? { failure } : {}),
});

export const ratio = (numerator, denominator) =>
	denominator === 0 || denominator == null || numerator == null
		? null
		: numerator / denominator;

export const difference = (left, right) =>
	left == null || right == null ? null : left - right;

export const recordExactFields = (failures, state, groups) => {
	for (const [expected, fields] of groups) {
		for (const field of fields) {
			if (state[field] !== expected) {
				failures.push(`${field}=${state[field]}, expected ${expected}`);
			}
		}
	}
};

export const changedStateFields = (seed, reopen, fields) =>
	fields.filter((field) => seed[field] !== reopen[field]);

export const buildLifecycleResourceComparison = (
	fresh,
	history,
	rssPhase = "afterValidation",
) => {
	const freshDisk = fresh.reopen.disk;
	const historyDisk = history.reopen.disk;
	const freshRss = fresh.reopen.memory[rssPhase].rss;
	const historyRss = history.reopen.memory[rssPhase].rss;
	const freshMeasuredRssBytes = freshRss - fresh.reopen.memory.beforeOpen.rss;
	const historyMeasuredRssBytes =
		historyRss - history.reopen.memory.beforeOpen.rss;
	return {
		disk: {
			freshLogicalBytes: freshDisk.logicalBytes,
			historyLogicalBytes: historyDisk.logicalBytes,
			logicalDiskOverheadBytes:
				historyDisk.logicalBytes - freshDisk.logicalBytes,
			logicalGrowthRatio: ratio(
				historyDisk.logicalBytes,
				freshDisk.logicalBytes,
			),
			freshAllocatedBytes: freshDisk.allocatedBytes,
			historyAllocatedBytes: historyDisk.allocatedBytes,
			allocatedDiskOverheadBytes: difference(
				historyDisk.allocatedBytes,
				freshDisk.allocatedBytes,
			),
			allocatedGrowthRatio: ratio(
				historyDisk.allocatedBytes,
				freshDisk.allocatedBytes,
			),
		},
		reopen: {
			freshMs: fresh.reopen.openMs,
			historyMs: history.reopen.openMs,
			reopenMsDelta: history.reopen.openMs - fresh.reopen.openMs,
			growthRatio: ratio(history.reopen.openMs, fresh.reopen.openMs),
			freshRssBytes: freshRss,
			historyRssBytes: historyRss,
			reopenRssDeltaBytes: historyRss - freshRss,
			rssGrowthRatio: ratio(historyRss, freshRss),
			freshMeasuredRssBytes,
			historyMeasuredRssBytes,
			measuredRssDeltaBytes: historyMeasuredRssBytes - freshMeasuredRssBytes,
			measuredRssGrowthRatio: ratio(
				historyMeasuredRssBytes,
				freshMeasuredRssBytes,
			),
		},
	};
};

export const validateStableReopen = ({ seed, reopen, fields, label }) => {
	const changedStableFields = changedStateFields(
		seed.state,
		reopen.state,
		fields,
	);
	if (changedStableFields.length > 0) {
		throw new Error(
			`${label} reopen changed stable state: ${changedStableFields
				.map(
					(field) =>
						`${field} (${seed.state[field]} -> ${reopen.state[field]})`,
				)
				.join(", ")}`,
		);
	}
	return { comparedStableFields: fields, changedStableFields };
};

export const collectGarbage = () => {
	if (typeof globalThis.gc !== "function") {
		throw new Error("lifecycle-census workers require Node.js --expose-gc");
	}
	globalThis.gc();
	globalThis.gc();
};

export const memorySnapshot = () => {
	const usage = process.memoryUsage();
	return {
		rss: usage.rss,
		heapTotal: usage.heapTotal,
		heapUsed: usage.heapUsed,
		external: usage.external,
		arrayBuffers: usage.arrayBuffers,
	};
};

export const elapsed = (started) =>
	Math.round((performance.now() - started) * 1000) / 1000;

export const fingerprint = (values) => {
	const digest = createHash("sha256");
	for (const value of [...values].sort()) {
		digest.update(String(value));
		digest.update("\0");
	}
	return digest.digest("hex");
};

export const collectionSize = (value) =>
	value instanceof Map || value instanceof Set || Array.isArray(value)
		? (value.size ?? value.length)
		: 0;

export const optionalSize = (value) =>
	value instanceof Map || value instanceof Set || Array.isArray(value)
		? (value.size ?? value.length)
		: null;

export const mapEdges = (value) => {
	if (!(value instanceof Map)) return null;
	let count = 0;
	for (const nested of value.values()) count += collectionSize(nested);
	return count;
};

export const collectLifecycleDebt = (
	log,
	backbone,
	includeLiveRepair = false,
) => {
	const cleanup = log._gidPeerHistoryCleanupState;
	const writeThrough = log.remoteBlocks?.localStore;
	const warmup = log.joinWarmup;
	const active = (owner, key) =>
		owner != null && key in owner ? Number(Boolean(owner[key])) : null;
	const values = {
		pendingGidCleanup: cleanup?.pending.size ?? null,
		pendingIHave: optionalSize(log._pendingIHave),
		pendingIHaveCallbacks: optionalSize(log._pendingIHaveCallbacks),
		pendingMaturityOwners: optionalSize(log.pendingMaturity),
		pendingMaturityRanges: mapEdges(log.pendingMaturity),
		repairRetryTimers: optionalSize(log._repairRetryTimers),
		repairPendingModes: optionalSize(log._repairSweepPendingModes),
		repairPendingPeers: mapEdges(log._repairSweepPendingPeersByMode),
		repairFrontierTargets: mapEdges(log._repairFrontierByMode),
		repairFrontierActiveTargets: mapEdges(
			log._repairFrontierActiveTargetsByMode,
		),
		repairFrontierBypassKnownPeers: mapEdges(
			log._repairFrontierBypassKnownPeersByMode,
		),
		repairOptimisticGids: optionalSize(
			log._repairSweepOptimisticGidPeersPending,
		),
		repairOptimisticPeers: optionalSize(log._repairSweepOptimisticGidsByPeer),
		appendBackfillTargets: optionalSize(log._appendBackfillPendingByTarget),
		appendBackfillRows: mapEdges(log._appendBackfillPendingByTarget),
		checkedPrunePendingDeletes: optionalSize(log._checkedPrune?.pendingDeletes),
		checkedPruneRetries: optionalSize(log._checkedPrune?.retries),
		writeThroughPendingDurableWrites: optionalSize(
			writeThrough?.pendingDurableWrites,
		),
		writeThroughDeleteTombstones: optionalSize(
			writeThrough?.nativeDeleteTombstones,
		),
		writeThroughPendingDeleteCleanup: optionalSize(
			writeThrough?.pendingNativeDeleteCleanup,
		),
		writeThroughStagedDeleteBatches: optionalSize(
			writeThrough?.stagedNativeDeleteCleanups,
		),
		writeThroughCommitOwnerships: optionalSize(
			writeThrough?.nativeCommitOwnerships,
		),
		coordinatePendingJournalRows: backbone.coordinatePendingJournalLength,
		coordinatePendingJournalBytes: backbone.coordinatePendingJournalByteLength,
		documentPendingJournalRows: backbone.documentPendingJournalLength,
		documentPendingJournalBytes: backbone.documentPendingJournalByteLength,
		documentSignerPendingJournalRows:
			backbone.documentSignerPendingJournalLength,
		documentSignerPendingJournalBytes:
			backbone.documentSignerPendingJournalByteLength,
		...(includeLiveRepair
			? {
					repairSweepRunning: active(log, "_repairSweepRunning"),
					joinAuthoritativeRepairTimers: optionalSize(
						log._joinAuthoritativeRepairTimersByDelay,
					),
					joinAuthoritativeRepairPeers: mapEdges(
						log._joinAuthoritativeRepairPeersByDelay,
					),
					repairSweepWarmupSessions: optionalSize(
						warmup?._repairSweepWarmupSessionByTarget,
					),
					joinWarmupRetryTimers: mapEdges(
						warmup?._joinWarmupRetryTimersByTarget,
					),
					joinWarmupScheduledRetryTargets: optionalSize(
						warmup?._joinWarmupScheduledRetriesByTarget,
					),
					appendBackfillTimer: active(log, "_appendBackfillTimer"),
					checkedPruneAuditTimer: active(log, "_checkedPruneAuditTimer"),
				}
			: {}),
	};
	return {
		values,
		unobserved: Object.entries(values)
			.filter(([, value]) => value == null)
			.map(([field]) => field),
		nonzero: Object.entries(values)
			.filter(([, value]) => value != null && value !== 0)
			.map(([field, value]) => ({ field, value })),
	};
};

export const waitForLifecycleQuiescence = async (
	log,
	{ cleanup = true, storage = true, label = "lifecycle cleanup" } = {},
) => {
	const started = performance.now();
	for (let attempt = 0; attempt < 100; attempt++) {
		const cleanupState = cleanup ? log._gidPeerHistoryCleanupState : undefined;
		await cleanupState?.tail;
		if (storage) {
			await log._coordinates?.flushNativeBackboneCoordinateJournal?.();
			await log.log.blocks.waitForDurableWrites?.();
		}
		const writeThrough = storage ? log.remoteBlocks?.localStore : undefined;
		if (writeThrough?.nativeDeleteCleanupRunning) {
			await writeThrough.nativeDeleteCleanupRunning;
		}
		await new Promise((resolve) => setImmediate(resolve));
		if (
			(!cleanupState ||
				(cleanupState.pending.size === 0 && cleanupState.draining === false)) &&
			!writeThrough?.nativeDeleteCleanupRunning
		) {
			return elapsed(started);
		}
	}
	throw new Error(`${label} did not quiesce`);
};

export const diskFootprint = async (root) => {
	const files = [];
	const visit = async (directory) => {
		for (const entry of await readdir(directory, { withFileTypes: true })) {
			const path = join(directory, entry.name);
			if (entry.isDirectory()) await visit(path);
			else if (entry.isFile()) {
				const stats = await lstat(path);
				const relativePath = relative(root, path).split(sep).join("/");
				files.push({
					path: relativePath,
					category: classifyLifecycleCensusFile(relativePath),
					logicalBytes: stats.size,
					allocatedBytes:
						typeof stats.blocks === "number" ? stats.blocks * 512 : null,
				});
			}
		}
	};
	await visit(root);
	files.sort((left, right) => left.path.localeCompare(right.path));
	const categories = {};
	for (const file of files) {
		const category = (categories[file.category] ??= {
			files: 0,
			logicalBytes: 0,
			allocatedBytes: 0,
		});
		category.files++;
		category.logicalBytes += file.logicalBytes;
		category.allocatedBytes =
			category.allocatedBytes == null || file.allocatedBytes == null
				? null
				: category.allocatedBytes + file.allocatedBytes;
	}
	const allocatedValues = files.map((file) => file.allocatedBytes);
	return {
		fileCount: files.length,
		logicalBytes: files.reduce((sum, file) => sum + file.logicalBytes, 0),
		allocatedBytes: allocatedValues.every((value) => value != null)
			? allocatedValues.reduce((sum, value) => sum + value, 0)
			: null,
		categories,
		files,
	};
};

export const loadLifecycleRuntime = async () => {
	const load = (path) => import(new URL(path, import.meta.url));
	const [peerbit, rust, document, testData, nativeBackbone, log] =
		await Promise.all(
			[
				"../../packages/clients/peerbit/dist/src/index.js",
				"../../packages/clients/peerbit/dist/src/rust.js",
				"../../packages/programs/data/document/document/dist/src/index.js",
				"../../packages/programs/data/document/document/dist/test/data.js",
				"../../packages/utils/native-backbone/dist/src/index.js",
				"../../packages/log/dist/src/index.js",
			].map(load),
		);
	return {
		Peerbit: peerbit.Peerbit,
		createRustPeerbitOptions: rust.createRustPeerbitOptions,
		Documents: document.Documents,
		policy: document.policy,
		transform: document.transform,
		Document: testData.Document,
		TestStore: testData.TestStore,
		NativeBackboneNodeCoordinatePersistence:
			nativeBackbone.NativeBackboneNodeCoordinatePersistence,
		EntryType: log.EntryType,
	};
};

export const createLifecycleStore = ({ Documents, TestStore }, id) => {
	const store = new TestStore({ docs: new Documents({ id }) });
	store.id = id;
	return store;
};

export const lifecycleOpenArgs = ({
	directory,
	retain,
	Document,
	policy,
	transform,
	NativeBackboneNodeCoordinatePersistence,
	compactMaxJournalBytes,
	compactMaxJournalRecords,
}) => ({
	mode: "native",
	replicate: { factor: 1 },
	timeUntilRoleMaturity: 0,
	...(retain != null ? { log: { trim: { type: "length", to: retain } } } : {}),
	nativeGraph: true,
	nativeBackbone: {
		optional: false,
		documentIndex: true,
		coordinatePersistence: new NativeBackboneNodeCoordinatePersistence(
			join(directory, "coordinate-wal"),
			{
				flushOnAppend: true,
				...(compactMaxJournalBytes != null ? { compactMaxJournalBytes } : {}),
				...(compactMaxJournalRecords != null
					? { compactMaxJournalRecords }
					: {}),
			},
		),
	},
	canPerform: policy.allowAll(),
	index: { type: Document, transform: transform.identity() },
});

export const closeLifecycleRuntime = async (store, client) => {
	const programStarted = performance.now();
	await store.close();
	const programMs = elapsed(programStarted);
	const clientStarted = performance.now();
	await client.stop();
	return { programMs, clientMs: elapsed(clientStarted) };
};

export const runOpenedLifecycleWorker = async ({
	options,
	storeId,
	peakRssField,
	work,
}) => {
	const runtime = await loadLifecycleRuntime();
	const clientStarted = performance.now();
	let client = await runtime.Peerbit.create({
		directory: options.directory,
		...runtime.createRustPeerbitOptions(),
	});
	const clientCreateMs = elapsed(clientStarted);
	collectGarbage();
	const beforeOpen = memorySnapshot();
	const store = createLifecycleStore(runtime, storeId);
	let closed = false;
	try {
		const openStarted = performance.now();
		await client.open(store, {
			args: lifecycleOpenArgs({ ...runtime, ...options }),
		});
		const openMs = elapsed(openStarted);
		collectGarbage();
		const afterOpen = memorySnapshot();
		const measured = await work({ runtime, client, store });
		collectGarbage();
		const afterValidation = memorySnapshot();
		const close = await closeLifecycleRuntime(store, client);
		closed = true;
		client = undefined;
		return {
			phase: options.phase,
			scenario: options.scenario,
			run: options.run,
			...measured,
			clientCreateMs,
			openMs,
			close,
			memory: { beforeOpen, afterOpen, afterValidation },
			[peakRssField]: process.resourceUsage().maxRSS * 1024,
			disk: await diskFootprint(options.directory),
		};
	} finally {
		if (!closed) await client?.stop();
	}
};

export const runJsonWorker = ({
	scriptPath,
	args,
	description,
	maxBuffer,
	streamStderr = false,
}) => {
	const child = spawnSync(
		process.execPath,
		["--expose-gc", scriptPath, ...args],
		{
			encoding: "utf8",
			env: process.env,
			maxBuffer,
			...(streamStderr ? { stdio: ["ignore", "pipe", "inherit"] } : {}),
		},
	);
	if (child.status !== 0) {
		throw new Error(`${description}\n${child.stderr || child.stdout}`);
	}
	try {
		return JSON.parse(child.stdout);
	} catch (error) {
		throw new Error(`${description} produced invalid JSON: ${child.stdout}`, {
			cause: error,
		});
	}
};

export const lifecycleWorkerArgs = (options, workloadOptions) => {
	const args = [
		"--worker",
		"--scenario",
		options.scenario,
		"--phase",
		options.phase,
		"--run",
		String(options.run),
		"--directory",
		options.directory,
	];
	for (const [name, key] of workloadOptions) {
		args.push(`--${name}`, String(options[key]));
	}
	for (const [name, value] of [
		["compact-max-journal-bytes", options.compactMaxJournalBytes],
		["compact-max-journal-records", options.compactMaxJournalRecords],
	]) {
		if (value != null) args.push(`--${name}`, String(value));
	}
	return args;
};

export const runLifecycleScenario = async ({
	options,
	scenario,
	run,
	root,
	runWorker,
	reopenOptions = () => ({}),
	onPhase = async () => {},
	stableFields,
	validation,
}) => {
	const directory = join(root, scenario);
	await mkdir(directory, { recursive: true });
	const common = { ...options, scenario, run, directory };
	const seed = runWorker({ ...common, phase: "seed" });
	await onPhase(scenario, "seed", seed);
	const reopen = runWorker({
		...common,
		phase: "reopen",
		...reopenOptions(seed),
	});
	await onPhase(scenario, "reopen", reopen);
	const stable = validateStableReopen({
		seed,
		reopen,
		fields: stableFields,
		label: scenario,
	});
	return { scenario, seed, reopen, validation: validation(stable) };
};

export const withMatchedScenarios = async ({
	prefix,
	options,
	run,
	order,
	runScenario,
}) => {
	const root = await mkdtemp(join(os.tmpdir(), prefix));
	try {
		const scenarios = {};
		for (const scenario of order) {
			scenarios[scenario] = await runScenario(options, scenario, run, root);
		}
		return scenarios;
	} finally {
		await rm(root, { recursive: true, force: true });
	}
};

export const lifecycleHostMetadata = () => ({
	node: process.version,
	v8: process.versions.v8,
	platform: process.platform,
	arch: process.arch,
	cpu: os.cpus()[0]?.model ?? "unknown",
	logicalCpus: os.cpus().length,
	totalMemoryBytes: os.totalmem(),
});

export const writeLifecycleReport = async (path, report) => {
	const destination = resolve(path);
	await mkdir(dirname(destination), { recursive: true });
	const temporary = `${destination}.${process.pid}.tmp`;
	await writeFile(temporary, `${JSON.stringify(report, null, 2)}\n`);
	await rename(temporary, destination);
};

export const runCheckpointedCensus = async ({
	options,
	scenarios,
	buildReport,
	runRow,
	renderHuman,
	logRun,
	onCheckpoint,
	preserveActiveOnFailure = false,
}) => {
	const rows = [];
	let activeRow = null;
	let failure = null;
	const host = lifecycleHostMetadata();
	const report = () =>
		buildReport({ ...options, rows, host, activeRow, failure });
	const checkpoint = async () => {
		const current = report();
		await onCheckpoint?.(current);
		if (options.output) await writeLifecycleReport(options.output, current);
	};
	const setActive = async (value) => {
		activeRow = value;
		await checkpoint();
	};
	await checkpoint();
	for (let run = 1; run <= options.runs; run++) {
		await setActive({ run, scenarios });
		logRun(run);
		try {
			rows.push(await runRow(options, run, setActive));
		} catch (error) {
			failure = {
				run,
				message: error instanceof Error ? error.message : String(error),
			};
			if (!preserveActiveOnFailure) activeRow = null;
			await checkpoint();
			throw error;
		}
		await setActive(null);
	}
	const completed = report();
	if (options.json) console.log(JSON.stringify(completed, null, 2));
	else renderHuman(completed);
	return completed;
};
