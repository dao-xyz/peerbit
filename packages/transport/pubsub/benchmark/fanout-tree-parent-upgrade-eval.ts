import {
	runFanoutTreeSim,
	type FanoutTreeSimParams,
	type FanoutTreeSimResult,
} from "./fanout-tree-sim-lib.js";

type EvalScenarioName = "mild" | "tight" | "live-like";
type EvalMode = "off" | "on";

type MetricKey =
	| "joinedPct"
	| "deliveredPct"
	| "deliveredWithinDeadlinePct"
	| "latencyP95"
	| "treeLevelP95"
	| "formationScore"
	| "maintReparentsPerMin"
	| "maintRecoveryP95Ms"
	| "trackerBpp"
	| "repairBpp"
	| "protocolControlBytesSentTracker"
	| "treeRootChildren";

type ScenarioSummary = {
	seeds: number[];
	off: Record<MetricKey, number>;
	on: Record<MetricKey, number>;
	delta: Record<MetricKey, number>;
};

const HELP_TEXT = [
	"fanout-tree-parent-upgrade-eval.ts",
	"",
	"Runs an A/B fanout-tree simulation sweep with parent upgrades disabled and enabled.",
	"",
	"Args:",
	"  --scenario NAME     scenario to run (mild|tight|live-like|all, default: all)",
	"  --seeds LIST        comma-separated seeds (default: 1,2,3)",
	"  --parentUpgradeIntervalMs MS  upgrade interval for the enabled variant (default: 200)",
	"  --parentUpgradeLeafOnly 0|1   restrict upgrades to leaves (default: 1)",
	"",
	"Example:",
	"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario all --seeds 1,2,3",
].join("\n");

const METRICS: MetricKey[] = [
	"joinedPct",
	"deliveredPct",
	"deliveredWithinDeadlinePct",
	"latencyP95",
	"treeLevelP95",
	"formationScore",
	"maintReparentsPerMin",
	"maintRecoveryP95Ms",
	"trackerBpp",
	"repairBpp",
	"protocolControlBytesSentTracker",
	"treeRootChildren",
];

const SCENARIOS: Record<EvalScenarioName, Partial<FanoutTreeSimParams>> = {
	mild: {
		nodes: 40,
		bootstraps: 1,
		subscribers: 30,
		relayFraction: 0.35,
		messages: 40,
		msgRate: 50,
		msgSize: 64,
		settleMs: 5_000,
		deadlineMs: 500,
		timeoutMs: 40_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 64,
		relayMaxChildren: 32,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.1,
		churnEveryMs: 200,
		churnDownMs: 100,
		churnFraction: 0.05,
	},
	tight: {
		nodes: 60,
		bootstraps: 1,
		subscribers: 45,
		relayFraction: 0.3,
		messages: 50,
		msgRate: 40,
		msgSize: 64,
		settleMs: 4_000,
		deadlineMs: 500,
		timeoutMs: 50_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 8,
		relayMaxChildren: 8,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.05,
		churnEveryMs: 300,
		churnDownMs: 120,
		churnFraction: 0.04,
	},
	"live-like": {
		nodes: 120,
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		subscribers: 90,
		relayFraction: 0.25,
		messages: 180,
		msgRate: 30,
		msgSize: 1024,
		settleMs: 2_000,
		deadlineMs: 2_000,
		timeoutMs: 120_000,
		allowKick: true,
		bidPerByteRelay: 1,
		bidPerByteLeaf: 0,
		repair: true,
		repairMaxBackfillMessages: 60,
		neighborRepair: true,
		neighborRepairPeers: 3,
		joinPhases: true,
		joinPhaseSettleMs: 2_000,
		rootUploadLimitBps: 20_000_000,
		relayUploadLimitBps: 10_000_000,
		rootMaxChildren: 64,
		relayMaxChildren: 32,
		dropDataFrameRate: 0.01,
		churnEveryMs: 2_000,
		churnDownMs: 1_000,
		churnFraction: 0.01,
	},
};

const avg = (rows: FanoutTreeSimResult[], metric: MetricKey) =>
	rows.reduce((sum, row) => sum + row[metric], 0) / rows.length;

const pickMetrics = (rows: FanoutTreeSimResult[]) =>
	Object.fromEntries(
		METRICS.map((metric) => [metric, avg(rows, metric)]),
	) as Record<MetricKey, number>;

const parseArgs = () => {
	const argv = process.argv.slice(2);
	const args = argv[0] === "--" ? argv.slice(1) : argv;

	const readArg = (name: string) => {
		const index = args.indexOf(name);
		return index >= 0 ? args[index + 1] : undefined;
	};

	const scenarioArg = readArg("--scenario") ?? "all";
	if (scenarioArg !== "all" && !(scenarioArg in SCENARIOS)) {
		throw new Error(
			`Unknown scenario '${scenarioArg}'. Expected mild, tight, live-like, or all.`,
		);
	}

	const seedsArg = readArg("--seeds") ?? "1,2,3";
	const seeds = seedsArg
		.split(",")
		.map((value) => Number(value.trim()))
		.filter((value) => Number.isFinite(value) && value > 0);
	if (!seeds.length) {
		throw new Error(`Invalid --seeds value '${seedsArg}'.`);
	}

	return {
		scenarios:
			scenarioArg === "all"
				? (Object.keys(SCENARIOS) as EvalScenarioName[])
				: [scenarioArg as EvalScenarioName],
		seeds,
		parentUpgradeIntervalMs: Number(
			readArg("--parentUpgradeIntervalMs") ?? 200,
		),
		parentUpgradeLeafOnly:
			(readArg("--parentUpgradeLeafOnly") ?? "1") !== "0",
	};
};

const runScenario = async (
	name: EvalScenarioName,
	seeds: number[],
	parentUpgradeIntervalMs: number,
	parentUpgradeLeafOnly: boolean,
) => {
	const scenario = SCENARIOS[name];
	const byMode: Record<EvalMode, FanoutTreeSimResult[]> = {
		off: [],
		on: [],
	};

	for (const seed of seeds) {
		byMode.off.push(await runFanoutTreeSim({ ...scenario, seed }));
		byMode.on.push(
			await runFanoutTreeSim({
				...scenario,
				seed,
				parentUpgradeIntervalMs,
				parentUpgradeLeafOnly,
			}),
		);
	}

	const off = pickMetrics(byMode.off);
	const on = pickMetrics(byMode.on);
	const delta = Object.fromEntries(
		METRICS.map((metric) => [metric, on[metric] - off[metric]]),
	) as Record<MetricKey, number>;

	return {
		seeds,
		off,
		on,
		delta,
	} satisfies ScenarioSummary;
};

if (process.argv.includes("--help") || process.argv.includes("-h")) {
	console.log(HELP_TEXT);
	process.exit(0);
}

const { scenarios, seeds, parentUpgradeIntervalMs, parentUpgradeLeafOnly } =
	parseArgs();

const results = Object.fromEntries(
	await Promise.all(
		scenarios.map(async (scenario) => [
			scenario,
			await runScenario(
				scenario,
				seeds,
				parentUpgradeIntervalMs,
				parentUpgradeLeafOnly,
			),
		]),
	),
);

console.log(JSON.stringify(results, null, 2));
