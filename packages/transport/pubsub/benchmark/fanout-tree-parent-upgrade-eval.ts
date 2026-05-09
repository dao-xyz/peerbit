/**
 * A/B evidence harness for proactive FanoutTree parent upgrades.
 *
 * This intentionally runs the same scenario and seed twice: first with the
 * default stability-first behavior, then with bounded parent upgrades enabled.
 */
import {
	type FanoutTreeSimParams,
	type FanoutTreeSimResult,
	formatFanoutTreeSimResult,
	runFanoutTreeSim,
} from "./fanout-tree-sim-lib.js";

type ScenarioName = "ci-small" | "ci-loss";

type EvalArgs = {
	scenarios: ScenarioName[];
	seeds: number[];
	parentUpgradeIntervalMs: number;
	parentUpgradeLeafOnly: boolean;
	maxCostRatio: number;
	maxReparentsPerMin: number;
	maxReparentsPerPeer: number;
	maxOrphanAreaRatio: number;
	strict: boolean;
};

type Failure = {
	metric: string;
	baseline: number;
	upgrade: number;
	limit: number;
};

const HELP_TEXT = [
	"fanout-tree-parent-upgrade-eval.ts",
	"",
	"Args:",
	"  --scenario NAME              scenario to run (ci-small|ci-loss|all, default: all)",
	"  --seeds CSV                  seeds to run for each scenario (default: 1,2,3)",
	"  --parentUpgradeIntervalMs MS upgrade check interval for treatment run (default: 1000)",
	"  --parentUpgradeLeafOnly 0|1  restrict treatment upgrades to leaves (default: 1)",
	"  --maxCostRatio R             max treatment/base ratio for control/tracker/repair bpp (default: 1.15)",
	"  --maxReparentsPerMin N       max treatment reparent events per minute (default: 500)",
	"  --maxReparentsPerPeer N      max treatment reparent events for one peer (default: 20)",
	"  --maxOrphanAreaRatio R       max treatment/base ratio for orphan area (default: 1.15)",
	"  --strict 0|1                 exit non-zero on evidence failure (default: 0)",
	"",
	"Example:",
	"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-eval --scenario ci-small --seeds 1,2,3",
].join("\n");

const SCENARIOS: Record<ScenarioName, Partial<FanoutTreeSimParams>> = {
	"ci-small": {
		nodes: 25,
		bootstraps: 1,
		subscribers: 20,
		relayFraction: 0.3,
		candidateScoringMode: "weighted",
		messages: 20,
		msgRate: 50,
		msgSize: 64,
		settleMs: 500,
		deadlineMs: 500,
		timeoutMs: 20_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 64,
		relayMaxChildren: 32,
		dropDataFrameRate: 0,
	},
	"ci-loss": {
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
};

const parseBool01 = (value: string | undefined, fallback: boolean) => {
	if (value === undefined) return fallback;
	return value === "1";
};

const parseCsvNumbers = (value: string | undefined, fallback: number[]) => {
	if (!value) return fallback;
	const parsed = value
		.split(",")
		.map((part) => Number(part.trim()))
		.filter((part) => Number.isFinite(part));
	return parsed.length > 0 ? parsed : fallback;
};

const parseScenarios = (value: string | undefined): ScenarioName[] => {
	if (!value || value === "all") return ["ci-small", "ci-loss"];
	const parsed = value
		.split(",")
		.map((part) => part.trim())
		.filter(
			(part): part is ScenarioName => part === "ci-small" || part === "ci-loss",
		);
	if (parsed.length === 0) {
		throw new Error(`Unknown scenario: ${value}`);
	}
	return parsed;
};

const parseArgs = (argv: string[]): EvalArgs => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		return idx === -1 ? undefined : argv[idx + 1];
	};

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(HELP_TEXT);
		process.exit(0);
	}

	return {
		scenarios: parseScenarios(get("--scenario")),
		seeds: parseCsvNumbers(get("--seeds"), [1, 2, 3]),
		parentUpgradeIntervalMs: Number(get("--parentUpgradeIntervalMs") ?? 1_000),
		parentUpgradeLeafOnly: parseBool01(get("--parentUpgradeLeafOnly"), true),
		maxCostRatio: Number(get("--maxCostRatio") ?? 1.15),
		maxReparentsPerMin: Number(get("--maxReparentsPerMin") ?? 500),
		maxReparentsPerPeer: Number(get("--maxReparentsPerPeer") ?? 20),
		maxOrphanAreaRatio: Number(get("--maxOrphanAreaRatio") ?? 1.15),
		strict: parseBool01(get("--strict"), false),
	};
};

const ratioLimit = (baseline: number, ratio: number, absoluteSlack = 1e-9) =>
	Math.max(absoluteSlack, baseline * ratio + absoluteSlack);

const failIfGreater = (
	failures: Failure[],
	metric: string,
	baseline: number,
	upgrade: number,
	limit: number,
) => {
	if (!Number.isFinite(baseline) || !Number.isFinite(upgrade)) return;
	if (upgrade > limit) {
		failures.push({ metric, baseline, upgrade, limit });
	}
};

const failIfLess = (
	failures: Failure[],
	metric: string,
	baseline: number,
	upgrade: number,
	limit: number,
) => {
	if (!Number.isFinite(baseline) || !Number.isFinite(upgrade)) return;
	if (upgrade < limit) {
		failures.push({ metric, baseline, upgrade, limit });
	}
};

const evaluateRun = (
	params: FanoutTreeSimParams,
	baseline: FanoutTreeSimResult,
	upgrade: FanoutTreeSimResult,
	args: EvalArgs,
) => {
	const failures: Failure[] = [];

	failIfGreater(
		failures,
		"formationScore",
		baseline.formationScore,
		upgrade.formationScore,
		baseline.formationScore,
	);
	failIfGreater(
		failures,
		"treeLevelP95",
		baseline.treeLevelP95,
		upgrade.treeLevelP95,
		baseline.treeLevelP95,
	);
	failIfGreater(
		failures,
		"formationStretchP95",
		baseline.formationStretchP95,
		upgrade.formationStretchP95,
		baseline.formationStretchP95,
	);
	failIfLess(
		failures,
		"deliveredWithinDeadlinePct",
		baseline.deliveredWithinDeadlinePct,
		upgrade.deliveredWithinDeadlinePct,
		baseline.deliveredWithinDeadlinePct,
	);

	failIfGreater(
		failures,
		"controlBpp",
		baseline.controlBpp,
		upgrade.controlBpp,
		ratioLimit(baseline.controlBpp, args.maxCostRatio, 0.001),
	);
	failIfGreater(
		failures,
		"trackerBpp",
		baseline.trackerBpp,
		upgrade.trackerBpp,
		ratioLimit(baseline.trackerBpp, args.maxCostRatio, 0.001),
	);
	failIfGreater(
		failures,
		"repairBpp",
		baseline.repairBpp,
		upgrade.repairBpp,
		ratioLimit(baseline.repairBpp, args.maxCostRatio, 0.001),
	);
	failIfGreater(
		failures,
		"maintReparentsPerMin",
		baseline.maintReparentsPerMin,
		upgrade.maintReparentsPerMin,
		args.maxReparentsPerMin,
	);
	failIfGreater(
		failures,
		"maintMaxReparentsPerPeer",
		baseline.maintMaxReparentsPerPeer,
		upgrade.maintMaxReparentsPerPeer,
		args.maxReparentsPerPeer,
	);
	failIfGreater(
		failures,
		"maintOrphanArea",
		baseline.maintOrphanArea,
		upgrade.maintOrphanArea,
		ratioLimit(baseline.maintOrphanArea, args.maxOrphanAreaRatio, 1),
	);
	failIfGreater(
		failures,
		"treeRootChildren",
		baseline.treeRootChildren,
		upgrade.treeRootChildren,
		params.rootMaxChildren,
	);

	return failures;
};

const printComparison = (
	scenario: ScenarioName,
	seed: number,
	baseline: FanoutTreeSimResult,
	upgrade: FanoutTreeSimResult,
	failures: Failure[],
) => {
	const delta = (after: number, before: number) =>
		Number.isFinite(after) && Number.isFinite(before) ? after - before : NaN;

	console.log(
		[
			`parent-upgrade-eval scenario=${scenario} seed=${seed} viable=${failures.length === 0}`,
			`  formationScore ${baseline.formationScore.toFixed(2)} -> ${upgrade.formationScore.toFixed(2)} delta=${delta(upgrade.formationScore, baseline.formationScore).toFixed(2)}`,
			`  treeLevelP95 ${baseline.treeLevelP95.toFixed(1)} -> ${upgrade.treeLevelP95.toFixed(1)} delta=${delta(upgrade.treeLevelP95, baseline.treeLevelP95).toFixed(1)}`,
			`  formationStretchP95 ${baseline.formationStretchP95.toFixed(2)} -> ${upgrade.formationStretchP95.toFixed(2)} delta=${delta(upgrade.formationStretchP95, baseline.formationStretchP95).toFixed(2)}`,
			`  deliveredWithinDeadlinePct ${baseline.deliveredWithinDeadlinePct.toFixed(2)} -> ${upgrade.deliveredWithinDeadlinePct.toFixed(2)} delta=${delta(upgrade.deliveredWithinDeadlinePct, baseline.deliveredWithinDeadlinePct).toFixed(2)}`,
			`  bpp control ${baseline.controlBpp.toFixed(4)} -> ${upgrade.controlBpp.toFixed(4)} tracker ${baseline.trackerBpp.toFixed(4)} -> ${upgrade.trackerBpp.toFixed(4)} repair ${baseline.repairBpp.toFixed(4)} -> ${upgrade.repairBpp.toFixed(4)}`,
			`  maintenance reparentsPerMin ${baseline.maintReparentsPerMin.toFixed(2)} -> ${upgrade.maintReparentsPerMin.toFixed(2)} maxReparentsPerPeer ${baseline.maintMaxReparentsPerPeer} -> ${upgrade.maintMaxReparentsPerPeer} orphanArea ${baseline.maintOrphanArea.toFixed(1)} -> ${upgrade.maintOrphanArea.toFixed(1)}`,
			`  rootChildren ${baseline.treeRootChildren} -> ${upgrade.treeRootChildren} proactiveUpgrades=${upgrade.reparentUpgradeTotal}`,
			...(failures.length > 0
				? failures.map(
						(f) =>
							`  FAIL ${f.metric}: baseline=${f.baseline.toFixed(4)} upgrade=${f.upgrade.toFixed(4)} limit=${f.limit.toFixed(4)}`,
					)
				: []),
		].join("\n"),
	);
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	let failureCount = 0;

	for (const scenario of args.scenarios) {
		for (const seed of args.seeds) {
			const baseParams = {
				...SCENARIOS[scenario],
				seed,
				parentUpgradeIntervalMs: 0,
			};
			const upgradeParams = {
				...SCENARIOS[scenario],
				seed,
				parentUpgradeIntervalMs: args.parentUpgradeIntervalMs,
				parentUpgradeLeafOnly: args.parentUpgradeLeafOnly,
			};

			console.log(`\n[baseline] scenario=${scenario} seed=${seed}`);
			const baseline = await runFanoutTreeSim(baseParams);
			console.log(formatFanoutTreeSimResult(baseline));

			console.log(`\n[parent-upgrade] scenario=${scenario} seed=${seed}`);
			const upgrade = await runFanoutTreeSim(upgradeParams);
			console.log(formatFanoutTreeSimResult(upgrade));

			const failures = evaluateRun(upgrade.params, baseline, upgrade, args);
			printComparison(scenario, seed, baseline, upgrade, failures);
			failureCount += failures.length;
		}
	}

	if (args.strict && failureCount > 0) {
		process.exit(2);
	}
};

try {
	await main();
} catch (err: any) {
	console.error(err?.message ?? String(err));
	process.exit(1);
}
