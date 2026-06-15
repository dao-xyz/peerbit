#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	DEFAULT_PARENT_UPGRADE_FAST_SEED_CSV,
	DEFAULT_PARENT_UPGRADE_SEED_CSV,
	defaultCandidateArgs,
} from "./fanout-tree-parent-upgrade-preset.js";

const args = process.argv.slice(2);
const benchmarkDir = fileURLToPath(new URL(".", import.meta.url));
const packageRoot = resolve(benchmarkDir, "..");
const repoRoot = resolve(packageRoot, "../../..");

const getArg = (name: string, fallback?: string) => {
	const index = args.indexOf(name);
	return index === -1 ? fallback : args[index + 1];
};

const hasFlag = (name: string) => args.includes(name);

const usage = () => {
	console.log(
		[
			"fanout-tree-parent-upgrade-default-ready.ts",
			"",
			"Runs the bounded PR/default-readiness gate for fanout parent upgrades.",
			"",
			"Options:",
			"  --outDir DIR             output directory (default: sim-results/fanout-parent-upgrade-default-ready)",
			"  --seeds CSV              seeds for live safety runs (default: 1)",
			"  --idle-safety-seeds CSV  seeds for idle safety/timing runs (default: 1,2,3 unless --seeds is explicit)",
			"  --benefit-seeds CSV      deprecated alias for --idle-safety-seeds",
			"  --no-build               skip the @peerbit/pubsub build step",
			"  --help                   show this message",
			"",
			"Example:",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-default-ready --no-build --seeds 1 --idle-safety-seeds 1",
		].join("\n"),
	);
};

if (hasFlag("--help") || hasFlag("-h")) {
	usage();
	process.exit(0);
}

const pnpm = process.env.PNPM ?? "pnpm";
const skipBuild = hasFlag("--no-build") || process.env.FANOUT_SKIP_BUILD === "1";
const outDir = resolve(
	repoRoot,
	getArg(
		"--outDir",
		process.env.FANOUT_PARENT_UPGRADE_OUT_DIR ??
			"sim-results/fanout-parent-upgrade-default-ready",
	)!,
);
mkdirSync(outDir, { recursive: true });

const jsonOut = (name: string) => [
	"--jsonOut",
	resolve(outDir, `${name}.json`),
];

const pubsubBench = (benchmark: string, ...benchmarkArgs: string[]) => [
	"-C",
	"packages/transport/pubsub",
	"run",
	"bench",
	"--",
	benchmark,
	...benchmarkArgs,
];

const run = (label: string, commandArgs: string[]) => {
	console.log(`\n[fanout-default-ready] ${label}`);
	console.log(`${pnpm} ${commandArgs.join(" ")}`);
	const result = spawnSync(pnpm, commandArgs, {
		cwd: repoRoot,
		stdio: "inherit",
		env: process.env,
	});
	if (result.error) {
		console.error(result.error.message);
		process.exit(1);
	}
	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
};

const runPubsubBench = (
	label: string,
	benchmark: string,
	...benchmarkArgs: string[]
) => {
	run(label, pubsubBench(benchmark, ...benchmarkArgs));
};

if (!skipBuild) {
	run("build pubsub", ["--filter", "@peerbit/pubsub", "build"]);
}

const explicitSeeds =
	args.includes("--seeds") || process.env.FANOUT_PARENT_UPGRADE_SEEDS != null;
const seeds = getArg(
	"--seeds",
	process.env.FANOUT_PARENT_UPGRADE_SEEDS ??
		DEFAULT_PARENT_UPGRADE_FAST_SEED_CSV,
)!;
const idleSafetySeeds = getArg(
	"--idle-safety-seeds",
	getArg(
		"--benefit-seeds",
		process.env.FANOUT_PARENT_UPGRADE_IDLE_SAFETY_SEEDS ??
			process.env.FANOUT_PARENT_UPGRADE_BENEFIT_SEEDS ??
			(explicitSeeds ? seeds : DEFAULT_PARENT_UPGRADE_SEED_CSV),
	),
)!;

runPubsubBench(
	"active shadow dual-path mechanism gate",
	"fanout-tree-sim",
	"--nodes",
	"42",
	"--bootstraps",
	"1",
	"--subscribers",
	"32",
	"--relayFraction",
	"0.5",
	"--candidateScoringMode",
	"weighted",
	"--joinConcurrency",
	"1",
	"--joinPhases",
	"1",
	"--joinPhaseSettleMs",
	"300",
	"--messages",
	"180",
	"--msgRate",
	"60",
	"--msgSize",
	"128",
	"--streamRxDelayMs",
	"1",
	"--settleMs",
	"2000",
	"--deadlineMs",
	"1000",
	"--timeoutMs",
	"60000",
	"--trackerQueryIntervalMs",
	"500",
	"--repair",
	"1",
	"--rootUploadLimitBps",
	"100000000",
	"--relayUploadLimitBps",
	"100000000",
	"--rootMaxChildren",
	"2",
	"--relayMaxChildren",
	"4",
	"--dropDataFrameRate",
	"0",
	"--churnEveryMs",
	"0",
	"--lateRootConnectAfterMs",
	"750",
	"--lateRootDuringPublish",
	"1",
	"--lateRootMaxChildren",
	"12",
	"--lateRootConnectFraction",
	"0.6",
	"--parentUpgradeIntervalMs",
	"250",
	"--parentUpgradeLeafOnly",
	"0",
	"--parentUpgradeMinLevelGain",
	"1",
	"--parentUpgradeRootMinLevelGain",
	"1",
	"--parentUpgradeRootMinSubtreeGain",
	"1",
	"--parentUpgradeNonRootMinLevelGain",
	"1",
	"--parentUpgradeMinFreeSlots",
	"0",
	"--parentUpgradeRootMinFreeSlots",
	"0",
	"--parentUpgradeMaxChildLoadRatio",
	"1",
	"--parentUpgradeRootMaxChildLoadRatio",
	"1",
	"--parentUpgradeCooldownMs",
	"500",
	"--parentUpgradeQuietMs",
	"0",
	"--parentUpgradeRepairQuietMs",
	"0",
	"--parentUpgradeMaxPerPeer",
	"1",
	"--parentUpgradeRepairGuard",
	"0",
	"--parentUpgradeDataGuard",
	"0",
	"--parentUpgradeStaleRootProbeProbability",
	"1",
	"--parentProbeTimeoutMs",
	"300",
	"--parentProbeMaxPerRound",
	"1",
	"--parentProbeMaxLagMessages",
	"100",
	"--parentShadowObserveMs",
	"0",
	"--parentShadowMinObservations",
	"1",
	"--parentShadowDualPathMs",
	"1000",
	"--parentShadowDualPathMinMessages",
	"1",
	"--assertMinJoinedPct",
	"99",
	"--assertMinDeliveryPct",
	"99",
	"--assertMinDeadlineDeliveryPct",
	"95",
	"--assertMaxOverheadFactor",
	"1.5",
	"--assertMinReparentUpgradeTotal",
	"1",
	"--assertMinActiveShadowPromoteTotal",
	"1",
);

runPubsubBench(
	"single-writer live default-candidate safety",
	"fanout-tree-parent-upgrade-eval",
	"--scenario",
	"ci-live-stream",
	"--seeds",
	seeds,
	...defaultCandidateArgs(),
	"--maxDataOverheadRatio",
	"1.05",
	...jsonOut("single-live"),
	"--strict",
	"1",
);

runPubsubBench(
	"multi-writer live default-candidate safety",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-live,ci-multi-live-churn,ci-multi-video-live",
	"--seeds",
	seeds,
	...defaultCandidateArgs(),
	"--maxDataOverheadRatio",
	"1.05",
	...jsonOut("multi-live"),
	"--strict",
	"1",
);

runPubsubBench(
	"multi-writer idle default-candidate safety",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-idle,ci-multi-sparse-idle",
	"--seeds",
	idleSafetySeeds,
	...defaultCandidateArgs(),
	"--maxUsefulIdleDeadlinePctDelta",
	"2",
	"--maxDataOverheadRatio",
	"1.05",
	...jsonOut("multi-idle"),
	"--strict",
	"1",
);

runPubsubBench(
	"multi-writer hotspot idle timing evidence",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-hotspot-idle",
	"--seeds",
	idleSafetySeeds,
	...defaultCandidateArgs(),
	"--maxUsefulIdleDeadlinePctDelta",
	"2",
	"--maxDataOverheadRatio",
	"1.05",
	...jsonOut("multi-hotspot-idle"),
	"--strict",
	"0",
);

runPubsubBench(
	"multi-writer slow hotspot timing evidence",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-hotspot-idle",
	"--seeds",
	idleSafetySeeds,
	...defaultCandidateArgs(),
	"--streamRxDelayMs",
	"12",
	"--maxUsefulIdleDeadlinePctDelta",
	"2",
	"--maxDataOverheadRatio",
	"1.05",
	...jsonOut("multi-hotspot-slow"),
	"--strict",
	"0",
);
