#!/usr/bin/env node
import { spawnSync } from "node:child_process";

const args = process.argv.slice(2);

const getArg = (name, fallback) => {
	const index = args.indexOf(name);
	return index === -1 ? fallback : args[index + 1];
};

const hasFlag = (name) => args.includes(name);

const pnpm = process.env.PNPM ?? "pnpm";
const seeds = getArg("--seeds", process.env.FANOUT_PARENT_UPGRADE_SEEDS ?? "1");
const skipBuild = hasFlag("--no-build") || process.env.FANOUT_SKIP_BUILD === "1";

const run = (label, commandArgs) => {
	console.log(`\n[fanout-default-ready] ${label}`);
	console.log(`${pnpm} ${commandArgs.join(" ")}`);
	const result = spawnSync(pnpm, commandArgs, {
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

if (!skipBuild) {
	run("build pubsub", ["--filter", "@peerbit/pubsub", "build"]);
}

run("active shadow dual-path mechanism gate", [
	"-C",
	"packages/transport/pubsub",
	"run",
	"bench",
	"--",
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
	"--parentUpgradeMode",
	"shadow",
	"--parentUpgradeVerifyStaleRootCapacity",
	"1",
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
]);

run("single-writer live default-candidate safety", [
	"-C",
	"packages/transport/pubsub",
	"run",
	"bench",
	"--",
	"fanout-tree-parent-upgrade-eval",
	"--scenario",
	"ci-live-stream",
	"--seeds",
	seeds,
	"--parentUpgradePreset",
	"default-candidate",
	"--strict",
	"1",
]);

run("multi-writer live default-candidate safety", [
	"-C",
	"packages/transport/pubsub",
	"run",
	"bench",
	"--",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-live,ci-multi-live-churn,ci-multi-video-live",
	"--seeds",
	seeds,
	"--parentUpgradePreset",
	"default-candidate",
	"--strict",
	"1",
]);

run("multi-writer idle default-candidate benefit", [
	"-C",
	"packages/transport/pubsub",
	"run",
	"bench",
	"--",
	"fanout-tree-parent-upgrade-multi-eval",
	"--scenario",
	"ci-multi-idle,ci-multi-sparse-idle,ci-multi-hotspot-idle",
	"--seeds",
	seeds,
	"--parentUpgradePreset",
	"default-candidate",
	"--strict",
	"1",
]);
