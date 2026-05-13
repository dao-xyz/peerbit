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

type ScenarioName =
	| "ci-small"
	| "ci-loss"
	| "ci-constrained"
	| "ci-live-stream"
	| "ci-idle-upgrade"
	| "ci-idle-upgrade-large";
type UpgradeMode = "direct" | "probe" | "shadow";
type UpgradePreset = "raw" | "default-candidate";

type EvalArgs = {
	scenarios: ScenarioName[];
	seeds: number[];
	parentUpgradePreset: UpgradePreset;
	parentUpgradeIntervalMs: number;
	parentUpgradeLeafOnly: boolean;
	parentUpgradeMinLevelGain: number;
	parentUpgradeRootMinLevelGain: number;
	parentUpgradeRootMinSubtreeGain: number;
	parentUpgradeNonRootMinLevelGain: number;
	parentUpgradeMinFreeSlots: number;
	parentUpgradeRootMinFreeSlots: number;
	parentUpgradeMaxChildLoadRatio: number;
	parentUpgradeRootMaxChildLoadRatio: number;
	parentUpgradeCooldownMs: number;
	parentUpgradeFailedBackoffMinMs: number;
	parentUpgradeFailedBackoffMaxMs: number;
	parentUpgradeQuietMs: number;
	parentUpgradeRepairQuietMs: number;
	parentUpgradeMaxPerPeer: number;
	parentUpgradeRepairGuard: boolean;
	parentUpgradeDataGuard: boolean;
	parentUpgradeMode: UpgradeMode;
	parentUpgradeVerifyStaleRootCapacity: boolean;
	parentUpgradeStaleRootProbeProbability: number;
	compareModes: boolean;
	parentProbeTimeoutMs: number;
	parentProbeMaxPerRound: number;
	parentProbeMaxLagMessages: number;
	parentProbeRejectCooldownMs: number;
	parentProbeRejectCooldownMaxMs: number;
	parentShadowObserveMs: number;
	parentShadowMinObservations: number;
	streamRxDelayMs: number | undefined;
	maxCostRatio: number;
	maxFormationScoreDelta: number;
	maxLiveDeadlinePctDelta: number;
	maxSecondBatchLatencyP95DeltaMs: number;
	maxSecondBatchLatencyP95DeltaRatio: number;
	maxProbePerUpgrade: number;
	maxRootChildrenDelta: number;
	maxRootUploadPctDelta: number;
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

type EvalEffect = "no-op" | "guarded" | "promoted" | "regressed";

type SummarySample = {
	scenario: ScenarioName;
	mode: UpgradeMode;
	seed: number;
	baseline: FanoutTreeSimResult;
	upgrade: FanoutTreeSimResult;
	failures: Failure[];
};

const quantile = (values: number[], q: number) => {
	if (values.length === 0) return NaN;
	const pos = Math.min(
		values.length - 1,
		Math.max(0, Math.ceil(values.length * q) - 1),
	);
	return values[pos]!;
};

const HELP_TEXT = [
	"fanout-tree-parent-upgrade-eval.ts",
	"",
	"Args:",
	"  --scenario NAME              scenario to run (ci-small|ci-loss|ci-constrained|ci-live-stream|ci-idle-upgrade|ci-idle-upgrade-large|all, default: all)",
	"  --seeds CSV                  seeds to run for each scenario (default: 1,2,3)",
	"  --parentUpgradePreset NAME   preset to evaluate (raw|default-candidate, default: raw)",
	"  --parentUpgradeIntervalMs MS upgrade check interval for treatment run (default: 1000)",
	"  --parentUpgradeLeafOnly 0|1  restrict treatment upgrades to leaves (default: 1)",
	"  --parentUpgradeMinLevelGain N min tree-level gain for treatment upgrades (default: 2)",
	"  --parentUpgradeRootMinLevelGain N min tree-level gain for root treatment targets (default: 3)",
	"  --parentUpgradeRootMinSubtreeGain N min level-gain times local branch size for root treatment targets (default: parentUpgradeRootMinLevelGain)",
	"  --parentUpgradeNonRootMinLevelGain N min tree-level gain for non-root treatment targets (default: 2)",
	"  --parentUpgradeMinFreeSlots N min free slots for treatment upgrade targets (default: 8)",
	"  --parentUpgradeRootMinFreeSlots N min free slots for root treatment targets (default: parentUpgradeMinFreeSlots)",
	"  --parentUpgradeMaxChildLoadRatio R max child load ratio after accepting treatment child (default: 0.5)",
	"  --parentUpgradeRootMaxChildLoadRatio R max root child load ratio after accepting treatment child (default: min(parentUpgradeMaxChildLoadRatio, 0.4))",
	"  --parentUpgradeCooldownMs MS  cooldown after successful treatment upgrades (default: 5000)",
	"  --parentUpgradeFailedBackoffMinMs MS initial backoff after failed probe/shadow rounds (default: 5000)",
	"  --parentUpgradeFailedBackoffMaxMs MS max backoff after failed probe/shadow rounds (default: 60000)",
	"  --parentUpgradeQuietMs MS     min quiet time since parent data before upgrades (default: 5000)",
	"  --parentUpgradeRepairQuietMs MS min quiet time since repair before upgrades (default: parentUpgradeQuietMs)",
	"  --parentUpgradeMaxPerPeer N   max successful treatment upgrades per peer, 0 = unlimited (default: 2)",
	"  --parentUpgradeRepairGuard 0|1 skip treatment upgrades while missing data (default: 1)",
	"  --parentUpgradeDataGuard 0|1 wait for finite channel completion before treatment upgrades (default: 1)",
	"  --parentUpgradeMode MODE      treatment upgrade mode (direct|probe|shadow, default: direct)",
	"  --parentUpgradeVerifyStaleRootCapacity 0|1 allow shadow probes against tracker-full root (default: 0)",
	"  --parentUpgradeStaleRootProbeProbability R base sample for tracker-full root probes per peer (default: 0.03125)",
	"  --compareModes 0|1           run direct, probe, and shadow against one baseline (default: 0)",
	"  --parentProbeTimeoutMs MS     timeout for probe-mode parent checks (default: 500)",
	"  --parentProbeMaxPerRound N    max probe-mode candidates per upgrade check (default: 2)",
	"  --parentProbeMaxLagMessages N max sequence lag for probe-mode candidates (default: 0)",
	"  --parentProbeRejectCooldownMs MS cooldown after rejected parent probes (default: 10000)",
	"  --parentProbeRejectCooldownMaxMs MS max adaptive cooldown after rejected parent probes (default: 60000)",
	"  --parentShadowObserveMs MS    min healthy shadow observation window before promotion (default: 2000)",
	"  --parentShadowMinObservations N min successful shadow observations before promotion (default: 2)",
	"  --streamRxDelayMs MS          override scenario per-chunk inbound delay in shim",
	"  --maxCostRatio R             max treatment/base ratio for control/tracker/repair bpp (default: 1.15)",
	"  --maxFormationScoreDelta N   absolute formation score jitter tolerated (default: 0.05)",
	"  --maxLiveDeadlinePctDelta N  max live-flow deadline pct jitter tolerated (default: 2)",
	"  --maxSecondBatchLatencyP95DeltaMs N max idle second-batch p95 latency jitter tolerated (default: 3)",
	"  --maxSecondBatchLatencyP95DeltaRatio R max idle second-batch p95 latency relative jitter tolerated (default: 0.15)",
	"  --maxProbePerUpgrade N       max parent probes per successful proactive upgrade (default: 2)",
	"  --maxRootChildrenDelta N     max root child-count increase over baseline (default: 4, default-candidate: 2)",
	"  --maxRootUploadPctDelta N    max root upload pct-of-cap increase over baseline (default: 1)",
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
	"ci-constrained": {
		nodes: 55,
		bootstraps: 1,
		subscribers: 42,
		relayFraction: 0.4,
		candidateScoringMode: "weighted",
		messages: 50,
		msgRate: 60,
		msgSize: 96,
		settleMs: 5_000,
		deadlineMs: 750,
		timeoutMs: 50_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 6,
		relayMaxChildren: 4,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.05,
		churnEveryMs: 250,
		churnDownMs: 125,
		churnFraction: 0.05,
	},
	"ci-live-stream": {
		nodes: 60,
		bootstraps: 1,
		subscribers: 48,
		relayFraction: 0.5,
		candidateScoringMode: "weighted",
		joinConcurrency: 1,
		joinPhases: true,
		joinPhaseSettleMs: 500,
		messages: 300,
		msgRate: 60,
		msgSize: 256,
		streamRxDelayMs: 2,
		settleMs: 2_000,
		deadlineMs: 750,
		timeoutMs: 90_000,
		trackerQueryIntervalMs: 1_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 3,
		relayMaxChildren: 4,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0,
		churnEveryMs: 500,
		churnDownMs: 150,
		churnFraction: 0.03,
		lateRootConnectAfterMs: 1_000,
		lateRootDuringPublish: true,
		lateRootMaxChildren: 16,
		lateRootConnectFraction: 0.5,
	},
	"ci-idle-upgrade": {
		nodes: 45,
		bootstraps: 1,
		subscribers: 36,
		relayFraction: 0.5,
		candidateScoringMode: "weighted",
		joinConcurrency: 1,
		joinPhases: true,
		joinPhaseSettleMs: 500,
		messages: 20,
		secondBatchMessages: 80,
		secondBatchSettleMs: 1_000,
		msgRate: 50,
		msgSize: 64,
		streamRxDelayMs: 3,
		settleMs: 10_000,
		deadlineMs: 500,
		timeoutMs: 60_000,
		trackerQueryIntervalMs: 1_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 2,
		relayMaxChildren: 4,
		dropDataFrameRate: 0,
		churnEveryMs: 0,
		lateRootConnectAfterMs: 1_000,
		lateRootMaxChildren: 12,
		lateRootConnectFraction: 0.45,
	},
	"ci-idle-upgrade-large": {
		nodes: 90,
		bootstraps: 1,
		subscribers: 72,
		relayFraction: 0.5,
		candidateScoringMode: "weighted",
		joinConcurrency: 1,
		joinPhases: true,
		joinPhaseSettleMs: 500,
		messages: 20,
		secondBatchMessages: 80,
		secondBatchSettleMs: 1_500,
		msgRate: 50,
		msgSize: 64,
		streamRxDelayMs: 3,
		settleMs: 12_000,
		deadlineMs: 500,
		timeoutMs: 120_000,
		trackerQueryIntervalMs: 1_000,
		repair: true,
		rootUploadLimitBps: 100_000_000,
		relayUploadLimitBps: 100_000_000,
		rootMaxChildren: 3,
		relayMaxChildren: 4,
		dropDataFrameRate: 0,
		churnEveryMs: 0,
		lateRootConnectAfterMs: 1_000,
		lateRootMaxChildren: 24,
		lateRootConnectFraction: 0.4,
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
	if (!value || value === "all") {
		return ["ci-small", "ci-loss", "ci-constrained", "ci-idle-upgrade"];
	}
	const parsed = value
		.split(",")
		.map((part) => part.trim())
		.filter(
			(part): part is ScenarioName =>
				part === "ci-small" ||
				part === "ci-loss" ||
				part === "ci-constrained" ||
				part === "ci-live-stream" ||
				part === "ci-idle-upgrade" ||
				part === "ci-idle-upgrade-large",
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

	const presetRaw = get("--parentUpgradePreset") ?? "raw";
	if (presetRaw !== "raw" && presetRaw !== "default-candidate") {
		throw new Error(`Unknown parent upgrade preset: ${presetRaw}`);
	}
	const parentUpgradePreset = presetRaw as UpgradePreset;
	const defaultCandidate = parentUpgradePreset === "default-candidate";
	const parentUpgradeQuietMs = Number(get("--parentUpgradeQuietMs") ?? 5_000);
	const parentUpgradeMaxChildLoadRatio = Number(
		get("--parentUpgradeMaxChildLoadRatio") ?? 0.5,
	);
	const parentUpgradeRootMaxChildLoadRatio = Number(
		get("--parentUpgradeRootMaxChildLoadRatio") ??
			Math.min(parentUpgradeMaxChildLoadRatio, 0.4),
	);
	const parentUpgradeModeRaw = get("--parentUpgradeMode");
	const parentUpgradeMode =
		parentUpgradeModeRaw === "probe" || parentUpgradeModeRaw === "shadow"
			? parentUpgradeModeRaw
			: parentUpgradeModeRaw === "direct"
				? "direct"
				: defaultCandidate
					? "shadow"
					: "direct";
	return {
		scenarios: parseScenarios(get("--scenario")),
		seeds: parseCsvNumbers(get("--seeds"), [1, 2, 3]),
		parentUpgradePreset,
		parentUpgradeIntervalMs: Number(get("--parentUpgradeIntervalMs") ?? 1_000),
		parentUpgradeLeafOnly: parseBool01(
			get("--parentUpgradeLeafOnly"),
			defaultCandidate ? false : true,
		),
		parentUpgradeMinLevelGain: Number(get("--parentUpgradeMinLevelGain") ?? 2),
		parentUpgradeRootMinLevelGain: Number(
			get("--parentUpgradeRootMinLevelGain") ?? 3,
		),
		parentUpgradeRootMinSubtreeGain: Number(
			get("--parentUpgradeRootMinSubtreeGain") ??
				get("--parentUpgradeRootMinLevelGain") ??
				3,
		),
		parentUpgradeNonRootMinLevelGain: Number(
			get("--parentUpgradeNonRootMinLevelGain") ?? 2,
		),
		parentUpgradeMinFreeSlots: Number(get("--parentUpgradeMinFreeSlots") ?? 8),
		parentUpgradeRootMinFreeSlots: Number(
			get("--parentUpgradeRootMinFreeSlots") ??
				get("--parentUpgradeMinFreeSlots") ??
				8,
		),
		parentUpgradeMaxChildLoadRatio,
		parentUpgradeRootMaxChildLoadRatio,
		parentUpgradeCooldownMs: Number(get("--parentUpgradeCooldownMs") ?? 5_000),
		parentUpgradeFailedBackoffMinMs: Number(
			get("--parentUpgradeFailedBackoffMinMs") ?? 5_000,
		),
		parentUpgradeFailedBackoffMaxMs: Number(
			get("--parentUpgradeFailedBackoffMaxMs") ?? 60_000,
		),
		parentUpgradeQuietMs,
		parentUpgradeRepairQuietMs: Number(
			get("--parentUpgradeRepairQuietMs") ?? parentUpgradeQuietMs,
		),
		parentUpgradeMaxPerPeer: Number(get("--parentUpgradeMaxPerPeer") ?? 2),
		parentUpgradeRepairGuard: parseBool01(
			get("--parentUpgradeRepairGuard"),
			true,
		),
		parentUpgradeDataGuard: parseBool01(get("--parentUpgradeDataGuard"), true),
		parentUpgradeMode,
		parentUpgradeVerifyStaleRootCapacity: parseBool01(
			get("--parentUpgradeVerifyStaleRootCapacity"),
			defaultCandidate,
		),
		parentUpgradeStaleRootProbeProbability: Number(
			get("--parentUpgradeStaleRootProbeProbability") ?? 0.03125,
		),
		compareModes: parseBool01(get("--compareModes"), false),
		parentProbeTimeoutMs: Number(get("--parentProbeTimeoutMs") ?? 500),
		parentProbeMaxPerRound: Number(get("--parentProbeMaxPerRound") ?? 2),
		parentProbeMaxLagMessages: Number(get("--parentProbeMaxLagMessages") ?? 0),
		parentProbeRejectCooldownMs: Number(
			get("--parentProbeRejectCooldownMs") ?? 10_000,
		),
		parentProbeRejectCooldownMaxMs: Number(
			get("--parentProbeRejectCooldownMaxMs") ?? 60_000,
		),
		parentShadowObserveMs: Number(get("--parentShadowObserveMs") ?? 2_000),
		parentShadowMinObservations: Number(
			get("--parentShadowMinObservations") ?? 2,
		),
		streamRxDelayMs:
			get("--streamRxDelayMs") == null
				? undefined
				: Number(get("--streamRxDelayMs")),
		maxCostRatio: Number(get("--maxCostRatio") ?? 1.15),
		maxFormationScoreDelta: Number(get("--maxFormationScoreDelta") ?? 0.05),
		maxLiveDeadlinePctDelta: Number(get("--maxLiveDeadlinePctDelta") ?? 2),
		maxSecondBatchLatencyP95DeltaMs: Number(
			get("--maxSecondBatchLatencyP95DeltaMs") ?? 3,
		),
		maxSecondBatchLatencyP95DeltaRatio: Number(
			get("--maxSecondBatchLatencyP95DeltaRatio") ?? 0.15,
		),
		maxProbePerUpgrade: Number(get("--maxProbePerUpgrade") ?? 2),
		maxRootChildrenDelta: Number(
			get("--maxRootChildrenDelta") ?? (defaultCandidate ? 2 : 4),
		),
		maxRootUploadPctDelta: Number(get("--maxRootUploadPctDelta") ?? 1),
		maxReparentsPerMin: Number(get("--maxReparentsPerMin") ?? 500),
		maxReparentsPerPeer: Number(get("--maxReparentsPerPeer") ?? 20),
		maxOrphanAreaRatio: Number(get("--maxOrphanAreaRatio") ?? 1.15),
		strict: parseBool01(get("--strict"), false),
	};
};

const ratioLimit = (baseline: number, ratio: number, absoluteSlack = 1e-9) =>
	Math.max(absoluteSlack, baseline * ratio + absoluteSlack);

const peerLatencyP95For = (result: FanoutTreeSimResult, hashes: string[]) => {
	const values: number[] = [];
	for (const hash of hashes) {
		const value = result.secondBatchLatencyP95ByHash[hash];
		if (Number.isFinite(value)) values.push(value);
	}
	values.sort((a, b) => a - b);
	return quantile(values, 0.95);
};

const peerCoveragePct = (result: FanoutTreeSimResult, hashes: string[]) => {
	if (result.subscriberCount <= 0) return 0;
	return (100 * new Set(hashes).size) / result.subscriberCount;
};

const isIdleUpgradeScenario = (scenario: ScenarioName) =>
	scenario === "ci-idle-upgrade" || scenario === "ci-idle-upgrade-large";
const isLiveStreamScenario = (scenario: ScenarioName) =>
	scenario === "ci-live-stream";
const hasLivePublishPhase = (scenario: ScenarioName) =>
	scenario === "ci-live-stream";

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
	scenario: ScenarioName,
	params: FanoutTreeSimParams,
	baseline: FanoutTreeSimResult,
	upgrade: FanoutTreeSimResult,
	args: EvalArgs,
) => {
	const failures: Failure[] = [];
	const promoted = upgrade.reparentUpgradeTotal > 0;
	const treeLevelP95Gain =
		Number.isFinite(baseline.treeLevelP95) &&
		Number.isFinite(upgrade.treeLevelP95)
			? baseline.treeLevelP95 - upgrade.treeLevelP95
			: NaN;
	const treeLevelAvgGain =
		Number.isFinite(baseline.treeLevelAvg) &&
		Number.isFinite(upgrade.treeLevelAvg)
			? baseline.treeLevelAvg - upgrade.treeLevelAvg
			: NaN;
	const usefulDepthGain = Math.max(treeLevelP95Gain, treeLevelAvgGain);
	const secondBatchLatencyP95Gain =
		Number.isFinite(baseline.secondBatchLatencyP95) &&
		Number.isFinite(upgrade.secondBatchLatencyP95)
			? baseline.secondBatchLatencyP95 - upgrade.secondBatchLatencyP95
			: NaN;
	const secondBatchLatencyP95SlackMs = Number.isFinite(
		baseline.secondBatchLatencyP95,
	)
		? Math.max(
				0,
				args.maxSecondBatchLatencyP95DeltaMs,
				baseline.secondBatchLatencyP95 *
					Math.max(0, args.maxSecondBatchLatencyP95DeltaRatio),
			)
		: Math.max(0, args.maxSecondBatchLatencyP95DeltaMs);
	const promotedPeerBaselineSecondBatchLatencyP95 = peerLatencyP95For(
		baseline,
		upgrade.upgradedPeerHashes,
	);
	const promotedPeerUpgradeSecondBatchLatencyP95 = peerLatencyP95For(
		upgrade,
		upgrade.upgradedPeerHashes,
	);
	const promotedBranchBaselineSecondBatchLatencyP95 = peerLatencyP95For(
		baseline,
		upgrade.upgradedBranchPeerHashes,
	);
	const promotedBranchUpgradeSecondBatchLatencyP95 = peerLatencyP95For(
		upgrade,
		upgrade.upgradedBranchPeerHashes,
	);
	const promotedBranchSecondBatchLatencyP95Gain =
		Number.isFinite(promotedBranchBaselineSecondBatchLatencyP95) &&
		Number.isFinite(promotedBranchUpgradeSecondBatchLatencyP95)
			? promotedBranchBaselineSecondBatchLatencyP95 -
				promotedBranchUpgradeSecondBatchLatencyP95
			: NaN;
	const usefulIdleGain = Math.max(
		usefulDepthGain,
		secondBatchLatencyP95Gain,
		promotedBranchSecondBatchLatencyP95Gain,
	);
	const usefulPromotions =
		upgrade.reparentUpgradeTotal > 0 &&
		(isIdleUpgradeScenario(scenario) ? usefulIdleGain >= 1 : usefulDepthGain > 0.05)
			? upgrade.reparentUpgradeTotal
			: 0;
	const upgradeActivity =
		upgrade.reparentUpgradeTotal > 0 ||
		upgrade.parentProbeReqSentTotal > 0 ||
		upgrade.parentShadowStartTotal > 0;

	if (hasLivePublishPhase(scenario)) {
		failIfGreater(
			failures,
			"liveActiveParentProbeReqSent",
			0,
			upgrade.publishActiveParentProbeReqSentTotal,
			0,
		);
		failIfGreater(
			failures,
			"liveActiveParentShadowStart",
			0,
			upgrade.publishActiveParentShadowStartTotal,
			0,
		);
		failIfGreater(
			failures,
			"liveActiveParentShadowPromote",
			0,
			upgrade.publishActiveParentShadowPromoteTotal,
			0,
		);
		failIfGreater(
			failures,
			"liveActiveReparentUpgrade",
			0,
			upgrade.publishActiveReparentUpgradeTotal,
			0,
		);
		failIfLess(
			failures,
			"liveActiveDataGuardSkips",
			0,
			upgrade.publishActiveReparentUpgradeSkipDataTotal,
			1,
		);
	}
	if (isLiveStreamScenario(scenario)) {
		failIfGreater(
			failures,
			"liveTotalParentProbeReqSent",
			0,
			upgrade.parentProbeReqSentTotal,
			0,
		);
		failIfGreater(
			failures,
			"liveTotalParentShadowStart",
			0,
			upgrade.parentShadowStartTotal,
			0,
		);
		failIfGreater(
			failures,
			"liveTotalReparentUpgrade",
			0,
			upgrade.reparentUpgradeTotal,
			0,
		);
	}

	if (isIdleUpgradeScenario(scenario)) {
		failIfLess(failures, "idlePromotions", 0, upgrade.reparentUpgradeTotal, 1);
		failIfLess(failures, "idleUsefulPromotions", 0, usefulPromotions, 1);
		failIfLess(failures, "idleUsefulGain", 0, usefulIdleGain, 1);
		if (upgradeActivity) {
			failIfGreater(
				failures,
				"idleProbePerUpgrade",
				0,
				upgrade.reparentUpgradeTotal > 0
					? upgrade.parentProbeReqSentTotal / upgrade.reparentUpgradeTotal
					: Number.POSITIVE_INFINITY,
				args.maxProbePerUpgrade,
			);
			failIfGreater(
				failures,
				"idleMaxReparentsPerPeer",
				baseline.maintMaxReparentsPerPeer,
				upgrade.maintMaxReparentsPerPeer,
				1,
			);
			failIfLess(
				failures,
				"idleSecondBatchDeadlinePct",
				baseline.secondBatchDeliveredWithinDeadlinePct,
				upgrade.secondBatchDeliveredWithinDeadlinePct,
				baseline.secondBatchDeliveredWithinDeadlinePct,
			);
			failIfGreater(
				failures,
				"idleSecondBatchLatencyP95",
				baseline.secondBatchLatencyP95,
				upgrade.secondBatchLatencyP95,
				baseline.secondBatchLatencyP95 + secondBatchLatencyP95SlackMs,
			);
			failIfLess(
				failures,
				"idleSecondBatchLatencyP95OrBranchGain",
				0,
				Math.max(
					secondBatchLatencyP95Gain,
					promotedBranchSecondBatchLatencyP95Gain,
				),
				1,
			);
			failIfGreater(
				failures,
				"idlePromotedPeerSecondBatchLatencyP95",
				promotedPeerBaselineSecondBatchLatencyP95,
				promotedPeerUpgradeSecondBatchLatencyP95,
				promotedPeerBaselineSecondBatchLatencyP95,
			);
			failIfGreater(
				failures,
				"idlePromotedBranchSecondBatchLatencyP95",
				promotedBranchBaselineSecondBatchLatencyP95,
				promotedBranchUpgradeSecondBatchLatencyP95,
				promotedBranchBaselineSecondBatchLatencyP95,
			);
		}
	}

	if (
		!isIdleUpgradeScenario(scenario) &&
		!isLiveStreamScenario(scenario) &&
		upgrade.reparentUpgradeTotal === 0 &&
		upgrade.parentProbeReqSentTotal === 0 &&
		upgrade.parentShadowStartTotal === 0
	) {
		return failures;
	}

	if (promoted) {
		if (!isIdleUpgradeScenario(scenario)) {
			failIfGreater(
				failures,
				"formationScore",
				baseline.formationScore,
				upgrade.formationScore,
				baseline.formationScore + args.maxFormationScoreDelta,
			);
		}
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
	}
	failIfLess(
		failures,
		"deliveredWithinDeadlinePct",
		baseline.deliveredWithinDeadlinePct,
		upgrade.deliveredWithinDeadlinePct,
		hasLivePublishPhase(scenario)
			? baseline.deliveredWithinDeadlinePct -
				Math.max(0, args.maxLiveDeadlinePctDelta)
			: baseline.deliveredWithinDeadlinePct,
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
	if (isLiveStreamScenario(scenario) && !upgradeActivity) {
		return failures;
	}
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
		params.lateRootMaxChildren > 0
			? params.lateRootMaxChildren
			: params.rootMaxChildren,
	);
	if (upgradeActivity) {
		failIfGreater(
			failures,
			"rootChildrenDelta",
			baseline.treeRootChildren,
			upgrade.treeRootChildren,
			baseline.treeRootChildren + Math.max(0, args.maxRootChildrenDelta),
		);
		failIfGreater(
			failures,
			"rootUploadPctDelta",
			baseline.rootUploadFracPct,
			upgrade.rootUploadFracPct,
			baseline.rootUploadFracPct + Math.max(0, args.maxRootUploadPctDelta),
		);
	}

	return failures;
};

const classifyEffect = (
	upgrade: FanoutTreeSimResult,
	failures: Failure[],
): EvalEffect => {
	if (
		upgrade.reparentUpgradeTotal === 0 &&
		upgrade.parentProbeReqSentTotal === 0 &&
		upgrade.parentShadowStartTotal === 0
	) {
		return "no-op";
	}
	if (failures.length > 0) return "regressed";
	if (upgrade.reparentUpgradeTotal > 0) return "promoted";
	return "guarded";
};

const printComparison = (
	scenario: ScenarioName,
	seed: number,
	mode: UpgradeMode,
	baseline: FanoutTreeSimResult,
	upgrade: FanoutTreeSimResult,
	failures: Failure[],
) => {
	const delta = (after: number, before: number) =>
		Number.isFinite(after) && Number.isFinite(before) ? after - before : NaN;
	const effect = classifyEffect(upgrade, failures);
	const treeLevelP95Gain = delta(baseline.treeLevelP95, upgrade.treeLevelP95);
	const treeLevelAvgGain = delta(baseline.treeLevelAvg, upgrade.treeLevelAvg);
	const promotedPeerBaselineSecondBatchLatencyP95 = peerLatencyP95For(
		baseline,
		upgrade.upgradedPeerHashes,
	);
	const promotedPeerUpgradeSecondBatchLatencyP95 = peerLatencyP95For(
		upgrade,
		upgrade.upgradedPeerHashes,
	);
	const promotedBranchBaselineSecondBatchLatencyP95 = peerLatencyP95For(
		baseline,
		upgrade.upgradedBranchPeerHashes,
	);
	const promotedBranchUpgradeSecondBatchLatencyP95 = peerLatencyP95For(
		upgrade,
		upgrade.upgradedBranchPeerHashes,
	);
	const promotedBranchCoveragePct = peerCoveragePct(
		upgrade,
		upgrade.upgradedBranchPeerHashes,
	);
	const secondBatchLatencyP95Gain =
		baseline.secondBatchLatencyP95 - upgrade.secondBatchLatencyP95;
	const promotedBranchSecondBatchLatencyP95Gain =
		promotedBranchBaselineSecondBatchLatencyP95 -
		promotedBranchUpgradeSecondBatchLatencyP95;
	const usefulPromotions =
		upgrade.reparentUpgradeTotal > 0 &&
		(isIdleUpgradeScenario(scenario)
			? Math.max(
					treeLevelP95Gain,
					treeLevelAvgGain,
					secondBatchLatencyP95Gain,
					promotedBranchSecondBatchLatencyP95Gain,
				) >= 1
			: treeLevelP95Gain > 0 || treeLevelAvgGain > 0.05)
			? upgrade.reparentUpgradeTotal
			: 0;

	console.log(
		[
			`parent-upgrade-eval scenario=${scenario} seed=${seed} mode=${mode} viable=${failures.length === 0} effect=${effect}`,
			`  formationScore ${baseline.formationScore.toFixed(2)} -> ${upgrade.formationScore.toFixed(2)} delta=${delta(upgrade.formationScore, baseline.formationScore).toFixed(2)}`,
			`  treeLevelP95 ${baseline.treeLevelP95.toFixed(1)} -> ${upgrade.treeLevelP95.toFixed(1)} delta=${delta(upgrade.treeLevelP95, baseline.treeLevelP95).toFixed(1)}`,
			`  treeLevelAvg ${baseline.treeLevelAvg.toFixed(2)} -> ${upgrade.treeLevelAvg.toFixed(2)} delta=${delta(upgrade.treeLevelAvg, baseline.treeLevelAvg).toFixed(2)}`,
			`  formationStretchP95 ${baseline.formationStretchP95.toFixed(2)} -> ${upgrade.formationStretchP95.toFixed(2)} delta=${delta(upgrade.formationStretchP95, baseline.formationStretchP95).toFixed(2)}`,
			`  deliveredWithinDeadlinePct ${baseline.deliveredWithinDeadlinePct.toFixed(2)} -> ${upgrade.deliveredWithinDeadlinePct.toFixed(2)} delta=${delta(upgrade.deliveredWithinDeadlinePct, baseline.deliveredWithinDeadlinePct).toFixed(2)}`,
			...(baseline.secondBatchExpected > 0 || upgrade.secondBatchExpected > 0
				? [
						`  secondBatchDeadlinePct ${baseline.secondBatchDeliveredWithinDeadlinePct.toFixed(2)} -> ${upgrade.secondBatchDeliveredWithinDeadlinePct.toFixed(2)} delta=${delta(upgrade.secondBatchDeliveredWithinDeadlinePct, baseline.secondBatchDeliveredWithinDeadlinePct).toFixed(2)}`,
						`  secondBatchLatencyP95 ${baseline.secondBatchLatencyP95.toFixed(1)} -> ${upgrade.secondBatchLatencyP95.toFixed(1)} delta=${delta(upgrade.secondBatchLatencyP95, baseline.secondBatchLatencyP95).toFixed(1)}`,
						`  promotedPeerSecondBatchLatencyP95 ${promotedPeerBaselineSecondBatchLatencyP95.toFixed(1)} -> ${promotedPeerUpgradeSecondBatchLatencyP95.toFixed(1)} peers=${upgrade.upgradedPeerHashes.length}`,
						`  promotedBranchSecondBatchLatencyP95 ${promotedBranchBaselineSecondBatchLatencyP95.toFixed(1)} -> ${promotedBranchUpgradeSecondBatchLatencyP95.toFixed(1)} peers=${upgrade.upgradedBranchPeerHashes.length} coverage=${promotedBranchCoveragePct.toFixed(1)}%`,
					]
				: []),
			`  bpp control ${baseline.controlBpp.toFixed(4)} -> ${upgrade.controlBpp.toFixed(4)} tracker ${baseline.trackerBpp.toFixed(4)} -> ${upgrade.trackerBpp.toFixed(4)} repair ${baseline.repairBpp.toFixed(4)} -> ${upgrade.repairBpp.toFixed(4)}`,
			`  maintenance reparentsPerMin ${baseline.maintReparentsPerMin.toFixed(2)} -> ${upgrade.maintReparentsPerMin.toFixed(2)} maxReparentsPerPeer ${baseline.maintMaxReparentsPerPeer} -> ${upgrade.maintMaxReparentsPerPeer} orphanArea ${baseline.maintOrphanArea.toFixed(1)} -> ${upgrade.maintOrphanArea.toFixed(1)}`,
			`  rootChildren ${baseline.treeRootChildren} -> ${upgrade.treeRootChildren} rootUploadPct ${baseline.rootUploadFracPct.toFixed(2)} -> ${upgrade.rootUploadFracPct.toFixed(2)} proactiveUpgrades=${upgrade.reparentUpgradeTotal} usefulPromotions=${usefulPromotions} treeLevelP95Gain=${treeLevelP95Gain.toFixed(1)} treeLevelAvgGain=${treeLevelAvgGain.toFixed(2)}`,
			...(hasLivePublishPhase(scenario)
				? [
						`  publishActive upgrade=${upgrade.publishActiveReparentUpgradeTotal} dataSkips=${upgrade.publishActiveReparentUpgradeSkipDataTotal} repairSkips=${upgrade.publishActiveReparentUpgradeSkipRepairTotal} quietSkips=${upgrade.publishActiveReparentUpgradeSkipQuietTotal} probes=${upgrade.publishActiveParentProbeReqSentTotal} shadowStart=${upgrade.publishActiveParentShadowStartTotal} shadowPromote=${upgrade.publishActiveParentShadowPromoteTotal}`,
					]
				: []),
			`  skipped leaf=${upgrade.reparentUpgradeSkipLeafTotal} repair=${upgrade.reparentUpgradeSkipRepairTotal} data=${upgrade.reparentUpgradeSkipDataTotal} cooldown=${upgrade.reparentUpgradeSkipCooldownTotal} quiet=${upgrade.reparentUpgradeSkipQuietTotal} budget=${upgrade.reparentUpgradeSkipBudgetTotal} candidateLevel=${upgrade.reparentUpgradeSkipCandidateLevelTotal} candidateSlots=${upgrade.reparentUpgradeSkipCandidateSlotsTotal} candidatePressure=${upgrade.reparentUpgradeSkipCandidatePressureTotal} rootPressure=${upgrade.reparentUpgradeSkipRootPressureTotal}`,
			`  probes req=${upgrade.parentProbeReqSentTotal}/${upgrade.parentProbeReqReceivedTotal} reply=${upgrade.parentProbeReplySentTotal}/${upgrade.parentProbeReplyReceivedTotal} noReply=${upgrade.reparentUpgradeSkipProbeNoReplyTotal} notRooted=${upgrade.reparentUpgradeSkipProbeNotRootedTotal} repair=${upgrade.reparentUpgradeSkipProbeRepairTotal} lag=${upgrade.reparentUpgradeSkipProbeLagTotal} overloaded=${upgrade.reparentUpgradeSkipProbeOverloadedTotal} cooldown=${upgrade.reparentUpgradeSkipProbeCooldownTotal}`,
			`  root reservations created=${upgrade.parentUpgradeRootReservationCreatedTotal} consumed=${upgrade.parentUpgradeRootReservationConsumedTotal} rejected=${upgrade.parentUpgradeRootReservationRejectedTotal} marginRejected=${upgrade.parentUpgradeRootReservationMarginRejectedTotal} blocked=${upgrade.parentUpgradeRootReservationBlockedTotal} expired=${upgrade.parentUpgradeRootReservationExpiredTotal}`,
			`  shadow start=${upgrade.parentShadowStartTotal} observe=${upgrade.parentShadowObserveTotal} promote=${upgrade.parentShadowPromoteTotal} reset=${upgrade.parentShadowResetTotal} reject noReply=${upgrade.parentShadowRejectNoReplyTotal} notRooted=${upgrade.parentShadowRejectNotRootedTotal} capacity=${upgrade.parentShadowRejectCapacityTotal} repair=${upgrade.parentShadowRejectRepairTotal} lag=${upgrade.parentShadowRejectLagTotal} overloaded=${upgrade.parentShadowRejectOverloadedTotal} level=${upgrade.parentShadowRejectLevelTotal}`,
			...(failures.length > 0
				? failures.map(
						(f) =>
							`  FAIL ${f.metric}: baseline=${f.baseline.toFixed(4)} upgrade=${f.upgrade.toFixed(4)} limit=${f.limit.toFixed(4)}`,
					)
				: []),
		].join("\n"),
	);
};

const formatDelta = (after: number, before: number, digits = 2) =>
	Number.isFinite(after) && Number.isFinite(before)
		? (after - before).toFixed(digits)
		: "NaN";

const avgFinite = (values: number[]) => {
	const finite = values.filter((value) => Number.isFinite(value));
	if (finite.length === 0) return NaN;
	return finite.reduce((sum, value) => sum + value, 0) / finite.length;
};

const maxFinite = (values: number[]) => {
	const finite = values.filter((value) => Number.isFinite(value));
	return finite.length > 0 ? Math.max(...finite) : NaN;
};

const fmt = (value: number, digits = 2) =>
	Number.isFinite(value) ? value.toFixed(digits) : "NaN";

const printModeTable = (
	scenario: ScenarioName,
	seed: number,
	baseline: FanoutTreeSimResult,
	rows: Array<{
		mode: "off" | UpgradeMode;
		result: FanoutTreeSimResult;
		failures: Failure[];
	}>,
) => {
	const lines = [
		`parent-upgrade-mode-table scenario=${scenario} seed=${seed}`,
		"mode viable effect upgrades probes shadowPromote treeP95Delta treeAvgDelta secondBatchP95Delta promotedBranchGain promotedBranchCoverage rootUploadPctDelta deadlineDelta controlBppDelta repairBppDelta orphanAreaDelta maxReparents",
	];
	for (const row of rows) {
		const r = row.result;
		const promotedBranchBaselineSecondBatchLatencyP95 = peerLatencyP95For(
			baseline,
			r.upgradedBranchPeerHashes,
		);
		const promotedBranchUpgradeSecondBatchLatencyP95 = peerLatencyP95For(
			r,
			r.upgradedBranchPeerHashes,
		);
		lines.push(
			[
				row.mode,
				row.failures.length === 0 ? "yes" : "no",
				classifyEffect(r, row.failures),
				r.reparentUpgradeTotal,
				r.parentProbeReqSentTotal,
				r.parentShadowPromoteTotal,
				formatDelta(r.treeLevelP95, baseline.treeLevelP95, 1),
				formatDelta(r.treeLevelAvg, baseline.treeLevelAvg, 2),
				formatDelta(r.secondBatchLatencyP95, baseline.secondBatchLatencyP95, 1),
				fmt(
					promotedBranchBaselineSecondBatchLatencyP95 -
						promotedBranchUpgradeSecondBatchLatencyP95,
					1,
				),
				fmt(peerCoveragePct(r, r.upgradedBranchPeerHashes), 1),
				formatDelta(r.rootUploadFracPct, baseline.rootUploadFracPct, 2),
				formatDelta(
					r.deliveredWithinDeadlinePct,
					baseline.deliveredWithinDeadlinePct,
					2,
				),
				formatDelta(r.controlBpp, baseline.controlBpp, 4),
				formatDelta(r.repairBpp, baseline.repairBpp, 4),
				formatDelta(r.maintOrphanArea, baseline.maintOrphanArea, 1),
				r.maintMaxReparentsPerPeer,
			].join(" "),
		);
	}
	console.log(lines.join("\n"));
};

const printAggregateSummary = (samples: SummarySample[]) => {
	if (samples.length === 0) return;
	const groups = new Map<string, SummarySample[]>();
	for (const sample of samples) {
		const key = `${sample.scenario}:${sample.mode}`;
		const group = groups.get(key) ?? [];
		group.push(sample);
		groups.set(key, group);
	}

	const lines = [
		"",
		"parent-upgrade-summary",
		"scenario mode seeds viable effects upgrades probes activeUpgrades activeProbes activeGuardSkips treeAvgGainAvg secondBatchP95DeltaAvg/Max promotedBranchGainAvg promotedBranchCoverageAvg controlBppDeltaPctAvg rootChildrenDeltaMax rootUploadPctDeltaMax maxReparents failures",
	];
	for (const group of groups.values()) {
		const first = group[0]!;
		const effects = {
			"no-op": 0,
			guarded: 0,
			promoted: 0,
			regressed: 0,
		} satisfies Record<EvalEffect, number>;
		for (const sample of group) {
			effects[classifyEffect(sample.upgrade, sample.failures)] += 1;
		}
		const treeAvgGains = group.map(
			(sample) => sample.baseline.treeLevelAvg - sample.upgrade.treeLevelAvg,
		);
		const secondBatchP95Deltas = group.map((sample) =>
			sample.baseline.secondBatchExpected > 0 ||
			sample.upgrade.secondBatchExpected > 0
				? sample.upgrade.secondBatchLatencyP95 -
					sample.baseline.secondBatchLatencyP95
				: NaN,
		);
		const branchGains = group.map((sample) => {
			const baseline = peerLatencyP95For(
				sample.baseline,
				sample.upgrade.upgradedBranchPeerHashes,
			);
			const upgrade = peerLatencyP95For(
				sample.upgrade,
				sample.upgrade.upgradedBranchPeerHashes,
			);
			return baseline - upgrade;
		});
		const branchCoverages = group.map((sample) =>
			sample.upgrade.upgradedBranchPeerHashes.length > 0
				? peerCoveragePct(sample.upgrade, sample.upgrade.upgradedBranchPeerHashes)
				: NaN,
		);
		const controlBppDeltaPct = group.map((sample) =>
			sample.baseline.controlBpp > 0
				? (100 * (sample.upgrade.controlBpp - sample.baseline.controlBpp)) /
					sample.baseline.controlBpp
				: NaN,
		);
		const rootChildrenDeltas = group.map(
			(sample) =>
				sample.upgrade.treeRootChildren - sample.baseline.treeRootChildren,
		);
		const rootUploadPctDeltas = group.map(
			(sample) =>
				sample.upgrade.rootUploadFracPct - sample.baseline.rootUploadFracPct,
		);
		const failureTotal = group.reduce(
			(sum, sample) => sum + sample.failures.length,
			0,
		);
		const effectText = [
			`promoted=${effects.promoted}`,
			`guarded=${effects.guarded}`,
			`no-op=${effects["no-op"]}`,
			`regressed=${effects.regressed}`,
		].join(",");
		lines.push(
			[
				first.scenario,
				first.mode,
				group.length,
				`${group.filter((sample) => sample.failures.length === 0).length}/${group.length}`,
				effectText,
				group.reduce(
					(sum, sample) => sum + sample.upgrade.reparentUpgradeTotal,
					0,
				),
				group.reduce(
					(sum, sample) => sum + sample.upgrade.parentProbeReqSentTotal,
					0,
				),
				group.reduce(
					(sum, sample) =>
						sum + sample.upgrade.publishActiveReparentUpgradeTotal,
					0,
				),
				group.reduce(
					(sum, sample) =>
						sum + sample.upgrade.publishActiveParentProbeReqSentTotal,
					0,
				),
				group.reduce(
					(sum, sample) =>
						sum +
						sample.upgrade.publishActiveReparentUpgradeSkipDataTotal +
						sample.upgrade.publishActiveReparentUpgradeSkipRepairTotal +
						sample.upgrade.publishActiveReparentUpgradeSkipQuietTotal,
					0,
				),
				fmt(avgFinite(treeAvgGains), 2),
				`${fmt(avgFinite(secondBatchP95Deltas), 1)}/${fmt(maxFinite(secondBatchP95Deltas), 1)}`,
				fmt(avgFinite(branchGains), 1),
				fmt(avgFinite(branchCoverages), 1),
				fmt(avgFinite(controlBppDeltaPct), 1),
				fmt(maxFinite(rootChildrenDeltas), 0),
				fmt(maxFinite(rootUploadPctDeltas), 2),
				Math.max(
					...group.map((sample) => sample.upgrade.maintMaxReparentsPerPeer),
				),
				failureTotal,
			].join(" "),
		);
	}
	console.log(lines.join("\n"));
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	let failureCount = 0;
	const summarySamples: SummarySample[] = [];
	const modes: UpgradeMode[] = args.compareModes
		? ["direct", "probe", "shadow"]
		: [args.parentUpgradeMode];

	for (const scenario of args.scenarios) {
		for (const seed of args.seeds) {
			const baseParams = {
				...SCENARIOS[scenario],
				seed,
				parentUpgradeIntervalMs: 0,
				...(args.streamRxDelayMs == null
					? {}
					: { streamRxDelayMs: args.streamRxDelayMs }),
			};
			const upgradeParams = {
				...SCENARIOS[scenario],
				seed,
				...(args.streamRxDelayMs == null
					? {}
					: { streamRxDelayMs: args.streamRxDelayMs }),
				parentUpgradeIntervalMs: args.parentUpgradeIntervalMs,
				parentUpgradeLeafOnly: args.parentUpgradeLeafOnly,
				parentUpgradeMinLevelGain: args.parentUpgradeMinLevelGain,
				parentUpgradeRootMinLevelGain: args.parentUpgradeRootMinLevelGain,
				parentUpgradeRootMinSubtreeGain:
					args.parentUpgradeRootMinSubtreeGain,
				parentUpgradeNonRootMinLevelGain: args.parentUpgradeNonRootMinLevelGain,
				parentUpgradeMinFreeSlots: args.parentUpgradeMinFreeSlots,
				parentUpgradeRootMinFreeSlots: args.parentUpgradeRootMinFreeSlots,
				parentUpgradeMaxChildLoadRatio: args.parentUpgradeMaxChildLoadRatio,
				parentUpgradeRootMaxChildLoadRatio:
					args.parentUpgradeRootMaxChildLoadRatio,
				parentUpgradeCooldownMs: args.parentUpgradeCooldownMs,
				parentUpgradeFailedBackoffMinMs: args.parentUpgradeFailedBackoffMinMs,
				parentUpgradeFailedBackoffMaxMs: args.parentUpgradeFailedBackoffMaxMs,
				parentUpgradeQuietMs: args.parentUpgradeQuietMs,
				parentUpgradeRepairQuietMs: args.parentUpgradeRepairQuietMs,
				parentUpgradeMaxPerPeer: args.parentUpgradeMaxPerPeer,
				parentUpgradeRepairGuard: args.parentUpgradeRepairGuard,
				parentUpgradeDataGuard: args.parentUpgradeDataGuard,
				parentUpgradeMode: args.parentUpgradeMode,
				parentUpgradeVerifyStaleRootCapacity:
					args.parentUpgradeVerifyStaleRootCapacity,
				parentUpgradeStaleRootProbeProbability:
					args.parentUpgradeStaleRootProbeProbability,
				parentProbeTimeoutMs: args.parentProbeTimeoutMs,
				parentProbeMaxPerRound: args.parentProbeMaxPerRound,
				parentProbeMaxLagMessages: args.parentProbeMaxLagMessages,
				parentProbeRejectCooldownMs: args.parentProbeRejectCooldownMs,
				parentProbeRejectCooldownMaxMs: args.parentProbeRejectCooldownMaxMs,
				parentShadowObserveMs: args.parentShadowObserveMs,
				parentShadowMinObservations: args.parentShadowMinObservations,
			};

			console.log(`\n[baseline] scenario=${scenario} seed=${seed}`);
			const baseline = await runFanoutTreeSim(baseParams);
			console.log(formatFanoutTreeSimResult(baseline));

			const rows: Array<{
				mode: "off" | UpgradeMode;
				result: FanoutTreeSimResult;
				failures: Failure[];
			}> = [{ mode: "off", result: baseline, failures: [] }];
			for (const mode of modes) {
				console.log(
					`\n[parent-upgrade:${mode}] scenario=${scenario} seed=${seed}`,
				);
				const upgrade = await runFanoutTreeSim({
					...upgradeParams,
					parentUpgradeMode: mode,
				});
				console.log(formatFanoutTreeSimResult(upgrade));

				const failures = evaluateRun(
					scenario,
					upgrade.params,
					baseline,
					upgrade,
					args,
				);
				printComparison(scenario, seed, mode, baseline, upgrade, failures);
				rows.push({ mode, result: upgrade, failures });
				summarySamples.push({
					scenario,
					mode,
					seed,
					baseline,
					upgrade,
					failures,
				});
				failureCount += failures.length;
			}
			if (args.compareModes) {
				printModeTable(scenario, seed, baseline, rows);
			}
		}
	}
	printAggregateSummary(summarySamples);

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
