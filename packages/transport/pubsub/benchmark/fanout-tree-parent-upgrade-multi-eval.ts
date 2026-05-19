import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import {
	InMemoryNetwork,
	InMemorySession,
} from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { delay } from "@peerbit/time";
import { anySignal } from "any-signal";
import { FanoutTree } from "../src/index.js";
import {
	int,
	mulberry32,
	quantile,
	runWithConcurrency,
} from "./sim/bench-utils.js";

/**
 * Shared-network A/B evidence for proactive parent upgrades.
 *
 * The single-tree simulator is intentionally kept focused on one root/topic. This
 * harness creates several writer roots and topics inside one in-memory network so
 * probes, root reservations, tracker state, subscriber timers, and publish loops
 * contend in the same process. That is the default-readiness risk this PR needs
 * to measure: a policy can be quiet for one writer while still multiplying
 * control-plane pressure across many independent writer trees.
 */
class SimFanoutTree extends FanoutTree {
	constructor(c: any, opts?: any) {
		super(c, opts);
		this.sign = async () =>
			new SignatureWithKey({
				signature: new Uint8Array([0]),
				publicKey: this.publicKey,
				prehash: PreHash.NONE,
			});
	}

	public async verifyAndProcess(message: any) {
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

type ScenarioName =
	| "ci-multi-live"
	| "ci-multi-live-churn"
	| "ci-multi-video-live"
	| "ci-multi-idle"
	| "ci-multi-sparse-idle"
	| "ci-multi-hotspot-idle";
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
	parentProbeTimeoutMs: number;
	parentProbeMaxPerRound: number;
	parentProbeMaxLagMessages: number;
	parentProbeRejectCooldownMs: number;
	parentProbeRejectCooldownMaxMs: number;
	parentShadowObserveMs: number;
	parentShadowMinObservations: number;
	nodes?: number;
	writers?: number;
	activeWriters?: number;
	subscribersPerTree?: number;
	streamRxDelayMs?: number;
	timeoutMs?: number;
	maxCostRatio: number;
	maxLiveDeadlinePctDelta: number;
	maxLiveChurnGuardSkipsPerSlot: number;
	maxSecondBatchLatencyP95DeltaMs: number;
	maxSecondBatchLatencyP95DeltaRatio: number;
	maxProbePerUpgrade: number;
	maxRootChildrenDelta: number;
	maxRootUploadPctDelta: number;
	strict: boolean;
};

type MultiWriterParams = {
	scenario: ScenarioName;
	seed: number;
	nodes: number;
	writers: number;
	activeWriters: number;
	bootstraps: number;
	subscribersPerTree: number;
	relayFraction: number;
	joinConcurrency: number;
	joinPhaseSettleMs: number;
	messages: number;
	secondBatchMessages: number;
	secondBatchSettleMs: number;
	msgRate: number;
	msgSize: number;
	intervalMs: number;
	settleMs: number;
	deadlineMs: number;
	timeoutMs: number;
	topicPrefix: string;
	rootUploadLimitBps: number;
	rootMaxChildren: number;
	relayUploadLimitBps: number;
	relayMaxChildren: number;
	repair: boolean;
	repairWindowMessages: number;
	repairIntervalMs: number;
	repairMaxPerReq: number;
	neighborRepair: boolean;
	neighborRepairPeers: number;
	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;
	dialDelayMs: number;
	candidateScoringMode: "ranked-shuffle" | "ranked-strict" | "weighted";
	trackerQueryIntervalMs: number;
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
	parentProbeTimeoutMs: number;
	parentProbeMaxPerRound: number;
	parentProbeMaxLagMessages: number;
	parentProbeRejectCooldownMs: number;
	parentProbeRejectCooldownMaxMs: number;
	parentShadowObserveMs: number;
	parentShadowMinObservations: number;
	lateRootConnectAfterMs: number;
	lateRootDuringPublish: boolean;
	lateRootMaxChildren: number;
	lateRootConnectFraction: number;
	churnEveryMs: number;
	churnDownMs: number;
	churnFraction: number;
};

const effectiveMaxChildrenForUpload = (
	params: { msgRate: number; msgSize: number },
	ch: { uploadLimitBps?: number; uploadOverheadBytes?: number },
	maxChildren: number,
) => {
	const requested = Math.max(0, Math.floor(maxChildren));
	const uploadLimitBps = Math.max(0, Math.floor(ch.uploadLimitBps ?? 0));
	if (uploadLimitBps <= 0) return 0;
	const msgRate = Math.max(1, Math.floor(params.msgRate));
	const msgSize = Math.max(1, Math.floor(params.msgSize));
	const uploadOverheadBytes = Math.max(
		0,
		Math.floor(ch.uploadOverheadBytes ?? 128),
	);
	const perChildBytes = Math.max(1, 1 + msgSize + uploadOverheadBytes);
	const perChildBps = Math.max(1, Math.floor(msgRate * perChildBytes));
	return Math.max(
		0,
		Math.min(requested, Math.floor(uploadLimitBps / perChildBps)),
	);
};

type ParentUpgradeActivity = {
	reparentUpgrade: number;
	reparentUpgradeSkipData: number;
	reparentUpgradeSkipRepair: number;
	reparentUpgradeSkipQuiet: number;
	parentProbeReqSent: number;
	parentShadowStart: number;
	parentShadowPromote: number;
};

type TreeShape = {
	treeMaxLevel: number;
	treeLevelP95: number;
	treeLevelAvg: number;
	treeRootChildren: number;
	treeOrphans: number;
	childrenByHash: Map<string, string[]>;
};

type TreeResult = {
	tree: number;
	active: boolean;
	topic: string;
	rootHash: string;
	subscriberCount: number;
	joinedCount: number;
	formationTreeLevelP95: number;
	formationTreeLevelAvg: number;
	formationRootChildren: number;
	treeLevelP95: number;
	treeLevelAvg: number;
	treeRootChildren: number;
	treeOrphans: number;
	expected: number;
	delivered: number;
	deliveredPct: number;
	deliveredWithinDeadlinePct: number;
	secondBatchExpected: number;
	secondBatchDeliveredWithinDeadlinePct: number;
	secondBatchLatencyP95: number;
	secondBatchLatencyP95ByHash: Record<string, number>;
	reparentUpgradeTotal: number;
	publishActiveReparentUpgradeTotal: number;
	publishActiveReparentUpgradeSkipDataTotal: number;
	publishActiveReparentUpgradeSkipRepairTotal: number;
	publishActiveReparentUpgradeSkipQuietTotal: number;
	publishActiveParentProbeReqSentTotal: number;
	publishActiveParentShadowStartTotal: number;
	publishActiveParentShadowPromoteTotal: number;
	parentProbeReqSentTotal: number;
	parentShadowStartTotal: number;
	parentShadowPromoteTotal: number;
	maxReparentUpgradePerPeer: number;
	upgradedPeerHashes: string[];
	upgradedBranchPeerHashes: string[];
	rootUploadFracPct: number;
	controlBytesSent: number;
	controlBytesSentRepair: number;
	controlBytesSentTracker: number;
	dataPayloadBytesSent: number;
};

type MultiWriterResult = {
	params: MultiWriterParams;
	trees: TreeResult[];
	joinedCount: number;
	subscriberSlots: number;
	expected: number;
	delivered: number;
	deliveredPct: number;
	deliveredWithinDeadlinePct: number;
	secondBatchExpected: number;
	secondBatchDeliveredWithinDeadlinePct: number;
	secondBatchLatencyP95: number;
	reparentUpgradeTotal: number;
	parentProbeReqSentTotal: number;
	parentShadowStartTotal: number;
	parentShadowPromoteTotal: number;
	publishActiveReparentUpgradeTotal: number;
	publishActiveParentProbeReqSentTotal: number;
	publishActiveParentShadowStartTotal: number;
	publishActiveParentShadowPromoteTotal: number;
	publishActiveGuardSkipsTotal: number;
	activeGuardedTrees: number;
	treeLevelAvg: number;
	treeLevelP95: number;
	rootChildrenSum: number;
	rootChildrenMax: number;
	rootUploadPctMax: number;
	maxReparentUpgradePerPeer: number;
	controlBpp: number;
	trackerBpp: number;
	repairBpp: number;
	network: InMemoryNetwork["metrics"];
};

type Failure = {
	metric: string;
	baseline: number;
	upgrade: number;
	limit: number;
};

type SummarySample = {
	scenario: ScenarioName;
	seed: number;
	baseline: MultiWriterResult;
	upgrade: MultiWriterResult;
	failures: Failure[];
	usefulPromotedTrees: number;
	promotedBranchGainAvg: number;
};

const SCENARIOS: Record<ScenarioName, Partial<MultiWriterParams>> = {
	"ci-multi-live": {
		scenario: "ci-multi-live",
		nodes: 40,
		writers: 4,
		bootstraps: 1,
		subscribersPerTree: 28,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 120,
		secondBatchMessages: 0,
		secondBatchSettleMs: 0,
		msgRate: 60,
		msgSize: 192,
		settleMs: 1_000,
		deadlineMs: 750,
		timeoutMs: 120_000,
		rootUploadLimitBps: 100_000_000,
		rootMaxChildren: 2,
		relayUploadLimitBps: 100_000_000,
		relayMaxChildren: 4,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 2,
		streamHighWaterMarkBytes: 256 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 700,
		lateRootDuringPublish: true,
		lateRootMaxChildren: 12,
		lateRootConnectFraction: 0.5,
		churnEveryMs: 0,
		churnDownMs: 0,
		churnFraction: 0,
	},
	"ci-multi-live-churn": {
		scenario: "ci-multi-live-churn",
		nodes: 60,
		writers: 6,
		bootstraps: 1,
		subscribersPerTree: 42,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 120,
		secondBatchMessages: 0,
		secondBatchSettleMs: 0,
		msgRate: 60,
		msgSize: 192,
		settleMs: 1_000,
		deadlineMs: 750,
		timeoutMs: 150_000,
		rootUploadLimitBps: 100_000_000,
		rootMaxChildren: 2,
		relayUploadLimitBps: 100_000_000,
		relayMaxChildren: 4,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 2,
		streamHighWaterMarkBytes: 256 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 700,
		lateRootDuringPublish: true,
		lateRootMaxChildren: 16,
		lateRootConnectFraction: 0.5,
		churnEveryMs: 500,
		churnDownMs: 150,
		churnFraction: 0.03,
	},
	"ci-multi-video-live": {
		scenario: "ci-multi-video-live",
		nodes: 48,
		writers: 4,
		bootstraps: 1,
		subscribersPerTree: 32,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 80,
		secondBatchMessages: 0,
		secondBatchSettleMs: 0,
		msgRate: 24,
		msgSize: 1200,
		settleMs: 1_000,
		deadlineMs: 1_500,
		timeoutMs: 150_000,
		rootUploadLimitBps: 150_000,
		rootMaxChildren: 2,
		relayUploadLimitBps: 150_000,
		relayMaxChildren: 3,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 4,
		streamHighWaterMarkBytes: 512 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 700,
		lateRootDuringPublish: true,
		lateRootMaxChildren: 6,
		lateRootConnectFraction: 0.5,
		churnEveryMs: 0,
		churnDownMs: 0,
		churnFraction: 0,
	},
	"ci-multi-idle": {
		scenario: "ci-multi-idle",
		nodes: 40,
		writers: 4,
		activeWriters: 4,
		bootstraps: 1,
		subscribersPerTree: 28,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 12,
		secondBatchMessages: 48,
		secondBatchSettleMs: 1_000,
		msgRate: 50,
		msgSize: 96,
		settleMs: 9_000,
		deadlineMs: 750,
		timeoutMs: 140_000,
		rootUploadLimitBps: 100_000_000,
		rootMaxChildren: 1,
		relayUploadLimitBps: 100_000_000,
		relayMaxChildren: 4,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 3,
		streamHighWaterMarkBytes: 256 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 1_000,
		lateRootDuringPublish: false,
		lateRootMaxChildren: 10,
		lateRootConnectFraction: 0.75,
		churnEveryMs: 0,
		churnDownMs: 0,
		churnFraction: 0,
	},
	"ci-multi-sparse-idle": {
		scenario: "ci-multi-sparse-idle",
		nodes: 72,
		writers: 12,
		activeWriters: 4,
		bootstraps: 1,
		subscribersPerTree: 36,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 12,
		secondBatchMessages: 48,
		secondBatchSettleMs: 1_000,
		msgRate: 50,
		msgSize: 96,
		settleMs: 9_000,
		deadlineMs: 750,
		timeoutMs: 180_000,
		rootUploadLimitBps: 100_000_000,
		rootMaxChildren: 1,
		relayUploadLimitBps: 100_000_000,
		relayMaxChildren: 4,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 3,
		streamHighWaterMarkBytes: 256 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 1_000,
		lateRootDuringPublish: false,
		lateRootMaxChildren: 10,
		lateRootConnectFraction: 0.75,
		churnEveryMs: 0,
		churnDownMs: 0,
		churnFraction: 0,
	},
	"ci-multi-hotspot-idle": {
		scenario: "ci-multi-hotspot-idle",
		nodes: 64,
		writers: 4,
		activeWriters: 4,
		bootstraps: 1,
		subscribersPerTree: 48,
		relayFraction: 0.5,
		joinConcurrency: 12,
		joinPhaseSettleMs: 500,
		messages: 12,
		secondBatchMessages: 48,
		secondBatchSettleMs: 1_000,
		msgRate: 50,
		msgSize: 96,
		settleMs: 9_000,
		deadlineMs: 750,
		timeoutMs: 180_000,
		rootUploadLimitBps: 100_000_000,
		rootMaxChildren: 1,
		relayUploadLimitBps: 100_000_000,
		relayMaxChildren: 4,
		repair: true,
		repairWindowMessages: 512,
		repairIntervalMs: 200,
		repairMaxPerReq: 64,
		neighborRepair: true,
		neighborRepairPeers: 3,
		streamRxDelayMs: 3,
		streamHighWaterMarkBytes: 256 * 1024,
		dialDelayMs: 0,
		candidateScoringMode: "weighted",
		trackerQueryIntervalMs: 1_000,
		lateRootConnectAfterMs: 1_000,
		lateRootDuringPublish: false,
		lateRootMaxChildren: 12,
		lateRootConnectFraction: 1,
		churnEveryMs: 0,
		churnDownMs: 0,
		churnFraction: 0,
	},
};

const usage = () => {
	console.log(
		[
			"fanout-tree-parent-upgrade-multi-eval.ts",
			"",
			"Options:",
			"  --scenario NAME              ci-multi-live|ci-multi-live-churn|ci-multi-video-live|ci-multi-idle|ci-multi-sparse-idle|ci-multi-hotspot-idle|all (default: all)",
			"  --seeds CSV                  seeds to run for each scenario (default: 1,2,3)",
			"  --parentUpgradePreset NAME   raw|default-candidate (default: raw)",
			"  --parentUpgradeIntervalMs MS treatment upgrade interval (default: 1000)",
			"  --parentUpgradeMode MODE     direct|probe|shadow (default: preset-dependent)",
			"  --maxLiveChurnGuardSkipsPerSlot N max active guard skips per subscriber slot for ci-multi-live-churn (default: 1)",
			"  --nodes N                    override scenario node count",
			"  --writers N                  override scenario writer/root count",
			"  --activeWriters N            override active publisher count; remaining joined writer trees stay idle",
			"  --subscribersPerTree N       override scenario subscriber slots per writer",
			"  --timeoutMs MS               override scenario timeout",
			"  --strict 0|1                 exit non-zero on evidence failure (default: 0)",
			"",
			"Examples:",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-live --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-live-churn --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-video-live --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-sparse-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-multi-eval --scenario ci-multi-hotspot-idle --seeds 1,2,3 --parentUpgradePreset default-candidate --strict 1",
		].join("\n"),
	);
};

const parseBool01 = (value: string | undefined, fallback: boolean) => {
	if (value === undefined) return fallback;
	return value === "1";
};

const parseCsvNumbers = (value: string | undefined, fallback: number[]) => {
	if (!value) return fallback;
	return value
		.split(",")
		.map((part) => Number(part.trim()))
		.filter((n) => Number.isFinite(n));
};

const parseScenarios = (value: string | undefined): ScenarioName[] => {
	if (!value || value === "all") {
		return [
			"ci-multi-live",
			"ci-multi-live-churn",
			"ci-multi-video-live",
			"ci-multi-idle",
			"ci-multi-sparse-idle",
			"ci-multi-hotspot-idle",
		];
	}
	const scenarios = value.split(",").map((part) => part.trim());
	for (const scenario of scenarios) {
		if (
			scenario !== "ci-multi-live" &&
			scenario !== "ci-multi-live-churn" &&
			scenario !== "ci-multi-video-live" &&
			scenario !== "ci-multi-idle" &&
			scenario !== "ci-multi-sparse-idle" &&
			scenario !== "ci-multi-hotspot-idle"
		) {
			throw new Error(`Unknown scenario: ${scenario}`);
		}
	}
	return scenarios as ScenarioName[];
};

const isLiveScenario = (scenario: ScenarioName) =>
	scenario === "ci-multi-live" ||
	scenario === "ci-multi-live-churn" ||
	scenario === "ci-multi-video-live";
const isLiveChurnScenario = (scenario: ScenarioName) =>
	scenario === "ci-multi-live-churn";
const isPositiveIdleScenario = (scenario: ScenarioName) =>
	scenario === "ci-multi-idle";
const isHotspotIdleScenario = (scenario: ScenarioName) =>
	scenario === "ci-multi-hotspot-idle";
const isSparseIdleScenario = (scenario: ScenarioName) =>
	scenario === "ci-multi-sparse-idle";

const parseArgs = (argv: string[]): EvalArgs => {
	const get = (name: string) => {
		const i = argv.indexOf(name);
		return i >= 0 ? argv[i + 1] : undefined;
	};
	if (argv.includes("--help") || argv.includes("-h")) {
		usage();
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
		nodes: get("--nodes") == null ? undefined : Number(get("--nodes")),
		writers: get("--writers") == null ? undefined : Number(get("--writers")),
		activeWriters:
			get("--activeWriters") == null
				? undefined
				: Number(get("--activeWriters")),
		subscribersPerTree:
			get("--subscribersPerTree") == null
				? undefined
				: Number(get("--subscribersPerTree")),
		streamRxDelayMs:
			get("--streamRxDelayMs") == null
				? undefined
				: Number(get("--streamRxDelayMs")),
		timeoutMs:
			get("--timeoutMs") == null ? undefined : Number(get("--timeoutMs")),
		maxCostRatio: Number(get("--maxCostRatio") ?? 1.15),
		maxLiveDeadlinePctDelta: Number(get("--maxLiveDeadlinePctDelta") ?? 2),
		maxLiveChurnGuardSkipsPerSlot: Number(
			get("--maxLiveChurnGuardSkipsPerSlot") ?? 1,
		),
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
		strict: parseBool01(get("--strict"), false),
	};
};

const resolveParams = (
	scenario: ScenarioName,
	seed: number,
	args: EvalArgs,
	upgradeEnabled: boolean,
): MultiWriterParams => {
	const base = SCENARIOS[scenario];
	const msgRate = Number(base.msgRate ?? 30);
	const writers = Number(args.writers ?? base.writers ?? 4);
	const activeWriters = Math.max(
		0,
		Math.min(
			writers,
			Math.floor(Number(args.activeWriters ?? base.activeWriters ?? writers)),
		),
	);
	return {
		scenario,
		seed,
		nodes: Number(args.nodes ?? base.nodes ?? 40),
		writers,
		activeWriters,
		bootstraps: Number(base.bootstraps ?? 1),
		subscribersPerTree: Number(
			args.subscribersPerTree ?? base.subscribersPerTree ?? 28,
		),
		relayFraction: Number(base.relayFraction ?? 0.5),
		joinConcurrency: Number(base.joinConcurrency ?? 12),
		joinPhaseSettleMs: Number(base.joinPhaseSettleMs ?? 500),
		messages: Number(base.messages ?? 120),
		secondBatchMessages: Number(base.secondBatchMessages ?? 0),
		secondBatchSettleMs: Number(base.secondBatchSettleMs ?? 0),
		msgRate,
		msgSize: Number(base.msgSize ?? 128),
		intervalMs: msgRate > 0 ? Math.floor(1000 / msgRate) : 0,
		settleMs: Number(base.settleMs ?? 1_000),
		deadlineMs: Number(base.deadlineMs ?? 750),
		timeoutMs: Number(args.timeoutMs ?? base.timeoutMs ?? 120_000),
		topicPrefix: `multi-${scenario}-${seed}`,
		rootUploadLimitBps: Number(base.rootUploadLimitBps ?? 100_000_000),
		rootMaxChildren: Number(base.rootMaxChildren ?? 2),
		relayUploadLimitBps: Number(base.relayUploadLimitBps ?? 100_000_000),
		relayMaxChildren: Number(base.relayMaxChildren ?? 4),
		repair: Boolean(base.repair ?? true),
		repairWindowMessages: Number(base.repairWindowMessages ?? 512),
		repairIntervalMs: Number(base.repairIntervalMs ?? 200),
		repairMaxPerReq: Number(base.repairMaxPerReq ?? 64),
		neighborRepair: Boolean(base.neighborRepair ?? true),
		neighborRepairPeers: Number(base.neighborRepairPeers ?? 3),
		streamRxDelayMs: Number(args.streamRxDelayMs ?? base.streamRxDelayMs ?? 0),
		streamHighWaterMarkBytes: Number(
			base.streamHighWaterMarkBytes ?? 256 * 1024,
		),
		dialDelayMs: Number(base.dialDelayMs ?? 0),
		candidateScoringMode:
			base.candidateScoringMode === "ranked-strict" ||
			base.candidateScoringMode === "weighted" ||
			base.candidateScoringMode === "ranked-shuffle"
				? base.candidateScoringMode
				: "weighted",
		trackerQueryIntervalMs: Number(base.trackerQueryIntervalMs ?? 1_000),
		parentUpgradeIntervalMs: upgradeEnabled ? args.parentUpgradeIntervalMs : 0,
		parentUpgradeLeafOnly: args.parentUpgradeLeafOnly,
		parentUpgradeMinLevelGain: args.parentUpgradeMinLevelGain,
		parentUpgradeRootMinLevelGain: args.parentUpgradeRootMinLevelGain,
		parentUpgradeRootMinSubtreeGain: args.parentUpgradeRootMinSubtreeGain,
		parentUpgradeNonRootMinLevelGain: args.parentUpgradeNonRootMinLevelGain,
		parentUpgradeMinFreeSlots: args.parentUpgradeMinFreeSlots,
		parentUpgradeRootMinFreeSlots: args.parentUpgradeRootMinFreeSlots,
		parentUpgradeMaxChildLoadRatio: args.parentUpgradeMaxChildLoadRatio,
		parentUpgradeRootMaxChildLoadRatio: args.parentUpgradeRootMaxChildLoadRatio,
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
		lateRootConnectAfterMs: Number(base.lateRootConnectAfterMs ?? -1),
		lateRootDuringPublish: Boolean(base.lateRootDuringPublish ?? false),
		lateRootMaxChildren: Number(base.lateRootMaxChildren ?? 0),
		lateRootConnectFraction: Number(base.lateRootConnectFraction ?? 1),
		churnEveryMs: Number(base.churnEveryMs ?? 0),
		churnDownMs: Number(base.churnDownMs ?? 0),
		churnFraction: Number(base.churnFraction ?? 0),
	};
};

const parseSimPeerIndex = (peerId: any): number => {
	const s = String(peerId?.toString?.() ?? "");
	const m = s.match(/sim-(\d+)/);
	if (!m) return 0;
	const n = Number(m[1]);
	return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
};

const pickDistinct = (
	rng: () => number,
	values: number[],
	count: number,
): number[] => {
	const target = Math.max(0, Math.min(values.length, Math.floor(count)));
	const chosen = new Set<number>();
	while (chosen.size < target) {
		chosen.add(values[int(rng, values.length)]!);
	}
	return [...chosen];
};

const fmt = (value: number, digits = 2) =>
	Number.isFinite(value) ? value.toFixed(digits) : "NaN";

const ratioLimit = (baseline: number, ratio: number, absoluteSlack = 1e-9) =>
	Math.max(absoluteSlack, baseline * ratio + absoluteSlack);

const failIfGreater = (
	failures: Failure[],
	metric: string,
	baseline: number,
	upgrade: number,
	limit: number,
) => {
	if (!Number.isFinite(upgrade) || upgrade > limit) {
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
	if (!Number.isFinite(upgrade) || upgrade < limit) {
		failures.push({ metric, baseline, upgrade, limit });
	}
};

const peerLatencyP95For = (result: TreeResult, hashes: string[]): number => {
	const values: number[] = [];
	for (const hash of hashes) {
		const value = result.secondBatchLatencyP95ByHash[hash];
		if (Number.isFinite(value)) values.push(value);
	}
	values.sort((a, b) => a - b);
	return quantile(values, 0.95);
};

const avgFinite = (values: number[]) => {
	const finite = values.filter((value) => Number.isFinite(value));
	return finite.length === 0
		? NaN
		: finite.reduce((sum, value) => sum + value, 0) / finite.length;
};

const runMultiWriterSim = async (
	params: MultiWriterParams,
): Promise<MultiWriterResult> => {
	const timeoutMs = Math.max(0, Math.floor(params.timeoutMs));
	const timeoutController = new AbortController();
	const timeoutSignal = timeoutController.signal;
	let timer: ReturnType<typeof setTimeout> | undefined;
	if (timeoutMs > 0) {
		timer = setTimeout(() => {
			timeoutController.abort(
				new Error(
					`fanout-tree-parent-upgrade-multi-eval timed out after ${timeoutMs}ms`,
				),
			);
		}, timeoutMs);
	}

	const rng = mulberry32(params.seed);
	const rootIndices = Array.from({ length: params.writers }, (_, i) => i);
	const bootstrapIndices = Array.from(
		{ length: params.bootstraps },
		(_, i) => params.writers + i,
	).filter((i) => i < params.nodes);
	const reserved = new Set([...rootIndices, ...bootstrapIndices]);
	const subscriberPool = Array.from(
		{ length: params.nodes },
		(_, i) => i,
	).filter((i) => !reserved.has(i));
	const subscriberIndices = pickDistinct(
		rng,
		subscriberPool,
		params.subscribersPerTree,
	).sort((a, b) => a - b);
	const relayCount = Math.max(
		1,
		Math.floor(subscriberIndices.length * params.relayFraction),
	);
	const relaySet = new Set(pickDistinct(rng, subscriberIndices, relayCount));
	const bootstrapIndexSet = new Set(bootstrapIndices);
	const rootIndexSet = new Set(rootIndices);

	const network = new InMemoryNetwork({
		streamRxDelayMs: params.streamRxDelayMs,
		streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
		dialDelayMs: params.dialDelayMs,
		dropSeed: params.seed,
	});
	const maxConnectionsFor = (index: number) => {
		if (rootIndexSet.has(index)) {
			return Math.max(256, params.lateRootMaxChildren * params.writers * 8);
		}
		if (bootstrapIndexSet.has(index)) return Math.max(512, params.nodes * 8);
		if (relaySet.has(index)) {
			return Math.max(128, params.relayMaxChildren * params.writers * 4);
		}
		return Math.max(32, params.writers * 8);
	};
	const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
		params.nodes,
		{
			network,
			basePort: 31_000,
			services: {
				fanout: (c) => {
					const index = parseSimPeerIndex(c?.peerId);
					return new SimFanoutTree(c, {
						connectionManager: {
							minConnections: 0,
							maxConnections: maxConnectionsFor(index),
							dialer: false,
							pruner: { interval: 1_000 },
						},
						seenCacheMax: rootIndexSet.has(index)
							? 200_000
							: bootstrapIndexSet.has(index)
								? 100_000
								: relaySet.has(index)
									? 50_000
									: 20_000,
						seenCacheTtlMs:
							rootIndexSet.has(index) || bootstrapIndexSet.has(index)
								? 120_000
								: 60_000,
						random: mulberry32((params.seed >>> 0) ^ index),
					});
				},
			},
		},
	);

	try {
		const bootstrapAddrs = bootstrapIndices.flatMap((i) =>
			session.peers[i]!.getMultiaddrs(),
		);
		if (bootstrapAddrs.length === 0) {
			throw new Error("No bootstrap addrs; scenario needs bootstraps >= 1");
		}
		for (const p of session.peers) {
			p.services.fanout.setBootstraps(bootstrapAddrs);
		}

		const trees = rootIndices.map((rootIndex, tree) => {
			const root = session.peers[rootIndex]!.services.fanout;
			const rootHash = root.publicKeyHash;
			const topic = `${params.topicPrefix}-${tree}`;
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: params.msgRate,
				msgSize: params.msgSize,
				uploadLimitBps: params.rootUploadLimitBps,
				maxChildren: params.rootMaxChildren,
				repair: params.repair,
				repairWindowMessages: params.repairWindowMessages,
				repairIntervalMs: params.repairIntervalMs,
				repairMaxPerReq: params.repairMaxPerReq,
				neighborRepair: params.neighborRepair,
				neighborRepairPeers: params.neighborRepairPeers,
			});
			return {
				tree,
				active: tree < params.activeWriters,
				rootIndex,
				root,
				rootHash,
				topic,
				joinedIndices: new Set<number>(),
			};
		});
		const activeTrees = trees.filter((tree) => tree.active);

		const joinOne = async (
			tree: (typeof trees)[number],
			index: number,
		): Promise<boolean> => {
			const node = session.peers[index]!.services.fanout;
			const isRelay = relaySet.has(index);
			try {
				await node.joinChannel(
					tree.topic,
					tree.rootHash,
					{
						msgRate: params.msgRate,
						msgSize: params.msgSize,
						uploadLimitBps: isRelay ? params.relayUploadLimitBps : 0,
						maxChildren: isRelay ? params.relayMaxChildren : 0,
						repair: params.repair,
						repairWindowMessages: params.repairWindowMessages,
						repairIntervalMs: params.repairIntervalMs,
						repairMaxPerReq: params.repairMaxPerReq,
						neighborRepair: params.neighborRepair,
						neighborRepairPeers: params.neighborRepairPeers,
					},
					{
						timeoutMs: Math.max(
							10_000,
							Math.min(120_000, timeoutMs || 120_000),
						),
						candidateScoringMode: params.candidateScoringMode,
						trackerQueryIntervalMs: params.trackerQueryIntervalMs,
						parentUpgradeIntervalMs: params.parentUpgradeIntervalMs,
						parentUpgradeLeafOnly: params.parentUpgradeLeafOnly,
						parentUpgradeMinLevelGain: params.parentUpgradeMinLevelGain,
						parentUpgradeRootMinLevelGain: params.parentUpgradeRootMinLevelGain,
						parentUpgradeRootMinSubtreeGain:
							params.parentUpgradeRootMinSubtreeGain,
						parentUpgradeNonRootMinLevelGain:
							params.parentUpgradeNonRootMinLevelGain,
						parentUpgradeMinFreeSlots: params.parentUpgradeMinFreeSlots,
						parentUpgradeRootMinFreeSlots: params.parentUpgradeRootMinFreeSlots,
						parentUpgradeMaxChildLoadRatio:
							params.parentUpgradeMaxChildLoadRatio,
						parentUpgradeRootMaxChildLoadRatio:
							params.parentUpgradeRootMaxChildLoadRatio,
						parentUpgradeCooldownMs: params.parentUpgradeCooldownMs,
						parentUpgradeFailedBackoffMinMs:
							params.parentUpgradeFailedBackoffMinMs,
						parentUpgradeFailedBackoffMaxMs:
							params.parentUpgradeFailedBackoffMaxMs,
						parentUpgradeQuietMs: params.parentUpgradeQuietMs,
						parentUpgradeRepairQuietMs: params.parentUpgradeRepairQuietMs,
						parentUpgradeMaxPerPeer: params.parentUpgradeMaxPerPeer,
						parentUpgradeRepairGuard: params.parentUpgradeRepairGuard,
						parentUpgradeDataGuard: params.parentUpgradeDataGuard,
						parentUpgradeMode: params.parentUpgradeMode,
						parentUpgradeVerifyStaleRootCapacity:
							params.parentUpgradeVerifyStaleRootCapacity,
						parentUpgradeStaleRootProbeProbability:
							params.parentUpgradeStaleRootProbeProbability,
						parentProbeTimeoutMs: params.parentProbeTimeoutMs,
						parentProbeMaxPerRound: params.parentProbeMaxPerRound,
						parentProbeMaxLagMessages: params.parentProbeMaxLagMessages,
						parentProbeRejectCooldownMs: params.parentProbeRejectCooldownMs,
						parentProbeRejectCooldownMaxMs:
							params.parentProbeRejectCooldownMaxMs,
						parentShadowObserveMs: params.parentShadowObserveMs,
						parentShadowMinObservations: params.parentShadowMinObservations,
						signal: timeoutSignal,
					},
				);
				tree.joinedIndices.add(index);
				return true;
			} catch {
				return false;
			}
		};

		const relayTasks: Array<() => Promise<boolean>> = [];
		const leafTasks: Array<() => Promise<boolean>> = [];
		for (const tree of trees) {
			for (const index of subscriberIndices) {
				const task = () => joinOne(tree, index);
				if (relaySet.has(index)) relayTasks.push(task);
				else leafTasks.push(task);
			}
		}
		await runWithConcurrency(relayTasks, params.joinConcurrency);
		if (params.joinPhaseSettleMs > 0) {
			await delay(params.joinPhaseSettleMs, { signal: timeoutSignal });
		}
		await runWithConcurrency(leafTasks, params.joinConcurrency);

		const computeTreeShape = (tree: (typeof trees)[number]): TreeShape => {
			const levels: number[] = [];
			const childrenByHash = new Map<string, string[]>();
			let treeRootChildren = 0;
			let treeOrphans = 0;
			for (const p of session.peers) {
				const hash = p.services.fanout.publicKeyHash;
				const s = p.services.fanout.getChannelStats(tree.topic, tree.rootHash);
				if (!s) continue;
				if (!childrenByHash.has(hash)) childrenByHash.set(hash, []);
				if (Number.isFinite(s.level)) levels.push(s.level);
				if (s.parent) {
					const children = childrenByHash.get(s.parent) ?? [];
					children.push(hash);
					childrenByHash.set(s.parent, children);
				}
				if (s.level === 0) {
					treeRootChildren = s.children;
				} else if (Number.isFinite(s.level) && !s.parent) {
					treeOrphans += 1;
				}
			}
			levels.sort((a, b) => a - b);
			return {
				treeMaxLevel: levels.length > 0 ? levels[levels.length - 1]! : 0,
				treeLevelP95: levels.length > 0 ? quantile(levels, 0.95) : 0,
				treeLevelAvg:
					levels.length > 0
						? levels.reduce((sum, value) => sum + value, 0) / levels.length
						: 0,
				treeRootChildren,
				treeOrphans,
				childrenByHash,
			};
		};

		const formationByTree = trees.map((tree) => computeTreeShape(tree));
		const totalMessages = params.messages + params.secondBatchMessages;
		const secondBatchStartSeq = params.messages;
		const payload = new Uint8Array(Math.max(0, params.msgSize));
		for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;

		const deliveryByTree = trees.map((tree) => {
			const joinedHashes = [...tree.joinedIndices].map(
				(i) => session.peers[i]!.services.fanout.publicKeyHash,
			);
			const hashToIndex = new Map<string, number>();
			for (let i = 0; i < joinedHashes.length; i++) {
				hashToIndex.set(joinedHashes[i]!, i);
			}
			const bitsetBytes = Math.ceil(totalMessages / 8);
			return {
				publishAt: new Map<number, number>(),
				joinedHashes,
				hashToIndex,
				receivedBits: joinedHashes.map(() => new Uint8Array(bitsetBytes)),
				receivedCounts: new Uint32Array(joinedHashes.length),
				secondBatchReceivedCounts: new Uint32Array(joinedHashes.length),
				deliveredWithinDeadline: 0,
				secondBatchDeliveredWithinDeadline: 0,
				duplicates: 0,
				secondBatchLatencySamples: [] as number[],
				secondBatchLatencySamplesByPeer: joinedHashes.map(() => [] as number[]),
			};
		});

		const treeByKey = new Map<string, number>();
		for (const tree of trees)
			treeByKey.set(`${tree.topic}:${tree.rootHash}`, tree.tree);
		const makeOnData = (localHash: string) => (ev: any) => {
			const d = ev?.detail;
			if (!d) return;
			const treeIndex = treeByKey.get(`${d.topic}:${d.root}`);
			if (treeIndex == null) return;
			const delivery = deliveryByTree[treeIndex]!;
			const peerIndex = delivery.hashToIndex.get(localHash);
			if (peerIndex == null) return;
			const seq = d.seq >>> 0;
			if (seq >= totalMessages) return;
			const bits = delivery.receivedBits[peerIndex]!;
			const byteIndex = seq >>> 3;
			const mask = 1 << (seq & 7);
			if ((bits[byteIndex]! & mask) !== 0) {
				delivery.duplicates += 1;
				return;
			}
			bits[byteIndex] |= mask;
			delivery.receivedCounts[peerIndex] += 1;
			if (seq >= secondBatchStartSeq) {
				delivery.secondBatchReceivedCounts[peerIndex] += 1;
			}
			const sentAt = delivery.publishAt.get(seq);
			if (sentAt == null) return;
			const latency = Date.now() - sentAt;
			if (params.deadlineMs > 0 && latency <= params.deadlineMs) {
				delivery.deliveredWithinDeadline += 1;
				if (seq >= secondBatchStartSeq) {
					delivery.secondBatchDeliveredWithinDeadline += 1;
				}
			}
			if (seq >= secondBatchStartSeq) {
				delivery.secondBatchLatencySamples.push(latency);
				delivery.secondBatchLatencySamplesByPeer[peerIndex]!.push(latency);
			}
		};

		for (const index of subscriberIndices) {
			const node = session.peers[index]!.services.fanout;
			node.addEventListener(
				"fanout:data",
				makeOnData(node.publicKeyHash) as any,
			);
		}

		const collectParentUpgradeActivity = (
			tree: (typeof trees)[number],
		): ParentUpgradeActivity => {
			const out: ParentUpgradeActivity = {
				reparentUpgrade: 0,
				reparentUpgradeSkipData: 0,
				reparentUpgradeSkipRepair: 0,
				reparentUpgradeSkipQuiet: 0,
				parentProbeReqSent: 0,
				parentShadowStart: 0,
				parentShadowPromote: 0,
			};
			for (const p of session.peers) {
				const m = p.services.fanout.getChannelMetrics(
					tree.topic,
					tree.rootHash,
				);
				out.reparentUpgrade += m.reparentUpgrade;
				out.reparentUpgradeSkipData += m.reparentUpgradeSkipData;
				out.reparentUpgradeSkipRepair += m.reparentUpgradeSkipRepair;
				out.reparentUpgradeSkipQuiet += m.reparentUpgradeSkipQuiet;
				out.parentProbeReqSent += m.parentProbeReqSent;
				out.parentShadowStart += m.parentShadowStart;
				out.parentShadowPromote += m.parentShadowPromote;
			}
			return out;
		};
		const diffActivity = (
			after: ParentUpgradeActivity,
			before: ParentUpgradeActivity,
		): ParentUpgradeActivity => ({
			reparentUpgrade: Math.max(
				0,
				after.reparentUpgrade - before.reparentUpgrade,
			),
			reparentUpgradeSkipData: Math.max(
				0,
				after.reparentUpgradeSkipData - before.reparentUpgradeSkipData,
			),
			reparentUpgradeSkipRepair: Math.max(
				0,
				after.reparentUpgradeSkipRepair - before.reparentUpgradeSkipRepair,
			),
			reparentUpgradeSkipQuiet: Math.max(
				0,
				after.reparentUpgradeSkipQuiet - before.reparentUpgradeSkipQuiet,
			),
			parentProbeReqSent: Math.max(
				0,
				after.parentProbeReqSent - before.parentProbeReqSent,
			),
			parentShadowStart: Math.max(
				0,
				after.parentShadowStart - before.parentShadowStart,
			),
			parentShadowPromote: Math.max(
				0,
				after.parentShadowPromote - before.parentShadowPromote,
			),
		});

		const applyLateRootTopology = async () => {
			if (params.lateRootMaxChildren > 0) {
				for (const tree of trees) {
					const id = tree.root.getChannelId(tree.topic, tree.rootHash);
					const ch = (tree.root as any).channelsBySuffixKey?.get?.(
						id.suffixKey,
					);
					if (ch) {
						ch.maxChildren = Math.max(
							ch.maxChildren ?? 0,
							params.lateRootMaxChildren,
						);
						const uploadBoundedMaxChildren = effectiveMaxChildrenForUpload(
							params,
							ch,
							params.lateRootMaxChildren,
						);
						ch.effectiveMaxChildren = Math.max(
							ch.effectiveMaxChildren ?? 0,
							uploadBoundedMaxChildren,
						);
						void (tree.root as any)
							.announceToTrackers?.(ch, timeoutSignal)
							?.catch?.(() => {});
					}
				}
			}
			const target = Math.min(
				subscriberIndices.length,
				Math.max(
					0,
					Math.ceil(subscriberIndices.length * params.lateRootConnectFraction),
				),
			);
			for (const tree of trees) {
				for (let i = 0; i < target; i++) {
					const idx =
						subscriberIndices[(i + tree.tree * 3) % subscriberIndices.length]!;
					try {
						await session.peers[idx]!.dial(
							session.peers[tree.rootIndex]!.getMultiaddrs(),
						);
					} catch {
						// best-effort late underlay shortcut
					}
				}
			}
		};

		let lateRootApplied = false;
		const applyLateRootOnce = async () => {
			if (lateRootApplied || params.lateRootConnectAfterMs < 0) return;
			lateRootApplied = true;
			await applyLateRootTopology();
		};

		const churnController = new AbortController();
		const churnSignal = anySignal([
			timeoutSignal,
			churnController.signal,
		]) as AbortSignal & { clear?: () => void };
		const churnLoop = async () => {
			if (
				params.churnEveryMs <= 0 ||
				params.churnDownMs <= 0 ||
				params.churnFraction <= 0
			) {
				return;
			}
			for (;;) {
				if (churnSignal.aborted) return;
				await delay(params.churnEveryMs, { signal: churnSignal });
				const target = Math.max(
					1,
					Math.floor(subscriberIndices.length * params.churnFraction),
				);
				const chosen = pickDistinct(rng, subscriberIndices, target);
				const now = Date.now();
				await Promise.all(
					chosen.map(async (idx) => {
						const peer = session.peers[idx]!;
						network.setPeerOffline(peer.peerId, params.churnDownMs, now);
						await network.disconnectPeer(peer.peerId);
					}),
				);
			}
		};
		const churnPromise = churnLoop().catch(() => {});

		const publishStartActivity = trees.map((tree) =>
			collectParentUpgradeActivity(tree),
		);
		let activeActivity = publishStartActivity.map((activity) =>
			diffActivity(activity, activity),
		);
		const lateRootDuringPublishPromise =
			params.lateRootDuringPublish && params.lateRootConnectAfterMs >= 0
				? (async () => {
						if (params.lateRootConnectAfterMs > 0) {
							await delay(params.lateRootConnectAfterMs, {
								signal: timeoutSignal,
							});
						}
						await applyLateRootOnce();
					})()
				: undefined;

		const publishRange = async (from: number, to: number) => {
			await Promise.all(
				activeTrees.map(async (tree) => {
					const delivery = deliveryByTree[tree.tree]!;
					for (let seq = from; seq < to; seq++) {
						if (timeoutSignal.aborted) {
							throw timeoutSignal.reason ?? new Error("multi eval aborted");
						}
						delivery.publishAt.set(seq, Date.now());
						await tree.root.publishData(tree.topic, tree.rootHash, payload);
						if (params.intervalMs > 0) {
							await delay(params.intervalMs, { signal: timeoutSignal });
						}
					}
				}),
			);
		};

		try {
			await publishRange(0, params.messages);
		} finally {
			activeActivity = trees.map((tree, i) =>
				diffActivity(
					collectParentUpgradeActivity(tree),
					publishStartActivity[i]!,
				),
			);
			await lateRootDuringPublishPromise;
		}

		if (params.repair && params.messages > 0) {
			await Promise.all(
				activeTrees.map((tree) =>
					tree.root.publishEnd(tree.topic, tree.rootHash, params.messages),
				),
			);
		}

		if (params.settleMs > 0) {
			if (
				!params.lateRootDuringPublish &&
				params.lateRootConnectAfterMs >= 0 &&
				params.lateRootConnectAfterMs < params.settleMs
			) {
				if (params.lateRootConnectAfterMs > 0) {
					await delay(params.lateRootConnectAfterMs, { signal: timeoutSignal });
				}
				await applyLateRootOnce();
				const remaining = Math.max(
					0,
					params.settleMs - params.lateRootConnectAfterMs,
				);
				if (remaining > 0) await delay(remaining, { signal: timeoutSignal });
			} else {
				await delay(params.settleMs, { signal: timeoutSignal });
			}
		}

		if (params.secondBatchMessages > 0) {
			await publishRange(
				secondBatchStartSeq,
				secondBatchStartSeq + params.secondBatchMessages,
			);
			if (params.repair) {
				await Promise.all(
					activeTrees.map((tree) =>
						tree.root.publishEnd(
							tree.topic,
							tree.rootHash,
							secondBatchStartSeq + params.secondBatchMessages,
						),
					),
				);
			}
			if (params.secondBatchSettleMs > 0) {
				await delay(params.secondBatchSettleMs, { signal: timeoutSignal });
			}
		}
		churnController.abort();
		await churnPromise;
		churnSignal.clear?.();

		const finalShapeByTree = trees.map((tree) => computeTreeShape(tree));
		const treeResults: TreeResult[] = [];
		for (const tree of trees) {
			const formation = formationByTree[tree.tree]!;
			const shape = finalShapeByTree[tree.tree]!;
			const delivery = deliveryByTree[tree.tree]!;
			const joinedCount = delivery.joinedHashes.length;
			const treeMessageCount = tree.active ? totalMessages : 0;
			const treeSecondBatchMessages = tree.active
				? params.secondBatchMessages
				: 0;
			let delivered = 0;
			for (const c of delivery.receivedCounts) delivered += c;
			let secondBatchDelivered = 0;
			for (const c of delivery.secondBatchReceivedCounts) {
				secondBatchDelivered += c;
			}
			const expected = joinedCount * treeMessageCount;
			const secondBatchExpected = joinedCount * treeSecondBatchMessages;
			const secondBatchLatencySamples = delivery.secondBatchLatencySamples.sort(
				(a, b) => a - b,
			);
			const secondBatchLatencyP95ByHash: Record<string, number> = {};
			for (let i = 0; i < delivery.joinedHashes.length; i++) {
				const samples = delivery.secondBatchLatencySamplesByPeer[i]!;
				if (samples.length === 0) continue;
				samples.sort((a, b) => a - b);
				secondBatchLatencyP95ByHash[delivery.joinedHashes[i]!] = quantile(
					samples,
					0.95,
				);
			}

			let controlBytesSent = 0;
			let controlBytesSentRepair = 0;
			let controlBytesSentTracker = 0;
			let dataPayloadBytesSent = 0;
			let reparentUpgradeTotal = 0;
			let parentProbeReqSentTotal = 0;
			let parentShadowStartTotal = 0;
			let parentShadowPromoteTotal = 0;
			let maxReparentUpgradePerPeer = 0;
			const upgradedPeerHashes: string[] = [];

			for (const p of session.peers) {
				const nodeHash = p.services.fanout.publicKeyHash;
				const m = p.services.fanout.getChannelMetrics(
					tree.topic,
					tree.rootHash,
				);
				controlBytesSent += m.controlBytesSent;
				controlBytesSentRepair += m.controlBytesSentRepair;
				controlBytesSentTracker += m.controlBytesSentTracker;
				dataPayloadBytesSent += m.dataPayloadBytesSent;
				reparentUpgradeTotal += m.reparentUpgrade;
				parentProbeReqSentTotal += m.parentProbeReqSent;
				parentShadowStartTotal += m.parentShadowStart;
				parentShadowPromoteTotal += m.parentShadowPromote;
				maxReparentUpgradePerPeer = Math.max(
					maxReparentUpgradePerPeer,
					m.reparentUpgrade,
				);
				if (m.reparentUpgrade > 0) upgradedPeerHashes.push(nodeHash);
			}

			const upgradedBranchHashSet = new Set<string>();
			for (const hash of upgradedPeerHashes) {
				const stack = [hash];
				while (stack.length > 0) {
					const next = stack.pop()!;
					if (upgradedBranchHashSet.has(next)) continue;
					if (delivery.hashToIndex.has(next)) upgradedBranchHashSet.add(next);
					for (const child of shape.childrenByHash.get(next) ?? []) {
						stack.push(child);
					}
				}
			}
			const active = activeActivity[tree.tree]!;
			const rootUploadBps =
				session.network.peerMetricsByHash.get(tree.rootHash)
					?.maxBytesPerSecond ?? 0;
			treeResults.push({
				tree: tree.tree,
				active: tree.active,
				topic: tree.topic,
				rootHash: tree.rootHash,
				subscriberCount: subscriberIndices.length,
				joinedCount,
				formationTreeLevelP95: formation.treeLevelP95,
				formationTreeLevelAvg: formation.treeLevelAvg,
				formationRootChildren: formation.treeRootChildren,
				treeLevelP95: shape.treeLevelP95,
				treeLevelAvg: shape.treeLevelAvg,
				treeRootChildren: shape.treeRootChildren,
				treeOrphans: shape.treeOrphans,
				expected,
				delivered,
				deliveredPct: expected === 0 ? 100 : (100 * delivered) / expected,
				deliveredWithinDeadlinePct:
					expected === 0
						? 100
						: (100 * delivery.deliveredWithinDeadline) / expected,
				secondBatchExpected,
				secondBatchDeliveredWithinDeadlinePct:
					secondBatchExpected === 0
						? 100
						: (100 * delivery.secondBatchDeliveredWithinDeadline) /
							secondBatchExpected,
				secondBatchLatencyP95:
					secondBatchLatencySamples.length > 0
						? quantile(secondBatchLatencySamples, 0.95)
						: NaN,
				secondBatchLatencyP95ByHash,
				reparentUpgradeTotal,
				publishActiveReparentUpgradeTotal: active.reparentUpgrade,
				publishActiveReparentUpgradeSkipDataTotal:
					active.reparentUpgradeSkipData,
				publishActiveReparentUpgradeSkipRepairTotal:
					active.reparentUpgradeSkipRepair,
				publishActiveReparentUpgradeSkipQuietTotal:
					active.reparentUpgradeSkipQuiet,
				publishActiveParentProbeReqSentTotal: active.parentProbeReqSent,
				publishActiveParentShadowStartTotal: active.parentShadowStart,
				publishActiveParentShadowPromoteTotal: active.parentShadowPromote,
				parentProbeReqSentTotal,
				parentShadowStartTotal,
				parentShadowPromoteTotal,
				maxReparentUpgradePerPeer,
				upgradedPeerHashes,
				upgradedBranchPeerHashes: [...upgradedBranchHashSet],
				rootUploadFracPct:
					params.rootUploadLimitBps > 0
						? (100 * rootUploadBps) / params.rootUploadLimitBps
						: 0,
				controlBytesSent,
				controlBytesSentRepair,
				controlBytesSentTracker,
				dataPayloadBytesSent,
			});
		}

		const expected = treeResults.reduce((sum, tree) => sum + tree.expected, 0);
		const delivered = treeResults.reduce(
			(sum, tree) => sum + tree.delivered,
			0,
		);
		const secondBatchExpected = treeResults.reduce(
			(sum, tree) => sum + tree.secondBatchExpected,
			0,
		);
		const deliveredWithinDeadline = treeResults.reduce(
			(sum, tree) =>
				sum + (tree.deliveredWithinDeadlinePct * tree.expected) / 100,
			0,
		);
		const secondBatchDeliveredWithinDeadline = treeResults.reduce(
			(sum, tree) =>
				sum +
				(tree.secondBatchDeliveredWithinDeadlinePct *
					tree.secondBatchExpected) /
					100,
			0,
		);
		const secondBatchLatencies = treeResults
			.map((tree) => tree.secondBatchLatencyP95)
			.filter((value) => Number.isFinite(value))
			.sort((a, b) => a - b);
		const deliveredPayloadBytes = delivered * Math.max(0, params.msgSize);
		const controlBytesSent = treeResults.reduce(
			(sum, tree) => sum + tree.controlBytesSent,
			0,
		);
		const controlBytesSentTracker = treeResults.reduce(
			(sum, tree) => sum + tree.controlBytesSentTracker,
			0,
		);
		const controlBytesSentRepair = treeResults.reduce(
			(sum, tree) => sum + tree.controlBytesSentRepair,
			0,
		);
		return {
			params,
			trees: treeResults,
			joinedCount: treeResults.reduce((sum, tree) => sum + tree.joinedCount, 0),
			subscriberSlots: params.writers * subscriberIndices.length,
			expected,
			delivered,
			deliveredPct: expected === 0 ? 100 : (100 * delivered) / expected,
			deliveredWithinDeadlinePct:
				expected === 0 ? 100 : (100 * deliveredWithinDeadline) / expected,
			secondBatchExpected,
			secondBatchDeliveredWithinDeadlinePct:
				secondBatchExpected === 0
					? 100
					: (100 * secondBatchDeliveredWithinDeadline) / secondBatchExpected,
			secondBatchLatencyP95:
				secondBatchLatencies.length > 0
					? quantile(secondBatchLatencies, 0.95)
					: NaN,
			reparentUpgradeTotal: treeResults.reduce(
				(sum, tree) => sum + tree.reparentUpgradeTotal,
				0,
			),
			parentProbeReqSentTotal: treeResults.reduce(
				(sum, tree) => sum + tree.parentProbeReqSentTotal,
				0,
			),
			parentShadowStartTotal: treeResults.reduce(
				(sum, tree) => sum + tree.parentShadowStartTotal,
				0,
			),
			parentShadowPromoteTotal: treeResults.reduce(
				(sum, tree) => sum + tree.parentShadowPromoteTotal,
				0,
			),
			publishActiveReparentUpgradeTotal: treeResults.reduce(
				(sum, tree) => sum + tree.publishActiveReparentUpgradeTotal,
				0,
			),
			publishActiveParentProbeReqSentTotal: treeResults.reduce(
				(sum, tree) => sum + tree.publishActiveParentProbeReqSentTotal,
				0,
			),
			publishActiveParentShadowStartTotal: treeResults.reduce(
				(sum, tree) => sum + tree.publishActiveParentShadowStartTotal,
				0,
			),
			publishActiveParentShadowPromoteTotal: treeResults.reduce(
				(sum, tree) => sum + tree.publishActiveParentShadowPromoteTotal,
				0,
			),
			publishActiveGuardSkipsTotal: treeResults.reduce(
				(sum, tree) =>
					sum +
					tree.publishActiveReparentUpgradeSkipDataTotal +
					tree.publishActiveReparentUpgradeSkipRepairTotal +
					tree.publishActiveReparentUpgradeSkipQuietTotal,
				0,
			),
			activeGuardedTrees: treeResults.filter(
				(tree) =>
					tree.publishActiveReparentUpgradeSkipDataTotal +
						tree.publishActiveReparentUpgradeSkipRepairTotal +
						tree.publishActiveReparentUpgradeSkipQuietTotal >
					0,
			).length,
			treeLevelAvg: avgFinite(treeResults.map((tree) => tree.treeLevelAvg)),
			treeLevelP95: avgFinite(treeResults.map((tree) => tree.treeLevelP95)),
			rootChildrenSum: treeResults.reduce(
				(sum, tree) => sum + tree.treeRootChildren,
				0,
			),
			rootChildrenMax: Math.max(
				0,
				...treeResults.map((tree) => tree.treeRootChildren),
			),
			rootUploadPctMax: Math.max(
				0,
				...treeResults.map((tree) => tree.rootUploadFracPct),
			),
			maxReparentUpgradePerPeer: Math.max(
				0,
				...treeResults.map((tree) => tree.maxReparentUpgradePerPeer),
			),
			controlBpp:
				deliveredPayloadBytes <= 0
					? 0
					: controlBytesSent / deliveredPayloadBytes,
			trackerBpp:
				deliveredPayloadBytes <= 0
					? 0
					: controlBytesSentTracker / deliveredPayloadBytes,
			repairBpp:
				deliveredPayloadBytes <= 0
					? 0
					: controlBytesSentRepair / deliveredPayloadBytes,
			network: session.network.metrics,
		};
	} finally {
		if (timer) clearTimeout(timer);
		try {
			await session.stop();
		} catch {
			// ignore teardown aborts in the shim
		}
	}
};

const formatResult = (result: MultiWriterResult) => {
	const p = result.params;
	return [
		"fanout-tree-parent-upgrade-multi-eval",
		`scenario=${p.scenario} seed=${p.seed} writers=${p.writers} activeWriters=${p.activeWriters} nodes=${p.nodes} subscribersPerTree=${p.subscribersPerTree}`,
		`joinedSlots=${result.joinedCount}/${result.subscriberSlots} delivered=${result.delivered}/${result.expected} (${fmt(result.deliveredPct)}%) deadline=${fmt(result.deliveredWithinDeadlinePct)}%`,
		...(result.secondBatchExpected > 0
			? [
					`secondBatch deadline=${fmt(result.secondBatchDeliveredWithinDeadlinePct)}% p95=${fmt(result.secondBatchLatencyP95, 1)}ms`,
				]
			: []),
		`tree avgLevel=${fmt(result.treeLevelAvg)} p95Level=${fmt(result.treeLevelP95, 1)} rootChildren sum=${result.rootChildrenSum} max=${result.rootChildrenMax} rootUploadPctMax=${fmt(result.rootUploadPctMax)}`,
		`parentUpgrade upgrades=${result.reparentUpgradeTotal} probes=${result.parentProbeReqSentTotal} shadowStart=${result.parentShadowStartTotal} shadowPromote=${result.parentShadowPromoteTotal} maxPerPeer=${result.maxReparentUpgradePerPeer}`,
		`publishActive upgrades=${result.publishActiveReparentUpgradeTotal} probes=${result.publishActiveParentProbeReqSentTotal} shadowStart=${result.publishActiveParentShadowStartTotal} shadowPromote=${result.publishActiveParentShadowPromoteTotal} guardSkips=${result.publishActiveGuardSkipsTotal} guardedTrees=${result.activeGuardedTrees}/${p.writers}`,
		`cost controlBpp=${fmt(result.controlBpp, 4)} trackerBpp=${fmt(result.trackerBpp, 4)} repairBpp=${fmt(result.repairBpp, 4)}`,
		`network dials=${result.network.dials} connsOpened=${result.network.connectionsOpened} streamsOpened=${result.network.streamsOpened} bytesSent=${result.network.bytesSent}`,
		...result.trees.map(
			(tree) =>
				`tree[${tree.tree}${tree.active ? "" : ":idle"}] joined=${tree.joinedCount}/${tree.subscriberCount} upgrades=${tree.reparentUpgradeTotal} probes=${tree.parentProbeReqSentTotal} active(probes/upgrades/guards)=${tree.publishActiveParentProbeReqSentTotal}/${tree.publishActiveReparentUpgradeTotal}/${tree.publishActiveReparentUpgradeSkipDataTotal + tree.publishActiveReparentUpgradeSkipRepairTotal + tree.publishActiveReparentUpgradeSkipQuietTotal} rootChildren=${tree.treeRootChildren} rootUploadPct=${fmt(tree.rootUploadFracPct)} secondP95=${fmt(tree.secondBatchLatencyP95, 1)}`,
		),
	].join("\n");
};

const analyzeUsefulPromotions = (
	baseline: MultiWriterResult,
	upgrade: MultiWriterResult,
) => {
	let usefulPromotedTrees = 0;
	const branchGains: number[] = [];
	for (const tree of upgrade.trees) {
		if (tree.reparentUpgradeTotal <= 0) continue;
		const baseTree = baseline.trees[tree.tree];
		if (!baseTree) continue;
		const treeLevelAvgGain = baseTree.treeLevelAvg - tree.treeLevelAvg;
		const secondBatchP95Gain =
			baseTree.secondBatchLatencyP95 - tree.secondBatchLatencyP95;
		const branchBase = peerLatencyP95For(
			baseTree,
			tree.upgradedBranchPeerHashes,
		);
		const branchUpgrade = peerLatencyP95For(
			tree,
			tree.upgradedBranchPeerHashes,
		);
		const branchGain = branchBase - branchUpgrade;
		if (Number.isFinite(branchGain)) branchGains.push(branchGain);
		if (
			Math.max(treeLevelAvgGain, secondBatchP95Gain, branchGain) >= 1 ||
			treeLevelAvgGain > 0.05
		) {
			usefulPromotedTrees += 1;
		}
	}
	return {
		usefulPromotedTrees,
		promotedBranchGainAvg: avgFinite(branchGains),
	};
};

const evaluateRun = (
	scenario: ScenarioName,
	baseline: MultiWriterResult,
	upgrade: MultiWriterResult,
	args: EvalArgs,
) => {
	const failures: Failure[] = [];
	const useful = analyzeUsefulPromotions(baseline, upgrade);
	const costRatio = isHotspotIdleScenario(scenario)
		? Math.max(args.maxCostRatio, 1.2)
		: args.maxCostRatio;
	const sentProactiveUpgradeTraffic =
		upgrade.reparentUpgradeTotal > 0 ||
		upgrade.parentProbeReqSentTotal > 0 ||
		upgrade.parentShadowStartTotal > 0;
	// Live churn is a no-work safety gate; ordinary reconnect timing can move
	// delivery/root shape even when parent-upgrade sends zero traffic.
	// The other live scenarios have the same no-work contract: if the policy
	// stays limited to local guard checks, async delivery jitter is reported but
	// not used as a product failure signal.
	const compareDeliveryAndCost =
		!isLiveChurnScenario(scenario) &&
		(!isLiveScenario(scenario) || sentProactiveUpgradeTraffic);

	if (compareDeliveryAndCost) {
		failIfLess(
			failures,
			"deliveredWithinDeadlinePct",
			baseline.deliveredWithinDeadlinePct,
			upgrade.deliveredWithinDeadlinePct,
			isLiveScenario(scenario)
				? baseline.deliveredWithinDeadlinePct -
						Math.max(0, args.maxLiveDeadlinePctDelta)
				: baseline.deliveredWithinDeadlinePct,
		);
		failIfGreater(
			failures,
			"controlBpp",
			baseline.controlBpp,
			upgrade.controlBpp,
			ratioLimit(baseline.controlBpp, costRatio, 0.001),
		);
		failIfGreater(
			failures,
			"trackerBpp",
			baseline.trackerBpp,
			upgrade.trackerBpp,
			ratioLimit(baseline.trackerBpp, costRatio, 0.001),
		);
		failIfGreater(
			failures,
			"repairBpp",
			baseline.repairBpp,
			upgrade.repairBpp,
			ratioLimit(baseline.repairBpp, costRatio, 0.001),
		);
	}

	if (isLiveScenario(scenario)) {
		failIfGreater(
			failures,
			"activeProbes",
			0,
			upgrade.publishActiveParentProbeReqSentTotal,
			0,
		);
		failIfGreater(
			failures,
			"activeShadowStarts",
			0,
			upgrade.publishActiveParentShadowStartTotal,
			0,
		);
		failIfGreater(
			failures,
			"activeReparentUpgrades",
			0,
			upgrade.publishActiveReparentUpgradeTotal,
			0,
		);
		failIfGreater(
			failures,
			"totalProbes",
			0,
			upgrade.parentProbeReqSentTotal,
			0,
		);
		failIfGreater(
			failures,
			"totalReparentUpgrades",
			0,
			upgrade.reparentUpgradeTotal,
			0,
		);
		failIfLess(
			failures,
			"activeGuardedTrees",
			0,
			upgrade.activeGuardedTrees,
			1,
		);
		if (isLiveChurnScenario(scenario)) {
			failIfGreater(
				failures,
				"activeGuardSkipsPerSlot",
				0,
				upgrade.publishActiveGuardSkipsTotal /
					Math.max(1, upgrade.subscriberSlots),
				Math.max(0, args.maxLiveChurnGuardSkipsPerSlot),
			);
		}
	}

	if (isPositiveIdleScenario(scenario)) {
		failIfLess(
			failures,
			"usefulPromotedTrees",
			0,
			useful.usefulPromotedTrees,
			1,
		);
		failIfLess(
			failures,
			"reparentUpgradeTotal",
			0,
			upgrade.reparentUpgradeTotal,
			1,
		);
		failIfGreater(
			failures,
			"probePerUpgrade",
			0,
			upgrade.reparentUpgradeTotal > 0
				? upgrade.parentProbeReqSentTotal / upgrade.reparentUpgradeTotal
				: Number.POSITIVE_INFINITY,
			args.maxProbePerUpgrade,
		);
		failIfGreater(
			failures,
			"maxReparentUpgradePerPeer",
			baseline.maxReparentUpgradePerPeer,
			upgrade.maxReparentUpgradePerPeer,
			1,
		);
		failIfLess(
			failures,
			"secondBatchDeadlinePct",
			baseline.secondBatchDeliveredWithinDeadlinePct,
			upgrade.secondBatchDeliveredWithinDeadlinePct,
			baseline.secondBatchDeliveredWithinDeadlinePct,
		);
		failIfLess(
			failures,
			"secondBatchOrBranchGain",
			0,
			Math.max(
				baseline.secondBatchLatencyP95 - upgrade.secondBatchLatencyP95,
				useful.promotedBranchGainAvg,
			),
			1,
		);
	}

	if (isHotspotIdleScenario(scenario)) {
		failIfLess(
			failures,
			"usefulPromotedTrees",
			0,
			useful.usefulPromotedTrees,
			1,
		);
		failIfLess(
			failures,
			"reparentUpgradeTotal",
			0,
			upgrade.reparentUpgradeTotal,
			1,
		);
		failIfGreater(
			failures,
			"maxReparentUpgradePerPeer",
			baseline.maxReparentUpgradePerPeer,
			upgrade.maxReparentUpgradePerPeer,
			1,
		);
		failIfLess(
			failures,
			"secondBatchOrBranchGain",
			0,
			Math.max(
				baseline.secondBatchLatencyP95 - upgrade.secondBatchLatencyP95,
				useful.promotedBranchGainAvg,
			),
			1,
		);
	}

	if (isSparseIdleScenario(scenario)) {
		const inactiveTrees = upgrade.trees.filter((tree) => !tree.active);
		const inactiveUpgrades = inactiveTrees.reduce(
			(sum, tree) => sum + tree.reparentUpgradeTotal,
			0,
		);
		const inactiveProbes = inactiveTrees.reduce(
			(sum, tree) => sum + tree.parentProbeReqSentTotal,
			0,
		);
		const inactiveShadowStarts = inactiveTrees.reduce(
			(sum, tree) => sum + tree.parentShadowStartTotal,
			0,
		);
		failIfGreater(failures, "inactiveTreeUpgrades", 0, inactiveUpgrades, 0);
		failIfGreater(failures, "inactiveTreeProbes", 0, inactiveProbes, 0);
		failIfGreater(
			failures,
			"inactiveTreeShadowStarts",
			0,
			inactiveShadowStarts,
			0,
		);
	}

	if (!isLiveChurnScenario(scenario)) {
		for (const tree of upgrade.trees) {
			const baselineTree = baseline.trees[tree.tree];
			if (!baselineTree) continue;
			failIfGreater(
				failures,
				`tree${tree.tree}RootChildrenDelta`,
				baselineTree.treeRootChildren,
				tree.treeRootChildren,
				baselineTree.treeRootChildren + Math.max(0, args.maxRootChildrenDelta),
			);
			failIfGreater(
				failures,
				`tree${tree.tree}RootUploadPctDelta`,
				baselineTree.rootUploadFracPct,
				tree.rootUploadFracPct,
				baselineTree.rootUploadFracPct +
					Math.max(0, args.maxRootUploadPctDelta),
			);
		}
	}

	return { failures, ...useful };
};

const printComparison = (
	scenario: ScenarioName,
	seed: number,
	baseline: MultiWriterResult,
	upgrade: MultiWriterResult,
	failures: Failure[],
	usefulPromotedTrees: number,
	promotedBranchGainAvg: number,
) => {
	const rootDeltas = upgrade.trees.map((tree) => {
		const base = baseline.trees[tree.tree]!;
		return tree.treeRootChildren - base.treeRootChildren;
	});
	const rootUploadDeltas = upgrade.trees.map((tree) => {
		const base = baseline.trees[tree.tree]!;
		return tree.rootUploadFracPct - base.rootUploadFracPct;
	});
	console.log(
		[
			`parent-upgrade-multi-eval scenario=${scenario} seed=${seed} viable=${failures.length === 0}`,
			`  joinedSlots ${baseline.joinedCount}/${baseline.subscriberSlots} -> ${upgrade.joinedCount}/${upgrade.subscriberSlots}`,
			`  deadline ${fmt(baseline.deliveredWithinDeadlinePct)} -> ${fmt(upgrade.deliveredWithinDeadlinePct)} secondP95 ${fmt(baseline.secondBatchLatencyP95, 1)} -> ${fmt(upgrade.secondBatchLatencyP95, 1)}`,
			`  upgrades=${upgrade.reparentUpgradeTotal} usefulPromotedTrees=${usefulPromotedTrees} probes=${upgrade.parentProbeReqSentTotal} shadowStart=${upgrade.parentShadowStartTotal} branchGainAvg=${fmt(promotedBranchGainAvg, 1)}`,
			`  active upgrades=${upgrade.publishActiveReparentUpgradeTotal} probes=${upgrade.publishActiveParentProbeReqSentTotal} shadowStart=${upgrade.publishActiveParentShadowStartTotal} guardSkips=${upgrade.publishActiveGuardSkipsTotal} guardedTrees=${upgrade.activeGuardedTrees}/${upgrade.params.writers}`,
			`  rootChildrenDelta sum=${rootDeltas.reduce((sum, value) => sum + value, 0)} max=${Math.max(...rootDeltas)} rootUploadPctDeltaMax=${fmt(Math.max(...rootUploadDeltas), 2)}`,
			`  cost controlBpp ${fmt(baseline.controlBpp, 4)} -> ${fmt(upgrade.controlBpp, 4)} trackerBpp ${fmt(baseline.trackerBpp, 4)} -> ${fmt(upgrade.trackerBpp, 4)} repairBpp ${fmt(baseline.repairBpp, 4)} -> ${fmt(upgrade.repairBpp, 4)}`,
			...(failures.length > 0
				? [
						`  failures ${failures
							.map(
								(f) =>
									`${f.metric}: baseline=${fmt(f.baseline)} upgrade=${fmt(f.upgrade)} limit=${fmt(f.limit)}`,
							)
							.join("; ")}`,
					]
				: []),
		].join("\n"),
	);
};

const printSummary = (samples: SummarySample[]) => {
	if (samples.length === 0) return;
	const groups = new Map<ScenarioName, SummarySample[]>();
	for (const sample of samples) {
		const group = groups.get(sample.scenario) ?? [];
		group.push(sample);
		groups.set(sample.scenario, group);
	}
	console.log(
		[
			"",
			"parent-upgrade-multi-summary",
			"scenario seeds viable usefulPromotedTrees upgrades probes activeUpgrades activeProbes activeGuardSkips branchGainAvg controlBppDeltaPctAvg rootChildrenDeltaMax rootChildrenDeltaSumMax rootUploadPctDeltaMax maxPerPeer failures",
			...[...groups.entries()].map(([scenario, group]) => {
				const controlDeltas = group.map((sample) =>
					sample.baseline.controlBpp > 0
						? (100 * (sample.upgrade.controlBpp - sample.baseline.controlBpp)) /
							sample.baseline.controlBpp
						: NaN,
				);
				const rootChildrenDeltaMax = Math.max(
					...group.flatMap((sample) =>
						sample.upgrade.trees.map(
							(tree) =>
								tree.treeRootChildren -
								sample.baseline.trees[tree.tree]!.treeRootChildren,
						),
					),
				);
				const rootChildrenDeltaSumMax = Math.max(
					...group.map((sample) =>
						sample.upgrade.trees.reduce(
							(sum, tree) =>
								sum +
								tree.treeRootChildren -
								sample.baseline.trees[tree.tree]!.treeRootChildren,
							0,
						),
					),
				);
				const rootUploadDeltaMax = Math.max(
					...group.flatMap((sample) =>
						sample.upgrade.trees.map(
							(tree) =>
								tree.rootUploadFracPct -
								sample.baseline.trees[tree.tree]!.rootUploadFracPct,
						),
					),
				);
				return [
					scenario,
					group.length,
					`${group.filter((sample) => sample.failures.length === 0).length}/${group.length}`,
					group.reduce((sum, sample) => sum + sample.usefulPromotedTrees, 0),
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
						(sum, sample) => sum + sample.upgrade.publishActiveGuardSkipsTotal,
						0,
					),
					fmt(
						avgFinite(group.map((sample) => sample.promotedBranchGainAvg)),
						1,
					),
					fmt(avgFinite(controlDeltas), 1),
					rootChildrenDeltaMax,
					rootChildrenDeltaSumMax,
					fmt(rootUploadDeltaMax, 2),
					Math.max(
						...group.map((sample) => sample.upgrade.maxReparentUpgradePerPeer),
					),
					group.reduce((sum, sample) => sum + sample.failures.length, 0),
				].join(" ");
			}),
		].join("\n"),
	);
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const samples: SummarySample[] = [];
	let failureCount = 0;
	for (const scenario of args.scenarios) {
		for (const seed of args.seeds) {
			console.log(`\n[multi-baseline] scenario=${scenario} seed=${seed}`);
			const baseline = await runMultiWriterSim(
				resolveParams(scenario, seed, args, false),
			);
			console.log(formatResult(baseline));

			console.log(`\n[multi-parent-upgrade] scenario=${scenario} seed=${seed}`);
			const upgrade = await runMultiWriterSim(
				resolveParams(scenario, seed, args, true),
			);
			console.log(formatResult(upgrade));

			const evaluated = evaluateRun(scenario, baseline, upgrade, args);
			printComparison(
				scenario,
				seed,
				baseline,
				upgrade,
				evaluated.failures,
				evaluated.usefulPromotedTrees,
				evaluated.promotedBranchGainAvg,
			);
			failureCount += evaluated.failures.length;
			samples.push({
				scenario,
				seed,
				baseline,
				upgrade,
				failures: evaluated.failures,
				usefulPromotedTrees: evaluated.usefulPromotedTrees,
				promotedBranchGainAvg: evaluated.promotedBranchGainAvg,
			});
		}
	}
	printSummary(samples);
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
