/**
 * End-to-end FanoutTree simulator (real protocol + real @peerbit/stream).
 *
 * This is meant to validate that:
 * - join works via bootstrap trackers (announce/query + dial + join),
 * - the data plane scales with bounded per-node fanout (tree),
 * - pull repair does not explode (when enabled),
 * without using real sockets/crypto.
 */

import {
	formatFanoutTreeSimResult,
	resolveFanoutTreeSimParams,
	runFanoutTreeSim,
	type FanoutTreeSimParams,
	type FanoutTreeSimResult,
} from "./fanout-tree-sim-lib.js";

const HELP_TEXT = [
	"fanout-tree-sim.ts",
	"",
	"Args:",
	"  --preset NAME                preset workload (live|reliable|scale-5k|scale-10k|ci-small|ci-loss)",
	"  --nodes N                    total nodes (default: 2000)",
	"  --rootIndex I                root/publisher node index (default: 0)",
	"  --bootstraps N               bootstrap tracker nodes (default: 1)",
	"  --bootstrapMaxPeers N        max bootstraps to dial/query per node (default: 0, 0=all)",
	"  --subscribers N              total subscribers (default: nodes-1-bootstraps)",
	"  --relayFraction F            fraction of subscribers acting as relays (default: 0.25)",
	"  --messages M                 messages to publish (default: 200)",
	"  --msgRate R                  publish rate (msg/s, default: 30)",
	"  --msgSize BYTES              payload bytes (default: 1024)",
	"  --intervalMs MS              override publish interval (default: 0 => derived from msgRate)",
	"  --settleMs MS                wait after publish (default: 2000)",
	"  --deadlineMs MS              count delivery-within-deadline (default: 0, off)",
	"  --maxDataAgeMs MS            drop forwarding of stale data (default: 0, off)",
	"  --timeoutMs MS               global timeout (default: 300000)",
	"  --seed S                     RNG seed (default: 1)",
	"  --topic NAME                 topic name (default: concert)",
	"  --rootUploadLimitBps BPS      (default: 20000000)",
	"  --rootMaxChildren N           (default: 64)",
	"  --relayUploadLimitBps BPS     (default: 10000000)",
	"  --relayMaxChildren N          (default: 32)",
	"  --allowKick 0|1               allow bid-based kicking when full (default: 0)",
	"  --bidPerByte N                bid offered by joiners (default: 0)",
	"  --bidPerByteRelay N           bid for relay joiners (default: bidPerByte)",
	"  --bidPerByteLeaf N            bid for leaf joiners (default: bidPerByte)",
	"  --repair 0|1                  enable pull repair (default: 1)",
	"  --repairWindowMessages N      (default: 1024)",
	"  --repairMaxBackfillMessages N max missing lag to repair (-1 = same as window)",
	"  --repairIntervalMs MS         (default: 200)",
	"  --repairMaxPerReq N           (default: 64)",
	"  --neighborRepair 0|1          enable neighbor-assisted repair (default: 0)",
	"  --neighborRepairPeers N       extra peers to query per repair tick (default: 2)",
	"  --neighborMeshPeers N         lazy repair mesh peers (-1 = FanoutTree default)",
	"  --neighborAnnounceIntervalMs MS     IHAVE announce interval (-1 = FanoutTree default)",
	"  --neighborMeshRefreshIntervalMs MS  mesh refresh interval (-1 = FanoutTree default)",
	"  --neighborHaveTtlMs MS              IHAVE TTL when selecting fetch targets (-1 = FanoutTree default)",
	"  --neighborRepairBudgetBps BPS       budget for neighbor FETCH_REQ control traffic (-1 = FanoutTree default)",
	"  --neighborRepairBurstMs MS          burst window for neighbor FETCH_REQ budget (-1 = FanoutTree default)",
	"  --dialDelayMs MS              artificial dial delay (default: 0)",
	"  --streamRxDelayMs MS          per-chunk inbound delay in shim (default: 0)",
	"  --streamHighWaterMarkBytes B  backpressure threshold (default: 262144)",
	"  --joinConcurrency N           parallel join tasks (default: 256)",
	"  --joinPhases 0|1              join relays first, then leaves (default: 0)",
	"  --joinPhaseSettleMs MS        wait between join phases (default: 2000)",
	"  --joinReqTimeoutMs MS         join request timeout per candidate (default: 2000)",
	"  --candidateShuffleTopK N      shuffle only within top K candidates (default: 8)",
	"  --candidateScoringMode MODE   join scoring (ranked-shuffle|ranked-strict|weighted, default: ranked-shuffle)",
	"  --bootstrapEnsureIntervalMs MS  min interval between bootstrap re-dials (-1 = FanoutTree default)",
	"  --trackerQueryIntervalMs MS     min interval between tracker queries (-1 = FanoutTree default)",
	"  --joinAttemptsPerRound N        max join candidates tried per retry round (-1 = FanoutTree default)",
	"  --candidateCooldownMs MS        cooldown applied to bad join candidates (-1 = FanoutTree default)",
	"  --maxLatencySamples N         reservoir sample size (default: 1000000)",
	"  --profile 0|1                 collect CPU/mem/event-loop delay stats (default: 0)",
	"  --progress 0|1                log join progress + memory stats (default: 0)",
	"  --progressEveryMs MS          progress log interval (default: 5000)",
	"  --dropDataFrameRate P         drop rate for stream data frames (default: 0)",
	"  --churnEveryMs MS             churn interval (default: 0, off)",
	"  --churnDownMs MS              offline duration per churn (default: 0, off)",
	"  --churnFraction F             fraction to churn per event (default: 0, off)",
	"  --assertMinJoinedPct PCT      (default: 0)",
	"  --assertMinDeliveryPct PCT    (default: 0)",
	"  --assertMinDeadlineDeliveryPct PCT  (default: 0)",
	"  --assertMaxUploadFracPct PCT  max peak upload vs cap (default: 0, off)",
	"  --assertMaxOverheadFactor X   max data overhead factor vs ideal tree (default: 0, off)",
	"  --assertMaxControlBpp X       max control bytes per payload byte delivered (default: 0, off)",
	"  --assertMaxTrackerBpp X       max tracker bytes per payload byte delivered (default: 0, off)",
	"  --assertMaxRepairBpp X        max repair bytes per payload byte delivered (default: 0, off)",
	"  --assertAttachP95Ms MS        max p95 time-to-attach since join start (default: 0, off)",
	"  --assertMaxTreeLevelP95 N     max p95 tree depth/level (default: 0, off)",
	"  --assertMaxFormationScore X   max formationScore (default: 0, off)",
	"  --assertMaxOrphans N          max online orphans during publish+settle (default: 0, off)",
	"  --assertMaxOrphanArea S       max orphan-area (orphan-seconds) during publish+settle (default: 0, off)",
	"  --assertRecoveryP95Ms MS      max p95 recovery time after churn (default: 0, off)",
	"  --assertMaxReparentsPerMin N  max reparent events per minute (default: 0, off)",
	"",
	"Example:",
	"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-sim --preset live --nodes 2000 --bootstraps 1 --seed 1",
].join("\n");

const PRESET_OPTIONS: Record<string, Partial<FanoutTreeSimParams>> = {
	live: {
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		msgRate: 30,
		msgSize: 1024,
		messages: 30 * 60,
		settleMs: 2_000,
		deadlineMs: 2_000,
		allowKick: true,
		bidPerByteRelay: 1,
		bidPerByteLeaf: 0,
		joinPhases: true,
		joinPhaseSettleMs: 2_000,
		repair: true,
		repairMaxBackfillMessages: 60,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.01,
		churnEveryMs: 2_000,
		churnDownMs: 1_000,
		churnFraction: 0.005,
	},
	reliable: {
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		msgRate: 30,
		msgSize: 1024,
		messages: 30 * 60,
		settleMs: 10_000,
		deadlineMs: 10_000,
		allowKick: true,
		bidPerByteRelay: 1,
		bidPerByteLeaf: 0,
		joinPhases: true,
		joinPhaseSettleMs: 2_000,
		repair: true,
		repairMaxBackfillMessages: 1024,
		neighborRepair: true,
		neighborRepairPeers: 4,
		dropDataFrameRate: 0.01,
		churnEveryMs: 2_000,
		churnDownMs: 1_000,
		churnFraction: 0.005,
	},
	"scale-5k": {
		nodes: 5000,
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		subscribers: 4800,
		relayFraction: 0.25,
		msgRate: 30,
		msgSize: 1024,
		messages: 10,
		settleMs: 5_000,
		deadlineMs: 10_000,
		timeoutMs: 600_000,
		seed: 1,
		allowKick: true,
		bidPerByteRelay: 1,
		bidPerByteLeaf: 0,
		// Avoid join storms (and excess temporary connections) in single-process sims.
		joinConcurrency: 128,
		joinPhases: true,
		joinPhaseSettleMs: 2_000,
		joinReqTimeoutMs: 1_000,
		joinAttemptsPerRound: 2,
		trackerQueryIntervalMs: 10_000,
		bootstrapEnsureIntervalMs: 10_000,
		candidateCooldownMs: 5_000,
		repair: true,
		repairMaxBackfillMessages: 60,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.01,
		// Keep in-memory sims bounded (avoid buffering OOM at 5k+ nodes).
		streamHighWaterMarkBytes: 8 * 1024,
		// Bench assertions (tune as we learn; these should be achievable on a dev machine).
		assertMinJoinedPct: 99.5,
		assertMinDeliveryPct: 99.9,
		assertMinDeadlineDeliveryPct: 97.0,
		assertMaxOverheadFactor: 1.3,
		assertMaxUploadFracPct: 110,
		// Control-plane budgets (bytes per delivered payload byte). Tune as we learn.
		assertMaxControlBpp: 0.5,
		assertMaxTrackerBpp: 0.3,
		assertMaxRepairBpp: 0.3,
	},
	"scale-10k": {
		nodes: 10_000,
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		subscribers: 9_600,
		relayFraction: 0.25,
		msgRate: 30,
		msgSize: 1024,
		messages: 10,
		settleMs: 5_000,
		deadlineMs: 10_000,
		timeoutMs: 1_200_000,
		seed: 1,
		allowKick: true,
		bidPerByteRelay: 1,
		bidPerByteLeaf: 0,
		// Avoid join storms (and excess temporary connections) in single-process sims.
		joinConcurrency: 192,
		joinPhases: true,
		joinPhaseSettleMs: 2_000,
		joinReqTimeoutMs: 1_000,
		joinAttemptsPerRound: 2,
		trackerQueryIntervalMs: 10_000,
		bootstrapEnsureIntervalMs: 10_000,
		candidateCooldownMs: 5_000,
		repair: true,
		repairMaxBackfillMessages: 60,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.01,
		// Keep in-memory sims bounded (avoid buffering OOM at 10k nodes).
		streamHighWaterMarkBytes: 8 * 1024,
	},
	"ci-small": {
		nodes: 25,
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		subscribers: 20,
		relayFraction: 0.3,
		messages: 20,
		msgRate: 50,
		msgSize: 64,
		settleMs: 500,
		timeoutMs: 20_000,
		seed: 1,
		repair: true,
	},
	"ci-loss": {
		nodes: 40,
		bootstraps: 3,
		bootstrapMaxPeers: 1,
		subscribers: 30,
		relayFraction: 0.35,
		messages: 40,
		msgRate: 50,
		msgSize: 64,
		settleMs: 2_500,
		timeoutMs: 40_000,
		seed: 1,
		repair: true,
		neighborRepair: true,
		neighborRepairPeers: 3,
		dropDataFrameRate: 0.1,
		churnEveryMs: 200,
		churnDownMs: 100,
		churnFraction: 0.05,
	},
};

type ArgSpec = {
	flag: string;
	key: keyof FanoutTreeSimParams;
	parse: (value: string | undefined) => unknown;
};

const parseNumber = (value: string | undefined) => Number(value);
const parseString = (value: string | undefined) => String(value);
const parseBool01 = (fallback: "0" | "1") => (value: string | undefined) =>
	String(value ?? fallback) === "1";

const ARG_SPECS: ArgSpec[] = [
	{ flag: "--nodes", key: "nodes", parse: parseNumber },
	{ flag: "--rootIndex", key: "rootIndex", parse: parseNumber },
	{ flag: "--bootstraps", key: "bootstraps", parse: parseNumber },
	{ flag: "--bootstrapMaxPeers", key: "bootstrapMaxPeers", parse: parseNumber },
	{ flag: "--subscribers", key: "subscribers", parse: parseNumber },
	{ flag: "--relayFraction", key: "relayFraction", parse: parseNumber },
	{ flag: "--messages", key: "messages", parse: parseNumber },
	{ flag: "--msgRate", key: "msgRate", parse: parseNumber },
	{ flag: "--msgSize", key: "msgSize", parse: parseNumber },
	{ flag: "--intervalMs", key: "intervalMs", parse: parseNumber },
	{ flag: "--settleMs", key: "settleMs", parse: parseNumber },
	{ flag: "--deadlineMs", key: "deadlineMs", parse: parseNumber },
	{ flag: "--maxDataAgeMs", key: "maxDataAgeMs", parse: parseNumber },
	{ flag: "--timeoutMs", key: "timeoutMs", parse: parseNumber },
	{ flag: "--seed", key: "seed", parse: parseNumber },
	{ flag: "--topic", key: "topic", parse: parseString },
	{ flag: "--rootUploadLimitBps", key: "rootUploadLimitBps", parse: parseNumber },
	{ flag: "--rootMaxChildren", key: "rootMaxChildren", parse: parseNumber },
	{ flag: "--relayUploadLimitBps", key: "relayUploadLimitBps", parse: parseNumber },
	{ flag: "--relayMaxChildren", key: "relayMaxChildren", parse: parseNumber },
	{ flag: "--allowKick", key: "allowKick", parse: parseBool01("0") },
	{ flag: "--bidPerByte", key: "bidPerByte", parse: parseNumber },
	{ flag: "--bidPerByteRelay", key: "bidPerByteRelay", parse: parseNumber },
	{ flag: "--bidPerByteLeaf", key: "bidPerByteLeaf", parse: parseNumber },
	{ flag: "--repair", key: "repair", parse: parseBool01("1") },
	{ flag: "--repairWindowMessages", key: "repairWindowMessages", parse: parseNumber },
	{ flag: "--repairMaxBackfillMessages", key: "repairMaxBackfillMessages", parse: parseNumber },
	{ flag: "--repairIntervalMs", key: "repairIntervalMs", parse: parseNumber },
	{ flag: "--repairMaxPerReq", key: "repairMaxPerReq", parse: parseNumber },
	{ flag: "--neighborRepair", key: "neighborRepair", parse: parseBool01("0") },
	{ flag: "--neighborRepairPeers", key: "neighborRepairPeers", parse: parseNumber },
	{ flag: "--neighborMeshPeers", key: "neighborMeshPeers", parse: parseNumber },
	{ flag: "--neighborAnnounceIntervalMs", key: "neighborAnnounceIntervalMs", parse: parseNumber },
	{
		flag: "--neighborMeshRefreshIntervalMs",
		key: "neighborMeshRefreshIntervalMs",
		parse: parseNumber,
	},
	{ flag: "--neighborHaveTtlMs", key: "neighborHaveTtlMs", parse: parseNumber },
	{ flag: "--neighborRepairBudgetBps", key: "neighborRepairBudgetBps", parse: parseNumber },
	{ flag: "--neighborRepairBurstMs", key: "neighborRepairBurstMs", parse: parseNumber },
	{ flag: "--dialDelayMs", key: "dialDelayMs", parse: parseNumber },
	{ flag: "--streamRxDelayMs", key: "streamRxDelayMs", parse: parseNumber },
	{ flag: "--streamHighWaterMarkBytes", key: "streamHighWaterMarkBytes", parse: parseNumber },
	{ flag: "--joinConcurrency", key: "joinConcurrency", parse: parseNumber },
	{ flag: "--joinPhases", key: "joinPhases", parse: parseBool01("0") },
	{ flag: "--joinPhaseSettleMs", key: "joinPhaseSettleMs", parse: parseNumber },
	{ flag: "--joinReqTimeoutMs", key: "joinReqTimeoutMs", parse: parseNumber },
	{ flag: "--candidateShuffleTopK", key: "candidateShuffleTopK", parse: parseNumber },
	{ flag: "--candidateScoringMode", key: "candidateScoringMode", parse: parseString },
	{
		flag: "--bootstrapEnsureIntervalMs",
		key: "bootstrapEnsureIntervalMs",
		parse: parseNumber,
	},
	{ flag: "--trackerQueryIntervalMs", key: "trackerQueryIntervalMs", parse: parseNumber },
	{ flag: "--joinAttemptsPerRound", key: "joinAttemptsPerRound", parse: parseNumber },
	{ flag: "--candidateCooldownMs", key: "candidateCooldownMs", parse: parseNumber },
	{ flag: "--maxLatencySamples", key: "maxLatencySamples", parse: parseNumber },
	{ flag: "--profile", key: "profile", parse: parseBool01("0") },
	{ flag: "--progress", key: "progress", parse: parseBool01("0") },
	{ flag: "--progressEveryMs", key: "progressEveryMs", parse: parseNumber },
	{ flag: "--dropDataFrameRate", key: "dropDataFrameRate", parse: parseNumber },
	{ flag: "--churnEveryMs", key: "churnEveryMs", parse: parseNumber },
	{ flag: "--churnDownMs", key: "churnDownMs", parse: parseNumber },
	{ flag: "--churnFraction", key: "churnFraction", parse: parseNumber },
	{ flag: "--assertMinJoinedPct", key: "assertMinJoinedPct", parse: parseNumber },
	{ flag: "--assertMinDeliveryPct", key: "assertMinDeliveryPct", parse: parseNumber },
	{
		flag: "--assertMinDeadlineDeliveryPct",
		key: "assertMinDeadlineDeliveryPct",
		parse: parseNumber,
	},
	{ flag: "--assertMaxUploadFracPct", key: "assertMaxUploadFracPct", parse: parseNumber },
	{
		flag: "--assertMaxOverheadFactor",
		key: "assertMaxOverheadFactor",
		parse: parseNumber,
	},
	{ flag: "--assertMaxControlBpp", key: "assertMaxControlBpp", parse: parseNumber },
	{ flag: "--assertMaxTrackerBpp", key: "assertMaxTrackerBpp", parse: parseNumber },
	{ flag: "--assertMaxRepairBpp", key: "assertMaxRepairBpp", parse: parseNumber },
	{ flag: "--assertAttachP95Ms", key: "assertAttachP95Ms", parse: parseNumber },
	{ flag: "--assertMaxTreeLevelP95", key: "assertMaxTreeLevelP95", parse: parseNumber },
	{
		flag: "--assertMaxFormationScore",
		key: "assertMaxFormationScore",
		parse: parseNumber,
	},
	{ flag: "--assertMaxOrphans", key: "assertMaxOrphans", parse: parseNumber },
	{ flag: "--assertMaxOrphanArea", key: "assertMaxOrphanArea", parse: parseNumber },
	{ flag: "--assertRecoveryP95Ms", key: "assertRecoveryP95Ms", parse: parseNumber },
	{
		flag: "--assertMaxReparentsPerMin",
		key: "assertMaxReparentsPerMin",
		parse: parseNumber,
	},
];

const parseArgs = (argv: string[]) => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};
	const has = (key: string) => argv.includes(key);

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(HELP_TEXT);
		process.exit(0);
	}

	const preset = has("--preset") ? String(get("--preset")) : undefined;
	const presetOpts = (preset ? PRESET_OPTIONS[preset] : undefined) ?? {};

	const explicitOpts: Partial<FanoutTreeSimParams> = {};
	for (const spec of ARG_SPECS) {
		if (!has(spec.flag)) continue;
		(explicitOpts as Record<string, unknown>)[spec.key] = spec.parse(get(spec.flag));
	}

	const merged: Partial<FanoutTreeSimParams> = { ...presetOpts, ...explicitOpts };

	// Live workloads are deadline-oriented: drop forwarding of stale DATA once it
	// exceeds the deadline (unless explicitly overridden).
	if (preset === "live" && !("maxDataAgeMs" in explicitOpts)) {
		const d = Number(merged.deadlineMs ?? 0);
		if (d > 0) merged.maxDataAgeMs = d;
	}

	return resolveFanoutTreeSimParams(merged);
};

type AssertionParam =
	| "assertMinJoinedPct"
	| "assertMinDeliveryPct"
	| "assertMinDeadlineDeliveryPct"
	| "assertMaxUploadFracPct"
	| "assertMaxOverheadFactor"
	| "assertMaxControlBpp"
	| "assertMaxTrackerBpp"
	| "assertMaxRepairBpp"
	| "assertAttachP95Ms"
	| "assertMaxTreeLevelP95"
	| "assertMaxFormationScore"
	| "assertMaxOrphans"
	| "assertMaxOrphanArea"
	| "assertRecoveryP95Ms"
	| "assertMaxReparentsPerMin";

type AssertionSpec = {
	param: AssertionParam;
	label: string;
	mode: "min" | "max";
	value: (result: FanoutTreeSimResult) => number;
	formatActual?: (value: number) => string;
	formatExpected?: (value: number) => string;
};

const ASSERTION_SPECS: AssertionSpec[] = [
	{
		param: "assertMinJoinedPct",
		label: "joinedPct",
		mode: "min",
		value: (result) => result.joinedPct,
		formatActual: (value) => value.toFixed(2),
	},
	{
		param: "assertMinDeliveryPct",
		label: "deliveredPct",
		mode: "min",
		value: (result) => result.deliveredPct,
		formatActual: (value) => value.toFixed(2),
	},
	{
		param: "assertMaxUploadFracPct",
		label: "maxUploadFracPct",
		mode: "max",
		value: (result) => result.maxUploadFracPct,
		formatActual: (value) => value.toFixed(2),
	},
	{
		param: "assertMinDeadlineDeliveryPct",
		label: "deliveredWithinDeadlinePct",
		mode: "min",
		value: (result) => result.deliveredWithinDeadlinePct,
		formatActual: (value) => value.toFixed(2),
	},
	{
		param: "assertMaxOverheadFactor",
		label: "overheadFactorData",
		mode: "max",
		value: (result) => result.overheadFactorData,
		formatActual: (value) => value.toFixed(3),
	},
	{
		param: "assertMaxControlBpp",
		label: "controlBpp",
		mode: "max",
		value: (result) => result.controlBpp,
		formatActual: (value) => value.toFixed(4),
	},
	{
		param: "assertMaxTrackerBpp",
		label: "trackerBpp",
		mode: "max",
		value: (result) => result.trackerBpp,
		formatActual: (value) => value.toFixed(4),
	},
	{
		param: "assertMaxRepairBpp",
		label: "repairBpp",
		mode: "max",
		value: (result) => result.repairBpp,
		formatActual: (value) => value.toFixed(4),
	},
	{
		param: "assertAttachP95Ms",
		label: "attachP95",
		mode: "max",
		value: (result) => result.attachP95,
		formatActual: (value) => `${value.toFixed(1)}ms`,
		formatExpected: (value) => `${value}ms`,
	},
	{
		param: "assertMaxTreeLevelP95",
		label: "treeLevelP95",
		mode: "max",
		value: (result) => result.treeLevelP95,
		formatActual: (value) => value.toFixed(1),
	},
	{
		param: "assertMaxFormationScore",
		label: "formationScore",
		mode: "max",
		value: (result) => result.formationScore,
		formatActual: (value) => value.toFixed(2),
	},
	{
		param: "assertMaxOrphans",
		label: "maintMaxOrphans",
		mode: "max",
		value: (result) => result.maintMaxOrphans,
	},
	{
		param: "assertMaxOrphanArea",
		label: "maintOrphanArea",
		mode: "max",
		value: (result) => result.maintOrphanArea,
		formatActual: (value) => value.toFixed(1),
	},
	{
		param: "assertRecoveryP95Ms",
		label: "maintRecoveryP95Ms",
		mode: "max",
		value: (result) => result.maintRecoveryP95Ms,
		formatActual: (value) => `${value.toFixed(1)}ms`,
		formatExpected: (value) => `${value}ms`,
	},
	{
		param: "assertMaxReparentsPerMin",
		label: "maintReparentsPerMin",
		mode: "max",
		value: (result) => result.maintReparentsPerMin,
		formatActual: (value) => value.toFixed(2),
	},
];

const runAssertions = (params: FanoutTreeSimParams, result: FanoutTreeSimResult) => {
	for (const spec of ASSERTION_SPECS) {
		const threshold = params[spec.param];
		if (threshold <= 0) continue;
		const actual = spec.value(result);
		const failed =
			spec.mode === "min" ? actual + 1e-9 < threshold : actual - 1e-9 > threshold;
		if (!failed) continue;
		const actualText = spec.formatActual ? spec.formatActual(actual) : `${actual}`;
		const expectedText = spec.formatExpected ? spec.formatExpected(threshold) : `${threshold}`;
		const operator = spec.mode === "min" ? "<" : ">";
		console.error(`ASSERT FAILED: ${spec.label} ${actualText} ${operator} ${expectedText}`);
		process.exit(2);
	}
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));

	const result = await runFanoutTreeSim(params);
	console.log(formatFanoutTreeSimResult(result));
	runAssertions(params, result);
};

try {
	await main();
} catch (err: any) {
	console.error(err?.message ?? String(err));
	if (String(err?.message ?? "").includes("timed out")) {
		process.exit(124);
	}
	process.exit(1);
}
