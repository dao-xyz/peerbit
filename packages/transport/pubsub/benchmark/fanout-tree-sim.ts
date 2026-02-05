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
} from "./fanout-tree-sim-lib.js";

const parseArgs = (argv: string[]) => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};
	const has = (key: string) => argv.includes(key);
	const maybeNumber = (key: string) => (has(key) ? Number(get(key)) : undefined);
	const maybeString = (key: string) => (has(key) ? String(get(key)) : undefined);

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
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
				"  --bootstrapEnsureIntervalMs MS  min interval between bootstrap re-dials (-1 = FanoutTree default)",
				"  --trackerQueryIntervalMs MS     min interval between tracker queries (-1 = FanoutTree default)",
				"  --joinAttemptsPerRound N        max join candidates tried per retry round (-1 = FanoutTree default)",
					"  --candidateCooldownMs MS        cooldown applied to bad join candidates (-1 = FanoutTree default)",
					"  --maxLatencySamples N         reservoir sample size (default: 1000000)",
					"  --profile 0|1                 collect CPU/mem/event-loop delay stats (default: 0)",
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
				"",
				"Example:",
				"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-sim --preset live --nodes 2000 --bootstraps 1 --seed 1",
			].join("\n"),
		);
		process.exit(0);
	}

	const preset = maybeString("--preset");
	const presetOpts: Record<string, any> =
		preset === "live"
				? {
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
					}
				: preset === "reliable"
					? {
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
						}
					: preset === "scale-5k"
						? {
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
								joinConcurrency: 512,
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
							}
						: preset === "scale-10k"
							? {
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
									joinConcurrency: 768,
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
								}
						: preset === "ci-small"
							? {
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
						}
					: preset === "ci-loss"
						? {
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
							}
						: {};

	const explicitOpts: Record<string, any> = {
		...(maybeNumber("--nodes") != null ? { nodes: maybeNumber("--nodes") } : {}),
		...(maybeNumber("--rootIndex") != null ? { rootIndex: maybeNumber("--rootIndex") } : {}),
		...(maybeNumber("--bootstraps") != null ? { bootstraps: maybeNumber("--bootstraps") } : {}),
		...(maybeNumber("--bootstrapMaxPeers") != null
			? { bootstrapMaxPeers: maybeNumber("--bootstrapMaxPeers") }
			: {}),
		...(maybeNumber("--subscribers") != null
			? { subscribers: maybeNumber("--subscribers") }
			: {}),
		...(maybeNumber("--relayFraction") != null
			? { relayFraction: maybeNumber("--relayFraction") }
			: {}),
		...(maybeNumber("--messages") != null ? { messages: maybeNumber("--messages") } : {}),
		...(maybeNumber("--msgRate") != null ? { msgRate: maybeNumber("--msgRate") } : {}),
		...(maybeNumber("--msgSize") != null ? { msgSize: maybeNumber("--msgSize") } : {}),
		...(maybeNumber("--intervalMs") != null ? { intervalMs: maybeNumber("--intervalMs") } : {}),
		...(maybeNumber("--settleMs") != null ? { settleMs: maybeNumber("--settleMs") } : {}),
		...(maybeNumber("--deadlineMs") != null
			? { deadlineMs: maybeNumber("--deadlineMs") }
			: {}),
		...(maybeNumber("--maxDataAgeMs") != null
			? { maxDataAgeMs: maybeNumber("--maxDataAgeMs") }
			: {}),
		...(maybeNumber("--timeoutMs") != null
			? { timeoutMs: maybeNumber("--timeoutMs") }
			: {}),
		...(maybeNumber("--seed") != null ? { seed: maybeNumber("--seed") } : {}),
		...(maybeString("--topic") != null ? { topic: maybeString("--topic") } : {}),
		...(maybeNumber("--rootUploadLimitBps") != null
			? { rootUploadLimitBps: maybeNumber("--rootUploadLimitBps") }
			: {}),
		...(maybeNumber("--rootMaxChildren") != null
			? { rootMaxChildren: maybeNumber("--rootMaxChildren") }
			: {}),
		...(maybeNumber("--relayUploadLimitBps") != null
			? { relayUploadLimitBps: maybeNumber("--relayUploadLimitBps") }
			: {}),
		...(maybeNumber("--relayMaxChildren") != null
			? { relayMaxChildren: maybeNumber("--relayMaxChildren") }
			: {}),
		...(has("--allowKick") ? { allowKick: String(get("--allowKick") ?? "0") === "1" } : {}),
		...(maybeNumber("--bidPerByte") != null
			? { bidPerByte: maybeNumber("--bidPerByte") }
			: {}),
		...(maybeNumber("--bidPerByteRelay") != null
			? { bidPerByteRelay: maybeNumber("--bidPerByteRelay") }
			: {}),
		...(maybeNumber("--bidPerByteLeaf") != null
			? { bidPerByteLeaf: maybeNumber("--bidPerByteLeaf") }
			: {}),
		...(has("--repair") ? { repair: String(get("--repair") ?? "1") === "1" } : {}),
		...(maybeNumber("--repairWindowMessages") != null
			? { repairWindowMessages: maybeNumber("--repairWindowMessages") }
			: {}),
		...(maybeNumber("--repairMaxBackfillMessages") != null
			? { repairMaxBackfillMessages: maybeNumber("--repairMaxBackfillMessages") }
			: {}),
		...(maybeNumber("--repairIntervalMs") != null
			? { repairIntervalMs: maybeNumber("--repairIntervalMs") }
			: {}),
		...(maybeNumber("--repairMaxPerReq") != null
			? { repairMaxPerReq: maybeNumber("--repairMaxPerReq") }
			: {}),
		...(has("--neighborRepair")
			? { neighborRepair: String(get("--neighborRepair") ?? "0") === "1" }
			: {}),
		...(maybeNumber("--neighborRepairPeers") != null
			? { neighborRepairPeers: maybeNumber("--neighborRepairPeers") }
			: {}),
		...(maybeNumber("--neighborMeshPeers") != null
			? { neighborMeshPeers: maybeNumber("--neighborMeshPeers") }
			: {}),
		...(maybeNumber("--neighborAnnounceIntervalMs") != null
			? { neighborAnnounceIntervalMs: maybeNumber("--neighborAnnounceIntervalMs") }
			: {}),
		...(maybeNumber("--neighborMeshRefreshIntervalMs") != null
			? { neighborMeshRefreshIntervalMs: maybeNumber("--neighborMeshRefreshIntervalMs") }
			: {}),
		...(maybeNumber("--neighborHaveTtlMs") != null
			? { neighborHaveTtlMs: maybeNumber("--neighborHaveTtlMs") }
			: {}),
		...(maybeNumber("--neighborRepairBudgetBps") != null
			? { neighborRepairBudgetBps: maybeNumber("--neighborRepairBudgetBps") }
			: {}),
		...(maybeNumber("--neighborRepairBurstMs") != null
			? { neighborRepairBurstMs: maybeNumber("--neighborRepairBurstMs") }
			: {}),
		...(maybeNumber("--dialDelayMs") != null
			? { dialDelayMs: maybeNumber("--dialDelayMs") }
			: {}),
		...(maybeNumber("--streamRxDelayMs") != null
			? { streamRxDelayMs: maybeNumber("--streamRxDelayMs") }
			: {}),
		...(maybeNumber("--streamHighWaterMarkBytes") != null
			? { streamHighWaterMarkBytes: maybeNumber("--streamHighWaterMarkBytes") }
			: {}),
		...(maybeNumber("--joinConcurrency") != null
			? { joinConcurrency: maybeNumber("--joinConcurrency") }
			: {}),
		...(has("--joinPhases")
			? { joinPhases: String(get("--joinPhases") ?? "0") === "1" }
			: {}),
		...(maybeNumber("--joinPhaseSettleMs") != null
			? { joinPhaseSettleMs: maybeNumber("--joinPhaseSettleMs") }
			: {}),
		...(maybeNumber("--joinReqTimeoutMs") != null
			? { joinReqTimeoutMs: maybeNumber("--joinReqTimeoutMs") }
			: {}),
			...(maybeNumber("--candidateShuffleTopK") != null
				? { candidateShuffleTopK: maybeNumber("--candidateShuffleTopK") }
				: {}),
			...(maybeNumber("--bootstrapEnsureIntervalMs") != null
				? { bootstrapEnsureIntervalMs: maybeNumber("--bootstrapEnsureIntervalMs") }
				: {}),
			...(maybeNumber("--trackerQueryIntervalMs") != null
				? { trackerQueryIntervalMs: maybeNumber("--trackerQueryIntervalMs") }
				: {}),
			...(maybeNumber("--joinAttemptsPerRound") != null
				? { joinAttemptsPerRound: maybeNumber("--joinAttemptsPerRound") }
				: {}),
			...(maybeNumber("--candidateCooldownMs") != null
				? { candidateCooldownMs: maybeNumber("--candidateCooldownMs") }
				: {}),
				...(maybeNumber("--maxLatencySamples") != null
					? { maxLatencySamples: maybeNumber("--maxLatencySamples") }
					: {}),
			...(has("--profile") ? { profile: String(get("--profile") ?? "0") === "1" } : {}),
			...(maybeNumber("--dropDataFrameRate") != null
				? { dropDataFrameRate: maybeNumber("--dropDataFrameRate") }
				: {}),
		...(maybeNumber("--churnEveryMs") != null
			? { churnEveryMs: maybeNumber("--churnEveryMs") }
			: {}),
		...(maybeNumber("--churnDownMs") != null
			? { churnDownMs: maybeNumber("--churnDownMs") }
			: {}),
		...(maybeNumber("--churnFraction") != null
			? { churnFraction: maybeNumber("--churnFraction") }
			: {}),
		...(maybeNumber("--assertMinJoinedPct") != null
			? { assertMinJoinedPct: maybeNumber("--assertMinJoinedPct") }
			: {}),
		...(maybeNumber("--assertMinDeliveryPct") != null
			? { assertMinDeliveryPct: maybeNumber("--assertMinDeliveryPct") }
			: {}),
		...(maybeNumber("--assertMinDeadlineDeliveryPct") != null
			? { assertMinDeadlineDeliveryPct: maybeNumber("--assertMinDeadlineDeliveryPct") }
			: {}),
		...(maybeNumber("--assertMaxUploadFracPct") != null
			? { assertMaxUploadFracPct: maybeNumber("--assertMaxUploadFracPct") }
			: {}),
		...(maybeNumber("--assertMaxOverheadFactor") != null
			? { assertMaxOverheadFactor: maybeNumber("--assertMaxOverheadFactor") }
			: {}),
		...(maybeNumber("--assertMaxControlBpp") != null
			? { assertMaxControlBpp: maybeNumber("--assertMaxControlBpp") }
			: {}),
		...(maybeNumber("--assertMaxTrackerBpp") != null
			? { assertMaxTrackerBpp: maybeNumber("--assertMaxTrackerBpp") }
			: {}),
		...(maybeNumber("--assertMaxRepairBpp") != null
			? { assertMaxRepairBpp: maybeNumber("--assertMaxRepairBpp") }
			: {}),
		...(maybeNumber("--assertAttachP95Ms") != null
			? { assertAttachP95Ms: maybeNumber("--assertAttachP95Ms") }
			: {}),
		...(maybeNumber("--assertMaxTreeLevelP95") != null
			? { assertMaxTreeLevelP95: maybeNumber("--assertMaxTreeLevelP95") }
			: {}),
		...(maybeNumber("--assertMaxFormationScore") != null
			? { assertMaxFormationScore: maybeNumber("--assertMaxFormationScore") }
			: {}),
	};

	const merged: Record<string, any> = { ...presetOpts, ...explicitOpts };

	// Live workloads are deadline-oriented: drop forwarding of stale DATA once it
	// exceeds the deadline (unless explicitly overridden).
	if (preset === "live" && !("maxDataAgeMs" in explicitOpts)) {
		const d = Number(merged.deadlineMs ?? 0);
		if (d > 0) merged.maxDataAgeMs = d;
	}

	return resolveFanoutTreeSimParams(merged);
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));

	const result = await runFanoutTreeSim(params);
	console.log(formatFanoutTreeSimResult(result));

	if (
		params.assertMinJoinedPct > 0 &&
		result.joinedPct + 1e-9 < params.assertMinJoinedPct
	) {
		console.error(
			`ASSERT FAILED: joinedPct ${result.joinedPct.toFixed(2)} < ${params.assertMinJoinedPct}`,
		);
		process.exit(2);
	}
	if (
		params.assertMinDeliveryPct > 0 &&
		result.deliveredPct + 1e-9 < params.assertMinDeliveryPct
	) {
		console.error(
			`ASSERT FAILED: deliveredPct ${result.deliveredPct.toFixed(2)} < ${params.assertMinDeliveryPct}`,
		);
		process.exit(2);
	}
	if (
		params.assertMaxUploadFracPct > 0 &&
		result.maxUploadFracPct - 1e-9 > params.assertMaxUploadFracPct
	) {
		console.error(
			`ASSERT FAILED: maxUploadFracPct ${result.maxUploadFracPct.toFixed(2)} > ${params.assertMaxUploadFracPct}`,
		);
		process.exit(2);
	}
	if (
		params.assertMinDeadlineDeliveryPct > 0 &&
		result.deliveredWithinDeadlinePct + 1e-9 < params.assertMinDeadlineDeliveryPct
	) {
		console.error(
			`ASSERT FAILED: deliveredWithinDeadlinePct ${result.deliveredWithinDeadlinePct.toFixed(2)} < ${params.assertMinDeadlineDeliveryPct}`,
		);
		process.exit(2);
	}
	if (
		params.assertMaxOverheadFactor > 0 &&
		result.overheadFactorData - 1e-9 > params.assertMaxOverheadFactor
	) {
		console.error(
			`ASSERT FAILED: overheadFactorData ${result.overheadFactorData.toFixed(3)} > ${params.assertMaxOverheadFactor}`,
		);
		process.exit(2);
	}
	if (params.assertMaxControlBpp > 0 && result.controlBpp - 1e-9 > params.assertMaxControlBpp) {
		console.error(
			`ASSERT FAILED: controlBpp ${result.controlBpp.toFixed(4)} > ${params.assertMaxControlBpp}`,
		);
		process.exit(2);
	}
	if (
		params.assertMaxTrackerBpp > 0 &&
		result.trackerBpp - 1e-9 > params.assertMaxTrackerBpp
	) {
		console.error(
			`ASSERT FAILED: trackerBpp ${result.trackerBpp.toFixed(4)} > ${params.assertMaxTrackerBpp}`,
		);
		process.exit(2);
	}
	if (params.assertMaxRepairBpp > 0 && result.repairBpp - 1e-9 > params.assertMaxRepairBpp) {
		console.error(
			`ASSERT FAILED: repairBpp ${result.repairBpp.toFixed(4)} > ${params.assertMaxRepairBpp}`,
		);
		process.exit(2);
	}
	if (params.assertAttachP95Ms > 0 && result.attachP95 - 1e-9 > params.assertAttachP95Ms) {
		console.error(
			`ASSERT FAILED: attachP95 ${result.attachP95.toFixed(1)}ms > ${params.assertAttachP95Ms}ms`,
		);
		process.exit(2);
	}
	if (
		params.assertMaxTreeLevelP95 > 0 &&
		result.treeLevelP95 - 1e-9 > params.assertMaxTreeLevelP95
	) {
		console.error(
			`ASSERT FAILED: treeLevelP95 ${result.treeLevelP95.toFixed(1)} > ${params.assertMaxTreeLevelP95}`,
		);
		process.exit(2);
	}
	if (
		params.assertMaxFormationScore > 0 &&
		result.formationScore - 1e-9 > params.assertMaxFormationScore
	) {
		console.error(
			`ASSERT FAILED: formationScore ${result.formationScore.toFixed(2)} > ${params.assertMaxFormationScore}`,
		);
		process.exit(2);
	}
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
