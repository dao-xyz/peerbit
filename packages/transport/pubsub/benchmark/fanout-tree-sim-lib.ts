import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { delay } from "@peerbit/time";
import { anySignal } from "any-signal";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { FanoutTree } from "../src/index.js";
import { InMemoryNetwork, InMemorySession } from "./sim/inmemory-libp2p.js";

class SimFanoutTree extends FanoutTree {
	constructor(c: any, opts?: any) {
		super(c, opts);
		// Fast/mock signing: keep signer identity semantics but skip crypto work.
		this.sign = async () =>
			new SignatureWithKey({
				signature: new Uint8Array([0]),
				publicKey: this.publicKey,
				prehash: PreHash.NONE,
			});
	}

	public async verifyAndProcess(message: any) {
		// Skip expensive crypto verify for large sims, but keep session handling behavior
		// consistent with the real implementation.
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

export type FanoutTreeSimParams = {
	nodes: number;
	rootIndex: number;
	bootstraps: number;
	bootstrapMaxPeers: number;
	subscribers: number;
	relayFraction: number;

	messages: number;
	msgRate: number;
	msgSize: number;
	intervalMs: number;
	settleMs: number;
	deadlineMs: number;
	maxDataAgeMs: number;

	timeoutMs: number;
	seed: number;
	topic: string;

	rootUploadLimitBps: number;
	rootMaxChildren: number;
	relayUploadLimitBps: number;
	relayMaxChildren: number;
	allowKick: boolean;
	bidPerByte: number;
	bidPerByteRelay: number;
	bidPerByteLeaf: number;

	repair: boolean;
	repairWindowMessages: number;
	repairMaxBackfillMessages: number;
	repairIntervalMs: number;
	repairMaxPerReq: number;
	neighborRepair: boolean;
	neighborRepairPeers: number;
	neighborMeshPeers: number;
	neighborAnnounceIntervalMs: number;
	neighborMeshRefreshIntervalMs: number;
	neighborHaveTtlMs: number;
	neighborRepairBudgetBps: number;
	neighborRepairBurstMs: number;

	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;
	dialDelayMs: number;
	joinConcurrency: number;
	joinReqTimeoutMs: number;
	candidateShuffleTopK: number;
	candidateScoringMode: "ranked-shuffle" | "ranked-strict" | "weighted";
	bootstrapEnsureIntervalMs: number;
	trackerQueryIntervalMs: number;
	joinAttemptsPerRound: number;
	candidateCooldownMs: number;
	joinPhases: boolean;
	joinPhaseSettleMs: number;

	maxLatencySamples: number;
	profile: boolean;
	progress: boolean;
	progressEveryMs: number;

	dropDataFrameRate: number;

	churnEveryMs: number;
	churnDownMs: number;
	churnFraction: number;

	assertMinJoinedPct: number;
	assertMinDeliveryPct: number;
	assertMinDeadlineDeliveryPct: number;
	assertMaxUploadFracPct: number;
	assertMaxOverheadFactor: number;
	assertMaxControlBpp: number;
	assertMaxTrackerBpp: number;
	assertMaxRepairBpp: number;
	assertAttachP95Ms: number;
	assertMaxTreeLevelP95: number;
	assertMaxFormationScore: number;
	assertMaxOrphans: number;
	assertRecoveryP95Ms: number;
	assertMaxReparentsPerMin: number;
	assertMaxOrphanArea: number;
};

export type FanoutTreeSimResult = {
	params: FanoutTreeSimParams;

	bootstrapCount: number;
	subscriberCount: number;
	relayCount: number;

	joinedCount: number;
	joinedPct: number;
	joinMs: number;
	attachSamples: number;
	attachP50: number;
	attachP95: number;
	attachP99: number;
	attachMax: number;

	formationTreeMaxLevel: number;
	formationTreeLevelP95: number;
	formationTreeLevelAvg: number;
	formationTreeOrphans: number;
	formationTreeChildrenP95: number;
	formationTreeChildrenMax: number;
	formationTreeRootChildren: number;
	formationUnderlayEdges: number;
	formationUnderlayDistP95: number;
	formationUnderlayDistMax: number;
	formationStretchP95: number;
	formationStretchMax: number;
	formationScore: number;

	publishMs: number;
	expected: number;
	delivered: number;
	deliveredPct: number;
	deliveredWithinDeadline: number;
	deliveredWithinDeadlinePct: number;
	duplicates: number;

	latencySamples: number;
	latencyP50: number;
	latencyP95: number;
	latencyP99: number;
	latencyMax: number;

	droppedForwardsTotal: number;
	droppedForwardsMax: number;
	droppedForwardsMaxNode?: string;
	staleForwardsDroppedTotal: number;
	staleForwardsDroppedMax: number;
	staleForwardsDroppedMaxNode?: string;
	dataWriteDropsTotal: number;
	dataWriteDropsMax: number;
	dataWriteDropsMaxNode?: string;
	reparentDisconnectTotal: number;
	reparentStaleTotal: number;
	reparentKickedTotal: number;

	treeMaxLevel: number;
	treeLevelP95: number;
	treeLevelAvg: number;
	treeOrphans: number;
	treeChildrenP95: number;
	treeChildrenMax: number;
	treeRootChildren: number;

	maxUploadBps: number;
	maxUploadFracPct: number;
	maxUploadNode?: string;

	streamQueuedBytesTotal: number;
	streamQueuedBytesMax: number;
	streamQueuedBytesP95: number;
	streamQueuedBytesMaxNode?: string;
	streamQueuedBytesByLane: number[]; // lane 0..3 (0 = highest priority)

	churnEvents: number;
	churnedPeersTotal: number;

	maintDurationMs: number;
	maintSamples: number;
	maintMaxOrphans: number;
	maintOrphanArea: number; // integral of (orphansOnline) over time, in orphan-seconds
	maintRecoveryCount: number;
	maintRecoveryP50Ms: number;
	maintRecoveryP95Ms: number;
	maintReparentsPerMin: number;
	maintMaxReparentsPerPeer: number;
	maintLevelP95DriftMax: number;
	maintChildrenP95DriftMax: number;

	overheadFactorData: number;
	controlBpp: number;
	trackerBpp: number;
	repairBpp: number;
	earningsTotal: number;
	earningsRelayCount: number;
	earningsRelayP50: number;
	earningsRelayP95: number;
	earningsRelayMax: number;

	protocolControlSends: number;
	protocolControlBytesSent: number;
	protocolControlBytesSentJoin: number;
	protocolControlBytesSentRepair: number;
	protocolControlBytesSentTracker: number;
	protocolControlReceives: number;
	protocolControlBytesReceived: number;
	protocolDataSends: number;
	protocolDataPayloadBytesSent: number;
	protocolDataReceives: number;
	protocolDataPayloadBytesReceived: number;
	protocolRepairReqSent: number;
	protocolFetchReqSent: number;
	protocolIHaveSent: number;
	protocolTrackerFeedbackSent: number;
	protocolCacheHitsServed: number;
	protocolHoleFillsFromNeighbor: number;
	protocolRouteCacheHits: number;
	protocolRouteCacheMisses: number;
	protocolRouteCacheExpirations: number;
	protocolRouteCacheEvictions: number;
	protocolRouteProxyQueries: number;
	protocolRouteProxyTimeouts: number;
	protocolRouteProxyFanout: number;

	network: InMemoryNetwork["metrics"];
	profile?: {
		cpuUserMs: number;
		cpuSystemMs: number;
		rssMb: number;
		heapUsedMb: number;
		eventLoopDelayP95Ms: number;
		eventLoopDelayMaxMs: number;
	};
};

const mulberry32 = (seed: number) => {
	let t = seed >>> 0;
	return () => {
		t += 0x6d2b79f5;
		let x = t;
		x = Math.imul(x ^ (x >>> 15), x | 1);
		x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
		return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
	};
};

const int = (rng: () => number, maxExclusive: number) =>
	Math.floor(rng() * maxExclusive);

const pickDistinct = (
	rng: () => number,
	n: number,
	k: number,
	exclude: Set<number>,
): number[] => {
	if (k <= 0) return [];
	const out = new Set<number>();
	while (out.size < k) {
		const candidate = int(rng, n);
		if (exclude.has(candidate)) continue;
		out.add(candidate);
	}
	return [...out];
};

const runWithConcurrency = async <T>(
	tasks: Array<() => Promise<T>>,
	concurrency: number,
): Promise<T[]> => {
	const results: T[] = new Array(tasks.length);
	let index = 0;
	const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
		for (;;) {
			const i = index++;
			if (i >= tasks.length) return;
			results[i] = await tasks[i]!();
		}
	});
	await Promise.all(workers);
	return results;
};

const quantile = (sorted: number[], q: number) => {
	if (sorted.length === 0) return NaN;
	const idx = Math.min(
		sorted.length - 1,
		Math.max(0, Math.floor(q * (sorted.length - 1))),
	);
	return sorted[idx]!;
};

const parseSimPeerIndex = (peerId: any): number => {
	const s = String(peerId?.toString?.() ?? "");
	const m = s.match(/sim-(\d+)/);
	if (!m) return 0;
	const n = Number(m[1]);
	return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
};

export const resolveFanoutTreeSimParams = (
	input: Partial<FanoutTreeSimParams>,
): FanoutTreeSimParams => {
	const nodes = Number(input.nodes ?? 2000);
	const bootstraps = Number(input.bootstraps ?? 1);
	const subscribersDefault = Math.max(0, nodes - 1 - bootstraps);

	const msgRate = Number(input.msgRate ?? 30);
	const intervalMsRaw = Number(input.intervalMs ?? 0);
	const intervalMs =
		intervalMsRaw > 0
			? intervalMsRaw
			: msgRate > 0
				? Math.floor(1000 / msgRate)
				: 0;

		return {
		nodes,
		rootIndex: Number(input.rootIndex ?? 0),
		bootstraps,
		bootstrapMaxPeers: Number(input.bootstrapMaxPeers ?? 0),
		subscribers: Number(input.subscribers ?? subscribersDefault),
		relayFraction: Number(input.relayFraction ?? 0.25),

		messages: Number(input.messages ?? 200),
		msgRate,
		msgSize: Number(input.msgSize ?? 1024),
		intervalMs,
		settleMs: Number(input.settleMs ?? 2_000),
		deadlineMs: Number(input.deadlineMs ?? 0),
		maxDataAgeMs: Number(input.maxDataAgeMs ?? 0),

		timeoutMs: Number(input.timeoutMs ?? 300_000),
		seed: Number(input.seed ?? 1),
		topic: String(input.topic ?? "concert"),

		rootUploadLimitBps: Number(input.rootUploadLimitBps ?? 20_000_000),
		rootMaxChildren: Number(input.rootMaxChildren ?? 64),
		relayUploadLimitBps: Number(input.relayUploadLimitBps ?? 10_000_000),
		relayMaxChildren: Number(input.relayMaxChildren ?? 32),
		allowKick: Boolean(input.allowKick ?? false),
		bidPerByte: Number(input.bidPerByte ?? 0),
		bidPerByteRelay: Number(input.bidPerByteRelay ?? input.bidPerByte ?? 0),
		bidPerByteLeaf: Number(input.bidPerByteLeaf ?? input.bidPerByte ?? 0),

		repair: Boolean(input.repair ?? true),
		repairWindowMessages: Number(input.repairWindowMessages ?? 1024),
		repairMaxBackfillMessages: Number(input.repairMaxBackfillMessages ?? -1),
		repairIntervalMs: Number(input.repairIntervalMs ?? 200),
		repairMaxPerReq: Number(input.repairMaxPerReq ?? 64),
		neighborRepair: Boolean(input.neighborRepair ?? false),
		neighborRepairPeers: Number(input.neighborRepairPeers ?? 2),
		neighborMeshPeers: Number(input.neighborMeshPeers ?? -1),
		neighborAnnounceIntervalMs: Number(input.neighborAnnounceIntervalMs ?? -1),
		neighborMeshRefreshIntervalMs: Number(input.neighborMeshRefreshIntervalMs ?? -1),
		neighborHaveTtlMs: Number(input.neighborHaveTtlMs ?? -1),
		neighborRepairBudgetBps: Number(input.neighborRepairBudgetBps ?? -1),
		neighborRepairBurstMs: Number(input.neighborRepairBurstMs ?? -1),

		streamRxDelayMs: Number(input.streamRxDelayMs ?? 0),
		streamHighWaterMarkBytes: Number(input.streamHighWaterMarkBytes ?? 256 * 1024),
		dialDelayMs: Number(input.dialDelayMs ?? 0),
		joinConcurrency: Number(input.joinConcurrency ?? 256),
		joinReqTimeoutMs: Number(input.joinReqTimeoutMs ?? -1),
		candidateShuffleTopK: Number(input.candidateShuffleTopK ?? -1),
		candidateScoringMode:
			input.candidateScoringMode === "ranked-strict" ||
			input.candidateScoringMode === "weighted" ||
			input.candidateScoringMode === "ranked-shuffle"
				? input.candidateScoringMode
				: "ranked-shuffle",
		bootstrapEnsureIntervalMs: Number(input.bootstrapEnsureIntervalMs ?? -1),
		trackerQueryIntervalMs: Number(input.trackerQueryIntervalMs ?? -1),
		joinAttemptsPerRound: Number(input.joinAttemptsPerRound ?? -1),
		candidateCooldownMs: Number(input.candidateCooldownMs ?? -1),
		joinPhases: Boolean(input.joinPhases ?? false),
			joinPhaseSettleMs: Number(input.joinPhaseSettleMs ?? 2_000),

				maxLatencySamples: Number(input.maxLatencySamples ?? 1_000_000),
				profile: Boolean(input.profile ?? false),
				progress: Boolean(input.progress ?? false),
				progressEveryMs: Number(input.progressEveryMs ?? 5_000),

				dropDataFrameRate: Number(input.dropDataFrameRate ?? 0),

		churnEveryMs: Number(input.churnEveryMs ?? 0),
		churnDownMs: Number(input.churnDownMs ?? 0),
		churnFraction: Number(input.churnFraction ?? 0),

		assertMinJoinedPct: Number(input.assertMinJoinedPct ?? 0),
		assertMinDeliveryPct: Number(input.assertMinDeliveryPct ?? 0),
		assertMinDeadlineDeliveryPct: Number(input.assertMinDeadlineDeliveryPct ?? 0),
		assertMaxUploadFracPct: Number(input.assertMaxUploadFracPct ?? 0),
		assertMaxOverheadFactor: Number(input.assertMaxOverheadFactor ?? 0),
		assertMaxControlBpp: Number(input.assertMaxControlBpp ?? 0),
		assertMaxTrackerBpp: Number(input.assertMaxTrackerBpp ?? 0),
		assertMaxRepairBpp: Number(input.assertMaxRepairBpp ?? 0),
		assertAttachP95Ms: Number(input.assertAttachP95Ms ?? 0),
		assertMaxTreeLevelP95: Number(input.assertMaxTreeLevelP95 ?? 0),
		assertMaxFormationScore: Number(input.assertMaxFormationScore ?? 0),
		assertMaxOrphans: Number(input.assertMaxOrphans ?? 0),
		assertRecoveryP95Ms: Number(input.assertRecoveryP95Ms ?? 0),
		assertMaxReparentsPerMin: Number(input.assertMaxReparentsPerMin ?? 0),
		assertMaxOrphanArea: Number(input.assertMaxOrphanArea ?? 0),
	};
};

export const formatFanoutTreeSimResult = (r: FanoutTreeSimResult) => {
	const p = r.params;
		return [
		"fanout-tree-sim",
		`nodes=${p.nodes} bootstraps=${r.bootstrapCount} bootstrapMaxPeers=${p.bootstrapMaxPeers} subscribers=${r.subscriberCount} relays=${r.relayCount}`,
		`joined=${r.joinedCount}/${r.subscriberCount} (${r.joinedPct.toFixed(2)}%)`,
		`join: ${(r.joinMs / 1000).toFixed(3)}s`,
		`attachMs samples=${r.attachSamples} p50=${r.attachP50.toFixed(1)} p95=${r.attachP95.toFixed(1)} p99=${r.attachP99.toFixed(1)} max=${r.attachMax.toFixed(1)}`,
			`formationPaths: underlayEdges=${r.formationUnderlayEdges} dist(p95/max)=${r.formationUnderlayDistP95.toFixed(1)}/${r.formationUnderlayDistMax.toFixed(1)} stretch(p95/max)=${r.formationStretchP95.toFixed(2)}/${r.formationStretchMax.toFixed(2)} score=${r.formationScore.toFixed(2)}`,
			`formationTree: maxLevel=${r.formationTreeMaxLevel} p95Level=${r.formationTreeLevelP95.toFixed(1)} avgLevel=${r.formationTreeLevelAvg.toFixed(2)} orphans=${r.formationTreeOrphans} rootChildren=${r.formationTreeRootChildren} children(p95/max)=${r.formationTreeChildrenP95.toFixed(1)}/${r.formationTreeChildrenMax}`,
			`publish: ${(r.publishMs / 1000).toFixed(3)}s intervalMs=${p.intervalMs}`,
				`churn: everyMs=${p.churnEveryMs} downMs=${p.churnDownMs} fraction=${p.churnFraction} events=${r.churnEvents} peers=${r.churnedPeersTotal}`,
				...(r.maintSamples > 0
					? [
							`maintenance: maxOrphans=${r.maintMaxOrphans} orphanArea=${r.maintOrphanArea.toFixed(1)}s recoveryMs p50=${r.maintRecoveryP50Ms.toFixed(1)} p95=${r.maintRecoveryP95Ms.toFixed(1)} reparentsPerMin=${r.maintReparentsPerMin.toFixed(2)} flapMax=${r.maintMaxReparentsPerPeer} driftP95(level/children)=${r.maintLevelP95DriftMax.toFixed(1)}/${r.maintChildrenP95DriftMax.toFixed(1)}`,
						]
					: []),
				`delivered=${r.delivered}/${r.expected} (${r.deliveredPct.toFixed(2)}%) dup=${r.duplicates}`,
			p.deadlineMs > 0
				? `deadline=${p.deadlineMs}ms${p.maxDataAgeMs > 0 ? ` maxAgeMs=${p.maxDataAgeMs}` : ""} delivered=${r.deliveredWithinDeadline}/${r.expected} (${r.deliveredWithinDeadlinePct.toFixed(2)}%)`
				: `deadline=off${p.maxDataAgeMs > 0 ? ` maxAgeMs=${p.maxDataAgeMs}` : ""}`,
			`latencyMs p50=${r.latencyP50.toFixed(1)} p95=${r.latencyP95.toFixed(1)} p99=${r.latencyP99.toFixed(1)} max=${r.latencyMax.toFixed(1)}`,
			`drops: forward total=${r.droppedForwardsTotal} max=${r.droppedForwardsMax} node=${r.droppedForwardsMaxNode ?? "-"} stale total=${r.staleForwardsDroppedTotal} max=${r.staleForwardsDroppedMax} node=${r.staleForwardsDroppedMaxNode ?? "-"} write total=${r.dataWriteDropsTotal} max=${r.dataWriteDropsMax} node=${r.dataWriteDropsMaxNode ?? "-"}`,
			`reparent: disconnect=${r.reparentDisconnectTotal} stale=${r.reparentStaleTotal} kicked=${r.reparentKickedTotal}`,
			`tree: maxLevel=${r.treeMaxLevel} p95Level=${r.treeLevelP95.toFixed(1)} avgLevel=${r.treeLevelAvg.toFixed(2)} orphans=${r.treeOrphans} rootChildren=${r.treeRootChildren} children(p95/max)=${r.treeChildrenP95.toFixed(1)}/${r.treeChildrenMax}`,
			`upload: max=${r.maxUploadBps} B/s (${r.maxUploadFracPct.toFixed(1)}% of cap) node=${r.maxUploadNode ?? "-"}`,
			`stream: queuedBytes total=${r.streamQueuedBytesTotal} max=${r.streamQueuedBytesMax} p95=${r.streamQueuedBytesP95.toFixed(0)} node=${r.streamQueuedBytesMaxNode ?? "-"} lanes=${r.streamQueuedBytesByLane.join(",")}`,
		`overhead: dataFactor=${r.overheadFactorData.toFixed(3)} (sentPayloadBytes / ideal)`,
			`economics: earningsTotal=${r.earningsTotal} relayCount=${r.earningsRelayCount} p50=${r.earningsRelayP50} p95=${r.earningsRelayP95} max=${r.earningsRelayMax}`,
			`protocol: controlBytesSent=${r.protocolControlBytesSent} (join=${r.protocolControlBytesSentJoin} tracker=${r.protocolControlBytesSentTracker} repair=${r.protocolControlBytesSentRepair}) bpp=${r.controlBpp.toFixed(4)} (tracker=${r.trackerBpp.toFixed(4)} repair=${r.repairBpp.toFixed(4)}) dataPayloadBytesSent=${r.protocolDataPayloadBytesSent} fetchReqSent=${r.protocolFetchReqSent} ihaveSent=${r.protocolIHaveSent} trackerFeedbackSent=${r.protocolTrackerFeedbackSent} holeFills=${r.protocolHoleFillsFromNeighbor} routeCache(h/m/x/e)=${r.protocolRouteCacheHits}/${r.protocolRouteCacheMisses}/${r.protocolRouteCacheExpirations}/${r.protocolRouteCacheEvictions} routeProxy(q/t/f)=${r.protocolRouteProxyQueries}/${r.protocolRouteProxyTimeouts}/${r.protocolRouteProxyFanout}`,
			`network: dials=${r.network.dials} connsOpened=${r.network.connectionsOpened} streamsOpened=${r.network.streamsOpened} framesSent=${r.network.framesSent} bytesSent=${r.network.bytesSent} framesDropped=${r.network.framesDropped} bytesDropped=${r.network.bytesDropped}`,
			...(r.profile
				? [
						`profile: cpuUserMs=${r.profile.cpuUserMs.toFixed(1)} cpuSystemMs=${r.profile.cpuSystemMs.toFixed(1)} rssMb=${r.profile.rssMb.toFixed(1)} heapUsedMb=${r.profile.heapUsedMb.toFixed(1)} eldP95Ms=${r.profile.eventLoopDelayP95Ms.toFixed(2)} eldMaxMs=${r.profile.eventLoopDelayMaxMs.toFixed(2)}`,
					]
				: []),
		].join("\n");
	};

export const runFanoutTreeSim = async (
	input: Partial<FanoutTreeSimParams>,
): Promise<FanoutTreeSimResult> => {
	const params = resolveFanoutTreeSimParams(input);
	const timeoutMs = Math.max(0, Math.floor(params.timeoutMs));
	const rng = mulberry32(params.seed);
	const profileEnabled = params.profile === true;
	let profileCpuStart: ReturnType<typeof process.cpuUsage> | undefined;
	let profileEld: ReturnType<typeof monitorEventLoopDelay> | undefined;

	if (profileEnabled) {
		profileCpuStart = process.cpuUsage();
		try {
			profileEld = monitorEventLoopDelay({ resolution: 20 });
			profileEld.enable();
		} catch {
			// ignore
		}
	}

	const rootIndex = Math.max(0, Math.min(params.nodes - 1, params.rootIndex));

	const bootstrapCount = Math.max(
		0,
		Math.min(params.nodes - 1, Math.floor(params.bootstraps)),
	);
	const bootstrapIndices = Array.from({ length: bootstrapCount }, (_, i) => {
		const idx = i + 1;
		return idx >= params.nodes ? 0 : idx;
	}).filter((i) => i !== rootIndex);

	const exclude = new Set<number>([rootIndex, ...bootstrapIndices]);
	const subscriberCount = Math.max(
		0,
		Math.min(params.nodes - exclude.size, Math.floor(params.subscribers)),
	);
	const subscriberIndices = pickDistinct(rng, params.nodes, subscriberCount, exclude);

	// Ensure we have at least one relay when scale > root fanout.
	const wantsRelays = subscriberCount > Math.max(0, params.rootMaxChildren);
	let relayTarget = Math.floor(
		subscriberCount * Math.max(0, Math.min(1, params.relayFraction)),
	);
	if (wantsRelays && relayTarget === 0) relayTarget = Math.min(1, subscriberCount);

	const relaySet = new Set<number>();
	const shuffledSubscribers = [...subscriberIndices].sort(() => rng() - 0.5);
	for (const idx of shuffledSubscribers) {
		if (relaySet.size >= relayTarget) break;
		relaySet.add(idx);
	}

	const network = new InMemoryNetwork({
		streamRxDelayMs: params.streamRxDelayMs,
		streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
		dialDelayMs: params.dialDelayMs,
		dropDataFrameRate: params.dropDataFrameRate,
		dropSeed: params.seed,
	});

	const bootstrapIndexSet = new Set<number>(bootstrapIndices);
	const maxConnectionsFor = (index: number) => {
		if (index === rootIndex) return 256;
		if (bootstrapIndexSet.has(index)) return 128;
		if (relaySet.has(index)) return 64;
		return 16;
	};
	const seenCacheMaxFor = (index: number) => {
		if (index === rootIndex) return 200_000;
		if (bootstrapIndexSet.has(index)) return 100_000;
		if (relaySet.has(index)) return 50_000;
		return 20_000;
	};
	const seenCacheTtlMsFor = (index: number) => {
		if (index === rootIndex || bootstrapIndexSet.has(index)) return 120_000;
		return 60_000;
	};

	const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(params.nodes, {
		network,
		basePort: 30_000,
		services: {
			fanout: (c) => {
				const index = parseSimPeerIndex(c?.peerId);
				return new SimFanoutTree(c, {
					// Keep sims bounded: limit per-node connections to roughly the fanout degree,
					// so large networks can run in a single process without OOM.
					connectionManager: {
						minConnections: 0,
						maxConnections: maxConnectionsFor(index),
						dialer: false,
						pruner: { interval: 1_000 },
					},
					seenCacheMax: seenCacheMaxFor(index),
					seenCacheTtlMs: seenCacheTtlMsFor(index),
					random: mulberry32((params.seed >>> 0) ^ index),
				});
			},
		},
	});

	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeoutController = new AbortController();
	const timeoutSignal = timeoutController.signal;

	try {
		if (timeoutMs > 0) {
			timer = setTimeout(() => {
				timeoutController.abort(
					new Error(
						`fanout-tree-sim timed out after ${timeoutMs}ms (override with --timeoutMs)`,
					),
				);
			}, timeoutMs);
		}

			const run = async (): Promise<FanoutTreeSimResult> => {
				const bootstrapAddrs = bootstrapIndices.flatMap((i) =>
					session.peers[i]!.getMultiaddrs(),
				);
				if (bootstrapAddrs.length === 0) {
					throw new Error("No bootstrap addrs; pass --bootstraps >= 1");
				}

			for (const p of session.peers) {
				p.services.fanout.setBootstraps(bootstrapAddrs);
			}

			const root = session.peers[rootIndex]!.services.fanout;
			const rootId = root.publicKeyHash;

			// Root opens channel and starts announcing capacity to trackers immediately.
				root.openChannel(params.topic, rootId, {
					role: "root",
					msgRate: params.msgRate,
					msgSize: params.msgSize,
					...(params.maxDataAgeMs > 0 ? { maxDataAgeMs: params.maxDataAgeMs } : {}),
					uploadLimitBps: params.rootUploadLimitBps,
					maxChildren: params.rootMaxChildren,
					bidPerByte: params.bidPerByte,
					allowKick: params.allowKick,
				repair: params.repair,
				repairWindowMessages: params.repairWindowMessages,
				...(params.repairMaxBackfillMessages >= 0
					? { repairMaxBackfillMessages: params.repairMaxBackfillMessages }
					: {}),
				repairIntervalMs: params.repairIntervalMs,
				repairMaxPerReq: params.repairMaxPerReq,
				neighborRepair: params.neighborRepair,
				neighborRepairPeers: params.neighborRepairPeers,
				...(params.neighborMeshPeers >= 0
					? { neighborMeshPeers: params.neighborMeshPeers }
					: {}),
				...(params.neighborAnnounceIntervalMs >= 0
					? { neighborAnnounceIntervalMs: params.neighborAnnounceIntervalMs }
					: {}),
				...(params.neighborMeshRefreshIntervalMs >= 0
					? { neighborMeshRefreshIntervalMs: params.neighborMeshRefreshIntervalMs }
					: {}),
				...(params.neighborHaveTtlMs >= 0
					? { neighborHaveTtlMs: params.neighborHaveTtlMs }
					: {}),
				...(params.neighborRepairBudgetBps >= 0
					? { neighborRepairBudgetBps: params.neighborRepairBudgetBps }
					: {}),
				...(params.neighborRepairBurstMs >= 0
					? { neighborRepairBurstMs: params.neighborRepairBurstMs }
					: {}),
			});

				// Join subscribers (bounded concurrency).
				const joinStart = Date.now();
				const joined = new Array<boolean>(subscriberIndices.length).fill(false);
				const attachDurationsByPos = new Array<number>(subscriberIndices.length).fill(-1);
				let joinCompleted = 0;
				let joinOk = 0;
				const progressEveryMs = Math.max(250, Math.floor(params.progressEveryMs || 5_000));
				let joinProgressTimer: ReturnType<typeof setInterval> | undefined;
				if (params.progress) {
					console.log(
						`[fanout-tree-sim] phase=join subscribers=${subscriberIndices.length} relays=${relaySet.size} joinConcurrency=${params.joinConcurrency}`,
					);
					joinProgressTimer = setInterval(() => {
						const mu = process.memoryUsage();
						const rssMb = mu.rss / (1024 * 1024);
						const heapUsedMb = mu.heapUsed / (1024 * 1024);
						const openConns = Math.max(
							0,
							network.metrics.connectionsOpened - network.metrics.connectionsClosed,
						);
						console.log(
							`[fanout-tree-sim] join progress ok=${joinOk}/${subscriberIndices.length} done=${joinCompleted}/${subscriberIndices.length} openConns=${openConns} dials=${network.metrics.dials} streamsOpened=${network.metrics.streamsOpened} rssMb=${rssMb.toFixed(1)} heapUsedMb=${heapUsedMb.toFixed(1)}`,
						);
					}, progressEveryMs);
					joinProgressTimer.unref?.();
				}
				const joinOne = async (idx: number): Promise<boolean> => {
					const node = session.peers[idx]!.services.fanout;
					const isRelay = relaySet.has(idx);
					try {
						await node.joinChannel(
							params.topic,
							rootId,
							{
								msgRate: params.msgRate,
								msgSize: params.msgSize,
								...(params.maxDataAgeMs > 0 ? { maxDataAgeMs: params.maxDataAgeMs } : {}),
								uploadLimitBps: isRelay ? params.relayUploadLimitBps : 0,
								maxChildren: isRelay ? params.relayMaxChildren : 0,
								bidPerByte: isRelay ? params.bidPerByteRelay : params.bidPerByteLeaf,
								allowKick: params.allowKick,
							repair: params.repair,
							repairWindowMessages: params.repairWindowMessages,
							...(params.repairMaxBackfillMessages >= 0
								? { repairMaxBackfillMessages: params.repairMaxBackfillMessages }
								: {}),
							repairIntervalMs: params.repairIntervalMs,
							repairMaxPerReq: params.repairMaxPerReq,
							neighborRepair: params.neighborRepair,
							neighborRepairPeers: params.neighborRepairPeers,
							...(params.neighborMeshPeers >= 0
								? { neighborMeshPeers: params.neighborMeshPeers }
								: {}),
							...(params.neighborAnnounceIntervalMs >= 0
								? { neighborAnnounceIntervalMs: params.neighborAnnounceIntervalMs }
								: {}),
							...(params.neighborMeshRefreshIntervalMs >= 0
								? { neighborMeshRefreshIntervalMs: params.neighborMeshRefreshIntervalMs }
								: {}),
							...(params.neighborHaveTtlMs >= 0
								? { neighborHaveTtlMs: params.neighborHaveTtlMs }
								: {}),
							...(params.neighborRepairBudgetBps >= 0
								? { neighborRepairBudgetBps: params.neighborRepairBudgetBps }
								: {}),
							...(params.neighborRepairBurstMs >= 0
								? { neighborRepairBurstMs: params.neighborRepairBurstMs }
								: {}),
						},
								{
									timeoutMs: Math.max(10_000, Math.min(120_000, timeoutMs || 120_000)),
									...(params.maxDataAgeMs > 0 ? { staleAfterMs: params.maxDataAgeMs } : {}),
									...(params.joinReqTimeoutMs >= 0
										? { joinReqTimeoutMs: params.joinReqTimeoutMs }
										: {}),
									...(params.candidateShuffleTopK >= 0
									? { candidateShuffleTopK: params.candidateShuffleTopK }
									: {}),
									candidateScoringMode: params.candidateScoringMode,
								...(params.bootstrapEnsureIntervalMs >= 0
									? { bootstrapEnsureIntervalMs: params.bootstrapEnsureIntervalMs }
									: {}),
								...(params.trackerQueryIntervalMs >= 0
									? { trackerQueryIntervalMs: params.trackerQueryIntervalMs }
									: {}),
								...(params.joinAttemptsPerRound >= 0
									? { joinAttemptsPerRound: params.joinAttemptsPerRound }
									: {}),
								...(params.candidateCooldownMs >= 0
									? { candidateCooldownMs: params.candidateCooldownMs }
									: {}),
								signal: timeoutSignal,
								bootstrapMaxPeers: params.bootstrapMaxPeers,
							},
						);
							return true;
					} catch {
						return false;
					}
				};

				const runPhase = async (indices: number[]) => {
					const tasks = indices.map(
						(pos) => async () => {
							const ok = await joinOne(subscriberIndices[pos]!);
							joined[pos] = ok;
							if (ok) {
								joinOk += 1;
								attachDurationsByPos[pos] = Date.now() - joinStart;
							}
							joinCompleted += 1;
							return ok;
						},
					);
					await runWithConcurrency(tasks, params.joinConcurrency);
				};

				try {
					if (params.joinPhases) {
						const relayPositions: number[] = [];
						const leafPositions: number[] = [];
						for (let i = 0; i < subscriberIndices.length; i++) {
							const idx = subscriberIndices[i]!;
							if (relaySet.has(idx)) relayPositions.push(i);
							else leafPositions.push(i);
						}

						await runPhase(relayPositions);

						const settleMs = Math.max(0, Math.floor(params.joinPhaseSettleMs));
						if (settleMs > 0) {
							await delay(settleMs, { signal: timeoutSignal });
						}

						await runPhase(leafPositions);
					} else {
						await runPhase(subscriberIndices.map((_, i) => i));
					}
				} finally {
					if (joinProgressTimer) clearInterval(joinProgressTimer);
				}
				const joinDone = Date.now();
				if (params.progress) {
					const mu = process.memoryUsage();
					const rssMb = mu.rss / (1024 * 1024);
					const heapUsedMb = mu.heapUsed / (1024 * 1024);
					const openConns = Math.max(
						0,
						network.metrics.connectionsOpened - network.metrics.connectionsClosed,
					);
					console.log(
						`[fanout-tree-sim] phase=join_done ok=${joinOk}/${subscriberIndices.length} openConns=${openConns} rssMb=${rssMb.toFixed(1)} heapUsedMb=${heapUsedMb.toFixed(1)} joinMs=${joinDone - joinStart}`,
					);
				}

			const attachDurations = attachDurationsByPos.filter((d) => d >= 0).sort((a, b) => a - b);
			const attachSamples = attachDurations.length;
			const attachP50 = attachSamples > 0 ? quantile(attachDurations, 0.5) : NaN;
			const attachP95 = attachSamples > 0 ? quantile(attachDurations, 0.95) : NaN;
			const attachP99 = attachSamples > 0 ? quantile(attachDurations, 0.99) : NaN;
			const attachMax = attachSamples > 0 ? attachDurations[attachSamples - 1]! : NaN;

			const joinedHashes = new Set<string>(
				subscriberIndices
					.filter((_, i) => joined[i])
					.map((i) => session.peers[i]!.services.fanout.publicKeyHash),
			);
			const joinedCount = joinedHashes.size;
			const joinedSubscriberIndices = subscriberIndices.filter((_, i) => joined[i]);

			const computeTreeShapeStats = () => {
				const levels: number[] = [];
				const levelByIndex: number[] = new Array(params.nodes).fill(NaN);
				const childrenCounts: number[] = [];
				let treeOrphans = 0;
				let treeRootChildren = 0;
				for (let i = 0; i < session.peers.length; i++) {
					const s = session.peers[i]!.services.fanout.getChannelStats(params.topic, rootId);
					if (!s) continue;
					if (Number.isFinite(s.level)) {
						levels.push(s.level);
						levelByIndex[i] = s.level;
					}
					if (s.effectiveMaxChildren > 0) {
						childrenCounts.push(s.children);
					}
					if (s.level === 0) {
						treeRootChildren = s.children;
					} else if (Number.isFinite(s.level) && !s.parent) {
						treeOrphans += 1;
					}
				}
				levels.sort((a, b) => a - b);
				childrenCounts.sort((a, b) => a - b);
				const treeMaxLevel = levels.length > 0 ? levels[levels.length - 1]! : 0;
				const treeLevelP95 = levels.length > 0 ? quantile(levels, 0.95) : 0;
				const treeLevelAvg =
					levels.length > 0 ? levels.reduce((a, b) => a + b, 0) / levels.length : 0;
				const treeChildrenP95 =
					childrenCounts.length > 0 ? quantile(childrenCounts, 0.95) : 0;
				const treeChildrenMax =
					childrenCounts.length > 0 ? childrenCounts[childrenCounts.length - 1]! : 0;
				return {
					treeMaxLevel,
					treeLevelP95,
					treeLevelAvg,
					treeOrphans,
					treeChildrenP95,
					treeChildrenMax,
					treeRootChildren,
					levelByIndex,
				};
			};

			const formationTree = computeTreeShapeStats();

			// Underlay (libp2p connection graph) shortest paths, used to spot wasted
			// open connections that don't contribute to the overlay tree.
			const underlayAdj: Array<Set<number>> = Array.from({ length: params.nodes }, () => new Set());
			for (let i = 0; i < session.peers.length; i++) {
				for (const c of session.peers[i]!.getConnections()) {
					// @ts-ignore - bench shim uses the same field name as real libp2p connections
					if ((c as any).status && (c as any).status !== "open") continue;
					const j = parseSimPeerIndex((c as any).remotePeer);
					if (j === i) continue;
					if (j < 0 || j >= params.nodes) continue;
					underlayAdj[i]!.add(j);
				}
			}
			let formationUnderlayEdges = 0;
			for (const s of underlayAdj) formationUnderlayEdges += s.size;
			formationUnderlayEdges = Math.floor(formationUnderlayEdges / 2);

			const underlayDist = new Array<number>(params.nodes).fill(Infinity);
			const q: number[] = [];
			underlayDist[rootIndex] = 0;
			q.push(rootIndex);
			for (let qi = 0; qi < q.length; qi++) {
				const u = q[qi]!;
				const du = underlayDist[u]!;
				for (const v of underlayAdj[u]!) {
					if (underlayDist[v] !== Infinity) continue;
					underlayDist[v] = du + 1;
					q.push(v);
				}
			}

			const formationUnderlayDists = joinedSubscriberIndices
				.map((i) => underlayDist[i]!)
				.filter((d) => Number.isFinite(d))
				.sort((a, b) => a - b);
			const formationUnderlayDistP95 =
				formationUnderlayDists.length > 0 ? quantile(formationUnderlayDists, 0.95) : NaN;
			const formationUnderlayDistMax =
				formationUnderlayDists.length > 0
					? formationUnderlayDists[formationUnderlayDists.length - 1]!
					: NaN;

			const formationStretches = joinedSubscriberIndices
				.map((i) => {
					const overlay = formationTree.levelByIndex[i]!;
					const under = underlayDist[i]!;
					if (!Number.isFinite(overlay) || !Number.isFinite(under) || under <= 0) return NaN;
					return overlay / under;
				})
				.filter((x) => Number.isFinite(x))
				.sort((a, b) => a - b);
			const formationStretchP95 =
				formationStretches.length > 0 ? quantile(formationStretches, 0.95) : NaN;
			const formationStretchMax =
				formationStretches.length > 0 ? formationStretches[formationStretches.length - 1]! : NaN;

			const formationOrphanPct =
				joinedCount === 0 ? 0 : (100 * formationTree.treeOrphans) / joinedCount;
			const formationStretchPenalty = Number.isFinite(formationStretchP95)
				? Math.max(0, formationStretchP95 - 1) * 10
				: 0;
			const formationScore =
				(Number.isFinite(attachP95) ? attachP95 / 1000 : 0) +
				formationTree.treeLevelP95 +
				formationOrphanPct +
				formationStretchPenalty;

			const churnController = new AbortController();
			const churnSignal = anySignal([timeoutSignal, churnController.signal]) as AbortSignal & {
				clear?: () => void;
			};
			let churnEvents = 0;
			let churnedPeersTotal = 0;
			const wantsMaintenance =
				(params.churnEveryMs > 0 && params.churnDownMs > 0 && params.churnFraction > 0) ||
				params.assertMaxOrphans > 0 ||
				params.assertMaxOrphanArea > 0 ||
				params.assertRecoveryP95Ms > 0 ||
				params.assertMaxReparentsPerMin > 0;
			const maintenanceController = new AbortController();
			const maintenanceSignal = anySignal([
				timeoutSignal,
				maintenanceController.signal,
			]) as AbortSignal & {
				clear?: () => void;
			};
			let maintDurationMs = 0;
			let maintSamples = 0;
			let maintMaxOrphans = 0;
			let maintOrphanAreaMs = 0;
			const pendingRecoveryStarts: number[] = [];
			const recoveryDurations: number[] = [];
			let maintLevelP95DriftMax = 0;
			let maintChildrenP95DriftMax = 0;
			const reparentBaselineByHash = new Map<string, number>();
			let maintReparentsTotal = 0;
			let maintMaxReparentsPerPeer = 0;

			// Delivery tracking
			const publishAt = new Map<number, number>();
			const joinedHashList = [...joinedHashes];
			const hashToIndex = new Map<string, number>();
			for (let i = 0; i < joinedHashList.length; i++) {
				hashToIndex.set(joinedHashList[i]!, i);
			}
			const bitsetBytes = Math.ceil(Math.max(0, params.messages) / 8);
			const receivedBits = joinedHashList.map(() => new Uint8Array(bitsetBytes));
			const receivedCounts = new Uint32Array(joinedHashList.length);

			let duplicates = 0;
			let deliveredWithinDeadline = 0;
			let deliveredSamples: number[] = [];
			const sampleCap = Math.max(1, Math.floor(params.maxLatencySamples));
			let sampleSeen = 0;

			const makeOnData = (localHash: string) => (ev: any) => {
				const d = ev?.detail;
				if (!d) return;
				if (d.topic !== params.topic) return;
				if (d.root !== rootId) return;
				const seq = d.seq >>> 0;

				const index = hashToIndex.get(localHash);
				if (index == null) return; // not joined / not tracked
				if (seq >= params.messages) return;

				const bits = receivedBits[index]!;
				const byteIndex = seq >>> 3;
				const mask = 1 << (seq & 7);
				if (byteIndex >= bits.length) return;
				if ((bits[byteIndex]! & mask) !== 0) {
					duplicates += 1;
					return;
				}
				bits[byteIndex] |= mask;
				receivedCounts[index] += 1;

				const sentAt = publishAt.get(seq);
				if (sentAt != null) {
					const latency = Date.now() - sentAt;
					if (params.deadlineMs > 0 && latency <= params.deadlineMs) {
						deliveredWithinDeadline += 1;
					}
					sampleSeen += 1;
					if (deliveredSamples.length < sampleCap) {
						deliveredSamples.push(latency);
					} else {
						const j = int(rng, sampleSeen);
						if (j < sampleCap) deliveredSamples[j] = latency;
					}
				}
			};

			for (let i = 0; i < subscriberIndices.length; i++) {
				if (!joined[i]) continue;
				const idx = subscriberIndices[i]!;
				const node = session.peers[idx]!.services.fanout;
				const localHash = node.publicKeyHash;
				node.addEventListener("fanout:data", makeOnData(localHash) as any);
			}

			// Publish
			const payload = new Uint8Array(Math.max(0, params.msgSize));
			for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;

			const quantileFromCounts = (
				counts: number[],
				maxValue: number,
				total: number,
				q: number,
			) => {
				if (total <= 0) return 0;
				const target = Math.min(
					total - 1,
					Math.max(0, Math.floor(q * (total - 1))),
				);
				let seen = 0;
				for (let v = 0; v <= maxValue; v++) {
					seen += counts[v] ?? 0;
					if (seen > target) return v;
				}
				return maxValue;
			};

			const levelCounts: number[] = [];
			const childCounts: number[] = [];
			const sampleMaintenance = (now: number) => {
				let onlineJoined = 0;
				let orphansOnline = 0;

				let levelsTotal = 0;
				let levelsMax = 0;
				levelCounts.fill(0);
				let childrenTotal = 0;
				let childrenMax = 0;
				childCounts.fill(0);

				for (const idx of joinedSubscriberIndices) {
					const peer = session.peers[idx]!;
					if (network.isPeerOffline(peer.peerId, now)) continue;
					onlineJoined += 1;

					const s = peer.services.fanout.getChannelStats(params.topic, rootId);
					if (!s) {
						orphansOnline += 1;
						continue;
					}

					if (Number.isFinite(s.level)) {
						const lvl = Math.max(0, Math.floor(s.level));
						levelsMax = Math.max(levelsMax, lvl);
						while (levelCounts.length <= lvl) levelCounts.push(0);
						levelCounts[lvl] = (levelCounts[lvl] ?? 0) + 1;
						levelsTotal += 1;
					}
					if (s.effectiveMaxChildren > 0) {
						const c = Math.max(0, Math.floor(s.children));
						childrenMax = Math.max(childrenMax, c);
						while (childCounts.length <= c) childCounts.push(0);
						childCounts[c] = (childCounts[c] ?? 0) + 1;
						childrenTotal += 1;
					}

					if (s.level > 0 && !s.parent) {
						orphansOnline += 1;
					}
				}

				const levelP95 =
					levelsTotal > 0
						? quantileFromCounts(levelCounts, levelsMax, levelsTotal, 0.95)
						: NaN;
				const childrenP95 =
					childrenTotal > 0
						? quantileFromCounts(childCounts, childrenMax, childrenTotal, 0.95)
						: NaN;

				return {
					onlineJoined,
					orphansOnline,
					levelP95,
					childrenP95,
				};
			};

			const maintenanceLoop = async () => {
				if (!wantsMaintenance) return;
				const sampleEveryMs = Math.max(
					100,
					Math.min(1_000, Math.floor(params.nodes / 10)),
				);

				const baselineOrphans = formationTree.treeOrphans;
				const baselineLevelP95 = formationTree.treeLevelP95;
				const baselineChildrenP95 = formationTree.treeChildrenP95;

				let lastAt = Date.now();
				let lastOrphans = 0;
				const startAt = lastAt;
				try {
					for (;;) {
						if (maintenanceSignal.aborted) return;
						const now = Date.now();
						const dt = Math.max(0, now - lastAt);
						if (dt > 0) {
							maintOrphanAreaMs += lastOrphans * dt;
						}
						lastAt = now;

						const snap = sampleMaintenance(now);
						lastOrphans = snap.orphansOnline;
						maintMaxOrphans = Math.max(maintMaxOrphans, snap.orphansOnline);
						maintSamples += 1;

						if (Number.isFinite(snap.levelP95)) {
							maintLevelP95DriftMax = Math.max(
								maintLevelP95DriftMax,
								Math.abs(snap.levelP95 - baselineLevelP95),
							);
						}
						if (Number.isFinite(snap.childrenP95)) {
							maintChildrenP95DriftMax = Math.max(
								maintChildrenP95DriftMax,
								Math.abs(snap.childrenP95 - baselineChildrenP95),
							);
						}

						if (
							snap.orphansOnline <= baselineOrphans &&
							pendingRecoveryStarts.length > 0
						) {
							for (const s of pendingRecoveryStarts) recoveryDurations.push(now - s);
							pendingRecoveryStarts.length = 0;
						}

						await delay(sampleEveryMs, { signal: maintenanceSignal });
					}
				} finally {
					const endAt = Date.now();
					const dt = Math.max(0, endAt - lastAt);
					if (dt > 0) {
						maintOrphanAreaMs += lastOrphans * dt;
					}
					maintDurationMs = Math.max(0, endAt - startAt);

					for (const s of pendingRecoveryStarts) recoveryDurations.push(endAt - s);
					pendingRecoveryStarts.length = 0;
				}
			};

			const churnLoop = async () => {
				const everyMs = Math.max(0, Math.floor(params.churnEveryMs));
				const downMs = Math.max(0, Math.floor(params.churnDownMs));
				const fraction = Math.max(0, Math.min(1, Number(params.churnFraction)));
				if (everyMs <= 0 || downMs <= 0 || fraction <= 0) return;
				if (joinedSubscriberIndices.length === 0) return;

				for (;;) {
					if (churnSignal.aborted) return;
					await delay(everyMs, { signal: churnSignal });
					if (churnSignal.aborted) return;

					const target = Math.min(
						joinedSubscriberIndices.length,
						Math.max(1, Math.floor(joinedSubscriberIndices.length * fraction)),
					);
					const chosen = new Set<number>();
					const maxAttempts = Math.max(10, target * 20);
					for (let tries = 0; chosen.size < target && tries < maxAttempts; tries++) {
						const idx = joinedSubscriberIndices[int(rng, joinedSubscriberIndices.length)]!;
						const peer = session.peers[idx]!;
						if (network.isPeerOffline(peer.peerId)) continue;
						chosen.add(idx);
					}
					if (chosen.size === 0) continue;

					churnEvents += 1;
						churnedPeersTotal += chosen.size;

						const now = Date.now();
						if (wantsMaintenance) pendingRecoveryStarts.push(now);
						await Promise.all(
							[...chosen].map(async (idx) => {
								const peer = session.peers[idx]!;
								network.setPeerOffline(peer.peerId, downMs, now);
							await network.disconnectPeer(peer.peerId);
						}),
					);
				}
				};

			const publishStart = Date.now();
			if (wantsMaintenance) {
				for (const p of session.peers) {
					const nodeHash = p.services.fanout.publicKeyHash;
					const m = p.services.fanout.getChannelMetrics(params.topic, rootId);
					reparentBaselineByHash.set(
						nodeHash,
						m.reparentDisconnect + m.reparentStale + m.reparentKicked,
					);
				}
			}
			const maintenancePromise = maintenanceLoop().catch(() => {});
			const churnPromise = churnLoop().catch(() => {});
			try {
				for (let seq = 0; seq < params.messages; seq++) {
					if (timeoutSignal.aborted) {
						throw timeoutSignal.reason ?? new Error("fanout-tree-sim aborted");
					}
					publishAt.set(seq, Date.now());
					await root.publishData(params.topic, rootId, payload);
					if (params.intervalMs > 0) {
						await delay(params.intervalMs, { signal: timeoutSignal });
					}
				}
			} finally {
				churnController.abort();
				await churnPromise;
				churnSignal.clear?.();
			}
			const publishDone = Date.now();

			// Signal end-of-stream so subscribers can detect tail gaps and repair.
			if (params.repair && params.messages > 0) {
				await root.publishEnd(params.topic, rootId, params.messages);
			}

			if (params.settleMs > 0) {
				await delay(params.settleMs, { signal: timeoutSignal });
			}
			maintenanceController.abort();
			await maintenancePromise;
			maintenanceSignal.clear?.();

			recoveryDurations.sort((a, b) => a - b);
			const maintRecoveryCount = recoveryDurations.length;
			const maintRecoveryP50Ms =
				maintRecoveryCount > 0 ? quantile(recoveryDurations, 0.5) : 0;
			const maintRecoveryP95Ms =
				maintRecoveryCount > 0 ? quantile(recoveryDurations, 0.95) : 0;

			if (wantsMaintenance) {
				for (const p of session.peers) {
					const nodeHash = p.services.fanout.publicKeyHash;
					const m = p.services.fanout.getChannelMetrics(params.topic, rootId);
					const total = m.reparentDisconnect + m.reparentStale + m.reparentKicked;
					const base = reparentBaselineByHash.get(nodeHash) ?? 0;
					const delta = Math.max(0, total - base);
					maintReparentsTotal += delta;
					maintMaxReparentsPerPeer = Math.max(maintMaxReparentsPerPeer, delta);
				}
			}
			const maintDurationMin = maintDurationMs / 60_000;
			const maintReparentsPerMin =
				maintDurationMin > 0 ? maintReparentsTotal / maintDurationMin : 0;
			const maintOrphanArea = maintOrphanAreaMs / 1_000;

			// Compute delivery
			const expected = joinedCount * params.messages;
			let delivered = 0;
			for (const c of receivedCounts) delivered += c;

			const joinedPct =
				subscriberCount === 0 ? 100 : (100 * joinedCount) / subscriberCount;
			const deliveredPct = expected === 0 ? 100 : (100 * delivered) / expected;
			const deliveredWithinDeadlinePct =
				expected === 0 ? 100 : (100 * deliveredWithinDeadline) / expected;

				// Internal protocol drops (from upload shaping / overload logic)
				let droppedForwardsTotal = 0;
				let droppedForwardsMax = 0;
				let droppedForwardsMaxNode: string | undefined;
				for (const p of session.peers) {
					const s = p.services.fanout.getChannelStats(params.topic, rootId);
					if (!s) continue;
					droppedForwardsTotal += s.droppedForwards;
					if (s.droppedForwards > droppedForwardsMax) {
						droppedForwardsMax = s.droppedForwards;
						droppedForwardsMaxNode = p.services.fanout.publicKeyHash;
					}
				}

				// Tree shape stats (best-effort)
				const tree = computeTreeShapeStats();
				const treeMaxLevel = tree.treeMaxLevel;
				const treeLevelP95 = tree.treeLevelP95;
				const treeLevelAvg = tree.treeLevelAvg;
				const treeOrphans = tree.treeOrphans;
				const treeChildrenP95 = tree.treeChildrenP95;
				const treeChildrenMax = tree.treeChildrenMax;
				const treeRootChildren = tree.treeRootChildren;

				// Stream backpressure stats (queued bytes)
				const queuedBytesSamples: number[] = [];
				let streamQueuedBytesTotal = 0;
				let streamQueuedBytesMax = 0;
				let streamQueuedBytesMaxNode: string | undefined;
				for (const p of session.peers) {
					const q = Math.max(0, Math.floor(p.services.fanout.getQueuedBytes()));
					streamQueuedBytesTotal += q;
					queuedBytesSamples.push(q);
					if (q > streamQueuedBytesMax) {
						streamQueuedBytesMax = q;
						streamQueuedBytesMaxNode = p.services.fanout.publicKeyHash;
					}
				}
				queuedBytesSamples.sort((a, b) => a - b);
				const streamQueuedBytesP95 =
					queuedBytesSamples.length > 0 ? quantile(queuedBytesSamples, 0.95) : 0;

				const streamQueuedBytesByLane: number[] = [];
				for (const p of session.peers) {
					for (const ps of p.services.fanout.peers.values()) {
						const byLane: number[] =
							// @ts-ignore - optional debug helper (may not exist in built typings yet)
							(ps as any).getOutboundQueuedBytesByLane?.() ?? [0, 0, 0, 0];
						for (let lane = 0; lane < byLane.length; lane++) {
							streamQueuedBytesByLane[lane] =
								(streamQueuedBytesByLane[lane] ?? 0) + (byLane[lane] ?? 0);
						}
					}
				}
				for (let lane = 0; lane < 4; lane++) {
					streamQueuedBytesByLane[lane] = streamQueuedBytesByLane[lane] ?? 0;
				}

				// Peak upload vs cap (best-effort; counts framed bytes, including overhead)
				const uploadCapByHash = new Map<string, number>();
				if (params.rootUploadLimitBps > 0) {
					uploadCapByHash.set(root.publicKeyHash, params.rootUploadLimitBps);
				}
			for (let i = 0; i < subscriberIndices.length; i++) {
				if (!joined[i]) continue;
				const idx = subscriberIndices[i]!;
				if (!relaySet.has(idx)) continue;
				if (params.relayUploadLimitBps <= 0) continue;
				const h = session.peers[idx]!.services.fanout.publicKeyHash;
				uploadCapByHash.set(h, params.relayUploadLimitBps);
			}

			let maxUploadFracPct = 0;
			let maxUploadNode: string | undefined;
			let maxUploadBps = 0;
			for (const [hash, pm] of session.network.peerMetricsByHash) {
				const cap = uploadCapByHash.get(hash);
				if (!cap || cap <= 0) continue;
				const frac = (100 * pm.maxBytesPerSecond) / cap;
				if (frac > maxUploadFracPct) {
					maxUploadFracPct = frac;
					maxUploadNode = hash;
					maxUploadBps = pm.maxBytesPerSecond;
				}
			}

			deliveredSamples.sort((a, b) => a - b);

			let protocolControlSends = 0;
			let protocolControlBytesSent = 0;
			let protocolControlBytesSentJoin = 0;
			let protocolControlBytesSentRepair = 0;
			let protocolControlBytesSentTracker = 0;
			let protocolControlReceives = 0;
			let protocolControlBytesReceived = 0;
			let protocolDataSends = 0;
			let protocolDataPayloadBytesSent = 0;
			let protocolDataReceives = 0;
			let protocolDataPayloadBytesReceived = 0;
			let protocolRepairReqSent = 0;
				let protocolFetchReqSent = 0;
				let protocolIHaveSent = 0;
				let protocolTrackerFeedbackSent = 0;
				let protocolCacheHitsServed = 0;
				let protocolHoleFillsFromNeighbor = 0;
				let protocolRouteCacheHits = 0;
				let protocolRouteCacheMisses = 0;
				let protocolRouteCacheExpirations = 0;
				let protocolRouteCacheEvictions = 0;
				let protocolRouteProxyQueries = 0;
				let protocolRouteProxyTimeouts = 0;
				let protocolRouteProxyFanout = 0;
					let staleForwardsDroppedTotal = 0;
					let staleForwardsDroppedMax = 0;
					let staleForwardsDroppedMaxNode: string | undefined;
					let dataWriteDropsTotal = 0;
					let dataWriteDropsMax = 0;
					let dataWriteDropsMaxNode: string | undefined;
					let reparentDisconnectTotal = 0;
					let reparentStaleTotal = 0;
					let reparentKickedTotal = 0;
					let earningsTotal = 0;
					const earningsByHash = new Map<string, number>();

					for (const p of session.peers) {
						const nodeHash = p.services.fanout.publicKeyHash;
						const m = p.services.fanout.getChannelMetrics(params.topic, rootId);
						staleForwardsDroppedTotal += m.staleForwardsDropped;
						if (m.staleForwardsDropped > staleForwardsDroppedMax) {
							staleForwardsDroppedMax = m.staleForwardsDropped;
							staleForwardsDroppedMaxNode = nodeHash;
						}
						dataWriteDropsTotal += m.dataWriteDrops;
						if (m.dataWriteDrops > dataWriteDropsMax) {
							dataWriteDropsMax = m.dataWriteDrops;
							dataWriteDropsMaxNode = nodeHash;
						}
						reparentDisconnectTotal += m.reparentDisconnect;
						reparentStaleTotal += m.reparentStale;
						reparentKickedTotal += m.reparentKicked;
						earningsTotal += m.earnings;
						earningsByHash.set(nodeHash, m.earnings);
						protocolControlSends += m.controlSends;
						protocolControlBytesSent += m.controlBytesSent;
						protocolControlBytesSentJoin += m.controlBytesSentJoin;
						protocolControlBytesSentRepair += m.controlBytesSentRepair;
						protocolControlBytesSentTracker += m.controlBytesSentTracker;
				protocolControlReceives += m.controlReceives;
				protocolControlBytesReceived += m.controlBytesReceived;
				protocolDataSends += m.dataSends;
				protocolDataPayloadBytesSent += m.dataPayloadBytesSent;
				protocolDataReceives += m.dataReceives;
				protocolDataPayloadBytesReceived += m.dataPayloadBytesReceived;
				protocolRepairReqSent += m.repairReqSent;
				protocolFetchReqSent += m.fetchReqSent;
				protocolIHaveSent += m.ihaveSent;
					protocolTrackerFeedbackSent += m.trackerFeedbackSent;
					protocolCacheHitsServed += m.cacheHitsServed;
					protocolHoleFillsFromNeighbor += m.holeFillsFromNeighbor;
					protocolRouteCacheHits += m.routeCacheHits;
					protocolRouteCacheMisses += m.routeCacheMisses;
					protocolRouteCacheExpirations += m.routeCacheExpirations;
					protocolRouteCacheEvictions += m.routeCacheEvictions;
					protocolRouteProxyQueries += m.routeProxyQueries;
					protocolRouteProxyTimeouts += m.routeProxyTimeouts;
					protocolRouteProxyFanout += m.routeProxyFanout;
				}

			const relayHashes = [...uploadCapByHash.keys()];
			const relayEarnings = relayHashes.map((h) => earningsByHash.get(h) ?? 0).sort((a, b) => a - b);
			const earningsRelayCount = relayEarnings.length;
			const earningsRelayP50 = earningsRelayCount > 0 ? quantile(relayEarnings, 0.5) : 0;
			const earningsRelayP95 = earningsRelayCount > 0 ? quantile(relayEarnings, 0.95) : 0;
			const earningsRelayMax =
				earningsRelayCount > 0 ? relayEarnings[relayEarnings.length - 1]! : 0;

			const idealPayloadBytes = expected * Math.max(0, params.msgSize);
			const overheadFactorData =
				idealPayloadBytes <= 0
					? 1
					: protocolDataPayloadBytesSent / idealPayloadBytes;

			const deliveredPayloadBytes = delivered * Math.max(0, params.msgSize);
			const controlBpp =
				deliveredPayloadBytes <= 0 ? 0 : protocolControlBytesSent / deliveredPayloadBytes;
			const trackerBpp =
				deliveredPayloadBytes <= 0
					? 0
					: protocolControlBytesSentTracker / deliveredPayloadBytes;
			const repairBpp =
				deliveredPayloadBytes <= 0
					? 0
					: protocolControlBytesSentRepair / deliveredPayloadBytes;

			return {
				params,
				bootstrapCount: bootstrapIndices.length,
				subscriberCount,
				relayCount: relaySet.size,
				joinedCount,
				joinedPct,
				joinMs: joinDone - joinStart,
				attachSamples,
				attachP50,
				attachP95,
				attachP99,
				attachMax,
				formationTreeMaxLevel: formationTree.treeMaxLevel,
				formationTreeLevelP95: formationTree.treeLevelP95,
				formationTreeLevelAvg: formationTree.treeLevelAvg,
				formationTreeOrphans: formationTree.treeOrphans,
				formationTreeChildrenP95: formationTree.treeChildrenP95,
				formationTreeChildrenMax: formationTree.treeChildrenMax,
				formationTreeRootChildren: formationTree.treeRootChildren,
				formationUnderlayEdges,
				formationUnderlayDistP95,
				formationUnderlayDistMax,
				formationStretchP95,
				formationStretchMax,
				formationScore,
				publishMs: publishDone - publishStart,
				expected,
				delivered,
				deliveredPct,
				deliveredWithinDeadline,
				deliveredWithinDeadlinePct,
				duplicates,
				latencySamples: deliveredSamples.length,
				latencyP50: quantile(deliveredSamples, 0.5),
				latencyP95: quantile(deliveredSamples, 0.95),
				latencyP99: quantile(deliveredSamples, 0.99),
					latencyMax: deliveredSamples[deliveredSamples.length - 1] ?? NaN,
					droppedForwardsTotal,
					droppedForwardsMax,
					droppedForwardsMaxNode,
						staleForwardsDroppedTotal,
						staleForwardsDroppedMax,
						staleForwardsDroppedMaxNode,
						dataWriteDropsTotal,
						dataWriteDropsMax,
						dataWriteDropsMaxNode,
						reparentDisconnectTotal,
						reparentStaleTotal,
						reparentKickedTotal,
						treeMaxLevel,
						treeLevelP95,
						treeLevelAvg,
					treeOrphans,
					treeChildrenP95,
					treeChildrenMax,
					treeRootChildren,
					maxUploadBps,
					maxUploadFracPct,
					maxUploadNode,
					streamQueuedBytesTotal,
					streamQueuedBytesMax,
					streamQueuedBytesP95,
					streamQueuedBytesMaxNode,
						streamQueuedBytesByLane,
						churnEvents,
						churnedPeersTotal,
						maintDurationMs,
						maintSamples,
						maintMaxOrphans,
						maintOrphanArea,
						maintRecoveryCount,
						maintRecoveryP50Ms,
						maintRecoveryP95Ms,
						maintReparentsPerMin,
						maintMaxReparentsPerPeer,
						maintLevelP95DriftMax,
						maintChildrenP95DriftMax,
						overheadFactorData,
						controlBpp,
						trackerBpp,
					repairBpp,
					earningsTotal,
					earningsRelayCount,
				earningsRelayP50,
				earningsRelayP95,
				earningsRelayMax,
				protocolControlSends,
				protocolControlBytesSent,
				protocolControlBytesSentJoin,
				protocolControlBytesSentRepair,
				protocolControlBytesSentTracker,
				protocolControlReceives,
				protocolControlBytesReceived,
				protocolDataSends,
				protocolDataPayloadBytesSent,
				protocolDataReceives,
				protocolDataPayloadBytesReceived,
				protocolRepairReqSent,
				protocolFetchReqSent,
				protocolIHaveSent,
				protocolTrackerFeedbackSent,
				protocolCacheHitsServed,
				protocolHoleFillsFromNeighbor,
				protocolRouteCacheHits,
				protocolRouteCacheMisses,
				protocolRouteCacheExpirations,
				protocolRouteCacheEvictions,
				protocolRouteProxyQueries,
				protocolRouteProxyTimeouts,
				protocolRouteProxyFanout,
				network: session.network.metrics,
			};
		};

			const result = await run();

			if (profileEnabled && profileCpuStart) {
				try {
					profileEld?.disable();
				} catch {
					// ignore
				}
				const cpu = process.cpuUsage(profileCpuStart);
				const mem = process.memoryUsage();
				const p95 = profileEld ? profileEld.percentile(95) / 1e6 : 0;
				const max = profileEld ? profileEld.max / 1e6 : 0;
				result.profile = {
					cpuUserMs: cpu.user / 1_000,
					cpuSystemMs: cpu.system / 1_000,
					rssMb: mem.rss / 1e6,
					heapUsedMb: mem.heapUsed / 1e6,
					eventLoopDelayP95Ms: p95,
					eventLoopDelayMaxMs: max,
				};
			}

			return result;
		} finally {
			if (timer) clearTimeout(timer);
			try {
				profileEld?.disable();
			} catch {
				// ignore
			}
			try {
				await session.stop();
			} catch {
				// ignore teardown aborts in the shim
		}
	}
};
