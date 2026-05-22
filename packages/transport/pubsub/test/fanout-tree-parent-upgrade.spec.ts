import { expect } from "chai";
import { FanoutTree } from "../src/index.js";

const JOIN_REJECT_NO_CAPACITY = 2;
const TRACKER_FEEDBACK_JOIN_REJECT = 4;
const PARENT_PROBE_FLAG_ACCEPTING = 1 << 1;

type ImproveCandidate = {
	hash: string;
	addrs: [];
	level: number;
	freeSlots: number;
	bidPerByte: number;
};

type ImproveChannel = {
	parent: string;
	closed: boolean;
	isRoot: boolean;
	level: number;
	id: { root: string; key: Uint8Array; suffixKey?: string };
	metrics: {
		reparentUpgradeSkipCandidateLevel: number;
		reparentUpgradeSkipCandidateSlots: number;
		reparentUpgradeSkipCandidatePressure: number;
		reparentUpgradeSkipRootPressure: number;
		reparentUpgradeSkipProbeNoReply: number;
		reparentUpgradeSkipProbeNotRooted: number;
		reparentUpgradeSkipProbeRepair: number;
		reparentUpgradeSkipProbeLag: number;
		reparentUpgradeSkipProbeOverloaded: number;
		reparentUpgradeSkipProbeCooldown: number;
		parentShadowStart: number;
		parentShadowObserve: number;
		parentShadowPromote: number;
		parentShadowReset: number;
		parentShadowRejectNoReply: number;
		parentShadowRejectNotRooted: number;
		parentShadowRejectCapacity: number;
		parentShadowRejectRepair: number;
		parentShadowRejectLag: number;
		parentShadowRejectOverloaded: number;
		parentShadowRejectLevel: number;
	};
	cachedTrackerCandidates: ImproveCandidate[];
	children: Map<string, { bidPerByte: number }>;
	parentProbeRejectUntilByHash: Map<string, number>;
	parentProbeRejectBackoffMsByHash: Map<string, number>;
	parentShadow?: {
		hash: string;
		startedAt: number;
		observations: number;
		level: number;
		freeSlots: number;
		haveToExclusive: number;
	};
	missingSeqs: Set<number>;
	endSeqExclusive: number;
	nextExpectedSeq: number;
	maxSeqSeen: number;
	parentUpgradeLastAt: number;
	parentUpgradeBackoffMs: number;
	parentUpgradeBackoffUntil: number;
	parentUpgradeStaleRootProbeRound: number;
	parentUpgradeTrackerNoCapacityUntil: number;
	parentDataLatencySamples?: number;
	parentDataLatencyEwmaMs?: number;
	parentDataLatencyMaxMs?: number;
};

type ImproveOptions = {
	signal: AbortSignal;
	candidateShuffleTopK: number;
	candidateScoringMode: "ranked-shuffle" | "ranked-strict" | "weighted";
	candidateScoringWeights: {
		level: number;
		freeSlots: number;
		connected: number;
		bidPerByte: number;
		source: number;
	};
	joinAttemptsPerRound: number;
	joinReqTimeoutMs: number;
	leafOnly: boolean;
	minLevelGain: number;
	rootMinLevelGain?: number;
	rootMinSubtreeGain?: number;
	nonRootMinLevelGain?: number;
	minFreeSlots: number;
	rootMinFreeSlots?: number;
	maxChildLoadRatio?: number;
	rootMaxChildLoadRatio?: number;
	mode?: "direct" | "probe" | "shadow";
	trackerPeers?: string[];
	verifyStaleRootCapacity?: boolean;
	staleRootProbeProbability?: number;
	probeTimeoutMs?: number;
	probeMaxPerRound?: number;
	probeMaxLagMessages?: number;
	probeRejectCooldownMs?: number;
	probeRejectCooldownMaxMs?: number;
	shadowObserveMs?: number;
	shadowMinObservations?: number;
	failedBackoffMinMs?: number;
	failedBackoffMaxMs?: number;
};

type ImproveContext = {
	publicKeyHash: string;
	channelsBySuffixKey?: Map<string, ImproveChannel>;
	peers: Map<
		string,
		{ peerId: string; isReadable: boolean; isWritable: boolean }
	>;
	components: {
		connectionManager: {
			getConnections: (peerId: string) => unknown[];
		};
	};
	random: () => number;
	tryJoinOnce: (
		ch: ImproveChannel,
		parentHash: string,
		reqId: number,
		joinReqTimeoutMs: number,
		signal: AbortSignal,
		options?: {
			allowReplace?: boolean;
			parentUpgradeReservationToken?: number;
		},
	) => Promise<{ ok: boolean }>;
	probeParentCandidate: (
		ch: ImproveChannel,
		parentHash: string,
		timeoutMs: number,
		signal: AbortSignal,
		minFreeSlots?: number,
		reserveRootCapacity?: boolean,
	) => Promise<
		| {
				hash: string;
				rooted: boolean;
				accepting: boolean;
				repairing: boolean;
				overloaded: boolean;
				reservationToken: number;
				level: number;
				maxChildren: number;
				freeSlots: number;
				children: number;
				haveToExclusive: number;
				missingSeqs: number;
				dataWriteDrops: number;
				droppedForwards: number;
		  }
		| undefined
	>;
	queryTrackers: (
		ch: ImproveChannel,
		trackerPeers: string[],
		limit: number,
		timeoutMs: number,
		signal: AbortSignal,
	) => Promise<ImproveCandidate[]>;
	sendTrackerFeedback: (
		ch: ImproveChannel,
		trackerPeers: string[],
		candidateHash: string,
		event: number,
		reason?: number,
	) => Promise<void>;
	_sendControl: (to: string, payload: Uint8Array) => Promise<void>;
};

type ImproveTrackerFeedback = {
	trackerPeers: string[];
	candidateHash: string;
	event: number;
	reason: number;
};

type ImproveProbeReply = NonNullable<
	Awaited<ReturnType<ImproveContext["probeParentCandidate"]>>
>;

const createProbeReply = (
	hash: string,
	overrides: Partial<ImproveProbeReply> = {},
): ImproveProbeReply => ({
	hash,
	rooted: true,
	accepting: true,
	repairing: false,
	overloaded: false,
	reservationToken: 0,
	level: 0,
	maxChildren: 8,
	freeSlots: 4,
	children: 0,
	haveToExclusive: 0,
	missingSeqs: 0,
	dataWriteDrops: 0,
	droppedForwards: 0,
	...overrides,
});

const maybeImproveParent = Reflect.get(
	FanoutTree.prototype,
	"maybeImproveParent",
) as (
	this: ImproveContext,
	ch: ImproveChannel,
	options: ImproveOptions,
) => Promise<boolean>;

const stableUnitInterval = (input: string) => {
	let hash = 0x811c9dc5;
	for (let i = 0; i < input.length; i++) {
		hash ^= input.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0) / 0x100000000;
};

const sampledMultiChannelLeafSuffix = (
	rootHash: string,
	selfHash: string,
	maxSeqSeen: number,
	probability: number,
) => {
	for (let i = 0; i < 10_000; i++) {
		const suffixKey = `sampled-topic-${i}`;
		if (
			stableUnitInterval(
				`${suffixKey}:${selfHash}:${rootHash}:multi-channel-leaf-root-signal:${maxSeqSeen}`,
			) < probability
		) {
			return suffixKey;
		}
	}
	throw new Error("unable to find sampled suffix key");
};

const encodeParentProbeReplyForChannel = Reflect.get(
	FanoutTree.prototype,
	"encodeParentProbeReplyForChannel",
) as (
	this: any,
	ch: any,
	reqId: number,
	toHash: string,
	minFreeSlots?: number,
	reserveRootCapacity?: boolean,
) => Uint8Array;

const parentUpgradeReservationHelpers = {
	isRootedForParentProbe: Reflect.get(
		FanoutTree.prototype,
		"isRootedForParentProbe",
	),
	pruneParentUpgradeReservations: Reflect.get(
		FanoutTree.prototype,
		"pruneParentUpgradeReservations",
	),
	parentUpgradeReservationCount: Reflect.get(
		FanoutTree.prototype,
		"parentUpgradeReservationCount",
	),
	createParentUpgradeReservation: Reflect.get(
		FanoutTree.prototype,
		"createParentUpgradeReservation",
	),
	consumeParentUpgradeReservation: Reflect.get(
		FanoutTree.prototype,
		"consumeParentUpgradeReservation",
	),
};

const readTestU16BE = (buf: Uint8Array, offset: number) =>
	((buf[offset]! << 8) | buf[offset + 1]!) >>> 0;

const readTestU32BE = (buf: Uint8Array, offset: number) =>
	((buf[offset]! << 24) |
		(buf[offset + 1]! << 16) |
		(buf[offset + 2]! << 8) |
		buf[offset + 3]!) >>>
	0;

const createRootReservationMetrics = () => ({
	dataWriteDrops: 0,
	parentUpgradeRootReservationCreated: 0,
	parentUpgradeRootReservationConsumed: 0,
	parentUpgradeRootReservationRejected: 0,
	parentUpgradeRootReservationMarginRejected: 0,
	parentUpgradeRootReservationBlocked: 0,
	parentUpgradeRootReservationExpired: 0,
});

const createRootReservationChannel = (overrides: Record<string, any> = {}) => ({
	id: { key: new Uint8Array(32), root: "root" },
	isRoot: true,
	level: 0,
	children: new Map<string, unknown>(),
	effectiveMaxChildren: 1,
	parentUpgradeReservationsByHash: new Map<string, unknown>(),
	metrics: createRootReservationMetrics(),
	missingSeqs: new Set<number>(),
	overloadStreak: 0,
	maxSeqSeen: -1,
	droppedForwards: 0,
	...overrides,
});

const createParentUpgradeReservationContext = (random: () => number) => ({
	...parentUpgradeReservationHelpers,
	random,
	pruneDisconnectedChildren: () => {},
});

type ParentUpgradeGateChannel = {
	children: Map<string, unknown>;
	missingSeqs: Set<number>;
	endSeqExclusive: number;
	parentUpgradeCount: number;
	parentUpgradeLastAt: number;
	parentUpgradeBackoffUntil: number;
	lastParentDataAt: number;
	lastRepairSentAt: number;
};

type ParentUpgradeGateOptions = {
	leafOnly: boolean;
	repairGuard: boolean;
	dataGuard: boolean;
	endedAndComplete: boolean;
	maxPerPeer: number;
	cooldownMs: number;
	quietMs: number;
	repairQuietMs: number;
	now: number;
};

const evaluateParentUpgradeGate = Reflect.get(
	FanoutTree.prototype,
	"evaluateParentUpgradeGate",
) as (
	this: unknown,
	ch: ParentUpgradeGateChannel,
	options: ParentUpgradeGateOptions,
) => { run: true } | { run: false; reason: string };

const createImproveChannel = (
	overrides: Partial<ImproveChannel> = {},
): ImproveChannel => ({
	parent: "relay",
	closed: false,
	isRoot: false,
	level: 4,
	id: { root: "root", key: new Uint8Array([1, 2, 3]) },
	metrics: {
		reparentUpgradeSkipCandidateLevel: 0,
		reparentUpgradeSkipCandidateSlots: 0,
		reparentUpgradeSkipCandidatePressure: 0,
		reparentUpgradeSkipRootPressure: 0,
		reparentUpgradeSkipProbeNoReply: 0,
		reparentUpgradeSkipProbeNotRooted: 0,
		reparentUpgradeSkipProbeRepair: 0,
		reparentUpgradeSkipProbeLag: 0,
		reparentUpgradeSkipProbeOverloaded: 0,
		reparentUpgradeSkipProbeCooldown: 0,
		parentShadowStart: 0,
		parentShadowObserve: 0,
		parentShadowPromote: 0,
		parentShadowReset: 0,
		parentShadowRejectNoReply: 0,
		parentShadowRejectNotRooted: 0,
		parentShadowRejectCapacity: 0,
		parentShadowRejectRepair: 0,
		parentShadowRejectLag: 0,
		parentShadowRejectOverloaded: 0,
		parentShadowRejectLevel: 0,
	},
	cachedTrackerCandidates: [],
	children: new Map(),
	parentProbeRejectUntilByHash: new Map(),
	parentProbeRejectBackoffMsByHash: new Map(),
	missingSeqs: new Set(),
	endSeqExclusive: -1,
	nextExpectedSeq: 0,
	maxSeqSeen: -1,
	parentUpgradeLastAt: 0,
	parentUpgradeBackoffMs: 0,
	parentUpgradeBackoffUntil: 0,
	parentUpgradeStaleRootProbeRound: 0,
	parentUpgradeTrackerNoCapacityUntil: 0,
	...overrides,
});

const createParentUpgradeGateChannel = (
	overrides: Partial<ParentUpgradeGateChannel> = {},
): ParentUpgradeGateChannel => ({
	children: new Map(),
	missingSeqs: new Set(),
	endSeqExclusive: -1,
	parentUpgradeCount: 0,
	parentUpgradeLastAt: 0,
	parentUpgradeBackoffUntil: 0,
	lastParentDataAt: 0,
	lastRepairSentAt: 0,
	...overrides,
});

const runParentUpgradeGate = (
	channelOverrides: Partial<ParentUpgradeGateChannel> = {},
	optionOverrides: Partial<ParentUpgradeGateOptions> = {},
) =>
	evaluateParentUpgradeGate.call(
		{},
		createParentUpgradeGateChannel(channelOverrides),
		{
			leafOnly: true,
			repairGuard: true,
			dataGuard: true,
			endedAndComplete: false,
			maxPerPeer: 2,
			cooldownMs: 5_000,
			quietMs: 1_000,
			repairQuietMs: 1_000,
			now: 10_000,
			...optionOverrides,
		},
	);

const runMaybeImproveParent = async (args: {
	peerHashes: string[];
	cachedTrackerCandidates?: ImproveCandidate[];
	channel?: ImproveChannel;
	channelOverrides?: Partial<ImproveChannel>;
	getConnections?: (peerId: string) => unknown[];
	random?: () => number;
	channelsBySuffixKey?: Map<string, ImproveChannel>;
	options?: Partial<ImproveOptions>;
	tryJoinOnce?: ImproveContext["tryJoinOnce"];
	probeParentCandidate?: ImproveContext["probeParentCandidate"];
	queryTrackers?: ImproveContext["queryTrackers"];
}) => {
	const attempts: string[] = [];
	const feedback: ImproveTrackerFeedback[] = [];
	const ch =
		args.channel ??
		createImproveChannel({
			cachedTrackerCandidates: args.cachedTrackerCandidates ?? [],
			...args.channelOverrides,
		});
	if (args.channel) {
		ch.cachedTrackerCandidates =
			args.cachedTrackerCandidates ?? ch.cachedTrackerCandidates;
		Object.assign(ch, args.channelOverrides ?? {});
	}
	const ctx: ImproveContext = {
		publicKeyHash: "self",
		channelsBySuffixKey: args.channelsBySuffixKey,
		peers: new Map(
			args.peerHashes.map((hash) => [
				hash,
				{
					peerId: hash,
					isReadable: true,
					isWritable: false,
				},
			]),
		),
		components: {
			connectionManager: {
				getConnections:
					args.getConnections ??
					((peerId) => (args.peerHashes.includes(peerId) ? [{}] : [])),
			},
		},
		random: args.random ?? (() => 0),
		tryJoinOnce: async (
			channel,
			parentHash,
			reqId,
			joinReqTimeoutMs,
			signal,
			options,
		) => {
			attempts.push(parentHash);
			return (
				args.tryJoinOnce?.(
					channel,
					parentHash,
					reqId,
					joinReqTimeoutMs,
					signal,
					options,
				) ?? { ok: false }
			);
		},
		probeParentCandidate:
			args.probeParentCandidate ??
			(async (_channel, parentHash) => createProbeReply(parentHash)),
		queryTrackers: args.queryTrackers ?? (async () => []),
		sendTrackerFeedback: async (
			_channel,
			trackerPeers,
			candidateHash,
			event,
			reason = 0,
		) => {
			feedback.push({
				trackerPeers: [...trackerPeers],
				candidateHash,
				event,
				reason,
			});
		},
		_sendControl: async () => {},
	};

	const result = await maybeImproveParent.call(ctx, ch, {
		signal: new AbortController().signal,
		candidateShuffleTopK: 0,
		candidateScoringMode: "ranked-strict",
		candidateScoringWeights: {
			level: 1,
			freeSlots: 1,
			connected: 1,
			bidPerByte: 1,
			source: 1,
		},
		joinAttemptsPerRound: 8,
		joinReqTimeoutMs: 1_000,
		leafOnly: false,
		minLevelGain: 1,
		minFreeSlots: 1,
		failedBackoffMinMs: 5_000,
		failedBackoffMaxMs: 60_000,
		...args.options,
	});

	return { attempts, result, ch, feedback };
};

describe("fanout-tree parent upgrades", () => {
	it("falls back to peer readability when connection lookup throws", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["root", "relay"],
			getConnections: () => {
				throw new Error("boom");
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(true);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
	});

	it("orders ranked candidates by free slots, bid, source and hash", async () => {
		const byFreeSlots = await runMaybeImproveParent({
			peerHashes: ["relay", "slot-low", "slot-high"],
			cachedTrackerCandidates: [
				{ hash: "slot-low", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "slot-high", addrs: [], level: 1, freeSlots: 2, bidPerByte: 0 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byFreeSlots.attempts[0]).to.equal("slot-high");

		const byBid = await runMaybeImproveParent({
			peerHashes: ["relay", "bid-low", "bid-high"],
			cachedTrackerCandidates: [
				{ hash: "bid-low", addrs: [], level: 1, freeSlots: 2, bidPerByte: 1 },
				{ hash: "bid-high", addrs: [], level: 1, freeSlots: 2, bidPerByte: 2 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byBid.attempts[0]).to.equal("bid-high");

		const bySource = await runMaybeImproveParent({
			peerHashes: ["root", "relay", "tracker-root"],
			cachedTrackerCandidates: [
				{
					hash: "tracker-root",
					addrs: [],
					level: 0,
					freeSlots: 1,
					bidPerByte: 0,
				},
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(bySource.attempts[0]).to.equal("root");

		const byHash = await runMaybeImproveParent({
			peerHashes: ["relay", "alpha", "beta"],
			cachedTrackerCandidates: [
				{ hash: "beta", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "alpha", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byHash.attempts[0]).to.equal("alpha");
	});

	it("shuffles top ranked candidates when parent upgrades use ranked-shuffle", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "alpha", "gamma"],
			cachedTrackerCandidates: [
				{ hash: "alpha", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "gamma", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
			],
			options: {
				candidateScoringMode: "ranked-shuffle",
				candidateShuffleTopK: 2,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts.slice(0, 2)).to.deep.equal(["gamma", "alpha"]);
	});

	it("supports weighted parent-upgrade candidate selection", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "weighted-a", "weighted-b"],
			cachedTrackerCandidates: [
				{
					hash: "weighted-a",
					addrs: [],
					level: 1,
					freeSlots: 4,
					bidPerByte: 1,
				},
				{
					hash: "weighted-b",
					addrs: [],
					level: 2,
					freeSlots: 1,
					bidPerByte: 0,
				},
			],
			random: () => 0.999,
			options: {
				candidateScoringMode: "weighted",
				candidateShuffleTopK: 2,
				nonRootMinLevelGain: 1,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.have.length(2);
		expect(new Set(attempts)).to.deep.equal(
			new Set(["weighted-a", "weighted-b"]),
		);
	});

	it("falls back to the existing weighted order when all candidate weights collapse", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "nan-a", "nan-b"],
			cachedTrackerCandidates: [
				{ hash: "nan-a", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "nan-b", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
			],
			options: {
				candidateScoringMode: "weighted",
				candidateShuffleTopK: 2,
				candidateScoringWeights: {
					level: 0,
					freeSlots: 0,
					connected: 0,
					bidPerByte: 0,
					source: 0,
				},
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts.slice(0, 2)).to.deep.equal(["nan-a", "nan-b"]);
	});

	it("requires the configured minimum level gain for parent upgrades", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "marginal"],
			channelOverrides: {
				level: 3,
			},
			cachedTrackerCandidates: [
				{ hash: "marginal", addrs: [], level: 1, freeSlots: 4, bidPerByte: 0 },
			],
			options: {
				minLevelGain: 2,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateLevel).to.equal(1);
	});

	it("requires stronger default level gain for non-root upgrade targets", async () => {
		const relay = await runMaybeImproveParent({
			peerHashes: ["relay", "marginal-relay"],
			channelOverrides: {
				level: 3,
			},
			cachedTrackerCandidates: [
				{
					hash: "marginal-relay",
					addrs: [],
					level: 1,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(relay.result).to.equal(false);
		expect(relay.attempts).to.deep.equal([]);
		expect(relay.ch.metrics.reparentUpgradeSkipCandidateLevel).to.equal(1);

		const root = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: {
				level: 2,
			},
			options: {
				rootMinLevelGain: 1,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(root.result).to.equal(true);
		expect(root.attempts).to.deep.equal(["root"]);
		expect(root.ch.parent).to.equal("root");
	});

	it("requires stronger default level gain for direct-root upgrades", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: {
				level: 3,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateLevel).to.equal(1);
	});

	it("can admit direct-root upgrades by local subtree gain", async () => {
		const root = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: {
				level: 2,
				children: new Map([
					["child-a", { bidPerByte: 0 }],
					["child-b", { bidPerByte: 0 }],
				]),
			},
			options: {
				rootMinLevelGain: 3,
				rootMinSubtreeGain: 3,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(root.result).to.equal(true);
		expect(root.attempts).to.deep.equal(["root"]);
		expect(root.ch.parent).to.equal("root");

		const leaf = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: {
				level: 2,
			},
			options: {
				rootMinLevelGain: 3,
				rootMinSubtreeGain: 3,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(leaf.result).to.equal(false);
		expect(leaf.attempts).to.deep.equal([]);
		expect(leaf.ch.metrics.reparentUpgradeSkipCandidateLevel).to.equal(1);
	});

	it("requires advertised free slots for parent upgrades", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "full-parent"],
			cachedTrackerCandidates: [
				{
					hash: "full-parent",
					addrs: [],
					level: 0,
					freeSlots: 0,
					bidPerByte: 0,
				},
			],
			options: {
				minFreeSlots: 1,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
		expect(ch.cachedTrackerCandidates[0]?.freeSlots).to.equal(0);
	});

	it("can require a wider spare-slot margin for direct-root upgrades", async () => {
		const root = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: { level: 4 },
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 4, bidPerByte: 0 },
			],
			options: {
				minFreeSlots: 1,
				rootMinFreeSlots: 8,
				rootMinLevelGain: 1,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(root.result).to.equal(false);
		expect(root.attempts).to.deep.equal([]);
		expect(root.ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);

		const relay = await runMaybeImproveParent({
			peerHashes: ["relay", "spare-relay"],
			channelOverrides: { level: 4 },
			cachedTrackerCandidates: [
				{
					hash: "spare-relay",
					addrs: [],
					level: 0,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
			options: {
				minFreeSlots: 1,
				rootMinFreeSlots: 8,
				nonRootMinLevelGain: 1,
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(relay.result).to.equal(true);
		expect(relay.attempts).to.deep.equal(["spare-relay"]);
		expect(relay.ch.parent).to.equal("spare-relay");
	});

	it("does not invent root capacity when the tracker says root is full", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["root", "relay"],
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("probes live parent state before upgrading in probe mode", async () => {
		const probes: string[] = [];
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "probe-parent"],
			channelOverrides: { maxSeqSeen: 5 },
			cachedTrackerCandidates: [
				{
					hash: "probe-parent",
					addrs: [],
					level: 1,
					freeSlots: 1,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "probe",
				probeMaxPerRound: 1,
				probeMaxLagMessages: 0,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes.push(parentHash);
				return {
					hash: parentHash,
					rooted: true,
					accepting: true,
					repairing: false,
					overloaded: false,
					level: 0,
					maxChildren: 8,
					freeSlots: 2,
					children: 1,
					haveToExclusive: 6,
					missingSeqs: 0,
					dataWriteDrops: 0,
					droppedForwards: 0,
					reservationToken: 0,
				};
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(true);
		expect(probes).to.deep.equal(["probe-parent"]);
		expect(attempts).to.deep.equal(["probe-parent"]);
		expect(ch.parent).to.equal("probe-parent");
	});

	it("passes live root probe reservations into proactive root joins", async () => {
		let joinReservationToken = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: { level: 4 },
			options: {
				mode: "probe",
				rootMinLevelGain: 1,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					level: 0,
					reservationToken: 0x12345678,
				}),
			tryJoinOnce: async (
				channel,
				parentHash,
				_reqId,
				_timeout,
				_signal,
				options,
			) => {
				joinReservationToken = options?.parentUpgradeReservationToken ?? 0;
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(true);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
		expect(joinReservationToken).to.equal(0x12345678);
	});

	it("reserves scarce root capacity while replying to parent probes", async () => {
		const ch = createRootReservationChannel();
		let randomValue = 0.25;
		const ctx = createParentUpgradeReservationContext(() => randomValue);

		const first = encodeParentProbeReplyForChannel.call(ctx, ch, 1, "leaf-a");
		const firstToken = readTestU32BE(first, 60);
		expect(first[37]! & PARENT_PROBE_FLAG_ACCEPTING).to.not.equal(0);
		expect(readTestU16BE(first, 42)).to.equal(1);
		expect(firstToken).to.not.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationCreated).to.equal(1);

		randomValue = 0.5;
		const second = encodeParentProbeReplyForChannel.call(ctx, ch, 2, "leaf-b");
		expect(second[37]! & PARENT_PROBE_FLAG_ACCEPTING).to.equal(0);
		expect(readTestU16BE(second, 42)).to.equal(0);
		expect(readTestU32BE(second, 60)).to.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationBlocked).to.equal(1);

		expect(
			parentUpgradeReservationHelpers.consumeParentUpgradeReservation.call(
				ctx,
				ch,
				"leaf-a",
				firstToken + 1,
			),
		).to.equal(false);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(1);
		expect(
			parentUpgradeReservationHelpers.consumeParentUpgradeReservation.call(
				ctx,
				ch,
				"leaf-a",
				firstToken,
			),
		).to.equal(true);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationConsumed).to.equal(1);
	});

	it("does not reserve root capacity below the requested spare-slot margin", async () => {
		const ch = createRootReservationChannel({
			children: new Map([
				["child-a", {}],
				["child-b", {}],
			]),
			effectiveMaxChildren: 12,
		});
		const ctx = createParentUpgradeReservationContext(() => 0.25);

		const first = encodeParentProbeReplyForChannel.call(
			ctx,
			ch,
			1,
			"leaf-a",
			10,
		);
		expect(first[37]! & PARENT_PROBE_FLAG_ACCEPTING).to.not.equal(0);
		expect(readTestU16BE(first, 42)).to.equal(10);
		expect(readTestU32BE(first, 60)).to.not.equal(0);

		const second = encodeParentProbeReplyForChannel.call(
			ctx,
			ch,
			2,
			"leaf-b",
			10,
		);
		expect(second[37]! & PARENT_PROBE_FLAG_ACCEPTING).to.equal(0);
		expect(readTestU16BE(second, 42)).to.equal(9);
		expect(readTestU32BE(second, 60)).to.equal(0);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(1);
		expect(ch.metrics.parentUpgradeRootReservationCreated).to.equal(1);
		expect(ch.metrics.parentUpgradeRootReservationBlocked).to.equal(1);
	});

	it("reports pending root reservations as probe child pressure", async () => {
		const ch = createRootReservationChannel({
			children: new Map([
				["child-a", {}],
				["child-b", {}],
			]),
			effectiveMaxChildren: 12,
		});
		let randomValue = 0.25;
		const ctx = createParentUpgradeReservationContext(() => randomValue);

		const first = encodeParentProbeReplyForChannel.call(ctx, ch, 1, "leaf-a");
		expect(readTestU16BE(first, 44)).to.equal(2);
		expect(readTestU32BE(first, 60)).to.not.equal(0);

		randomValue = 0.5;
		const second = encodeParentProbeReplyForChannel.call(ctx, ch, 2, "leaf-b");
		expect(readTestU16BE(second, 44)).to.equal(3);
		expect(readTestU32BE(second, 60)).to.not.equal(0);
	});

	it("rejects root reservation tokens after the spare-slot margin evaporates", async () => {
		const ch = createRootReservationChannel({
			children: new Map(
				Array.from({ length: 4 }, (_value, index) => [`child-${index}`, {}]),
			),
			effectiveMaxChildren: 12,
		});
		const ctx = createParentUpgradeReservationContext(() => 0.25);

		const reply = encodeParentProbeReplyForChannel.call(
			ctx,
			ch,
			1,
			"leaf-a",
			8,
		);
		const token = readTestU32BE(reply, 60);
		expect(token).to.not.equal(0);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(1);

		ch.children.set("late-child", {});

		expect(
			parentUpgradeReservationHelpers.consumeParentUpgradeReservation.call(
				ctx,
				ch,
				"leaf-a",
				token,
			),
		).to.equal(false);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationConsumed).to.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationMarginRejected).to.equal(1);
	});

	it("can probe root capacity without reserving a root slot", async () => {
		const ch = createRootReservationChannel();
		const ctx = createParentUpgradeReservationContext(() => 0.25);

		const reply = encodeParentProbeReplyForChannel.call(
			ctx,
			ch,
			1,
			"leaf-a",
			1,
			false,
		);

		expect(reply[37]! & PARENT_PROBE_FLAG_ACCEPTING).to.not.equal(0);
		expect(readTestU16BE(reply, 42)).to.equal(1);
		expect(readTestU32BE(reply, 60)).to.equal(0);
		expect(ch.parentUpgradeReservationsByHash.size).to.equal(0);
		expect(ch.metrics.parentUpgradeRootReservationCreated).to.equal(0);
	});

	it("uses probe capacity instead of stale tracker capacity in probe mode", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "stale-parent"],
			cachedTrackerCandidates: [
				{
					hash: "stale-parent",
					addrs: [],
					level: 0,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
			options: { mode: "probe" },
			probeParentCandidate: async (_channel, parentHash) => ({
				hash: parentHash,
				rooted: true,
				accepting: false,
				repairing: false,
				overloaded: false,
				level: 0,
				maxChildren: 4,
				freeSlots: 0,
				children: 4,
				haveToExclusive: 0,
				missingSeqs: 0,
				dataWriteDrops: 0,
				droppedForwards: 0,
				reservationToken: 0,
			}),
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("feeds live probe capacity rejects back to trackers", async () => {
		const { attempts, feedback, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "full-parent"],
			cachedTrackerCandidates: [
				{
					hash: "full-parent",
					addrs: [],
					level: 0,
					freeSlots: 8,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "probe",
				minFreeSlots: 2,
				trackerPeers: ["tracker-a", "tracker-b"],
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					accepting: false,
					maxChildren: 4,
					freeSlots: 0,
					children: 4,
				}),
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(feedback).to.deep.equal([
			{
				trackerPeers: ["tracker-a", "tracker-b"],
				candidateHash: "full-parent",
				event: TRACKER_FEEDBACK_JOIN_REJECT,
				reason: JOIN_REJECT_NO_CAPACITY,
			},
		]);
	});

	it("keeps reserve-only probe rejects local", async () => {
		const { attempts, feedback, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "low-reserve-parent"],
			cachedTrackerCandidates: [
				{
					hash: "low-reserve-parent",
					addrs: [],
					level: 0,
					freeSlots: 8,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "probe",
				minFreeSlots: 4,
				trackerPeers: ["tracker-a"],
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					accepting: true,
					maxChildren: 8,
					freeSlots: 2,
					children: 6,
				}),
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(feedback).to.deep.equal([]);
	});

	it("uses live child pressure when admitting probed parents", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "busy-parent"],
			cachedTrackerCandidates: [
				{
					hash: "busy-parent",
					addrs: [],
					level: 0,
					freeSlots: 8,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "probe",
				minFreeSlots: 1,
				maxChildLoadRatio: 0.8,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					maxChildren: 10,
					freeSlots: 8,
					children: 8,
				}),
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("can apply stricter child pressure to direct-root upgrades", async () => {
		const root = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: { level: 4 },
			options: {
				mode: "probe",
				maxChildLoadRatio: 0.8,
				rootMaxChildLoadRatio: 0.25,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					level: 0,
					maxChildren: 8,
					freeSlots: 6,
					children: 2,
				}),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(root.result).to.equal(false);
		expect(root.attempts).to.deep.equal([]);
		expect(root.ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
		expect(root.ch.metrics.reparentUpgradeSkipCandidatePressure).to.equal(1);
		expect(root.ch.metrics.reparentUpgradeSkipRootPressure).to.equal(1);

		const relay = await runMaybeImproveParent({
			peerHashes: ["relay", "busy-relay"],
			channelOverrides: { level: 4 },
			cachedTrackerCandidates: [
				{
					hash: "busy-relay",
					addrs: [],
					level: 0,
					freeSlots: 6,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "probe",
				maxChildLoadRatio: 0.8,
				rootMaxChildLoadRatio: 0.25,
				nonRootMinLevelGain: 1,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					level: 0,
					maxChildren: 8,
					freeSlots: 6,
					children: 2,
				}),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(relay.result).to.equal(true);
		expect(relay.attempts).to.deep.equal(["busy-relay"]);
		expect(relay.ch.parent).to.equal("busy-relay");
	});

	it("tightens direct-root child pressure for multi-channel peers", async () => {
		const single = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channelOverrides: {
				level: 4,
				id: { root: "root", key: new Uint8Array([1, 2, 3]), suffixKey: "a" },
			},
			options: {
				mode: "probe",
				minFreeSlots: 1,
				maxChildLoadRatio: 0.5,
				rootMaxChildLoadRatio: 0.4,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					level: 0,
					maxChildren: 10,
					freeSlots: 6,
					children: 3,
				}),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(single.result).to.equal(true);
		expect(single.attempts).to.deep.equal(["root"]);

		const ch = createImproveChannel({
			level: 4,
			id: { root: "root", key: new Uint8Array([1, 2, 3]), suffixKey: "a" },
		});
		const other = createImproveChannel({
			id: {
				root: "other-root",
				key: new Uint8Array([4, 5, 6]),
				suffixKey: "b",
			},
		});
		const multi = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			channelsBySuffixKey: new Map([
				["a", ch],
				["b", other],
			]),
			options: {
				mode: "probe",
				minFreeSlots: 1,
				maxChildLoadRatio: 0.5,
				rootMaxChildLoadRatio: 0.4,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					level: 0,
					maxChildren: 10,
					freeSlots: 6,
					children: 3,
				}),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(multi.result).to.equal(false);
		expect(multi.attempts).to.deep.equal([]);
		expect(multi.ch.metrics.reparentUpgradeSkipCandidatePressure).to.equal(1);
		expect(multi.ch.metrics.reparentUpgradeSkipRootPressure).to.equal(1);
	});

	it("keeps single-probe mode bounded by stale tracker capacity", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "stale-parent"],
			cachedTrackerCandidates: [
				{
					hash: "stale-parent",
					addrs: [],
					level: 0,
					freeSlots: 0,
					bidPerByte: 0,
				},
			],
			options: { mode: "probe", minFreeSlots: 2 },
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("rejects lagging parent probes before replacing a live parent", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "lagging-parent"],
			channelOverrides: { maxSeqSeen: 9 },
			cachedTrackerCandidates: [
				{
					hash: "lagging-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
			options: { mode: "probe", probeMaxLagMessages: 0 },
			probeParentCandidate: async (_channel, parentHash) => ({
				hash: parentHash,
				rooted: true,
				accepting: true,
				repairing: false,
				overloaded: false,
				level: 0,
				maxChildren: 4,
				freeSlots: 2,
				children: 0,
				haveToExclusive: 5,
				missingSeqs: 0,
				dataWriteDrops: 0,
				droppedForwards: 0,
				reservationToken: 0,
			}),
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipProbeLag).to.equal(1);
	});

	it("cools down rejected parent probes in probe mode", async () => {
		let probes = 0;
		const first = await runMaybeImproveParent({
			peerHashes: ["relay", "full-parent"],
			cachedTrackerCandidates: [
				{
					hash: "full-parent",
					addrs: [],
					level: 0,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
			options: { mode: "probe", probeRejectCooldownMs: 60_000 },
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return {
					hash: parentHash,
					rooted: true,
					accepting: false,
					repairing: false,
					overloaded: false,
					level: 0,
					maxChildren: 4,
					freeSlots: 0,
					children: 4,
					haveToExclusive: 0,
					missingSeqs: 0,
					dataWriteDrops: 0,
					droppedForwards: 0,
					reservationToken: 0,
				};
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(first.result).to.equal(false);
		expect(probes).to.equal(1);
		expect(first.ch.parentProbeRejectUntilByHash.has("full-parent")).to.equal(
			true,
		);

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "full-parent"],
			channelOverrides: {
				parentProbeRejectUntilByHash: first.ch.parentProbeRejectUntilByHash,
				parentProbeRejectBackoffMsByHash:
					first.ch.parentProbeRejectBackoffMsByHash,
			},
			cachedTrackerCandidates: [
				{
					hash: "full-parent",
					addrs: [],
					level: 0,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
			options: { mode: "probe", probeRejectCooldownMs: 60_000 },
			probeParentCandidate: async () => {
				probes += 1;
				return undefined;
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(second.result).to.equal(false);
		expect(second.attempts).to.deep.equal([]);
		expect(probes).to.equal(1);
		expect(second.ch.metrics.reparentUpgradeSkipProbeCooldown).to.equal(1);
	});

	it("backs off repeatedly rejected probe candidates adaptively", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "silent-parent",
					addrs: [],
					level: 0,
					freeSlots: 4,
					bidPerByte: 0,
				},
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "probe",
			probeRejectCooldownMs: 100,
			probeRejectCooldownMaxMs: 250,
		};
		const probeParentCandidate: ImproveContext["probeParentCandidate"] =
			async () => undefined;

		await runMaybeImproveParent({
			peerHashes: ["relay", "silent-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});
		expect(ch.parentProbeRejectBackoffMsByHash.get("silent-parent")).to.equal(
			100,
		);

		ch.parentProbeRejectUntilByHash.set("silent-parent", 0);
		await runMaybeImproveParent({
			peerHashes: ["relay", "silent-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});
		expect(ch.parentProbeRejectBackoffMsByHash.get("silent-parent")).to.equal(
			200,
		);

		ch.parentProbeRejectUntilByHash.set("silent-parent", 0);
		await runMaybeImproveParent({
			peerHashes: ["relay", "silent-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});
		expect(ch.parentProbeRejectBackoffMsByHash.get("silent-parent")).to.equal(
			250,
		);
	});

	it("observes a shadow parent before promoting it", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "shadow-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			shadowObserveMs: 0,
			shadowMinObservations: 2,
			probeMaxPerRound: 1,
		};
		const probeParentCandidate: ImproveContext["probeParentCandidate"] = async (
			_channel,
			parentHash,
		) => ({
			hash: parentHash,
			rooted: true,
			accepting: true,
			repairing: false,
			overloaded: false,
			level: 0,
			maxChildren: 4,
			freeSlots: 2,
			children: 1,
			haveToExclusive: 0,
			missingSeqs: 0,
			dataWriteDrops: 0,
			droppedForwards: 0,
			reservationToken: 0,
		});

		const first = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate,
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});
		expect(first.result).to.equal(false);
		expect(first.attempts).to.deep.equal([]);
		expect(ch.parent).to.equal("relay");
		expect(ch.parentShadow?.hash).to.equal("shadow-parent");
		expect(ch.metrics.parentShadowStart).to.equal(1);

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate,
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(second.result).to.equal(true);
		expect(second.attempts).to.deep.equal(["shadow-parent"]);
		expect(ch.parent).to.equal("shadow-parent");
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.parentShadowObserve).to.equal(1);
		expect(ch.metrics.parentShadowPromote).to.equal(1);
	});

	it("lets shadow verify stale tracker capacity for a direct root candidate", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			minFreeSlots: 2,
			verifyStaleRootCapacity: true,
			staleRootProbeProbability: 1,
			shadowObserveMs: 0,
			shadowMinObservations: 2,
		};

		await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { freeSlots: 2 }),
		});

		expect(ch.parentShadow?.hash).to.equal("root");

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { freeSlots: 2 }),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(second.result).to.equal(true);
		expect(second.attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
		expect(ch.metrics.parentShadowPromote).to.equal(1);
	});

	it("reserves root capacity only when shadow observation can promote", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
		});
		const reserveRequests: boolean[] = [];
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			minFreeSlots: 2,
			verifyStaleRootCapacity: true,
			staleRootProbeProbability: 1,
			shadowObserveMs: 0,
			shadowMinObservations: 2,
		};
		const probeParentCandidate: ImproveContext["probeParentCandidate"] = async (
			_channel,
			parentHash,
			_timeoutMs,
			_signal,
			_minFreeSlots,
			reserveRootCapacity,
		) => {
			reserveRequests.push(reserveRootCapacity ?? true);
			return createProbeReply(parentHash, { freeSlots: 2 });
		};

		const first = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(first.result).to.equal(false);
		expect(ch.parentShadow?.hash).to.equal("root");
		expect(reserveRequests).to.deep.equal([false]);

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			options,
			probeParentCandidate,
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(second.result).to.equal(true);
		expect(second.attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
		expect(reserveRequests).to.deep.equal([false, true]);
	});

	it("promotes a completed direct-root shadow candidate after one live probe", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			endSeqExclusive: 5,
			nextExpectedSeq: 5,
			maxSeqSeen: 4,
		});
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			channel: ch,
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 1,
				shadowObserveMs: 10_000,
				shadowMinObservations: 3,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					freeSlots: 8,
					haveToExclusive: 5,
				}),
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(true);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.parentShadowStart).to.equal(1);
		expect(ch.metrics.parentShadowObserve).to.equal(0);
		expect(ch.metrics.parentShadowPromote).to.equal(1);
	});

	it("keeps stale root shadow candidates bounded by tracker capacity by default", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("samples stale root verification to avoid herd probes", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "root"],
			cachedTrackerCandidates: [
				{ hash: "root", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("honors configured sampling for multi-channel leaf root refresh", async () => {
		const rootHash = "sampled-root";
		const maxSeqSeen = 9;
		const suffixKey = sampledMultiChannelLeafSuffix(
			rootHash,
			"self",
			maxSeqSeen,
			0.015625,
		);
		let queries = 0;
		let probes = 0;
		const ch = createImproveChannel({
			id: {
				root: rootHash,
				key: new Uint8Array([1, 2, 3]),
				suffixKey,
			},
			endSeqExclusive: maxSeqSeen + 1,
			nextExpectedSeq: maxSeqSeen + 1,
			maxSeqSeen,
			parentDataLatencySamples: 8,
			parentDataLatencyEwmaMs: 128,
			parentDataLatencyMaxMs: 300,
		});
		const other = createImproveChannel({
			id: {
				root: "other-root",
				key: new Uint8Array([4, 5, 6]),
				suffixKey: "other-topic",
			},
		});
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", rootHash, "tracker"],
			channel: ch,
			channelsBySuffixKey: new Map([
				[suffixKey, ch],
				["other-topic", other],
			]),
			cachedTrackerCandidates: [
				{ hash: rootHash, addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				leafOnly: true,
				minFreeSlots: 2,
				trackerPeers: ["tracker"],
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			queryTrackers: async () => {
				queries += 1;
				return [
					{ hash: rootHash, addrs: [], level: 0, freeSlots: 2, bidPerByte: 0 },
				];
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(queries).to.equal(0);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
	});

	it("keeps leaf stale-root sampling below the branch boost", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "b"],
			channelOverrides: {
				id: { root: "b", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
			},
			cachedTrackerCandidates: [
				{ hash: "b", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0.0625,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("boosts stale-root sampling for branch peers with downstream benefit", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "x5"],
			channelOverrides: {
				children: new Map([["child", { bidPerByte: 0 }]]),
				id: { root: "x5", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
			},
			cachedTrackerCandidates: [
				{ hash: "x5", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0.0625,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(1);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow?.hash).to.equal("x5");
	});

	it("lets single-channel branches spend the wider stale-root sample budget", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "x5"],
			channelOverrides: {
				children: new Map([["child", { bidPerByte: 0 }]]),
				id: { root: "x5", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
			},
			cachedTrackerCandidates: [
				{ hash: "x5", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0.0625,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(1);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow?.hash).to.equal("x5");
	});

	it("raises stale root sampling for quiet single-channel leaves", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "x3"],
			channelOverrides: {
				parent: "relay",
				id: { root: "x3", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
				endSeqExclusive: 10,
				nextExpectedSeq: 10,
				maxSeqSeen: 9,
			},
			cachedTrackerCandidates: [
				{ hash: "x3", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0.03125,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, {
					freeSlots: 2,
					haveToExclusive: 10,
				});
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(1);
		expect(attempts).to.deep.equal(["x3"]);
		expect(ch.parentShadow?.hash).to.equal("x3");
	});

	it("does not boost stale-root sampling when multiple local channels compete", async () => {
		let probes = 0;
		const ch = createImproveChannel({
			children: new Map([["child", { bidPerByte: 0 }]]),
			id: { root: "b", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
		});
		const other = createImproveChannel({
			id: {
				root: "other-root",
				key: new Uint8Array([4, 5, 6]),
				suffixKey: "other-topic",
			},
		});
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "b"],
			channel: ch,
			channelsBySuffixKey: new Map([
				["topic", ch],
				["other-topic", other],
			]),
			cachedTrackerCandidates: [
				{ hash: "b", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				verifyStaleRootCapacity: true,
				staleRootProbeProbability: 0.0625,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow).to.equal(undefined);
	});

	it("rotates stale-root sampling for multi-channel peers over settled rounds", async () => {
		let probes = 0;
		const ch = createImproveChannel({
			children: new Map([["child", { bidPerByte: 0 }]]),
			id: { root: "x444", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
		});
		const other = createImproveChannel({
			id: {
				root: "other-root",
				key: new Uint8Array([4, 5, 6]),
				suffixKey: "other-topic",
			},
		});
		const run = () =>
			runMaybeImproveParent({
				peerHashes: ["relay", "x444"],
				channel: ch,
				channelsBySuffixKey: new Map([
					["topic", ch],
					["other-topic", other],
				]),
				cachedTrackerCandidates: [
					{ hash: "x444", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
				],
				options: {
					mode: "shadow",
					minFreeSlots: 2,
					verifyStaleRootCapacity: true,
					staleRootProbeProbability: 0.03125,
					shadowObserveMs: 0,
					shadowMinObservations: 2,
				},
				probeParentCandidate: async (_channel, parentHash) => {
					probes += 1;
					return createProbeReply(parentHash, { freeSlots: 2 });
				},
			});

		await run();
		await run();
		await run();
		const { attempts, result } = await run();

		expect(result).to.equal(false);
		expect(probes).to.equal(1);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow?.hash).to.equal("x444");
		expect(ch.parentUpgradeStaleRootProbeRound).to.equal(4);
	});

	it("keeps stale non-root shadow candidates bounded by tracker capacity", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "stale-parent"],
			cachedTrackerCandidates: [
				{
					hash: "stale-parent",
					addrs: [],
					level: 0,
					freeSlots: 0,
					bidPerByte: 0,
				},
			],
			options: {
				mode: "shadow",
				minFreeSlots: 2,
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) => {
				probes += 1;
				return createProbeReply(parentHash, { freeSlots: 2 });
			},
		});

		expect(result).to.equal(false);
		expect(probes).to.equal(0);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
	});

	it("resets a shadow parent when capacity disappears before promotion", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "shadow-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			shadowObserveMs: 0,
			shadowMinObservations: 2,
		};

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { freeSlots: 2 }),
		});

		expect(ch.parentShadow?.hash).to.equal("shadow-parent");

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, {
					accepting: false,
					freeSlots: 0,
					children: 4,
				}),
		});

		expect(second.result).to.equal(false);
		expect(second.attempts).to.deep.equal([]);
		expect(ch.parent).to.equal("relay");
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.parentShadowReset).to.equal(1);
		expect(ch.metrics.parentShadowRejectCapacity).to.equal(1);
		expect(ch.metrics.reparentUpgradeSkipCandidateSlots).to.equal(1);
		expect(ch.parentUpgradeBackoffMs).to.be.greaterThan(0);
		expect(ch.parentUpgradeBackoffUntil).to.be.greaterThan(Date.now());
	});

	it("backs off failed shadow probe rounds adaptively", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "shadow-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			minFreeSlots: 2,
			probeRejectCooldownMs: 0,
			failedBackoffMinMs: 100,
			failedBackoffMaxMs: 250,
		};
		const probeParentCandidate: ImproveContext["probeParentCandidate"] = async (
			_channel,
			parentHash,
		) => createProbeReply(parentHash, { accepting: false, freeSlots: 0 });

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(ch.parentUpgradeBackoffMs).to.equal(100);

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(ch.parentUpgradeBackoffMs).to.equal(200);

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(ch.parentUpgradeBackoffMs).to.equal(250);
	});

	it("backs off failed probe rounds adaptively", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "probe-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "probe",
			minFreeSlots: 2,
			probeRejectCooldownMs: 0,
			failedBackoffMinMs: 100,
			failedBackoffMaxMs: 250,
		};
		const probeParentCandidate: ImproveContext["probeParentCandidate"] = async (
			_channel,
			parentHash,
		) => createProbeReply(parentHash, { accepting: false, freeSlots: 0 });

		await runMaybeImproveParent({
			peerHashes: ["relay", "probe-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(ch.parentUpgradeBackoffMs).to.equal(100);

		await runMaybeImproveParent({
			peerHashes: ["relay", "probe-parent"],
			channel: ch,
			options,
			probeParentCandidate,
		});

		expect(ch.parentUpgradeBackoffMs).to.equal(200);
	});

	it("resets a shadow parent when the candidate lags the child", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "shadow-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
			maxSeqSeen: 9,
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			shadowObserveMs: 0,
			shadowMinObservations: 2,
			probeMaxLagMessages: 0,
		};

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { haveToExclusive: 10 }),
		});

		expect(ch.parentShadow?.hash).to.equal("shadow-parent");

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { haveToExclusive: 5 }),
		});

		expect(second.result).to.equal(false);
		expect(second.attempts).to.deep.equal([]);
		expect(ch.parent).to.equal("relay");
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.parentShadowReset).to.equal(1);
		expect(ch.metrics.parentShadowRejectLag).to.equal(1);
		expect(ch.metrics.reparentUpgradeSkipProbeLag).to.equal(1);
	});

	it("rejects an unrooted shadow candidate without starting observation", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{
					hash: "shadow-parent",
					addrs: [],
					level: 0,
					freeSlots: 2,
					bidPerByte: 0,
				},
			],
		});

		const result = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-parent"],
			channel: ch,
			options: {
				mode: "shadow",
				shadowObserveMs: 0,
				shadowMinObservations: 2,
			},
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { rooted: false }),
		});

		expect(result.result).to.equal(false);
		expect(result.attempts).to.deep.equal([]);
		expect(ch.parentShadow).to.equal(undefined);
		expect(ch.metrics.parentShadowStart).to.equal(0);
		expect(ch.metrics.parentShadowReset).to.equal(0);
		expect(ch.metrics.parentShadowRejectNotRooted).to.equal(1);
		expect(ch.metrics.reparentUpgradeSkipProbeNotRooted).to.equal(1);
	});

	it("restarts shadow observation when the best candidate changes", async () => {
		const ch = createImproveChannel({
			cachedTrackerCandidates: [
				{ hash: "shadow-a", addrs: [], level: 0, freeSlots: 2, bidPerByte: 0 },
			],
		});
		const options: Partial<ImproveOptions> = {
			mode: "shadow",
			shadowObserveMs: 0,
			shadowMinObservations: 2,
		};

		await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-a", "shadow-b"],
			channel: ch,
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash),
		});

		expect(ch.parentShadow?.hash).to.equal("shadow-a");

		const second = await runMaybeImproveParent({
			peerHashes: ["relay", "shadow-a", "shadow-b"],
			channel: ch,
			cachedTrackerCandidates: [
				{ hash: "shadow-b", addrs: [], level: 0, freeSlots: 3, bidPerByte: 0 },
			],
			options,
			probeParentCandidate: async (_channel, parentHash) =>
				createProbeReply(parentHash, { freeSlots: 3 }),
		});

		expect(second.result).to.equal(false);
		expect(second.attempts).to.deep.equal([]);
		expect(ch.parent).to.equal("relay");
		expect(ch.parentShadow?.hash).to.equal("shadow-b");
		expect(ch.parentShadow?.observations).to.equal(1);
		expect(ch.metrics.parentShadowStart).to.equal(2);
		expect(ch.metrics.parentShadowObserve).to.equal(0);
		expect(ch.metrics.parentShadowPromote).to.equal(0);
	});

	it("keeps guarded parent upgrades off during active streams", async () => {
		expect(runParentUpgradeGate().run).to.equal(false);
		expect((runParentUpgradeGate() as any).reason).to.equal("data");

		expect(
			runParentUpgradeGate({ endSeqExclusive: 10 }, { endedAndComplete: true })
				.run,
		).to.equal(true);

		expect(runParentUpgradeGate({}, { dataGuard: false }).run).to.equal(true);
	});

	it("skips guarded parent upgrades while repair is active", async () => {
		const gate = runParentUpgradeGate(
			{ missingSeqs: new Set([7]) },
			{ dataGuard: false },
		);

		expect(gate.run).to.equal(false);
		expect((gate as any).reason).to.equal("repair");

		const repairQuiet = runParentUpgradeGate(
			{ lastRepairSentAt: 9_500 },
			{ dataGuard: false, repairQuietMs: 1_000 },
		);

		expect(repairQuiet.run).to.equal(false);
		expect((repairQuiet as any).reason).to.equal("repair");
	});

	it("bounds guarded parent upgrades by cooldown and per-peer budget", async () => {
		const budget = runParentUpgradeGate(
			{ parentUpgradeCount: 2 },
			{ dataGuard: false, maxPerPeer: 2 },
		);
		expect(budget.run).to.equal(false);
		expect((budget as any).reason).to.equal("budget");

		const cooldown = runParentUpgradeGate(
			{ parentUpgradeLastAt: 9_900 },
			{ dataGuard: false, maxPerPeer: 3, cooldownMs: 500 },
		);
		expect(cooldown.run).to.equal(false);
		expect((cooldown as any).reason).to.equal("cooldown");

		const backoff = runParentUpgradeGate(
			{ parentUpgradeBackoffUntil: 11_000 },
			{ dataGuard: false, maxPerPeer: 3, cooldownMs: 500 },
		);
		expect(backoff.run).to.equal(false);
		expect((backoff as any).reason).to.equal("cooldown");
	});

	it("returns false when a join succeeds without actually replacing the parent", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["root", "relay"],
			tryJoinOnce: async () => ({ ok: true }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("relay");
	});
});
