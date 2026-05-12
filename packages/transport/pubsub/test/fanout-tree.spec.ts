import { TestSession } from "@peerbit/libp2p-test-utils";
import { TimeoutError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutChannel, FanoutTree } from "../src/index.js";

type FanoutServices = { fanout: FanoutTree };

const JOIN_REJECT_NO_CAPACITY = 2;
const TRACKER_FEEDBACK_JOIN_REJECT = 4;
const PARENT_PROBE_FLAG_ACCEPTING = 1 << 1;

const createFanoutService = (components: any) =>
	new FanoutTree(components, { connectionManager: false });

const createFanoutTestSession = (n: number) =>
	TestSession.disconnected<FanoutServices>(n, {
		services: {
			fanout: createFanoutService,
		},
	});

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
		minLevelGain: 1,
		minFreeSlots: 1,
		failedBackoffMinMs: 5_000,
		failedBackoffMaxMs: 60_000,
		...args.options,
	});

	return { attempts, result, ch, feedback };
};

describe("fanout-tree", () => {
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
		const ch = {
			id: { key: new Uint8Array(32), root: "root" },
			isRoot: true,
			level: 0,
			children: new Map(),
			effectiveMaxChildren: 1,
			parentUpgradeReservationsByHash: new Map(),
			metrics: {
				dataWriteDrops: 0,
				parentUpgradeRootReservationCreated: 0,
				parentUpgradeRootReservationConsumed: 0,
				parentUpgradeRootReservationRejected: 0,
				parentUpgradeRootReservationMarginRejected: 0,
				parentUpgradeRootReservationBlocked: 0,
				parentUpgradeRootReservationExpired: 0,
			},
			missingSeqs: new Set(),
			overloadStreak: 0,
			maxSeqSeen: -1,
			droppedForwards: 0,
		};
		let randomValue = 0.25;
		const ctx = {
			...parentUpgradeReservationHelpers,
			random: () => randomValue,
			pruneDisconnectedChildren: () => {},
		};

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
		const metrics = {
			dataWriteDrops: 0,
			parentUpgradeRootReservationCreated: 0,
			parentUpgradeRootReservationConsumed: 0,
			parentUpgradeRootReservationRejected: 0,
			parentUpgradeRootReservationBlocked: 0,
			parentUpgradeRootReservationExpired: 0,
		};
		const ch = {
			id: { key: new Uint8Array(32), root: "root" },
			isRoot: true,
			level: 0,
			children: new Map([
				["child-a", {}],
				["child-b", {}],
			]),
			effectiveMaxChildren: 12,
			parentUpgradeReservationsByHash: new Map(),
			metrics,
			missingSeqs: new Set(),
			overloadStreak: 0,
			maxSeqSeen: -1,
			droppedForwards: 0,
		};
		const ctx = {
			...parentUpgradeReservationHelpers,
			random: () => 0.25,
			pruneDisconnectedChildren: () => {},
		};

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
		expect(metrics.parentUpgradeRootReservationCreated).to.equal(1);
		expect(metrics.parentUpgradeRootReservationBlocked).to.equal(1);
	});

	it("reports pending root reservations as probe child pressure", async () => {
		const ch = {
			id: { key: new Uint8Array(32), root: "root" },
			isRoot: true,
			level: 0,
			children: new Map([
				["child-a", {}],
				["child-b", {}],
			]),
			effectiveMaxChildren: 12,
			parentUpgradeReservationsByHash: new Map(),
			metrics: {
				dataWriteDrops: 0,
				parentUpgradeRootReservationCreated: 0,
				parentUpgradeRootReservationConsumed: 0,
				parentUpgradeRootReservationRejected: 0,
				parentUpgradeRootReservationMarginRejected: 0,
				parentUpgradeRootReservationBlocked: 0,
				parentUpgradeRootReservationExpired: 0,
			},
			missingSeqs: new Set(),
			overloadStreak: 0,
			maxSeqSeen: -1,
			droppedForwards: 0,
		};
		let randomValue = 0.25;
		const ctx = {
			...parentUpgradeReservationHelpers,
			random: () => randomValue,
			pruneDisconnectedChildren: () => {},
		};

		const first = encodeParentProbeReplyForChannel.call(ctx, ch, 1, "leaf-a");
		expect(readTestU16BE(first, 44)).to.equal(2);
		expect(readTestU32BE(first, 60)).to.not.equal(0);

		randomValue = 0.5;
		const second = encodeParentProbeReplyForChannel.call(ctx, ch, 2, "leaf-b");
		expect(readTestU16BE(second, 44)).to.equal(3);
		expect(readTestU32BE(second, 60)).to.not.equal(0);
	});

	it("rejects root reservation tokens after the spare-slot margin evaporates", async () => {
		const ch = {
			id: { key: new Uint8Array(32), root: "root" },
			isRoot: true,
			level: 0,
			children: new Map(
				Array.from({ length: 4 }, (_value, index) => [`child-${index}`, {}]),
			),
			effectiveMaxChildren: 12,
			parentUpgradeReservationsByHash: new Map(),
			metrics: {
				dataWriteDrops: 0,
				parentUpgradeRootReservationCreated: 0,
				parentUpgradeRootReservationConsumed: 0,
				parentUpgradeRootReservationRejected: 0,
				parentUpgradeRootReservationMarginRejected: 0,
				parentUpgradeRootReservationBlocked: 0,
				parentUpgradeRootReservationExpired: 0,
			},
			missingSeqs: new Set(),
			overloadStreak: 0,
			maxSeqSeen: -1,
			droppedForwards: 0,
		};
		const ctx = {
			...parentUpgradeReservationHelpers,
			random: () => 0.25,
			pruneDisconnectedChildren: () => {},
		};

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
		const ch = {
			id: { key: new Uint8Array(32), root: "root" },
			isRoot: true,
			level: 0,
			children: new Map(),
			effectiveMaxChildren: 1,
			parentUpgradeReservationsByHash: new Map(),
			metrics: {
				dataWriteDrops: 0,
				parentUpgradeRootReservationCreated: 0,
				parentUpgradeRootReservationConsumed: 0,
				parentUpgradeRootReservationRejected: 0,
				parentUpgradeRootReservationBlocked: 0,
				parentUpgradeRootReservationExpired: 0,
			},
			missingSeqs: new Set(),
			overloadStreak: 0,
			maxSeqSeen: -1,
			droppedForwards: 0,
		};
		const ctx = {
			...parentUpgradeReservationHelpers,
			random: () => 0.25,
			pruneDisconnectedChildren: () => {},
		};

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
			peerHashes: ["relay", "b"],
			channelOverrides: {
				children: new Map([["child", { bidPerByte: 0 }]]),
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
		expect(probes).to.equal(1);
		expect(attempts).to.deep.equal([]);
		expect(ch.parentShadow?.hash).to.equal("b");
	});

	it("lets single-channel branches spend the wider stale-root sample budget", async () => {
		let probes = 0;
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["relay", "h2"],
			channelOverrides: {
				children: new Map([["child", { bidPerByte: 0 }]]),
				id: { root: "h2", key: new Uint8Array([1, 2, 3]), suffixKey: "topic" },
			},
			cachedTrackerCandidates: [
				{ hash: "h2", addrs: [], level: 0, freeSlots: 0, bidPerByte: 0 },
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
		expect(ch.parentShadow?.hash).to.equal("h2");
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

	it("bounds per-channel route token cache (LRU + TTL)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "route-cache";
			const root = fanout.publicKeyHash;

			const id = fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 128,
				repair: false,
				routeCacheMaxEntries: 3,
				routeCacheTtlMs: 0,
			});

			const ch = (fanout as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch).to.exist;

			const cacheRoute = (fanout as any).cacheRoute.bind(fanout) as (
				ch: any,
				route: string[],
			) => void;
			const getCachedRoute = (fanout as any).getCachedRoute.bind(fanout) as (
				ch: any,
				target: string,
			) => string[] | undefined;

			// Root route-cache entries must start with a valid child hop.
			ch.children.set("child1", { bidPerByte: 0 });

			cacheRoute(ch, [root, "child1", "p1"]);
			cacheRoute(ch, [root, "child1", "p2"]);
			cacheRoute(ch, [root, "child1", "p3"]);
			expect(ch.routeByPeer.size).to.equal(3);

			// LRU touch p1, then insert p4: p2 should be evicted.
			expect(getCachedRoute(ch, "p1")).to.deep.equal([root, "child1", "p1"]);
			cacheRoute(ch, [root, "child1", "p4"]);
			expect(ch.routeByPeer.size).to.equal(3);
			expect(ch.routeByPeer.has("p2")).to.equal(false);
			expect(ch.routeByPeer.has("p1")).to.equal(true);
			expect(ch.routeByPeer.has("p3")).to.equal(true);
			expect(ch.routeByPeer.has("p4")).to.equal(true);

			// TTL expiry prunes oldest entries.
			// Mutate timestamps to avoid relying on wall-clock timing (keeps this test deterministic).
			ch.routeCacheTtlMs = 25;
			const expiredAt = Date.now() - 1_000;
			for (const entry of ch.routeByPeer.values()) entry.updatedAt = expiredAt;
			expect(getCachedRoute(ch, "p1")).to.equal(undefined);
			expect(getCachedRoute(ch, "p3")).to.equal(undefined);
			expect(getCachedRoute(ch, "p4")).to.equal(undefined);
			expect(ch.routeByPeer.size).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	it("invalidates cached routes when root child set changes", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "route-cache-validity";
			const root = fanout.publicKeyHash;

			const id = fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 128,
				repair: false,
				routeCacheMaxEntries: 16,
				routeCacheTtlMs: 0,
			});

			const ch = (fanout as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch).to.exist;

			const cacheRoute = (fanout as any).cacheRoute.bind(fanout) as (
				ch: any,
				route: string[],
			) => void;
			const getCachedRoute = (fanout as any).getCachedRoute.bind(fanout) as (
				ch: any,
				target: string,
			) => string[] | undefined;

			// Root requires the first hop after root to be a current child.
			ch.children.set("child1", { bidPerByte: 0 });
			cacheRoute(ch, [root, "child1", "target"]);
			expect(getCachedRoute(ch, "target")).to.deep.equal([
				root,
				"child1",
				"target",
			]);

			// Drop child1, cached route must be treated as invalid and removed.
			ch.children.delete("child1");
			expect(getCachedRoute(ch, "target")).to.equal(undefined);
			expect(ch.routeByPeer.has("target")).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("does not miss channel attachment when parent appears during join-listener setup", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-race", root: "root" },
			};

			const originalAddEventListener = fanout.addEventListener.bind(fanout);
			fanout.addEventListener = ((
				type: string,
				listener: EventListenerOrEventListenerObject,
				options?: AddEventListenerOptions | boolean,
			) => {
				const result = originalAddEventListener(type, listener, options);
				if (type === "fanout:joined") {
					ch.parent = "parent";
				}
				return result;
			}) as typeof fanout.addEventListener;

			try {
				await waitForChannelAttachment(ch, 25);
			} finally {
				fanout.addEventListener = originalAddEventListener;
			}

			expect(ch.parent).to.equal("parent");
		} finally {
			await session.stop();
		}
	});

	it("accepts parent attachment that becomes visible before timeout even without a join event", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-timeout-fallback", root: "root" },
			};

			setTimeout(() => {
				ch.parent = "parent";
			}, 5);

			await waitForChannelAttachment(ch, 25);
			expect(ch.parent).to.equal("parent");
		} finally {
			await session.stop();
		}
	});

	it("reports attachment waits as delivery timeouts", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-timeout", root: "root" },
			};

			await expect(waitForChannelAttachment(ch, 5)).to.be.rejectedWith(
				TimeoutError,
				"fanout proxy publish timed out waiting for attachment",
			);
		} finally {
			await session.stop();
		}
	});

	it("returns false when maybe-publishing to a channel that is not open", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const ok = await fanout.publishToChannelMaybe(
				"missing-channel",
				fanout.publicKeyHash,
				new Uint8Array([1]),
			);
			expect(ok).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("returns false for late channel-close races on maybe-publish but still rethrows unexpected errors", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "maybe-publish-close-race";
			const root = fanout.publicKeyHash;

			fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const originalPublishToChannel = fanout.publishToChannel.bind(fanout);

			try {
				fanout.publishToChannel = (async () => {
					throw new Error(`Channel not open: ${topic} (${root})`);
				}) as typeof fanout.publishToChannel;

				const ok = await fanout.publishToChannelMaybe(
					topic,
					root,
					new Uint8Array([1]),
				);
				expect(ok).to.equal(false);

				fanout.publishToChannel = (async () => {
					throw new Error("unexpected publish failure");
				}) as typeof fanout.publishToChannel;

				await expect(
					fanout.publishToChannelMaybe(topic, root, new Uint8Array([1])),
				).to.be.rejectedWith("unexpected publish failure");
			} finally {
				fanout.publishToChannel =
					originalPublishToChannel as typeof fanout.publishToChannel;
			}
		} finally {
			await session.stop();
		}
	});

	it("forms a small tree and delivers data", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			// Connect 0<->1<->2 (line) so 2 can join via 1 if root is full.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay can accept one child.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf should end up attaching to relay (root is full).
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([1, 2, 3, 4]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("prioritizes direct root candidate during join when root is outside ranked top-K", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(10);

		try {
			const fanouts = session.peers.map((p) => p.services.fanout);
			const byHash = fanouts
				.map((f) => ({ hash: f.publicKeyHash, fanout: f }))
				.sort((a, b) => a.hash.localeCompare(b.hash));

			// Pick a root that is guaranteed to be outside the first ranked window.
			const rootEntry = byHash[byHash.length - 1]!;
			const root = rootEntry.fanout;
			const rootId = rootEntry.hash;

			const joinerEntry = byHash.find((x) => x.hash !== rootId)!;
			const joiner = joinerEntry.fanout;
			const joinerPeer = session.peers.find(
				(p) => p.services.fanout.publicKeyHash === joinerEntry.hash,
			)!;
			const starGroups = session.peers
				.filter((p) => p !== joinerPeer)
				.map((p) => [joinerPeer, p]);
			await session.connect(starGroups);

			const topic = "join-root-priority";
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 32,
				repair: true,
			});

			await joiner.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{
					timeoutMs: 10_000,
					joinReqTimeoutMs: 500,
					retryMs: 100,
				},
			);

			await waitForResolved(() =>
				expect(joiner.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
			);
		} finally {
			await session.stop();
		}
	});

	it("allows a child to leave and immediately frees parent capacity", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "leave-demo";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			await waitForResolved(() =>
				expect(relay.getChannelStats(topic, rootId)?.children).to.equal(1),
			);

			await leaf.closeChannel(topic, rootId);

			await waitForResolved(() =>
				expect(relay.getChannelStats(topic, rootId)?.children).to.equal(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("proxies publish from non-root via the root", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const publisher = session.peers[1].services.fanout;
			const subscriber = session.peers[2].services.fanout;

			const topic = "proxy-publish";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const publisherChannel = new FanoutChannel(publisher, {
				topic,
				root: rootId,
			});
			await publisherChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const subscriberChannel = new FanoutChannel(subscriber, {
				topic,
				root: rootId,
			});
			await subscriberChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let receivedBySubscriber: Uint8Array | undefined;
			subscriber.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				receivedBySubscriber = ev.detail.payload;
			});

			let receivedByPublisher: Uint8Array | undefined;
			publisher.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				receivedByPublisher = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 8, 7, 6]);
			await publisherChannel.publish(payload);

			await waitForResolved(() => expect(receivedBySubscriber).to.exist);
			expect([...receivedBySubscriber!]).to.deep.equal([...payload]);

			await waitForResolved(() => expect(receivedByPublisher).to.exist);
			expect([...receivedByPublisher!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("exposes channel peers for fanout membership-aware consumers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const leafA = session.peers[1].services.fanout;
			const leafB = session.peers[2].services.fanout;

			const topic = "peer-list-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const leafAChannel = new FanoutChannel(leafA, { topic, root: rootId });
			await leafAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const leafBChannel = new FanoutChannel(leafB, { topic, root: rootId });
			await leafBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			await waitForResolved(() => {
				const peers = new Set(rootChannel.getPeerHashes());
				expect(peers.has(leafA.publicKeyHash)).to.equal(true);
				expect(peers.has(leafB.publicKeyHash)).to.equal(true);
			});

			await waitForResolved(() => {
				const peers = new Set(leafAChannel.getPeerHashes());
				expect(peers.has(root.publicKeyHash)).to.equal(true);
			});
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast via route tokens through the root", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const sender = session.peers[1].services.fanout;
			const target = session.peers[2].services.fanout;

			const topic = "unicast-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let targetRoute: string[] | undefined;
			await waitForResolved(() => {
				targetRoute = targetChannel.getRouteToken();
				expect(targetRoute).to.exist;
			});
			expect(targetRoute![0]).to.equal(rootId);
			expect(targetRoute![targetRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

			let received: Uint8Array | undefined;
			let origin: string | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
				origin = ev.detail.origin;
			});

			const payload = new Uint8Array([4, 3, 2, 1]);
			await senderChannel.unicast(targetRoute!, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
			expect(origin).to.equal(sender.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens through control-plane proxy and unicasts across branches", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

		try {
			// Root <-> relayA and root <-> relayB. sender is only connected to relayA,
			// target is only connected to relayB.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[2], session.peers[4]],
			]);

			const root = session.peers[0].services.fanout;
			const relayA = session.peers[1].services.fanout;
			const relayB = session.peers[2].services.fanout;
			const sender = session.peers[3].services.fanout;
			const target = session.peers[4].services.fanout;

			const topic = "unicast-proxy-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const relayAChannel = new FanoutChannel(relayA, { topic, root: rootId });
			await relayAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const relayBChannel = new FanoutChannel(relayB, { topic, root: rootId });
			await relayBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let resolvedRoute: string[] | undefined;
			await waitForResolved(async () => {
				resolvedRoute = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 2_000,
					},
				);
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

			let received: Uint8Array | undefined;
			let origin: string | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
				origin = ev.detail.origin;
			});

			const payload = new Uint8Array([5, 6, 7, 8]);
			await senderChannel.unicastTo(target.publicKeyHash, payload, {
				timeoutMs: 2_000,
			});

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
			expect(origin).to.equal(sender.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast with ACKs (shared intermediate hop)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(4);

		try {
			// Root <-> relay. Two leaves only connect to relay. This creates a shared intermediate
			// hop so the unicast goes: sender -> relay -> root -> relay -> target.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[1], session.peers[3]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const sender = session.peers[2].services.fanout;
			const target = session.peers[3].services.fanout;

			const topic = "unicast-ack-shared-hop";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			const relayChannel = new FanoutChannel(relay, { topic, root: rootId });
			await relayChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let targetRoute: string[] | undefined;
			await waitForResolved(() => {
				targetRoute = targetChannel.getRouteToken();
				expect(targetRoute).to.exist;
			});

			let received: Uint8Array | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 8, 7, 6]);
			received = undefined;
			await senderChannel.unicast(targetRoute!, payload);
			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);

			received = undefined;
			await senderChannel.unicastAck(targetRoute!, payload, {
				timeoutMs: 2_000,
			});
			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast with ACKs across branches", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

		try {
			// Root <-> relayA and root <-> relayB. sender is only connected to relayA,
			// target is only connected to relayB.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[2], session.peers[4]],
			]);

			const root = session.peers[0].services.fanout;
			const relayA = session.peers[1].services.fanout;
			const relayB = session.peers[2].services.fanout;
			const sender = session.peers[3].services.fanout;
			const target = session.peers[4].services.fanout;

			const topic = "unicast-ack-branches";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const relayAChannel = new FanoutChannel(relayA, { topic, root: rootId });
			await relayAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const relayBChannel = new FanoutChannel(relayB, { topic, root: rootId });
			await relayBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([1, 3, 3, 7]);
			await senderChannel.unicastToAck(target.publicKeyHash, payload, {
				timeoutMs: 10_000,
			});

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("bounds route cache size and evicts old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[1], session.peers[4]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leafA = session.peers[2].services.fanout;
			const leafB = session.peers[3].services.fanout;
			const leafC = session.peers[4].services.fanout;

			const topic = "route-cache-bound";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
				routeCacheMaxEntries: 2,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 3,
					repair: true,
					routeCacheMaxEntries: 2,
				},
				{ timeoutMs: 10_000 },
			);

			for (const leaf of [leafA, leafB, leafC]) {
				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: true,
						routeCacheMaxEntries: 2,
					},
					{ timeoutMs: 10_000 },
				);
			}

			// Drive route discovery on-demand so the root cache actually fills and evicts.
			for (const leaf of [leafA, leafB, leafC]) {
				await waitForResolved(async () => {
					const route = await relay.resolveRouteToken(
						topic,
						rootId,
						leaf.publicKeyHash,
						{
							timeoutMs: 4_000,
						},
					);
					expect(route).to.exist;
				});
			}

			await waitForResolved(() =>
				expect(
					root.getChannelStats(topic, rootId)?.routeCacheEntries,
				).to.be.at.most(2),
			);
			await waitForResolved(() =>
				expect(
					root.getChannelMetrics(topic, rootId).routeCacheEvictions,
				).to.be.greaterThan(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("clamps requested route cache size to a hard safety cap", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const root = session.peers[0].services.fanout;
			const topic = "route-cache-hard-cap";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				routeCacheMaxEntries: 2_000_000_000,
			});

			const stats = root.getChannelStats(topic, rootId);
			expect(stats).to.exist;
			expect(stats?.routeCacheMaxEntries).to.equal(100_000);
		} finally {
			await session.stop();
		}
	});

	it("bounds peer hint cache size and prunes old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(8);

		try {
			// Star topology: all peers connect to the root so we can drive many JOIN_REQs.
			await session.connect(
				session.peers.slice(1).map((peer) => [session.peers[0], peer] as const),
			);

			const root = session.peers[0].services.fanout;
			const leaves = session.peers.slice(1).map((p) => p.services.fanout);

			const topic = "peer-hints-bound";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 32,
				repair: true,
				peerHintMaxEntries: 2,
			});

			for (const leaf of leaves) {
				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: true,
					},
					{ timeoutMs: 10_000 },
				);
			}

			const stats = root.getChannelStats(topic, rootId);
			expect(stats?.peerHintMaxEntries).to.equal(2);
			expect(stats?.peerHintEntries).to.equal(2);
		} finally {
			await session.stop();
		}
	});

	it("clamps requested peer hint size to a hard safety cap", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const root = session.peers[0].services.fanout;
			const topic = "peer-hints-hard-cap";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				peerHintMaxEntries: 2_000_000_000,
			});

			const stats = root.getChannelStats(topic, rootId);
			expect(stats).to.exist;
			expect(stats?.peerHintMaxEntries).to.equal(100_000);
		} finally {
			await session.stop();
		}
	});

	it("root resolves deep route tokens on-demand without route announcements", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "root-route-resolve";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
				routeCacheMaxEntries: 16,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
					routeCacheMaxEntries: 16,
				},
				{ timeoutMs: 10_000 },
			);

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
					routeCacheMaxEntries: 16,
				},
				{ timeoutMs: 10_000 },
			);

			const route = await root.resolveRouteToken(
				topic,
				rootId,
				leaf.publicKeyHash,
				{
					timeoutMs: 4_000,
				},
			);
			expect(route).to.exist;
			expect(route?.[0]).to.equal(rootId);
			expect(route?.[1]).to.equal(relay.publicKeyHash);
			expect(route?.[route.length - 1]).to.equal(leaf.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens after cache expiry via subtree fallback search", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(6);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[2], session.peers[4]],
				[session.peers[4], session.peers[5]],
			]);

			const root = session.peers[0].services.fanout;
			const relayA = session.peers[1].services.fanout;
			const relayB = session.peers[2].services.fanout;
			const sender = session.peers[3].services.fanout;
			const relayB2 = session.peers[4].services.fanout;
			const target = session.peers[5].services.fanout;

			const topic = "route-cache-subtree-fallback";
			const rootId = root.publicKeyHash;
			const routeCacheTtlMs = 40;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				routeCacheMaxEntries: 64,
				routeCacheTtlMs,
			});

			for (const [node, maxChildren] of [
				[relayA, 1],
				[relayB, 2],
				[sender, 0],
				[relayB2, 1],
				[target, 0],
			] as const) {
				const ch = new FanoutChannel(node, { topic, root: rootId });
				await ch.join(
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 1_000_000,
						maxChildren,
						repair: true,
						routeCacheMaxEntries: 64,
						routeCacheTtlMs,
					},
					{ timeoutMs: 10_000 },
				);
			}

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });

			// Warm caches once, then let route tokens expire before resolving again.
			await waitForResolved(async () => {
				const route = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 4_000,
					},
				);
				expect(route).to.exist;
			});

			await delay(160);
			const missesBefore = root.getChannelMetrics(
				topic,
				rootId,
			).routeCacheMisses;

			let resolvedRoute: string[] | undefined;
			await waitForResolved(async () => {
				resolvedRoute = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 4_000,
					},
				);
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

			const missesAfter = root.getChannelMetrics(
				topic,
				rootId,
			).routeCacheMisses;
			expect(missesAfter).to.be.greaterThan(missesBefore);
		} finally {
			await session.stop();
		}
	});

	it("uses JOIN_REJECT redirects to attach via relay without trackers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			// 0 connected to both 1 and 2. Leaf (2) should be able to re-attach to relay (1)
			// when root (0) is full, using JOIN_REJECT redirects (no bootstrap tracker).
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay consumes root's only slot.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf joins via root first (connected peer), gets rejected, then follows redirects to relay.
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const stats = leaf.getChannelStats(topic, rootId);
			expect(stats?.parent).to.equal(relay.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("joins via bootstrap tracker (dial + capacity announcements)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(4);

		try {
			// Star topology via a bootstrap node so join must happen via dial + tracker redirect.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[2], session.peers[1]],
				[session.peers[3], session.peers[1]],
			]);

			const root = session.peers[0].services.fanout;
			const bootstrap = session.peers[1];
			const relay = session.peers[2].services.fanout;
			const leaf = session.peers[3].services.fanout;

			const bootstrapAddrs = bootstrap.getMultiaddrs();
			root.setBootstraps(bootstrapAddrs);

			const topic = "concert";
			const rootId = root.publicKeyHash;

			// Root can only accept one child (the relay).
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{
					timeoutMs: 10_000,
					bootstrap: bootstrapAddrs,
					announceIntervalMs: 200,
					announceTtlMs: 5_000,
				},
			);

			// Leaf should end up attaching to relay (root is full).
			let parent: string | undefined;
			leaf.addEventListener("fanout:joined", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				parent = ev.detail.parent;
			});

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			await waitForResolved(() => expect(parent).to.exist);
			expect(parent).to.equal(relay.publicKeyHash);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 9, 9]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("re-parents when no data arrives within staleAfterMs", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;

			const topic = "stale";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				maxDataAgeMs: 10_000,
				repair: false,
			});

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					maxDataAgeMs: 10_000,
					repair: false,
				},
				{
					timeoutMs: 10_000,
					staleAfterMs: 200,
					retryMs: 50,
				},
			);

			await waitForResolved(() =>
				expect(
					leaf.getChannelMetrics(topic, rootId).reparentStale,
				).to.be.greaterThan(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("keeps rejoining after the initial join timeout has elapsed", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;

			const bootstrapAddrs = rootNode
				.getMultiaddrs()
				.filter((x) => !x.getComponents().some((c) => c.code === 290));

			const topic = "rejoin-timeout";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				maxDataAgeMs: 10_000,
				repair: false,
			});

			const timeoutMs = 2_000;
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					maxDataAgeMs: 10_000,
					repair: false,
				},
				{
					timeoutMs,
					bootstrap: bootstrapAddrs,
					staleAfterMs: 250,
					retryMs: 50,
				},
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);

			// Keep data flowing until after the initial `timeoutMs` has elapsed, so any later
			// detach/rejoin would have previously tripped the join-loop timeout bug.
			const keepAliveUntil = Date.now() + timeoutMs + 500;
			while (Date.now() < keepAliveUntil) {
				await root.publishData(topic, rootId, new Uint8Array([0x01]));
				// eslint-disable-next-line no-await-in-loop
				await delay(100);
			}

			// Stop sending for long enough to trigger stale re-parenting.
			await waitForResolved(
				() =>
					expect(
						leaf.getChannelMetrics(topic, rootId).reparentStale,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Once it has re-joined, it should receive fresh data again.
			let markerReceived = false;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if ((ev.detail.payload as Uint8Array)?.[0] !== 0x99) return;
				markerReceived = true;
			});
			for (let i = 0; i < 20 && !markerReceived; i++) {
				await root.publishData(topic, rootId, new Uint8Array([0x99]));
				// eslint-disable-next-line no-await-in-loop
				await delay(100);
			}
			expect(markerReceived).to.equal(true);
		} finally {
			await session.stop();
		}
	});

	it("re-parents when its parent disconnects", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			// Root connected to both relay and leaf. Leaf initially joins via relay (root full),
			// then relay disappears and leaf should attach directly to root.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relayNode = session.peers[1];
			const relay = relayNode.services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay consumes root's only slot.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf attaches via relay using JOIN_REJECT redirects.
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
				relay.publicKeyHash,
			);

			// Kill relay.
			await relayNode.stop();

			// Leaf should eventually attach directly to root.
			await waitForResolved(() =>
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
			);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([7, 7, 7]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("re-parents after repeated parent data write failures", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const rootNode = session.peers[0];
				const root = rootNode.services.fanout;
				const leaf = session.peers[1].services.fanout;
				const bootstrapAddrs = rootNode
					.getMultiaddrs()
					.filter((x) => !x.getComponents().some((c) => c.code === 290));

				const topic = "write-fail-reparent";
				const rootId = root.publicKeyHash;

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					allowKick: true,
					repair: true,
				});

				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						allowKick: true,
						repair: true,
					},
					{ timeoutMs: 10_000, bootstrap: bootstrapAddrs, retryMs: 50 },
				);

				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);

				const rootToLeaf = (root as any).peers.get(leaf.publicKeyHash);
				expect(rootToLeaf).to.exist;
				let failedWrites = 0;
				rootToLeaf.write = () => {
					failedWrites += 1;
					throw new Error("simulated fanout data write failure");
				};

				for (let i = 0; i < 3; i++) {
					// eslint-disable-next-line no-await-in-loop
					await root.publishData(topic, rootId, new Uint8Array([i]));
				}

				await waitForResolved(
					() => {
						expect(failedWrites).to.be.at.least(3);
						expect(root.getChannelStats(topic, rootId)?.children).to.equal(0);
						expect(root.getChannelMetrics(topic, rootId).dataWriteDrops).to.be.at.least(
							3,
						);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);

				await waitForResolved(
					() =>
						expect(
							leaf.getChannelMetrics(topic, rootId).reparentDisconnect,
						).to.be.greaterThan(0),
					{ timeout: 20_000, delayInterval: 50 },
				);

				await waitForResolved(
					() => expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
					{ timeout: 20_000, delayInterval: 50 },
				);

				let markerReceived = false;
				leaf.addEventListener("fanout:data", (ev: any) => {
					if (ev.detail.topic !== topic) return;
					if (ev.detail.root !== rootId) return;
					if ((ev.detail.payload as Uint8Array)?.[0] !== 0x88) return;
					markerReceived = true;
				});
				for (let i = 0; i < 20 && !markerReceived; i++) {
					// eslint-disable-next-line no-await-in-loop
					await root.publishData(topic, rootId, new Uint8Array([0x88]));
					// eslint-disable-next-line no-await-in-loop
					await delay(100);
				}
				expect(markerReceived).to.equal(true);
			} finally {
				await session.stop();
			}
		});

		it("sends the end watermark to children that join after publishEnd", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const root = session.peers[0].services.fanout;
				const leaf = session.peers[1].services.fanout;

				const topic = "late-end";
				const rootId = root.publicKeyHash;
				const channelId = root.getChannelId(topic, rootId);

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				});

				await root.publishEnd(topic, rootId, 3);
				expect(
					(root as any).channelsBySuffixKey.get(channelId.suffixKey)?.endSeqExclusive,
				).to.equal(3);

				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: true,
					},
					{ timeoutMs: 10_000 },
				);

				await waitForResolved(
					() => {
						const leafState = (leaf as any).channelsBySuffixKey.get(
							channelId.suffixKey,
						);
						expect(leafState?.endSeqExclusive).to.equal(3);
						expect([...leafState.missingSeqs].sort((a, b) => a - b)).to.deep.equal([
							0, 1, 2,
						]);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);
			} finally {
				await session.stop();
			}
		});

		it("retries the end watermark for existing children", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const root = session.peers[0].services.fanout;
				const leaf = session.peers[1].services.fanout;

				const topic = "end-heartbeat";
				const rootId = root.publicKeyHash;
				const channelId = root.getChannelId(topic, rootId);

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				});

				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: true,
					},
					{ timeoutMs: 10_000 },
				);

				await root.publishEnd(topic, rootId, 3);
				const leafState = (leaf as any).channelsBySuffixKey.get(
					channelId.suffixKey,
				);
				await waitForResolved(
					() => expect(leafState?.endSeqExclusive).to.equal(3),
					{ timeout: 10_000, delayInterval: 50 },
				);

				leafState.endSeqExclusive = -1;
				leafState.nextExpectedSeq = 0;
				leafState.missingSeqs.clear();

				const rootState = (root as any).channelsBySuffixKey.get(
					channelId.suffixKey,
				);
				rootState.lastIHaveSentAt = 0;
				await (root as any).maybeSendIHave(rootState, Date.now() + 5_000);

				await waitForResolved(
					() => {
						expect(leafState.endSeqExclusive).to.equal(3);
						expect([...leafState.missingSeqs].sort((a, b) => a - b)).to.deep.equal([
							0, 1, 2,
						]);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);
			} finally {
				await session.stop();
			}
		});

	it("prevents stable disconnected components when an intermediate relay loses the root", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const rootNode = session.peers[0];
			const relayNode = session.peers[1];

			const root = rootNode.services.fanout;
			const relay = relayNode.services.fanout;
			const leaf = session.peers[2].services.fanout;

			const bootstrapAddrs = rootNode
				.getMultiaddrs()
				.filter((x) => !x.getComponents().some((c) => c.code === 290));

			const topic = "partition";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: false,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
				relay.publicKeyHash,
			);

			// Break the relay<->root connection but keep relay alive.
			const rootConnMgr = (root as any)?.components?.connectionManager;
			const relayConnMgr = (relay as any)?.components?.connectionManager;
			expect(rootConnMgr).to.exist;
			expect(relayConnMgr).to.exist;
			const relayAsSeenByRoot = (root as any)?.peers?.get?.(
				relay.publicKeyHash,
			);
			const rootAsSeenByRelay = (relay as any)?.peers?.get?.(rootId);
			const relayPeerId = relayAsSeenByRoot?.peerId;
			const rootPeerId = rootAsSeenByRelay?.peerId;
			expect(relayPeerId).to.exist;
			expect(rootPeerId).to.exist;
			await Promise.allSettled([
				rootConnMgr?.closeConnections?.(relayPeerId),
				relayConnMgr?.closeConnections?.(rootPeerId),
			]);

			// Ensure the connection is actually down (otherwise the rest of the test is meaningless).
			await waitForResolved(
				() => {
					const a = rootConnMgr?.getConnections?.(relayPeerId) ?? [];
					const b = relayConnMgr?.getConnections?.(rootPeerId) ?? [];
					expect(a.length).to.equal(0);
					expect(b.length).to.equal(0);
				},
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Relay should detect the disconnect from its parent and trigger a reparent.
			// `stats.parent` can be transiently undefined and then quickly restored if the
			// root reconnects, so assert on the metric rather than the brief state.
			await waitForResolved(
				() =>
					expect(
						relay.getChannelMetrics(topic, rootId).reparentDisconnect,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Relay should kick its children once it loses the rooted route, and leaf should
			// rejoin directly to the root instead of stabilizing in a disconnected component.
			await waitForResolved(
				() =>
					expect(
						leaf.getChannelMetrics(topic, rootId).reparentKicked,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);
			await waitForResolved(
				() =>
					expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
				{ timeout: 20_000, delayInterval: 50 },
			);
		} finally {
			await session.stop();
		}
	});

	it("rate limits proxy publish ingress (abuse resistance)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;

			const topic = "proxy-publish-rate-limit";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
				// Deterministic drop: capacity=1 byte, but payload > 1 byte.
				proxyPublishBudgetBps: 1,
				proxyPublishBurstMs: 1_000,
			});

			const leafChannel = new FanoutChannel(leaf, { topic, root: rootId });
			await leafChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			let received = 0;
			leafChannel.addEventListener("data", () => {
				received += 1;
			});

			await leafChannel.publish(new Uint8Array(16).fill(7));
			await delay(200);
			expect(received).to.equal(0);

			const id = root.getChannelId(topic, rootId);
			const ch = (root as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch?.metrics?.proxyPublishDrops ?? 0).to.be.greaterThan(0);
		} finally {
			await session.stop();
		}
	});

	it("rate limits unicast ingress (abuse resistance)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const leafA = session.peers[1].services.fanout;
			const leafB = session.peers[2].services.fanout;

			const topic = "unicast-rate-limit";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: false,
				// Deterministic drop for unicast payload frames.
				unicastBudgetBps: 1,
				unicastBurstMs: 1_000,
			});

			const leafAChannel = new FanoutChannel(leafA, { topic, root: rootId });
			await leafAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			const leafBChannel = new FanoutChannel(leafB, { topic, root: rootId });
			await leafBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			leafBChannel.addEventListener("unicast", (ev: any) => {
				received = (ev?.detail as any)?.payload;
			});

			await leafAChannel.unicastTo(
				leafB.publicKeyHash,
				new Uint8Array([1, 2, 3]),
				{
					timeoutMs: 5_000,
				},
			);

			await delay(200);
			expect(received).to.equal(undefined);

			const id = root.getChannelId(topic, rootId);
			const ch = (root as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch?.metrics?.unicastDrops ?? 0).to.be.greaterThan(0);
		} finally {
			await session.stop();
		}
	});
});
