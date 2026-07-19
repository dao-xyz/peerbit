// Interfaces for the optional native (wasm) DirectStream core implemented by
// `@peerbit/network-rust` (the `peerbit_wire` crate's `direct_stream`
// modules). When `DirectStreamOptions.rustCore` is set, the routing table,
// seen-cache dedup, outbound lane scheduling and the seek-routing/relay/ack
// decisions run in Rust; the TS class remains the socket owner and byte
// pump. With the option unset the pure TS implementations are used,
// unchanged.
import type { Uint8ArrayList } from "uint8arraylist";
import type { PushableLanes } from "./pushable-lanes.js";
import type { RoutesLike } from "./routes.js";

/**
 * Native (wasm) batch decode+verify module for inbound frames, implemented by
 * `@peerbit/network-rust`. The result is a flat Uint32Array with
 * NATIVE_WIRE_RECORD_WORDS words per frame; the layout is defined by the
 * `peerbit_wire` crate (see RECORD_* in its lib.rs) and mirrored by the
 * NATIVE_WIRE_* constants in index.ts.
 */
export interface NativeWire {
	decodeAndVerifyBatch(frames: Uint8Array[], nowMs: number): Uint32Array;
}

/** Seen-cache dedup counter backed by the native core. */
export interface RustSeenCache {
	/**
	 * Bump the seen counter for a frame and return how many times it was seen
	 * before. `kind` 0 keys by message id (first 33 frame bytes, the
	 * `getMsgId` rule), 1 by sha256 of the whole frame (the ACK path).
	 */
	modify(bytes: Uint8Array, kind: 0 | 1): number;
	clear(): void;
}

/** Routing/relay/ack decision functions executed by the native core. */
export interface RustStreamDecisions {
	shouldIgnoreData(args: {
		seenBefore: number;
		acknowledgedMode: boolean;
		redundancy: number;
		hops: string[];
		me: string;
		signedBySelf: boolean;
	}): boolean;
	shouldAcknowledge(args: {
		isRecipient: boolean;
		seenBefore: number;
		redundancy: number;
	}): boolean;
	ackNextHop(
		trace: string[],
		me: string,
	): { myIndex: number; next?: string };
	seekAckRouteUpdate(args: {
		current: string;
		upstream?: string;
		downstream: string;
	}): { from: string; neighbour: string };
	filterFloodTargets(
		candidates: string[],
		from: string,
		signed: string[],
		hops: string[],
	): Uint32Array;
	filterSilentRelayRecipients(
		recipients: string[],
		me: string,
		from: string,
		connected: string[],
		hops: string[],
	): string[];
	selectRedundancyProbes(
		peers: string[],
		used: string[],
		redundancy: number,
	): string[];
}

export interface RustRoutesInit {
	me: string;
	routeMaxRetentionPeriod: number;
	signal?: AbortSignal;
	maxFromEntries?: number;
	maxTargetsPerFrom?: number;
	maxRelaysPerTarget?: number;
}

export interface RustLanesInit {
	lanes: number;
	maxBufferedBytes?: number;
	onBufferSize?(bufferedBytes: number): void;
	onPush?(value: { byteLength: number }, lane: number): void;
}

/** A decoded `/peerbit/direct-block` message produced by the native codec. */
export type RustDecodedBlockMessage =
	| { type: "request"; cid: string }
	| {
			type: "response";
			cid: string;
			/** View into the input payload (no copy). */
			bytes: Uint8Array;
	  };

/**
 * Provider-hint cache of `RemoteBlocks` backed by the native core
 * (`rememberProvider`/`rememberProviderHints`/lookup semantics).
 */
export interface RustBlockProviderCache {
	get(cid: string): string[] | undefined;
	rememberProvider(cid: string, provider: string): void;
	rememberHints(cid: string, providers: string[]): void;
	clear(): void;
}

export type RustEagerBlockCacheStats = {
	entries: number;
	bytes: number;
	peakEntries: number;
	peakBytes: number;
	evictions: number;
	expirations: number;
};

/**
 * Eager-block cache with native bookkeeping. Block bytes stay host-side;
 * the native index decides retention/eviction.
 */
export interface RustEagerBlockCache {
	add(cid: string, bytes: Uint8Array): boolean;
	get(cid: string): Uint8Array | undefined;
	del(cid: string): void;
	clear(): void;
	stats(): RustEagerBlockCacheStats;
}

/**
 * Native block-exchange components for `DirectBlock`/`RemoteBlocks`
 * (`@peerbit/blocks`): the `BlockMessage` codec, the provider resolution
 * rules and the caches. Part of the same rust-core mode as the DirectStream
 * state machine.
 */
export interface RustBlockExchange {
	encodeBlockRequest(cid: string): Uint8Array;
	encodeBlockResponse(cid: string, bytes: Uint8Array): Uint8Array;
	decodeBlockMessage(payload: Uint8Array): RustDecodedBlockMessage;
	normalizeProviderHints(
		providers: string[],
		me: string,
		limit: number,
	): string[];
	pickRequestBatch(providers: string[], me: string, attempt: number): string[];
	defaultProviderCandidates(
		negotiated: string[],
		connected: string[],
		me: string,
	): string[];
	createProviderCache(init: {
		me: string;
		maxEntries: number;
		ttlMs: number;
		maxProvidersPerCid: number;
	}): RustBlockProviderCache;
	createEagerCache(init: {
		maxEntries: number;
		maxBytes: number;
		ttlMs: number;
	}): RustEagerBlockCache;
}

/**
 * A decoded `/peerbit/topic-control-plane` message produced by the native
 * codec (borsh `PubSubMessage` variants 0-7).
 */
export type RustDecodedPubSubMessage =
	| {
			type: "data";
			topics: string[];
			strict: boolean;
			/** View into the input payload (no copy). */
			data: Uint8Array;
	  }
	| { type: "subscribe"; topics: string[]; requestSubscribers: boolean }
	| { type: "unsubscribe"; topics: string[] }
	| { type: "get-subscribers"; topics: string[] }
	| { type: "topic-root-candidates"; candidates: string[] }
	| {
			type: "peer-unavailable";
			publicKeyHash: string;
			session: bigint;
			timestamp: bigint;
			topics: string[];
	  }
	| { type: "topic-root-query"; requestId: number; topic: string }
	| {
			type: "topic-root-query-response";
			requestId: number;
			topic: string;
			root?: string;
	  };

/**
 * `TopicRootDirectory` root-resolution state backed by the native core
 * (explicit per-topic roots plus the normalized deterministic candidate
 * set). Trackers and the resolver callback stay host-side.
 */
export interface RustTopicRootDirectoryState {
	setRoot(topic: string, root: string): void;
	deleteRoot(topic: string): void;
	getRoot(topic: string): string | undefined;
	setDefaultCandidates(candidates: string[]): void;
	getDefaultCandidates(): string[];
	resolveDeterministicCandidate(topic: string): string | undefined;
}

/**
 * Native topic-control-plane components consumed by `@peerbit/pubsub`: the
 * `PubSubMessage` codec, the topic hashing that keys shard mapping and
 * deterministic root selection, the root-directory state and the
 * subscribe-state convergence rules. The observable subscription maps stay
 * host-side (they are public API); every protocol decision that feeds them
 * runs natively.
 */
export interface RustTopicControl {
	encodePubSubData(
		topics: string[],
		strict: boolean,
		data: Uint8Array,
	): Uint8Array;
	encodeSubscribe(topics: string[], requestSubscribers: boolean): Uint8Array;
	encodeUnsubscribe(topics: string[]): Uint8Array;
	encodeGetSubscribers(topics: string[]): Uint8Array;
	encodeTopicRootCandidates(candidates: string[]): Uint8Array;
	encodePeerUnavailable(
		publicKeyHash: string,
		session: bigint,
		timestamp: bigint,
		topics: string[],
	): Uint8Array;
	encodeTopicRootQuery(requestId: number, topic: string): Uint8Array;
	encodeTopicRootQueryResponse(
		requestId: number,
		topic: string,
		root?: string,
	): Uint8Array;
	decodePubSubMessage(payload: Uint8Array): RustDecodedPubSubMessage;
	/** `getShardTopicForUserTopic`: user topic -> internal shard topic. */
	shardTopic(topic: string, shardCount: number, prefix: string): string;
	/**
	 * `normalizeAutoTopicRootCandidates`: dedupe, include self, sort and cap
	 * at the auto-candidate bound.
	 */
	normalizeAutoCandidates(candidates: string[], me: string): string[];
	/**
	 * The `subscriptionStateIsLatest` comparison rule. `lasts` carries
	 * interleaved (session, timestamp) watermark pairs for the relevant
	 * topics that have one; the watermark write-back stays host-side.
	 */
	subscriptionIsLatest(
		lasts: BigUint64Array,
		session: bigint,
		timestamp: bigint,
	): boolean;
	/**
	 * The subscribe-apply replacement rule: replace tracked subscription data
	 * for new subscribers or strictly newer sessions, else only refresh.
	 */
	subscribeShouldReplace(
		existingSession: bigint | undefined,
		session: bigint,
	): boolean;
	createRootDirectoryState(): RustTopicRootDirectoryState;
}

/** Decoded fanout-tree frames produced by the native codec. The decoders
 * mirror the tolerance of the TS parsers exactly: `undefined` means the
 * frame fails the same minimum-length checks, and truncated lists end at
 * the same element. Multiaddr bytes are returned raw; validity filtering
 * (and `multiaddr()` construction) stays host-side in both modes. */
export type RustDecodedFanoutJoinReq = {
	reqId: number;
	bidPerByte: number;
	parentUpgradeReservationToken: number;
};

export type RustDecodedFanoutJoinAccept = {
	parentLevel: number;
	parentRouteFromRoot: string[];
	haveRange?: { haveFrom: number; haveToExclusive: number };
};

export type RustDecodedFanoutJoinReject = {
	reason: number;
	redirects: { hash: string; addrs: Uint8Array[] }[];
};

export type RustDecodedFanoutUnicast = {
	ackToken?: bigint;
	route: string[];
	replyRoute?: string[];
	payloadOffset: number;
};

export type RustDecodedFanoutTrackerReplyEntry = {
	hash: string;
	level: number;
	freeSlots: number;
	bidPerByte: number;
	addrs: Uint8Array[];
};

export type RustDecodedFanoutParentProbeReply = {
	reqId: number;
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
};

export type RustDecodedFanoutProviderEntry = {
	hash: string;
	addrs: Uint8Array[];
};

/** `normalizeParentUpgradePolicy` result shape (fanout-tree-parent-upgrade). */
export type RustParentUpgradePolicy = {
	intervalMs: number;
	leafOnly: boolean;
	minLevelGain: number;
	rootMinLevelGain: number;
	rootMinSubtreeGain: number;
	nonRootMinLevelGain: number;
	minFreeSlots: number;
	rootMinFreeSlots: number;
	maxChildLoadRatio: number;
	rootMaxChildLoadRatio: number;
	staleRootProbeProbability: number;
	cooldownMs: number;
	quietMs: number;
	repairQuietMs: number;
	maxPerPeer: number;
	repairGuard: boolean;
	dataGuard: boolean;
	mode: "direct" | "probe" | "shadow";
	verifyStaleRootCapacity: boolean;
	failedBackoff: { minMs: number; maxMs: number };
	probe: {
		timeoutMs: number;
		maxPerRound: number;
		maxLagMessages: number;
		rejectCooldownMs: number;
		rejectCooldownMaxMs: number;
	};
	shadow: {
		observeMs: number;
		minObservations: number;
		dualPathMs: number;
		dualPathMinMessages: number;
	};
};

export type RustParentUpgradeOptions = {
	parentUpgradeIntervalMs?: number;
	parentUpgradeLeafOnly?: boolean;
	parentUpgradeMinLevelGain?: number;
	parentUpgradeRootMinLevelGain?: number;
	parentUpgradeRootMinSubtreeGain?: number;
	parentUpgradeNonRootMinLevelGain?: number;
	parentUpgradeMinFreeSlots?: number;
	parentUpgradeRootMinFreeSlots?: number;
	parentUpgradeMaxChildLoadRatio?: number;
	parentUpgradeRootMaxChildLoadRatio?: number;
	parentUpgradeCooldownMs?: number;
	parentUpgradeFailedBackoffMinMs?: number;
	parentUpgradeFailedBackoffMaxMs?: number;
	parentUpgradeQuietMs?: number;
	parentUpgradeRepairQuietMs?: number;
	parentUpgradeMaxPerPeer?: number;
	parentUpgradeRepairGuard?: boolean;
	parentUpgradeDataGuard?: boolean;
	parentUpgradeMode?: string;
	parentUpgradeVerifyStaleRootCapacity?: boolean;
	parentUpgradeStaleRootProbeProbability?: number;
	parentProbeTimeoutMs?: number;
	parentProbeMaxPerRound?: number;
	parentProbeMaxLagMessages?: number;
	parentProbeRejectCooldownMs?: number;
	parentProbeRejectCooldownMaxMs?: number;
	parentShadowObserveMs?: number;
	parentShadowMinObservations?: number;
	parentShadowDualPathMs?: number;
	parentShadowDualPathMinMessages?: number;
};

export type RustParentUpgradeGateState = {
	children: { size: number };
	missingSeqs: { size: number };
	lastRepairSentAt: number;
	endSeqExclusive: number;
	parentUpgradeRetryAfterSeq: number;
	maxSeqSeen: number;
	parentUpgradeCount: number;
	parentUpgradeBackoffUntil: number;
	parentUpgradeLastAt: number;
	lastParentDataAt: number;
	lastParentUpgradeActivityAt?: number;
};

export type RustParentUpgradeGateOptions = {
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

/**
 * Native fanout-tree components consumed by `@peerbit/pubsub`'s FanoutTree
 * in rust-core mode: the complete `/peerbit/fanout-tree/0.5.0` big-endian
 * frame codec (byte-identical to `fanout-tree-codec.ts`, all message kinds
 * MSG_JOIN_REQ(1)..MSG_PARENT_PROBE_REPLY(41)) plus the parent-upgrade
 * policy/gate decisions merged in PR #911. The channel state machine,
 * timers and events stay host-side.
 */
export interface RustFanoutTree {
	encodeJoinReq(
		channelKey: Uint8Array,
		reqId: number,
		bidPerByte: number,
		parentUpgradeReservationToken?: number,
	): Uint8Array;
	encodeJoinAccept(
		channelKey: Uint8Array,
		reqId: number,
		level: number,
		parentRouteFromRoot?: string[],
		haveRange?: { haveFrom: number; haveToExclusive: number },
	): Uint8Array;
	encodeJoinReject(
		channelKey: Uint8Array,
		reqId: number,
		reason: number,
		redirects?: { hash: string; addrs: Uint8Array[] }[],
	): Uint8Array;
	encodeKick(channelKey: Uint8Array): Uint8Array;
	encodeEnd(channelKey: Uint8Array, lastSeqExclusive: number): Uint8Array;
	encodeRepairReq(
		channelKey: Uint8Array,
		reqId: number,
		missingSeqs: number[],
	): Uint8Array;
	encodeFetchReq(
		channelKey: Uint8Array,
		reqId: number,
		missingSeqs: number[],
	): Uint8Array;
	encodeIHave(
		channelKey: Uint8Array,
		haveFrom: number,
		haveToExclusive: number,
	): Uint8Array;
	encodeData(payload: Uint8Array): Uint8Array;
	encodePublishProxy(channelKey: Uint8Array, payload: Uint8Array): Uint8Array;
	encodeLeave(channelKey: Uint8Array): Uint8Array;
	encodeUnicast(
		channelKey: Uint8Array,
		route: string[],
		payload: Uint8Array,
		options?: { ackToken?: bigint; replyRoute?: string[] },
	): Uint8Array;
	encodeUnicastAck(
		channelKey: Uint8Array,
		ackToken: bigint,
		route: string[],
	): Uint8Array;
	encodeRouteQuery(
		channelKey: Uint8Array,
		reqId: number,
		targetHash: string,
	): Uint8Array;
	encodeRouteReply(
		channelKey: Uint8Array,
		reqId: number,
		route?: string[],
	): Uint8Array;
	encodeTrackerAnnounce(
		channelKey: Uint8Array,
		ttlMs: number,
		level: number,
		maxChildren: number,
		freeSlots: number,
		bidPerByte: number,
		addrs: { bytes: Uint8Array }[],
	): Uint8Array;
	encodeTrackerQuery(
		channelKey: Uint8Array,
		reqId: number,
		want: number,
	): Uint8Array;
	encodeTrackerReply(
		channelKey: Uint8Array,
		reqId: number,
		entries: {
			hash: string;
			level: number;
			freeSlots: number;
			bidPerByte: number;
			addrs: Uint8Array[];
		}[],
	): Uint8Array;
	encodeTrackerFeedback(
		channelKey: Uint8Array,
		candidateHash: string,
		event: number,
		reason: number,
	): Uint8Array;
	encodeParentProbeReq(
		channelKey: Uint8Array,
		reqId: number,
		minFreeSlots?: number,
		reserveRootCapacity?: boolean,
	): Uint8Array;
	encodeParentProbeReply(
		channelKey: Uint8Array,
		reqId: number,
		options: {
			flags: number;
			level: number;
			maxChildren: number;
			freeSlots: number;
			children: number;
			haveToExclusive: number;
			missingSeqs: number;
			dataWriteDrops: number;
			droppedForwards: number;
			reservationToken?: number;
		},
	): Uint8Array;
	encodeProviderAnnounce(
		namespaceKey: Uint8Array,
		ttlMs: number,
		addrs: { bytes: Uint8Array }[],
	): Uint8Array;
	encodeProviderQuery(
		namespaceKey: Uint8Array,
		reqId: number,
		want: number,
		seed: number,
	): Uint8Array;
	encodeProviderReply(
		namespaceKey: Uint8Array,
		reqId: number,
		entries: { hash: string; addrs: Uint8Array[] }[],
	): Uint8Array;
	encodeProviderSubscribe(
		namespaceKey: Uint8Array,
		want: number,
		ttlMs: number,
	): Uint8Array;
	encodeProviderUnsubscribe(namespaceKey: Uint8Array): Uint8Array;
	encodeProviderNotify(
		namespaceKey: Uint8Array,
		entries: { hash: string; addrs: Uint8Array[] }[],
	): Uint8Array;

	decodeJoinReq(data: Uint8Array): RustDecodedFanoutJoinReq | undefined;
	/** The shared JOIN_ACCEPT/JOIN_REJECT head (reqId) parsed before the
	 * pending-join lookup. */
	decodeJoinResponseReqId(data: Uint8Array): number | undefined;
	decodeJoinAccept(data: Uint8Array): RustDecodedFanoutJoinAccept | undefined;
	decodeJoinReject(data: Uint8Array): RustDecodedFanoutJoinReject | undefined;
	decodeEnd(data: Uint8Array): number | undefined;
	/** MSG_REPAIR_REQ / MSG_FETCH_REQ sequence list. */
	decodeRepairSeqs(data: Uint8Array): number[] | undefined;
	decodeIHave(
		data: Uint8Array,
	): { haveFrom: number; haveToExclusive: number } | undefined;
	decodeUnicast(data: Uint8Array): RustDecodedFanoutUnicast | undefined;
	decodeUnicastAck(
		data: Uint8Array,
	): { ackToken: bigint; route: string[] } | undefined;
	decodeRouteQuery(
		data: Uint8Array,
	): { reqId: number; targetHash?: string } | undefined;
	decodeRouteReply(
		data: Uint8Array,
	): { reqId: number; route: string[] } | undefined;
	decodeTrackerAnnounce(data: Uint8Array):
		| {
				ttlMs: number;
				level: number;
				freeSlots: number;
				bidPerByte: number;
				addrs: Uint8Array[];
		  }
		| undefined;
	decodeTrackerQuery(
		data: Uint8Array,
	): { reqId: number; want: number } | undefined;
	decodeTrackerReply(
		data: Uint8Array,
	):
		| { reqId: number; entries: RustDecodedFanoutTrackerReplyEntry[] }
		| undefined;
	decodeTrackerFeedback(
		data: Uint8Array,
	): { candidateHash: string; event: number; reason: number } | undefined;
	decodeParentProbeReq(data: Uint8Array):
		| {
				reqId: number;
				minFreeSlots: number;
				reserveRootCapacity: boolean;
		  }
		| undefined;
	decodeParentProbeReply(
		data: Uint8Array,
		hash: string,
	): RustDecodedFanoutParentProbeReply | undefined;
	decodeProviderAnnounce(
		data: Uint8Array,
	): { ttlMs: number; addrs: Uint8Array[] } | undefined;
	decodeProviderQuery(
		data: Uint8Array,
	): { reqId: number; want: number; seed: number } | undefined;
	decodeProviderReply(
		data: Uint8Array,
	):
		| { reqId: number; entries: RustDecodedFanoutProviderEntry[] }
		| undefined;
	decodeProviderNotify(
		data: Uint8Array,
	): { entries: RustDecodedFanoutProviderEntry[] } | undefined;
	decodeProviderSubscribe(
		data: Uint8Array,
	): { want: number; ttlMs: number } | undefined;

	/** `normalizeParentUpgradePolicy` (PR #911). */
	normalizeParentUpgradePolicy(
		options: RustParentUpgradeOptions,
	): RustParentUpgradePolicy;
	/**
	 * `evaluateParentUpgradeGate` (PR #911); applies the retry-after-seq
	 * reset to `state` in place, like the TS implementation.
	 */
	evaluateParentUpgradeGate(
		state: RustParentUpgradeGateState,
		options: RustParentUpgradeGateOptions,
	):
		| { run: true }
		| {
				run: false;
				reason: "leaf" | "repair" | "data" | "cooldown" | "quiet" | "budget";
		  };
}

/**
 * The full native DirectStream core: created by
 * `@peerbit/network-rust`'s `createRustCoreStream()` and passed as
 * `DirectStreamOptions.rustCore`.
 */
export interface RustCoreStream {
	/** Batched inbound decode + signature verification (implies nativeWire). */
	nativeWire?: NativeWire;
	createRoutes(init: RustRoutesInit): RoutesLike;
	createSeenCache(init: { max: number; ttl: number }): RustSeenCache;
	createLanes(init: RustLanesInit): PushableLanes<Uint8Array | Uint8ArrayList>;
	decisions: RustStreamDecisions;
	/** Native block-exchange components consumed by `@peerbit/blocks`. */
	blockExchange?: RustBlockExchange;
	/** Native topic-control-plane components consumed by `@peerbit/pubsub`. */
	topicControl?: RustTopicControl;
	/** Native fanout-tree components consumed by `@peerbit/pubsub`. */
	fanout?: RustFanoutTree;
}

/**
 * Test-only injection point: when the `PEERBIT_STREAM_RUST_CORE` env flag is
 * set, DirectStream instances constructed without an explicit `rustCore`
 * option pick up the core installed on globalThis under this key. This lets
 * the full @peerbit/stream test-suite run unmodified against the native core
 * (`rustCore: false` opts a specific instance out).
 */
export const RUST_CORE_GLOBAL_KEY = "__peerbitStreamRustCore";

export const resolveInjectedRustCore = (): RustCoreStream | undefined => {
	const processLike = (
		globalThis as {
			process?: { env?: Record<string, string | undefined> };
		}
	).process;
	if (!processLike?.env?.PEERBIT_STREAM_RUST_CORE) {
		return undefined;
	}
	return (globalThis as Record<string, unknown>)[RUST_CORE_GLOBAL_KEY] as
		| RustCoreStream
		| undefined;
};
