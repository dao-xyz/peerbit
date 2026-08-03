import { type PeerId as Libp2pPeerId } from "@libp2p/interface";
import {
	PublicSignKey,
	getPublicKeyFromPeerId,
	sha256Sync,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	assertCanonicalTopicRootCandidates,
	assertTopicRootCandidatesFrame,
	DataEvent,
	GetSubscribers,
	isCanonicalTopicRootCandidate,
	PeerUnavailable,
	type PubSub,
	PubSubData,
	type PubSubEvents,
	PubSubMessage,
	PublishEvent,
	Subscribe,
	SubscriptionData,
	SubscriptionEvent,
	TOPIC_ROOT_CANDIDATES_MAX,
	TOPIC_ROOT_CANDIDATE_CLAIMS_MAX,
	TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES,
	TopicRootCandidateClaims,
	TopicRootCandidates,
	TopicRootQuery,
	TopicRootQueryResponse,
	UnsubcriptionEvent,
	type UnsubscriptionReason,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import {
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
	type PeerStreams,
	type RustTopicControl,
} from "@peerbit/stream";
import {
	AcknowledgeAnyWhere,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	DeliveryError,
	type ExpiresAtOptions,
	type IdOptions,
	MessageHeader,
	NotStartedError,
	type PriorityOptions,
	type ResponsePriorityOptions,
	type RouteHint,
	SilentDelivery,
	type WithExtraSigners,
	deliveryModeHasReceiver,
	getMsgId,
	isAcknowledgeAnyWhereDeliveryMode,
	isAcknowledgeDeliveryMode,
	isAnyWhereDeliveryMode,
	isSilentDeliveryMode,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError, delay } from "@peerbit/time";
import { Uint8ArrayList } from "uint8arraylist";
import { equals as bytesEqual } from "uint8arrays";
import { toUint8Array } from "./bytes.js";
import {
	type DebouncedAccumulatorCounterMap,
	debouncedAccumulatorSetCounter,
} from "./debounced-set.js";
import { FanoutChannel } from "./fanout-channel.js";
import type {
	FanoutTree,
	FanoutTreeChannelOptions,
	FanoutTreeDataEvent,
	FanoutTreeJoinOptions,
} from "./fanout-tree.js";
import {
	TopicRootControlPlane,
	type TopicRootResolutionOptions,
} from "./topic-root-control-plane.js";

export * from "./fanout-tree.js";
// The complete /peerbit/fanout-tree/0.5.0 wire codec and the
// parent-upgrade decision module (the TS references the native rust-core
// implementations are parity-tested against).
export * as fanoutWire from "./fanout-tree-codec.js";
export * as fanoutParentUpgrade from "./fanout-tree-parent-upgrade.js";
export * from "./fanout-channel.js";
export * from "./topic-root-control-plane.js";
export { toUint8Array } from "./bytes.js";

export const logger = loggerFn("peerbit:transport:topic-control-plane");
const warn = logger.newScope("warn");

const utf8Encoder = new TextEncoder();
/**
 * Offset of the `data` bytes inside a borsh-serialized `PubSubData`
 * (variant byte + topics vec + strict bool + data length prefix).
 */
const preEncodedPubSubDataOffset = (topics: string[]): number => {
	let offset = 1 + 4; // variant + topics vec length
	for (const topic of topics) {
		offset += 4 + utf8Encoder.encode(topic).byteLength;
	}
	return offset + 1 + 4; // strict + data length prefix
};
const logError = (e?: { message: string }) => {
	logger.error(e?.message);
};
const logErrorIfStarted = (e?: { message: string }) => {
	e instanceof NotStartedError === false && logError(e);
};

const withAbort = async <T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> => {
	if (!signal) return promise;
	if (signal.aborted) {
		void promise.catch(() => {});
		throw signal.reason ?? new AbortError("Operation was aborted");
	}
	return new Promise<T>((resolve, reject) => {
		const onAbort = () => {
			cleanup();
			reject(signal.reason ?? new AbortError("Operation was aborted"));
		};
		const cleanup = () => {
			try {
				signal.removeEventListener("abort", onAbort);
			} catch {
				// ignore
			}
		};
		signal.addEventListener("abort", onAbort, { once: true });
		promise.then(
			(v) => {
				cleanup();
				resolve(v);
			},
			(e) => {
				cleanup();
				reject(e);
			},
		);
	});
};

const throwIfAborted = (signal?: AbortSignal): void => {
	if (signal?.aborted) {
		throw signal.reason ?? new AbortError("Operation was aborted");
	}
};

type AbortSignalLink = {
	signal: AbortSignal;
	clear: () => void;
};

const linkAbortSignals = (
	signals: Array<AbortSignal | undefined>,
): AbortSignalLink => {
	const unique = [...new Set(signals.filter((signal) => signal !== undefined))];
	if (unique.length === 1) {
		return { signal: unique[0]!, clear: () => {} };
	}

	const controller = new AbortController();
	const listeners = new Map<AbortSignal, () => void>();
	const clear = () => {
		for (const [signal, listener] of listeners) {
			signal.removeEventListener("abort", listener);
		}
		listeners.clear();
	};
	const abortFrom = (signal: AbortSignal) => {
		clear();
		controller.abort(
			signal.reason ?? new AbortError("Topic-root resolution aborted"),
		);
	};

	const aborted = unique.find((signal) => signal.aborted);
	if (aborted) {
		abortFrom(aborted);
	} else {
		for (const signal of unique) {
			const listener = () => abortFrom(signal);
			listeners.set(signal, listener);
			signal.addEventListener("abort", listener, { once: true });
		}
	}
	return { signal: controller.signal, clear };
};

const SUBSCRIBER_CACHE_MAX_ENTRIES_HARD_CAP = 100_000;
const SUBSCRIBER_CACHE_DEFAULT_MAX_ENTRIES = 4_096;
const DEFAULT_FANOUT_PUBLISH_IDLE_CLOSE_MS = 60_000;
const DEFAULT_FANOUT_PUBLISH_MAX_EPHEMERAL_CHANNELS = 64;
const DEFAULT_PUBSUB_SHARD_COUNT = 256;
const PUBSUB_SHARD_COUNT_HARD_CAP = 16_384;
const DEFAULT_PUBSUB_SHARD_TOPIC_PREFIX = "/peerbit/pubsub-shard/1/";
const AUTO_TOPIC_ROOT_CANDIDATE_UPDATE_COOLDOWN_MS = 2_000;
const TOPIC_CONTROL_PLANE_PROTOCOL_V2_1 = "/peerbit/topic-control-plane/2.1.0";
const TOPIC_CONTROL_PLANE_PROTOCOL_V2_0 = "/peerbit/topic-control-plane/2.0.0";
const TOPIC_ROOT_CANDIDATE_CLAIM_DOMAIN = utf8Encoder.encode(
	"peerbit/topic-root-candidate-claim/v1\0",
);
const TOPIC_ROOT_CANDIDATE_SCOPE_DOMAIN =
	"peerbit/topic-root-candidate-scope/v1";
const TOPIC_ROOT_CANDIDATE_CLAIM_MAX_LIFETIME_MS = 5 * 60_000;
const TOPIC_ROOT_CANDIDATE_CLAIM_FUTURE_SKEW_MS = 30_000;
const TOPIC_ROOT_CANDIDATE_CLAIM_REFRESH_MS = 60_000;
const TOPIC_ROOT_CANDIDATE_CLAIM_REFRESH_JITTER_MS = 10_000;
const TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_BURST = 128;
const TOPIC_ROOT_CANDIDATE_CLAIM_GLOBAL_VERIFY_BURST = 512;
const TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_REFILL_MS = 60_000;
const TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_PEERS_MAX = 4_096;
const sameCandidates = (left: string[], right: string[]) =>
	left.length === right.length &&
	left.every((candidate, index) => candidate === right[index]);
// Topic-root queries may need to wait for the responder to finish opening an
// outbound stream back to the requester after an inbound-only dial.
const DEFAULT_TOPIC_ROOT_QUERY_TIMEOUT_MS = 12_000;
const DIRECT_SHARD_ROOT_CONFIRM_TIMEOUT_MS = 2_000;

const DEFAULT_PUBSUB_FANOUT_CHANNEL_OPTIONS: Omit<
	FanoutTreeChannelOptions,
	"role"
> = {
	msgRate: 30,
	msgSize: 1024,
	uploadLimitBps: 5_000_000,
	maxChildren: 24,
	repair: true,
};

export type TopicControlPlaneOptions = DirectStreamOptions & {
	dispatchEventOnSelfPublish?: boolean;
	subscriptionDebounceDelay?: number;
	topicRootControlPlane?: TopicRootControlPlane;
	/**
	 * Fanout overlay used for sharded topic delivery.
	 */
	fanout: FanoutTree;
	/**
	 * Base fanout channel options for shard overlays (applies to both roots and nodes).
	 */
	fanoutChannel?: Partial<Omit<FanoutTreeChannelOptions, "role">>;
	/**
	 * Fanout channel overrides applied only when this node is the shard root.
	 */
	fanoutRootChannel?: Partial<Omit<FanoutTreeChannelOptions, "role">>;
	/**
	 * Fanout channel overrides applied when joining shard overlays as a node.
	 *
	 * This is the primary knob for "leaf-only" subscribers: set `maxChildren=0`
	 * for non-router nodes so they never become relays under churn.
	 */
	fanoutNodeChannel?: Partial<Omit<FanoutTreeChannelOptions, "role">>;
	/**
	 * Fanout join options for overlay topics.
	 */
	fanoutJoin?: FanoutTreeJoinOptions;
	/**
	 * Number of pubsub shards (overlays) used for topic delivery.
	 *
	 * Each user-topic deterministically maps to exactly one shard topic:
	 * `shard = hash(topic) % shardCount`, and subscription joins that shard overlay.
	 *
	 * Default: 256.
	 */
	shardCount?: number;
	/**
	 * Prefix used to form internal shard topics.
	 *
	 * Default: `/peerbit/pubsub-shard/1/`.
	 */
	shardTopicPrefix?: string;
	/**
	 * If enabled, this node will host (open as root) every shard for which it is
	 * the deterministically selected root.
	 *
	 * This is intended for "router"/"supernode" deployments.
	 *
	 * Default: `false`.
	 */
	hostShards?: boolean;
	/**
	 * Fanout-backed topics: require a local `subscribe(topic)` before `publish(topic)` is allowed.
	 *
	 * Default: `false` (publishing without subscribing will temporarily join the overlay).
	 */
	fanoutPublishRequiresSubscribe?: boolean;
	/**
	 * When publishing on a fanout topic without subscribing, keep the ephemeral join
	 * open for this long since the last publish, then auto-leave.
	 *
	 * Default: 60s. Set to `0` to close immediately after each publish.
	 */
	fanoutPublishIdleCloseMs?: number;
	/**
	 * Max number of ephemeral fanout channels kept open concurrently for publish-only usage.
	 *
	 * Default: 64. Set to `0` to disable caching (channels will close after publish).
	 */
	fanoutPublishMaxEphemeralChannels?: number;
	/**
	 * Best-effort bound on cached remote subscribers per topic.
	 *
	 * This controls memory growth at scale and bounds `getSubscribers()` and the
	 * receiver lists used for routing optimizations.
	 */
	subscriberCacheMaxEntries?: number;
};

type EnsureFanoutChannelOptions = {
	ephemeral?: boolean;
	pin?: boolean;
	root?: string;
	rootCandidateGeneration?: string;
	signal?: AbortSignal;
};

export type TopicControlPlaneComponents = DirectStreamComponents;

export type PeerId = Libp2pPeerId | PublicSignKey;

/**
 * Bounded, serializable snapshot of effective topic-control-plane settings.
 *
 * The snapshot intentionally contains values rather than the live fanout
 * option objects so diagnostics can expose it without leaking mutable runtime
 * state or native/WASM handles.
 */
export type TopicControlPlaneRuntimeSnapshot = Readonly<{
	fanout: Readonly<{
		root: Readonly<{ uploadLimitBps: number }>;
		node: Readonly<{ uploadLimitBps: number }>;
	}>;
}>;

const topicHash32 = (topic: string) => {
	let hash = 0x811c9dc5; // FNV-1a
	for (let index = 0; index < topic.length; index++) {
		hash ^= topic.charCodeAt(index);
		hash = (hash * 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

/**
 * Runtime control-plane implementation for pubsub topic membership + forwarding.
 */
export class TopicControlPlane
	extends DirectStream<PubSubEvents>
	implements PubSub
{
	// Tracked topics -> remote subscribers (best-effort).
	public topics: Map<string, Map<string, SubscriptionData>>;
	// Remote peer -> tracked topics.
	public peerToTopic: Map<string, Set<string>>;
	// Local topic -> reference count.
	public subscriptions: Map<string, { counter: number }>;
	// Local topics requested via debounced subscribe, not yet applied in `subscriptions`.
	private pendingSubscriptions: Set<string>;
	/**
	 * Per-shard batching of Subscribe{requestSubscribers} responses. When many
	 * peers announce subscriptions in a burst (group subscribe), answering each
	 * announcer with a routed unicast is quadratic in subscriber count and
	 * melts the fanout route-resolution machinery (#983); a short window lets
	 * one broadcast answer all concurrent announcers instead.
	 */
	private pendingSubscriberResponses: Map<
		string,
		{
			targets: Set<string>;
			topics: Set<string>;
			timer: ReturnType<typeof setTimeout>;
		}
	> = new Map();
	public lastSubscriptionMessages: Map<
		string,
		Map<string, { session: bigint; timestamp: bigint }>
	> = new Map();
	public dispatchEventOnSelfPublish: boolean;
	public readonly topicRootControlPlane: TopicRootControlPlane;
	public readonly subscriberCacheMaxEntries: number;
	public readonly fanout: FanoutTree;
	// Native topic-control components of the rust-core DirectStream engine
	// (`@peerbit/network-rust`). When set, the PubSubMessage codec, the topic
	// hashing behind shard mapping and root selection, the root-directory
	// state and the subscribe-state convergence rules run natively; the
	// observable subscription maps and all events stay unchanged. Unset (the
	// default) leaves every code path as-is.
	private readonly nativeTopicControl?: RustTopicControl;

	private debounceSubscribeAggregator: DebouncedAccumulatorCounterMap;
	private debounceUnsubscribeAggregator: DebouncedAccumulatorCounterMap;

	private readonly shardCount: number;
	private readonly shardTopicPrefix: string;
	private readonly topicRootCandidateClaimData: Uint8Array;
	private readonly hostShards: boolean;
	private readonly shardRootCache = new Map<string, { root: string; authoritative: boolean }>();
	private readonly shardTopicCache = new Map<string, string>();
	private readonly shardRefCounts = new Map<string, number>();
	private readonly pinnedShards = new Set<string>();

	private readonly fanoutRootChannelOptions: Omit<
		FanoutTreeChannelOptions,
		"role"
	>;
	private readonly fanoutNodeChannelOptions: Omit<
		FanoutTreeChannelOptions,
		"role"
	>;
	private readonly fanoutJoinOptions?: FanoutTreeJoinOptions;
	private readonly fanoutPublishRequiresSubscribe: boolean;
	private readonly fanoutPublishIdleCloseMs: number;
	private readonly fanoutPublishMaxEphemeralChannels: number;

	// If no shard-root candidates are configured, we fall back to an "auto" mode:
	// start with `[self]` and expand candidates as underlay peers connect.
	// This keeps small ad-hoc networks working without explicit bootstraps.
	private autoTopicRootCandidates = false;
	private autoTopicRootCandidateSet?: Set<string>;
	private pendingAutoTopicRootCandidates?: string[];
	private autoTopicRootCandidateUpdateTimer?: ReturnType<typeof setTimeout>;
	private reconcileShardOverlaysInFlight?: Promise<void>;
	private reconcileShardOverlaysDirty = false;
	private topicControlPlaneStopping = false;
	private topicControlPlaneLifecycleRevision = 0;
	private topicRootResolutionAbortController = new AbortController();
	private topicRootCandidateResolutionGeneration?: string;
	private topicRootCandidateResolutionAbortController = new AbortController();
	private hostOwnedShardRootsInFlight?: Promise<void>;
	private hostOwnedShardRootsDirty = false;
	private readonly signedTopicRootCandidateClaims = new Map<
		string,
		{ bytes: Uint8Array; timestamp: bigint; expires: bigint }
	>();
	private localSignedTopicRootCandidateClaim?: {
		bytes: Uint8Array;
		timestamp: bigint;
		expires: bigint;
	};
	private topicRootCandidateClaimMaintenanceTimer?: ReturnType<
		typeof setTimeout
	>;
	private readonly topicRootCandidateClaimVerifyBudgets = new Map<
		string,
		{ remaining: number; refilledAt: number }
	>();
	private topicRootCandidateClaimGlobalVerifyBudget = {
		remaining: TOPIC_ROOT_CANDIDATE_CLAIM_GLOBAL_VERIFY_BURST,
		refilledAt: Date.now(),
	};
	private _onFanoutPeerUnreachable?: (
		ev: CustomEvent<{ topic: string; root: string; publicKeyHash: string }>,
	) => void;

	private fanoutChannels = new Map<
		string,
		{
			root: string;
			channel: FanoutChannel;
			join: Promise<void>;
			onData: (ev: CustomEvent<FanoutTreeDataEvent>) => void;
			onUnicast: (ev: any) => void;
			ephemeral: boolean;
			lastUsedAt: number;
			idleCloseTimeout?: ReturnType<typeof setTimeout>;
		}
	>();
	private ensureFanoutChannelInFlight = new Map<
		string,
		{
			abortController: AbortController;
			candidateGeneration: string;
			lifecycleRevision: number;
			opening: Promise<void>;
			settled: Promise<void>;
		}
	>();
	private pendingTopicRootQueries = new Map<
		number,
		{
			expectedPeerHash: string;
			topic: string;
			resolve: (root: string | undefined) => void;
			timer: ReturnType<typeof setTimeout>;
		}
	>();
	private nextTopicRootQueryId = 1;

	constructor(
		components: TopicControlPlaneComponents,
		props?: TopicControlPlaneOptions,
	) {
		super(
			components,
			[TOPIC_CONTROL_PLANE_PROTOCOL_V2_1, TOPIC_CONTROL_PLANE_PROTOCOL_V2_0],
			props,
		);
		this.subscriptions = new Map();
		this.pendingSubscriptions = new Set();
		this.topics = new Map();
		this.peerToTopic = new Map();

		this.topicRootControlPlane =
			props?.topicRootControlPlane || new TopicRootControlPlane();
		this.nativeTopicControl = this.rustCore?.topicControl;
		if (this.nativeTopicControl) {
			this.topicRootControlPlane.adoptNativeDirectoryState(
				this.nativeTopicControl.createRootDirectoryState(),
			);
		}
		this.dispatchEventOnSelfPublish =
			props?.dispatchEventOnSelfPublish || false;

		if (!props?.fanout) {
			throw new Error(
				"TopicControlPlane requires a FanoutTree instance (options.fanout)",
			);
		}
		this.fanout = props.fanout;

		// Default to a local-only shard-root candidate set so standalone peers can
		// subscribe/publish without explicit bootstraps. Signed peer claims expand
		// the set opportunistically as the ad-hoc overlay connects.
		if (this.topicRootControlPlane.getTopicRootCandidates().length === 0) {
			this.autoTopicRootCandidates = true;
			this.autoTopicRootCandidateSet = new Set([this.publicKeyHash]);
			this.topicRootControlPlane.setTopicRootCandidates([this.publicKeyHash]);
		}

		const requestedShardCount = props?.shardCount ?? DEFAULT_PUBSUB_SHARD_COUNT;
		this.shardCount = Math.min(
			PUBSUB_SHARD_COUNT_HARD_CAP,
			Math.max(1, Math.floor(requestedShardCount)),
		);
		const prefix = props?.shardTopicPrefix ?? DEFAULT_PUBSUB_SHARD_TOPIC_PREFIX;
		this.shardTopicPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
		const scopeDigest = sha256Sync(
			utf8Encoder.encode(
				JSON.stringify([
					TOPIC_ROOT_CANDIDATE_SCOPE_DOMAIN,
					this.shardTopicPrefix,
					this.shardCount,
				]),
			),
		);
		this.topicRootCandidateClaimData = new Uint8Array(
			TOPIC_ROOT_CANDIDATE_CLAIM_DOMAIN.byteLength + scopeDigest.byteLength,
		);
		this.topicRootCandidateClaimData.set(TOPIC_ROOT_CANDIDATE_CLAIM_DOMAIN, 0);
		this.topicRootCandidateClaimData.set(
			scopeDigest,
			TOPIC_ROOT_CANDIDATE_CLAIM_DOMAIN.byteLength,
		);
		this.hostShards = props?.hostShards ?? false;

		const baseFanoutChannelOptions = {
			...DEFAULT_PUBSUB_FANOUT_CHANNEL_OPTIONS,
			...(props?.fanoutChannel || {}),
		} as Omit<FanoutTreeChannelOptions, "role">;
		this.fanoutRootChannelOptions = {
			...baseFanoutChannelOptions,
			...(props?.fanoutRootChannel || {}),
		} as Omit<FanoutTreeChannelOptions, "role">;
		this.fanoutNodeChannelOptions = {
			...baseFanoutChannelOptions,
			...(props?.fanoutNodeChannel || {}),
		} as Omit<FanoutTreeChannelOptions, "role">;
		this.fanoutJoinOptions = props?.fanoutJoin;

		this.fanoutPublishRequiresSubscribe =
			props?.fanoutPublishRequiresSubscribe ?? false;
		const requestedIdleCloseMs =
			props?.fanoutPublishIdleCloseMs ?? DEFAULT_FANOUT_PUBLISH_IDLE_CLOSE_MS;
		this.fanoutPublishIdleCloseMs = Math.max(
			0,
			Math.floor(requestedIdleCloseMs),
		);
		const requestedMaxEphemeral =
			props?.fanoutPublishMaxEphemeralChannels ??
			DEFAULT_FANOUT_PUBLISH_MAX_EPHEMERAL_CHANNELS;
		this.fanoutPublishMaxEphemeralChannels = Math.max(
			0,
			Math.floor(requestedMaxEphemeral),
		);

		const requestedSubscriberCacheMaxEntries =
			props?.subscriberCacheMaxEntries ?? SUBSCRIBER_CACHE_DEFAULT_MAX_ENTRIES;
		this.subscriberCacheMaxEntries = Math.min(
			SUBSCRIBER_CACHE_MAX_ENTRIES_HARD_CAP,
			Math.max(1, Math.floor(requestedSubscriberCacheMaxEntries)),
		);

		this.debounceSubscribeAggregator = debouncedAccumulatorSetCounter(
			(set) => this._subscribe([...set.values()]),
			props?.subscriptionDebounceDelay ?? 50,
			(error) =>
				logger.error(`Debounced subscribe failed: ${error?.message ?? error}`),
		);
		// NOTE: Unsubscribe should update local state immediately and batch only the
		// best-effort network announcements to avoid teardown stalls (program close).
		this.debounceUnsubscribeAggregator = debouncedAccumulatorSetCounter(
			(set) => this._announceUnsubscribe([...set.values()]),
			props?.subscriptionDebounceDelay ?? 50,
			(error) =>
				logger.error(
					`Debounced unsubscribe announce failed: ${error?.message ?? error}`,
				),
		);
	}

	/**
	 * Return a detached, read-only snapshot of effective runtime settings.
	 */
	public getRuntimeSnapshot(): TopicControlPlaneRuntimeSnapshot {
		return Object.freeze({
			fanout: Object.freeze({
				root: Object.freeze({
					uploadLimitBps: Math.max(
						0,
						Math.floor(this.fanoutRootChannelOptions.uploadLimitBps),
					),
				}),
				node: Object.freeze({
					uploadLimitBps: Math.max(
						0,
						Math.floor(this.fanoutNodeChannelOptions.uploadLimitBps),
					),
				}),
			}),
		});
	}

	/**
	 * Configure deterministic topic-root candidates and disable the pubsub "auto"
	 * candidate mode.
	 *
	 * Auto mode is a convenience for small ad-hoc networks where no bootstraps/
	 * routers are configured. Automatic claims authenticate key ownership and
	 * freshness, not liveness, admission, or Sybil resistance; hostile or isolated
	 * deployments should configure explicit candidates. When an explicit candidate
	 * set is provided (e.g. from bootstraps or a test harness), we stop mutating the
	 * candidate set; otherwise shard root resolution can diverge and overlays can
	 * partition (especially in sparse graphs).
	 */
	public setTopicRootCandidates(candidates: string[]) {
		this.topicRootControlPlane.setTopicRootCandidates(candidates);

		// Disable auto mode and clear its signed-claim state/timers.
		this.autoTopicRootCandidates = false;
		this.autoTopicRootCandidateSet = undefined;
		this.clearSignedTopicRootCandidateState();
		this.clearAutoTopicRootCandidateUpdateSchedule();

		// Re-resolve roots under the new mapping.
		this.shardRootCache.clear();
		// Only candidates can become deterministic roots. Avoid doing a full shard
		// scan on non-candidates in large sessions.
		if (candidates.includes(this.publicKeyHash)) {
			this.scheduleHostOwnedShardRoots();
		}
		this.scheduleReconcileShardOverlays();
	}

	public override async start() {
		this.topicControlPlaneStopping = false;
		if (
			this.autoTopicRootCandidates &&
			!this.localSignedTopicRootCandidateClaim
		) {
			this.autoTopicRootCandidateSet = new Set([this.publicKeyHash]);
			this.topicRootControlPlane.setTopicRootCandidates([this.publicKeyHash]);
			this.shardRootCache.clear();
		}
		if (this.topicRootResolutionAbortController.signal.aborted) {
			this.topicRootResolutionAbortController = new AbortController();
		}
		await this.fanout.start();
		this._onFanoutPeerUnreachable =
			this._onFanoutPeerUnreachable ||
			this.onFanoutPeerUnreachable.bind(this);
		await this.fanout.addEventListener(
			"fanout:peer-unreachable",
			this._onFanoutPeerUnreachable as any,
		);
		await super.start();
		if (this.autoTopicRootCandidates) {
			await this.refreshLocalTopicRootCandidateClaim();
			this.scheduleTopicRootCandidateClaimMaintenance();
		}

		if (this.hostShards) {
			await this.hostShardRootsNow();
		}
		if (this.hostOwnedShardRootsDirty) {
			this.scheduleHostOwnedShardRoots();
		}
		if (this.reconcileShardOverlaysDirty) {
			this.scheduleReconcileShardOverlays();
		}
	}

	public override async stop() {
		this.topicControlPlaneStopping = true;
		this.topicRootResolutionAbortController.abort(
			new AbortError("topic control plane stopped"),
		);
		this.clearAutoTopicRootCandidateUpdateSchedule();
		this.clearSignedTopicRootCandidateState();
		this.topicControlPlaneLifecycleRevision += 1;
		this.reconcileShardOverlaysDirty = false;
		for (const opening of this.ensureFanoutChannelInFlight.values()) {
			opening.abortController.abort(
				new AbortError("topic control plane stopped"),
			);
		}
		this.ensureFanoutChannelInFlight.clear();
		if (this._onFanoutPeerUnreachable) {
			this.fanout.removeEventListener(
				"fanout:peer-unreachable",
				this._onFanoutPeerUnreachable as any,
			);
		}
		for (const st of this.fanoutChannels.values()) {
			if (st.idleCloseTimeout) clearTimeout(st.idleCloseTimeout);
			try {
				st.channel.removeEventListener("data", st.onData as any);
			} catch {
				// ignore
			}
			try {
				st.channel.removeEventListener("unicast", st.onUnicast as any);
			} catch {
				// ignore
			}
			try {
				// Shutdown should be bounded and not depend on network I/O.
				await st.channel.leave({ notifyParent: false, kickChildren: false });
			} catch {
				try {
					st.channel.close();
				} catch {
					// ignore
				}
			}
		}
		this.fanoutChannels.clear();
		this.hostOwnedShardRootsDirty = false;

		this.subscriptions.clear();
		this.pendingSubscriptions.clear();
		for (const entry of this.pendingSubscriberResponses.values()) {
			clearTimeout(entry.timer);
		}
		this.pendingSubscriberResponses.clear();
		this.topics.clear();
		this.peerToTopic.clear();
		this.lastSubscriptionMessages.clear();
		this.shardRootCache.clear();
		this.shardTopicCache.clear();
		this.shardRefCounts.clear();
		this.pinnedShards.clear();
		for (const pending of this.pendingTopicRootQueries.values()) {
			clearTimeout(pending.timer);
			pending.resolve(undefined);
		}
		this.pendingTopicRootQueries.clear();

		this.debounceSubscribeAggregator.close();
		this.debounceUnsubscribeAggregator.close();
		await super.stop();
		this.reconcileShardOverlaysDirty = false;
	}

	// Candidate trust starts only after protocol negotiation in `addPeer()`;
	// a transport connection alone cannot bypass signed 2.1 self-claims.
	// Ensure auto-candidate mode converges even when libp2p topology callbacks
	// are delayed or only fire for one side of a connection. `addPeer()` runs for
	// both inbound + outbound protocol streams once the remote public key is known.
	public override addPeer(
		peerId: Libp2pPeerId,
		publicKey: PublicSignKey,
		protocol: string,
		connId: string,
	): PeerStreams {
		const existingPeer = this.peers.get(publicKey.hashcode());
		const peer = super.addPeer(peerId, publicKey, protocol, connId);
		if (this.autoTopicRootCandidates) {
			if (peer.protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_0) {
				this.rebuildAutoTopicRootCandidatesFromClaims();
			}
			if (peer.protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_1) {
				const sendWhenOutboundReady = () => {
					if (
						!this.autoTopicRootCandidates ||
						this.peers.get(publicKey.hashcode()) !== peer
					) {
						return;
					}
					void this.sendAutoTopicRootCandidates([peer]).catch(() => {});
				};
				if (peer.isWritable) {
					void this.sendAutoTopicRootCandidates([peer]).catch(() => {});
				}
				if (!existingPeer) {
					peer.addEventListener("stream:outbound", sendWhenOutboundReady);
					peer.addEventListener(
						"close",
						() =>
							peer.removeEventListener(
								"stream:outbound",
								sendWhenOutboundReady,
							),
						{ once: true },
					);
				}
			}
		}
		return peer;
	}

	protected override async _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const protocol = this.peers.get(hash)?.protocol;
		const removed = await super._removePeer(publicKey);
		if (
			this.autoTopicRootCandidates &&
			protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_0
		) {
			this.rebuildAutoTopicRootCandidatesFromClaims();
		}
		return removed;
	}

	private maybeDisableAutoTopicRootCandidatesIfExternallyConfigured(): boolean {
		if (!this.autoTopicRootCandidates) return false;

		const managed = this.autoTopicRootCandidateSet;
		if (!managed) return false;

		const current = this.topicRootControlPlane.getTopicRootCandidates();
		const externallyConfigured =
			current.length !== managed.size || current.some((c) => !managed.has(c));
		if (!externallyConfigured) return false;

		// Stop mutating the candidate set. Leave the externally configured candidates
		// intact and reconcile shard overlays under the new mapping.
		this.autoTopicRootCandidates = false;
		this.autoTopicRootCandidateSet = undefined;
		this.clearSignedTopicRootCandidateState();
		this.clearAutoTopicRootCandidateUpdateSchedule();
		this.shardRootCache.clear();

		// Ensure we host any shard roots we're now responsible for. This is important
		// in tests where candidates may be configured before protocol streams have
		// fully started; earlier `hostShardRootsNow()` attempts can be skipped,
		// leading to join timeouts.
		this.scheduleHostOwnedShardRoots();
		this.scheduleReconcileShardOverlays();
		return true;
	}

	private clearTopicRootCandidateClaimTimer() {
		if (this.topicRootCandidateClaimMaintenanceTimer) {
			clearTimeout(this.topicRootCandidateClaimMaintenanceTimer);
			this.topicRootCandidateClaimMaintenanceTimer = undefined;
		}
	}

	private clearSignedTopicRootCandidateState() {
		this.clearTopicRootCandidateClaimTimer();
		this.signedTopicRootCandidateClaims.clear();
		this.localSignedTopicRootCandidateClaim = undefined;
		this.topicRootCandidateClaimVerifyBudgets.clear();
		this.topicRootCandidateClaimGlobalVerifyBudget = {
			remaining: TOPIC_ROOT_CANDIDATE_CLAIM_GLOBAL_VERIFY_BURST,
			refilledAt: Date.now(),
		};
	}

	private async refreshLocalTopicRootCandidateClaim(): Promise<boolean> {
		if (
			!this.autoTopicRootCandidates ||
			!this.started ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			return false;
		}
		const lifecycleRevision = this.topicControlPlaneLifecycleRevision;
		const now = Date.now();
		const claim = await this.createMessage(this.topicRootCandidateClaimData, {
			mode: new AnyWhere(),
			priority: 1,
			expiresAt: now + TOPIC_ROOT_CANDIDATE_CLAIM_MAX_LIFETIME_MS,
			skipRecipientValidation: true,
		} as any);
		const bytes = toUint8Array(claim.bytes());
		if (bytes.byteLength > TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES) {
			throw new Error(
				`Local topic-root candidate claim exceeds ${TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES} bytes`,
			);
		}
		if (
			lifecycleRevision !== this.topicControlPlaneLifecycleRevision ||
			!this.autoTopicRootCandidates ||
			!this.started ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			return false;
		}
		const record = {
			bytes,
			timestamp: claim.header.timestamp,
			expires: claim.header.expires,
		};
		const current = this.localSignedTopicRootCandidateClaim;
		if (
			current &&
			(record.timestamp <= current.timestamp ||
				record.expires <= current.expires)
		) {
			return false;
		}
		this.localSignedTopicRootCandidateClaim = record;
		this.retainSignedTopicRootCandidateClaim(this.publicKeyHash, record);
		this.rebuildAutoTopicRootCandidatesFromClaims();
		return true;
	}

	private scheduleTopicRootCandidateClaimMaintenance(minDelayMs = 0) {
		if (
			!this.autoTopicRootCandidates ||
			!this.started ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			return;
		}
		this.clearTopicRootCandidateClaimTimer();
		const now = Date.now();
		const local = this.localSignedTopicRootCandidateClaim;
		let dueAt = local
			? Number(local.timestamp) +
				TOPIC_ROOT_CANDIDATE_CLAIM_REFRESH_MS +
				Math.floor(Math.random() * TOPIC_ROOT_CANDIDATE_CLAIM_REFRESH_JITTER_MS)
			: now;
		for (const claim of this.signedTopicRootCandidateClaims.values()) {
			dueAt = Math.min(dueAt, Number(claim.expires) + 1);
		}
		if (local) dueAt = Math.min(dueAt, Number(local.expires) + 1);
		const delayMs = Math.max(minDelayMs, Math.min(2_147_483_647, dueAt - now));
		const timer = setTimeout(() => {
			if (this.topicRootCandidateClaimMaintenanceTimer !== timer) return;
			this.topicRootCandidateClaimMaintenanceTimer = undefined;
			void (async () => {
				try {
					const removed = this.pruneExpiredTopicRootCandidateClaims();
					const currentLocal = this.localSignedTopicRootCandidateClaim;
					const refreshDue =
						!currentLocal ||
						Date.now() - Number(currentLocal.timestamp) >=
							TOPIC_ROOT_CANDIDATE_CLAIM_REFRESH_MS;
					if (removed) this.rebuildAutoTopicRootCandidatesFromClaims();
					if (
						refreshDue &&
						(await this.refreshLocalTopicRootCandidateClaim())
					) {
						await this.sendSignedTopicRootCandidateClaims([
							this.localSignedTopicRootCandidateClaim!.bytes,
						]);
					}
				} catch (error: any) {
					logErrorIfStarted(error);
					this.scheduleTopicRootCandidateClaimMaintenance(1_000);
					return;
				} finally {
					if (!this.topicRootCandidateClaimMaintenanceTimer) {
						this.scheduleTopicRootCandidateClaimMaintenance();
					}
				}
			})();
		}, delayMs);
		timer.unref?.();
		this.topicRootCandidateClaimMaintenanceTimer = timer;
	}

	private pruneExpiredTopicRootCandidateClaims(
		now: bigint = BigInt(Date.now()),
	) {
		const localClaim = this.localSignedTopicRootCandidateClaim;
		if (localClaim && localClaim.expires <= now) {
			this.localSignedTopicRootCandidateClaim = undefined;
		}
		let changed = false;
		for (const [origin, claim] of this.signedTopicRootCandidateClaims) {
			if (claim.expires <= now) {
				this.signedTopicRootCandidateClaims.delete(origin);
				changed = true;
			}
		}
		return changed;
	}

	private rebuildAutoTopicRootCandidatesFromClaims(): boolean {
		if (!this.autoTopicRootCandidates) return false;
		this.pruneExpiredTopicRootCandidateClaims();
		const legacyDirectCandidates = [...this.peers.values()]
			.filter((peer) => peer.protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_0)
			.map((peer) => peer.publicKey.hashcode());
		const next = [
			...(this.localSignedTopicRootCandidateClaim ? [] : [this.publicKeyHash]),
			...legacyDirectCandidates,
			...this.signedTopicRootCandidateClaims.keys(),
		]
			.filter(isCanonicalTopicRootCandidate)
			.filter(
				(candidate, index, candidates) =>
					candidates.indexOf(candidate) === index,
			)
			.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
			.slice(0, TOPIC_ROOT_CANDIDATES_MAX);
		return this.setAutoTopicRootCandidateSnapshot(next);
	}

	private retainSignedTopicRootCandidateClaim(
		origin: string,
		claim: { bytes: Uint8Array; timestamp: bigint; expires: bigint },
	): boolean {
		const existing = this.signedTopicRootCandidateClaims.get(origin);
		if (existing) {
			if (
				claim.timestamp <= existing.timestamp ||
				claim.expires <= existing.expires
			) {
				return false;
			}
		} else {
			if (
				this.signedTopicRootCandidateClaims.size >= TOPIC_ROOT_CANDIDATES_MAX
			) {
				const highest = [...this.signedTopicRootCandidateClaims.keys()]
					.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
					.at(-1)!;
				if (origin >= highest) return false;
				this.signedTopicRootCandidateClaims.delete(highest);
			}
		}
		this.signedTopicRootCandidateClaims.set(origin, claim);
		return true;
	}

	private consumeTopicRootCandidateClaimVerifyBudget(
		peerHash: string,
	): boolean {
		const now = Date.now();
		if (
			now - this.topicRootCandidateClaimGlobalVerifyBudget.refilledAt >=
			TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_REFILL_MS
		) {
			this.topicRootCandidateClaimGlobalVerifyBudget = {
				remaining: TOPIC_ROOT_CANDIDATE_CLAIM_GLOBAL_VERIFY_BURST,
				refilledAt: now,
			};
		}
		if (this.topicRootCandidateClaimGlobalVerifyBudget.remaining <= 0) {
			return false;
		}
		let budget = this.topicRootCandidateClaimVerifyBudgets.get(peerHash);
		if (!budget) {
			if (
				this.topicRootCandidateClaimVerifyBudgets.size >=
				TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_PEERS_MAX
			) {
				const oldest = this.topicRootCandidateClaimVerifyBudgets.keys().next()
					.value as string | undefined;
				if (oldest) this.topicRootCandidateClaimVerifyBudgets.delete(oldest);
			}
			budget = {
				remaining: TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_BURST,
				refilledAt: now,
			};
			this.topicRootCandidateClaimVerifyBudgets.set(peerHash, budget);
		} else if (
			now - budget.refilledAt >=
			TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_REFILL_MS
		) {
			budget.remaining = TOPIC_ROOT_CANDIDATE_CLAIM_VERIFY_BURST;
			budget.refilledAt = now;
			// Refresh insertion order so bounded eviction remains O(1) and LRU-like.
			this.topicRootCandidateClaimVerifyBudgets.delete(peerHash);
			this.topicRootCandidateClaimVerifyBudgets.set(peerHash, budget);
		}
		if (budget.remaining <= 0) return false;
		budget.remaining -= 1;
		this.topicRootCandidateClaimGlobalVerifyBudget.remaining -= 1;
		return true;
	}

	private async importSignedTopicRootCandidateClaim(
		rawClaim: Uint8Array,
		outerPeerHash: string,
	): Promise<boolean> {
		const lifecycleRevision = this.topicControlPlaneLifecycleRevision;
		if (
			rawClaim.byteLength === 0 ||
			rawClaim.byteLength > TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES
		) {
			return false;
		}
		let claim: DataMessage;
		try {
			claim = DataMessage.from(new Uint8ArrayList(rawClaim));
		} catch {
			return false;
		}
		const canonicalBytes = toUint8Array(claim.bytes());
		if (!bytesEqual(canonicalBytes, rawClaim)) return false;
		if (
			!claim.data ||
			!bytesEqual(claim.data, this.topicRootCandidateClaimData)
		) {
			return false;
		}
		const signatures = claim.header.signatures?.signatures;
		if (!signatures || signatures.length !== 1) return false;
		const signer = signatures[0]?.publicKey;
		if (!signer) return false;
		const origin = signer.hashcode();
		if (!isCanonicalTopicRootCandidate(origin)) return false;
		if (origin === this.publicKeyHash) return false;

		const now = BigInt(Date.now());
		const timestamp = claim.header.timestamp;
		const expires = claim.header.expires;
		if (
			expires <= now ||
			expires <= timestamp ||
			timestamp > now + BigInt(TOPIC_ROOT_CANDIDATE_CLAIM_FUTURE_SKEW_MS) ||
			expires - timestamp > BigInt(TOPIC_ROOT_CANDIDATE_CLAIM_MAX_LIFETIME_MS)
		) {
			return false;
		}

		const pruned = this.pruneExpiredTopicRootCandidateClaims(now);
		if (pruned) this.rebuildAutoTopicRootCandidatesFromClaims();
		const existing = this.signedTopicRootCandidateClaims.get(origin);
		if (existing) {
			if (bytesEqual(existing.bytes, rawClaim)) return false;
			if (timestamp <= existing.timestamp || expires <= existing.expires) {
				return false;
			}
		}

		if (
			!existing &&
			this.signedTopicRootCandidateClaims.size >= TOPIC_ROOT_CANDIDATES_MAX
		) {
			const highest = [...this.signedTopicRootCandidateClaims.keys()].sort(
				(a, b) => (a < b ? -1 : a > b ? 1 : 0),
			);
			if (origin >= highest.at(-1)!) return false;
		}
		if (!this.consumeTopicRootCandidateClaimVerifyBudget(outerPeerHash)) {
			return false;
		}
		this.seedNativeWireVerification(claim, rawClaim);
		let verified = false;
		try {
			verified = await claim.verify(true);
		} catch {
			return false;
		}
		if (!verified) return false;
		if (
			lifecycleRevision !== this.topicControlPlaneLifecycleRevision ||
			expires <= BigInt(Date.now()) ||
			!this.autoTopicRootCandidates ||
			!this.started ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			return false;
		}

		if (
			!this.retainSignedTopicRootCandidateClaim(origin, {
				bytes: rawClaim.slice(),
				timestamp,
				expires,
			})
		) {
			return false;
		}
		this.scheduleTopicRootCandidateClaimMaintenance();
		return true;
	}

	private async sendSignedTopicRootCandidateClaims(
		claims: Uint8Array[],
		targets?: PeerStreams[],
	) {
		if (claims.length === 0) return;
		const streams = (targets ?? [...this.peers.values()]).filter(
			(stream) => stream.protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_1,
		);
		if (streams.length === 0) return;
		for (
			let offset = 0;
			offset < claims.length;
			offset += TOPIC_ROOT_CANDIDATE_CLAIMS_MAX
		) {
			const message = new TopicRootCandidateClaims({
				claims: claims.slice(offset, offset + TOPIC_ROOT_CANDIDATE_CLAIMS_MAX),
			});
			const embedded = await this.createMessage(
				this.encodePubSubMessage(message),
				{
					mode: new AnyWhere(),
					priority: 1,
					skipRecipientValidation: true,
				} as any,
			);
			await this.publishMessageMaybe(this.publicKey, embedded, streams);
		}
	}

	private async sendAutoTopicRootCandidates(streams: PeerStreams[]) {
		if (!this.started) throw new NotStartedError();
		if (streams.length === 0) return;

		const signedStreams = streams.filter(
			(stream) => stream.protocol === TOPIC_CONTROL_PLANE_PROTOCOL_V2_1,
		);
		if (signedStreams.length > 0) {
			if (this.pruneExpiredTopicRootCandidateClaims()) {
				this.rebuildAutoTopicRootCandidatesFromClaims();
			}
			if (
				!this.localSignedTopicRootCandidateClaim &&
				!(await this.refreshLocalTopicRootCandidateClaim())
			) {
				return;
			}
			const localClaim = this.localSignedTopicRootCandidateClaim;
			if (!localClaim) return;
			const claims = [...this.signedTopicRootCandidateClaims.entries()]
				.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
				.map(([, claim]) => claim.bytes);
			await this.sendSignedTopicRootCandidateClaims(claims, signedStreams);
			if (!this.signedTopicRootCandidateClaims.has(this.publicKeyHash)) {
				await this.sendSignedTopicRootCandidateClaims(
					[localClaim.bytes],
					signedStreams,
				);
			}
		}
	}

	private setAutoTopicRootCandidateSnapshot(next: string[]): boolean {
		if (
			!this.autoTopicRootCandidates ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			return false;
		}
		if (this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured()) {
			return false;
		}
		const managed = this.autoTopicRootCandidateSet;
		if (!managed) return false;
		const before = this.pendingAutoTopicRootCandidates ?? [...managed];
		if (sameCandidates(before, next)) return false;

		if (this.autoTopicRootCandidateUpdateTimer) {
			this.pendingAutoTopicRootCandidates = next;
			return true;
		}

		this.scheduleAutoTopicRootCandidateUpdateCooldown();
		this.applyAutoTopicRootCandidates(next);
		return true;
	}

	private applyAutoTopicRootCandidates(next: string[]) {
		this.autoTopicRootCandidateSet = new Set(next);
		this.topicRootControlPlane.setTopicRootCandidates(next);
		this.shardRootCache.clear();
		this.scheduleReconcileShardOverlays();
		this.scheduleHostOwnedShardRoots();
	}

	private scheduleAutoTopicRootCandidateUpdateCooldown() {
		if (this.autoTopicRootCandidateUpdateTimer) return;
		const timer = setTimeout(() => {
			if (this.autoTopicRootCandidateUpdateTimer !== timer) return;
			this.flushPendingAutoTopicRootCandidateUpdate();
		}, AUTO_TOPIC_ROOT_CANDIDATE_UPDATE_COOLDOWN_MS);
		timer.unref?.();
		this.autoTopicRootCandidateUpdateTimer = timer;
	}

	private flushPendingAutoTopicRootCandidateUpdate() {
		if (this.autoTopicRootCandidateUpdateTimer) {
			clearTimeout(this.autoTopicRootCandidateUpdateTimer);
			this.autoTopicRootCandidateUpdateTimer = undefined;
		}
		const next = this.pendingAutoTopicRootCandidates;
		this.pendingAutoTopicRootCandidates = undefined;
		if (!next) return;
		if (
			!this.autoTopicRootCandidates ||
			this.stopping ||
			this.topicControlPlaneStopping
		)
			return;
		if (this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured())
			return;

		// The trailing update starts its own fixed window so another inbound update
		// cannot create a second candidate generation immediately afterwards.
		this.scheduleAutoTopicRootCandidateUpdateCooldown();
		this.applyAutoTopicRootCandidates(next);
	}

	private clearAutoTopicRootCandidateUpdateSchedule() {
		if (this.autoTopicRootCandidateUpdateTimer) {
			clearTimeout(this.autoTopicRootCandidateUpdateTimer);
			this.autoTopicRootCandidateUpdateTimer = undefined;
		}
		this.pendingAutoTopicRootCandidates = undefined;
	}

	private scheduleHostOwnedShardRoots() {
		this.hostOwnedShardRootsDirty = true;
		if (!this.started || this.stopping) return;
		if (this.hostOwnedShardRootsInFlight) return;
		this.hostOwnedShardRootsInFlight = (async () => {
			for (;;) {
				if (!this.started || this.stopping) return;
				if (!this.hostOwnedShardRootsDirty) return;
				this.hostOwnedShardRootsDirty = false;
				try {
					await this.hostShardRootsNow();
				} catch {
					if (!this.started || this.stopping) return;
					this.hostOwnedShardRootsDirty = true;
					await delay(250);
				}
			}
		})().finally(() => {
			this.hostOwnedShardRootsInFlight = undefined;
			if (this.hostOwnedShardRootsDirty && this.started && !this.stopping) {
				this.scheduleHostOwnedShardRoots();
			}
		});
	}

	private scheduleReconcileShardOverlays() {
		const candidateGeneration = this.getTopicRootCandidateGeneration();
		this.syncTopicRootCandidateResolutionSignal(candidateGeneration);
		for (const opening of this.ensureFanoutChannelInFlight.values()) {
			if (opening.candidateGeneration !== candidateGeneration) {
				opening.abortController.abort(
					new AbortError("topic root candidates changed"),
				);
			}
		}
		this.reconcileShardOverlaysDirty = true;
		if (!this.started || this.stopping || this.topicControlPlaneStopping)
			return;
		if (this.reconcileShardOverlaysInFlight) return;
		this.reconcileShardOverlaysInFlight = (async () => {
			for (;;) {
				if (!this.started || this.stopping || this.topicControlPlaneStopping)
					return;
				if (!this.reconcileShardOverlaysDirty) return;
				this.reconcileShardOverlaysDirty = false;
				try {
					await this.reconcileShardOverlays();
				} catch {
					// Fanout streams or roots might not be ready yet. Preserve both
					// failures and root changes that arrived during the active pass.
					if (!this.started || this.stopping || this.topicControlPlaneStopping)
						return;
					this.reconcileShardOverlaysDirty = true;
					await new Promise<void>((resolve) => {
						const timer = setTimeout(resolve, 250);
						timer.unref?.();
					});
				}
			}
		})().finally(() => {
			this.reconcileShardOverlaysInFlight = undefined;
			if (
				this.reconcileShardOverlaysDirty &&
				this.started &&
				!this.stopping &&
				!this.topicControlPlaneStopping
			) {
				this.scheduleReconcileShardOverlays();
			}
		});
	}

	private async reconcileShardOverlays() {
		if (!this.started || this.topicControlPlaneStopping) return;

		const byShard = new Map<string, string[]>();
		for (const topic of this.subscriptions.keys()) {
			const shardTopic = this.getShardTopicForUserTopic(topic);
			byShard.set(shardTopic, [...(byShard.get(shardTopic) ?? []), topic]);
		}

		// Ensure shard overlays are joined using the current root mapping (may
		// migrate channels if roots changed), then re-announce subscriptions.
		await Promise.all(
			[...byShard.entries()].map(async ([shardTopic, userTopics]) => {
				if (userTopics.length === 0) return;
				await this.ensureFanoutChannel(shardTopic, { ephemeral: false });

				const msg = new Subscribe({
					topics: userTopics,
					requestSubscribers: true,
				});
				const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
					mode: new AnyWhere(),
					priority: 1,
					skipRecipientValidation: true,
				} as any);
				const st = this.fanoutChannels.get(shardTopic);
				if (!st) return;
				await st.channel.publish(toUint8Array(embedded.bytes()));
				this.touchFanoutChannel(shardTopic);
			}),
		);
	}

	private isTrackedTopic(topic: string) {
		return this.topics.has(topic);
	}

	private initializeTopic(topic: string) {
		this.topics.get(topic) || this.topics.set(topic, new Map());
	}

	private untrackTopic(topic: string) {
		const peers = this.topics.get(topic);
		this.topics.delete(topic);
		if (!peers) return;
		for (const peerHash of peers.keys()) {
			this.peerToTopic.get(peerHash)?.delete(topic);
			this.lastSubscriptionMessages.get(peerHash)?.delete(topic);
			if (!this.peerToTopic.get(peerHash)?.size) {
				this.peerToTopic.delete(peerHash);
				this.lastSubscriptionMessages.delete(peerHash);
			}
		}
	}

	private initializePeer(publicKey: PublicSignKey) {
		// Remote subscribers can be non-neighbours (learned via relayed topic control).
		// Keep hash->key mapping so later unreachable events can evict stale
		// subscription state for abruptly departed peers.
		this.peerKeyHashToPublicKey.set(publicKey.hashcode(), publicKey);
		this.peerToTopic.get(publicKey.hashcode()) ||
			this.peerToTopic.set(publicKey.hashcode(), new Set());
	}

	private pruneTopicSubscribers(topic: string) {
		const peers = this.topics.get(topic);
		if (!peers) return;

		while (peers.size > this.subscriberCacheMaxEntries) {
			const oldest = peers.keys().next().value as string | undefined;
			if (!oldest) break;
			peers.delete(oldest);
			this.peerToTopic.get(oldest)?.delete(topic);
			this.lastSubscriptionMessages.get(oldest)?.delete(topic);
			if (!this.peerToTopic.get(oldest)?.size) {
				this.peerToTopic.delete(oldest);
				this.lastSubscriptionMessages.delete(oldest);
			}
		}
	}

	private getSubscriptionOverlap(topics?: string[]) {
		const subscriptions: string[] = [];
		if (topics) {
			for (const topic of topics) {
				if (
					this.subscriptions.get(topic) ||
					this.pendingSubscriptions.has(topic)
				) {
					subscriptions.push(topic);
				}
			}
			return subscriptions;
		}
		const seen = new Set<string>();
		for (const [topic] of this.subscriptions) {
			subscriptions.push(topic);
			seen.add(topic);
		}
		for (const topic of this.pendingSubscriptions) {
			if (seen.has(topic)) continue;
			subscriptions.push(topic);
		}
		return subscriptions;
	}

	private clearFanoutIdleClose(st: {
		idleCloseTimeout?: ReturnType<typeof setTimeout>;
	}) {
		if (st.idleCloseTimeout) {
			clearTimeout(st.idleCloseTimeout);
			st.idleCloseTimeout = undefined;
		}
	}

	private scheduleFanoutIdleClose(topic: string) {
		const st = this.fanoutChannels.get(topic);
		if (!st || !st.ephemeral) return;
		this.clearFanoutIdleClose(st);
		if (this.fanoutPublishIdleCloseMs <= 0) return;
		st.idleCloseTimeout = setTimeout(() => {
			const cur = this.fanoutChannels.get(topic);
			if (!cur || !cur.ephemeral) return;
			const idleMs = Date.now() - cur.lastUsedAt;
			if (idleMs >= this.fanoutPublishIdleCloseMs) {
				void this.closeFanoutChannel(topic);
				return;
			}
			this.scheduleFanoutIdleClose(topic);
		}, this.fanoutPublishIdleCloseMs);
	}

	private touchFanoutChannel(topic: string) {
		const st = this.fanoutChannels.get(topic);
		if (!st) return;
		st.lastUsedAt = Date.now();
		if (st.ephemeral) {
			this.scheduleFanoutIdleClose(topic);
		}
	}

	private evictEphemeralFanoutChannels(exceptTopic?: string) {
		const max = this.fanoutPublishMaxEphemeralChannels;
		if (max < 0) return;
		const exceptIsEphemeral = exceptTopic
			? this.fanoutChannels.get(exceptTopic)?.ephemeral === true
			: false;
		const keep = Math.max(0, max - (exceptIsEphemeral ? 1 : 0));

		const candidates: Array<[string, { lastUsedAt: number }]> = [];
		for (const [t, st] of this.fanoutChannels) {
			if (!st.ephemeral) continue;
			if (exceptTopic && t === exceptTopic) continue;
			candidates.push([t, st]);
		}
		if (candidates.length <= keep) return;

		candidates.sort((a, b) => a[1].lastUsedAt - b[1].lastUsedAt);
		const toClose = candidates.length - keep;
		for (let i = 0; i < toClose; i++) {
			const t = candidates[i]![0];
			void this.closeFanoutChannel(t);
		}
	}

	private getShardTopicForUserTopic(topic: string): string {
		const t = topic.toString();
		const cached = this.shardTopicCache.get(t);
		if (cached) return cached;
		const shardTopic = this.nativeTopicControl
			? this.nativeTopicControl.shardTopic(
					t,
					this.shardCount,
					this.shardTopicPrefix,
				)
			: `${this.shardTopicPrefix}${topicHash32(t) % this.shardCount}`;
		this.shardTopicCache.set(t, shardTopic);
		return shardTopic;
	}

	/**
	 * Serialize a control-plane message (borsh `PubSubMessage`); rust-core
	 * mode encodes natively (byte-identical, covered by the parity suite).
	 */
	private encodePubSubMessage(message: PubSubMessage): Uint8Array {
		const native = this.nativeTopicControl;
		if (native) {
			if (message instanceof PubSubData) {
				return native.encodePubSubData(
					message.topics,
					message.strict,
					message.data,
				);
			}
			if (message instanceof Subscribe) {
				return native.encodeSubscribe(
					message.topics,
					message.requestSubscribers,
				);
			}
			if (message instanceof Unsubscribe) {
				return native.encodeUnsubscribe(message.topics);
			}
			if (message instanceof GetSubscribers) {
				return native.encodeGetSubscribers(message.topics);
			}
			if (message instanceof TopicRootCandidates) {
				return native.encodeTopicRootCandidates(message.candidates);
			}
			if (message instanceof PeerUnavailable) {
				return native.encodePeerUnavailable(
					message.publicKeyHash,
					message.session,
					message.timestamp,
					message.topics,
				);
			}
			if (message instanceof TopicRootQuery) {
				return native.encodeTopicRootQuery(message.requestId, message.topic);
			}
			if (message instanceof TopicRootQueryResponse) {
				return native.encodeTopicRootQueryResponse(
					message.requestId,
					message.topic,
					message.root,
				);
			}
		}
		return toUint8Array(message.bytes());
	}

	/**
	 * Parse a control-plane payload; rust-core mode decodes natively into the
	 * same message classes (decode failures throw either way, so callers keep
	 * their fallback behavior).
	 */
	private decodePubSubMessage(bytes: Uint8Array): PubSubMessage {
		if (bytes[0] === 4) {
			assertTopicRootCandidatesFrame(bytes);
		}
		// Signed candidate claims are low-frequency opaque host-side frames. Keep
		// this explicit seam ahead of the native variants 0-7 codec.
		if (bytes[0] === 8) {
			return TopicRootCandidateClaims.from(bytes);
		}
		const native = this.nativeTopicControl;
		if (native) {
			const decoded = native.decodePubSubMessage(bytes);
			switch (decoded.type) {
				case "data":
					return new PubSubData({
						topics: decoded.topics,
						data: decoded.data,
						strict: decoded.strict,
					});
				case "subscribe":
					return new Subscribe({
						topics: decoded.topics,
						requestSubscribers: decoded.requestSubscribers,
					});
				case "unsubscribe":
					return new Unsubscribe({ topics: decoded.topics });
				case "get-subscribers":
					return new GetSubscribers({ topics: decoded.topics });
				case "topic-root-candidates":
					assertCanonicalTopicRootCandidates(decoded.candidates);
					return new TopicRootCandidates({ candidates: decoded.candidates });
				case "peer-unavailable":
					return new PeerUnavailable({
						publicKeyHash: decoded.publicKeyHash,
						session: decoded.session,
						timestamp: decoded.timestamp,
						topics: decoded.topics,
					});
				case "topic-root-query":
					return new TopicRootQuery({
						requestId: decoded.requestId,
						topic: decoded.topic,
					});
				case "topic-root-query-response":
					return new TopicRootQueryResponse({
						requestId: decoded.requestId,
						topic: decoded.topic,
						root: decoded.root,
					});
			}
		}
		return PubSubMessage.from(bytes);
	}

	private getTopicRootCandidateGeneration(): string {
		return JSON.stringify([
			this.autoTopicRootCandidates,
			this.topicRootControlPlane.getTopicRootCandidates(),
		]);
	}

	private assertTopicControlPlaneActive(lifecycleRevision: number): void {
		if (
			lifecycleRevision !== this.topicControlPlaneLifecycleRevision ||
			!this.started ||
			this.stopping ||
			this.topicControlPlaneStopping
		) {
			throw new NotStartedError();
		}
	}

	private syncTopicRootCandidateResolutionSignal(
		candidateGeneration = this.getTopicRootCandidateGeneration(),
	): AbortSignal {
		if (this.topicRootCandidateResolutionGeneration === undefined) {
			this.topicRootCandidateResolutionGeneration = candidateGeneration;
		} else if (
			candidateGeneration !== this.topicRootCandidateResolutionGeneration
		) {
			this.topicRootCandidateResolutionAbortController.abort(
				new AbortError("topic root candidates changed"),
			);
			this.topicRootCandidateResolutionAbortController = new AbortController();
			this.topicRootCandidateResolutionGeneration = candidateGeneration;
		}
		return this.topicRootCandidateResolutionAbortController.signal;
	}

	private async withTopicRootCandidateResolution<T>(
		operation: (context: {
			candidateGeneration: string;
			signal: AbortSignal;
		}) => Promise<T>,
		terminalSignals: Array<AbortSignal | undefined> = [],
	): Promise<T> {
		for (;;) {
			for (const signal of terminalSignals) throwIfAborted(signal);
			const candidateGeneration = this.getTopicRootCandidateGeneration();
			const candidateSignal = this.syncTopicRootCandidateResolutionSignal(
				candidateGeneration,
			);
			const linked = linkAbortSignals([...terminalSignals, candidateSignal]);
			try {
				const result = await operation({
					candidateGeneration,
					signal: linked.signal,
				});
				for (const signal of terminalSignals) throwIfAborted(signal);
				if (
					candidateSignal.aborted ||
					candidateGeneration !== this.getTopicRootCandidateGeneration()
				) {
					continue;
				}
				return result;
			} catch (error) {
				for (const signal of terminalSignals) {
					if (signal?.aborted) throw signal.reason ?? error;
				}
				if (
					candidateSignal.aborted ||
					candidateGeneration !== this.getTopicRootCandidateGeneration()
				) {
					continue;
				}
				throw error;
			} finally {
				linked.clear();
			}
		}
	}

	private normalizePeerTopicRootState(
		topic: string,
		root: string,
	): { root: string; authoritative: boolean } {
		const deterministic =
			this.topicRootControlPlane.resolveDeterministicTopicRoot(topic);
		if (
			this.autoTopicRootCandidates &&
			topic.startsWith(this.shardTopicPrefix) &&
			deterministic &&
			root !== deterministic
		) {
			return { root: deterministic, authoritative: false };
		}
		return { root, authoritative: true };
	}

	private async resolveTopicRootState(
		topic: string,
		options?: TopicRootResolutionOptions,
	): Promise<{ root?: string; authoritative: boolean }> {
		throwIfAborted(options?.signal);
		const tracked = await this.topicRootControlPlane.resolveTrackedTopicRoot(
			topic,
			options,
		);
		if (tracked) {
			return { root: tracked, authoritative: true };
		}

		const resolvedThroughPeers = await this.resolveTopicRootThroughPeers(
			topic,
			options,
		);
		if (resolvedThroughPeers) {
			// Unconfigured peer-query replies cannot override the locally
			// deterministic root for internal shards in auto mode. Roots, resolvers,
			// and trackers explicitly configured on this control plane are resolved
			// above and remain authoritative. A leaf relying on a queried gateway for
			// shard roots must first disable auto mode with setTopicRootCandidates([]).
			return this.normalizePeerTopicRootState(topic, resolvedThroughPeers);
		}

		const deterministic = this.topicRootControlPlane.resolveDeterministicTopicRoot(topic);
		if (
			deterministic === this.publicKeyHash &&
			this.autoTopicRootCandidates &&
			this.getConnectedTopicRootTrackers().length > 0
		) {
			for (let attempt = 0; attempt < 8; attempt++) {
				await withAbort(
					delay(150 * (attempt < 4 ? 1 : 2), options),
					options?.signal,
				);
				const retried = await this.resolveTopicRootThroughPeers(topic, options);
				if (retried) {
					return this.normalizePeerTopicRootState(topic, retried);
				}
			}
		}

		return {
			root: this.topicRootControlPlane.resolveDeterministicTopicRoot(topic),
			authoritative: false,
		};
	}

	public async resolveTopicRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	): Promise<string | undefined> {
		const lifecycleSignal =
			this.started && !this.stopping && !this.topicControlPlaneStopping
				? this.topicRootResolutionAbortController.signal
				: undefined;
		return this.withTopicRootCandidateResolution(
			async ({ signal }) =>
				(await this.resolveTopicRootState(topic, { signal })).root,
			[lifecycleSignal, options?.signal],
		);
	}

	private async resolveShardRootState(
		shardTopic: string,
		options?: TopicRootResolutionOptions,
	): Promise<{ root: string; candidateGeneration: string }> {
		const lifecycleRevision = this.topicControlPlaneLifecycleRevision;
		for (;;) {
			throwIfAborted(options?.signal);
			this.assertTopicControlPlaneActive(lifecycleRevision);
			// If someone configured topic-root candidates externally (e.g.
			// TestSession router selection or Peerbit.bootstrap) after this peer
			// entered auto mode, disable auto mode before caching a root.
			if (this.autoTopicRootCandidates) {
				this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured();
			}

			const candidateGeneration = this.getTopicRootCandidateGeneration();
			const hasConnectedTrackers =
				this.getConnectedTopicRootTrackers().length > 0;
			const cached = this.shardRootCache.get(shardTopic);
			if (cached && (cached.authoritative || !hasConnectedTrackers)) {
				return { root: cached.root, candidateGeneration };
			}

			const resolved = await this.resolveTopicRootState(shardTopic, options);
			this.assertTopicControlPlaneActive(lifecycleRevision);
			if (candidateGeneration !== this.getTopicRootCandidateGeneration()) {
				continue;
			}
			if (!resolved.root) {
				throw new Error(
					`No root resolved for shard topic ${shardTopic}. Configure TopicRootControlPlane candidates/resolver/trackers, or dial/bootstrap a peer that can resolve shard roots.`,
				);
			}
			if (resolved.authoritative || !hasConnectedTrackers) {
				this.shardRootCache.set(
					shardTopic,
					resolved as {
						root: string;
						authoritative: boolean;
					},
				);
			} else {
				this.shardRootCache.delete(shardTopic);
			}
			return { root: resolved.root, candidateGeneration };
		}
	}

	private getConnectedTopicRootTrackers(): PeerStreams[] {
		return [...this.peers.values()]
			.filter((peer) => peer.isReadable || peer.isWritable)
			.sort((a, b) =>
				a.publicKey.hashcode() < b.publicKey.hashcode()
					? -1
					: a.publicKey.hashcode() > b.publicKey.hashcode()
						? 1
						: 0,
			);
	}

	private nextTopicRootRequestIdValue() {
		let requestId = this.nextTopicRootQueryId >>> 0;
		do {
			requestId = requestId === 0 ? 1 : requestId;
			this.nextTopicRootQueryId = ((requestId + 1) >>> 0) || 1;
			if (!this.pendingTopicRootQueries.has(requestId)) {
				return requestId;
			}
			requestId = this.nextTopicRootQueryId >>> 0;
		} while (true);
	}

	private async sendDirectControlMessage(
		peer: PeerStreams,
		pubsubMessage: PubSubMessage,
	) {
		const embedded = await this.createMessage(this.encodePubSubMessage(pubsubMessage), {
			mode: new SilentDelivery({
				to: [peer.publicKey.hashcode()],
				redundancy: 1,
			}),
			priority: 1,
			skipRecipientValidation: true,
		} as any);
		await this.publishMessage(this.publicKey, embedded, [peer]);
	}

	private resolvePendingTopicRootQuery(
		message: TopicRootQueryResponse,
		responderPeerHash: string,
	): boolean {
		const pending = this.pendingTopicRootQueries.get(message.requestId);
		if (
			!pending ||
			pending.topic !== message.topic ||
			pending.expectedPeerHash !== responderPeerHash
		) {
			return false;
		}
		this.pendingTopicRootQueries.delete(message.requestId);
		clearTimeout(pending.timer);
		pending.resolve(message.root);
		return true;
	}

	private async queryTopicRootFromPeer(
		peer: PeerStreams,
		topic: string,
		timeoutMs = DEFAULT_TOPIC_ROOT_QUERY_TIMEOUT_MS,
		signal?: AbortSignal,
	): Promise<string | undefined> {
		throwIfAborted(signal);
		if (!this.started || this.stopping) return undefined;

		const expectedPeerHash = peer.publicKey.hashcode();
		const requestId = this.nextTopicRootRequestIdValue();
		const responsePromise = new Promise<string | undefined>((resolve) => {
			let settled = false;
			const settle = (root: string | undefined) => {
				if (settled) return;
				settled = true;
				const pending = this.pendingTopicRootQueries.get(requestId);
				if (pending?.resolve === settle) {
					this.pendingTopicRootQueries.delete(requestId);
				}
				clearTimeout(timer);
				if (signal && onAbort) {
					signal.removeEventListener("abort", onAbort);
				}
				resolve(root);
			};
			const onAbort = signal ? () => settle(undefined) : undefined;
			const timer = setTimeout(
				() => settle(undefined),
				Math.max(1, Math.floor(timeoutMs)),
			);
			timer.unref?.();
			this.pendingTopicRootQueries.set(requestId, {
				expectedPeerHash,
				topic,
				resolve: settle,
				timer,
			});
			if (signal && onAbort) {
				signal.addEventListener("abort", onAbort, { once: true });
				if (signal.aborted) onAbort();
			}
		});

		try {
			await withAbort(
				this.sendDirectControlMessage(
					peer,
					new TopicRootQuery({ requestId, topic }),
				),
				signal,
			);
		} catch (error) {
			const pending = this.pendingTopicRootQueries.get(requestId);
			if (pending) {
				this.pendingTopicRootQueries.delete(requestId);
				clearTimeout(pending.timer);
				pending.resolve(undefined);
			}
			if (signal?.aborted) throw error;
		}

		return withAbort(responsePromise, signal);
	}

	private async confirmDirectShardRoot(
		shardTopic: string,
		root: string,
		candidateGeneration: string,
		lifecycleRevision: number,
		signal?: AbortSignal,
	): Promise<string> {
		this.assertTopicControlPlaneActive(lifecycleRevision);
		if (root === this.publicKeyHash) return root;
		const rootPeer = this.peers.get(root);
		if (!rootPeer) return root;

		const confirmation = await withAbort(
			this.queryTopicRootFromPeer(
				rootPeer,
				shardTopic,
				DIRECT_SHARD_ROOT_CONFIRM_TIMEOUT_MS,
				signal,
			),
			signal,
		);
		this.assertTopicControlPlaneActive(lifecycleRevision);
		if (candidateGeneration !== this.getTopicRootCandidateGeneration()) {
			return root;
		}
		if (!confirmation) return root;
		const resolved = this.normalizePeerTopicRootState(shardTopic, confirmation);
		if (!resolved.authoritative) {
			this.shardRootCache.delete(shardTopic);
		} else if (resolved.root !== root) {
			this.shardRootCache.set(shardTopic, {
				root: resolved.root,
				authoritative: true,
			});
		}
		return resolved.root;
	}

	private async resolveQueryableTopicRoot(
		topic: string,
	): Promise<string | undefined> {
		const lifecycleRevision = this.topicControlPlaneLifecycleRevision;
		const lifecycleSignal = this.topicRootResolutionAbortController.signal;
		return this.withTopicRootCandidateResolution(
			async ({ candidateGeneration, signal }) => {
				throwIfAborted(signal);
				this.assertTopicControlPlaneActive(lifecycleRevision);
				const root =
					await this.topicRootControlPlane.resolveCanonicalTopicRoot(topic, {
						signal,
					});
				this.assertTopicControlPlaneActive(lifecycleRevision);
				if (root !== this.publicKeyHash) {
					return root;
				}
				if (!topic.startsWith(this.shardTopicPrefix)) {
					return root;
				}

				try {
					await this.ensureFanoutChannel(topic, {
						pin: true,
						root,
						rootCandidateGeneration: candidateGeneration,
						signal,
					});
					this.assertTopicControlPlaneActive(lifecycleRevision);
					return root;
				} catch (error) {
					if (
						signal.aborted ||
						candidateGeneration !== this.getTopicRootCandidateGeneration()
					) {
						throw error;
					}
					warn(
						`Failed to host shard root ${topic} before answering root query: ${
							error instanceof Error ? error.message : String(error)
						}`,
					);
					return undefined;
				}
			},
			[lifecycleSignal],
		);
	}

	private async resolveTopicRootThroughPeers(
		topic: string,
		options?: TopicRootResolutionOptions,
	): Promise<string | undefined> {
		throwIfAborted(options?.signal);
		const peers = this.getConnectedTopicRootTrackers();
		if (peers.length === 0) {
			return undefined;
		}

		for (let attempt = 0; attempt < 3; attempt++) {
			for (const peer of peers) {
				const resolved = await this.queryTopicRootFromPeer(
					peer,
					topic,
					DEFAULT_TOPIC_ROOT_QUERY_TIMEOUT_MS,
					options?.signal,
				);
				if (resolved) {
					return resolved;
				}
			}

			if (attempt < 2) {
				await withAbort(
					delay(150 * (attempt + 1), options),
					options?.signal,
				);
			}
		}
		return undefined;
	}

	private async ensureFanoutChannel(
		shardTopic: string,
		options?: EnsureFanoutChannelOptions,
	): Promise<void> {
		const topic = shardTopic.toString();
		const lifecycleRevision = this.topicControlPlaneLifecycleRevision;
		for (;;) {
			this.assertTopicControlPlaneActive(lifecycleRevision);
			if (options?.signal?.aborted) {
				throw (
					options.signal.reason ??
					new AbortError("fanout channel opening aborted")
				);
			}
			const candidateGeneration = this.getTopicRootCandidateGeneration();
			const active = this.ensureFanoutChannelInFlight.get(topic);
			if (
				active?.candidateGeneration === candidateGeneration &&
				active.lifecycleRevision === lifecycleRevision
			) {
				try {
					await withAbort(active.opening, options?.signal);
				} catch (error) {
					if (options?.signal?.aborted) throw error;
					this.assertTopicControlPlaneActive(lifecycleRevision);
				}
				await withAbort(active.settled, options?.signal);
				if (this.ensureFanoutChannelInFlight.get(topic) === active) {
					this.ensureFanoutChannelInFlight.delete(topic);
				}
				continue;
			}
			if (active) {
				active.abortController.abort(
					new AbortError("topic root candidates changed"),
				);
				await withAbort(active.settled, options?.signal);
				if (this.ensureFanoutChannelInFlight.get(topic) === active) {
					this.ensureFanoutChannelInFlight.delete(topic);
				}
				continue;
			}

			const abortController = new AbortController();
			const abortFromCaller = () => {
				abortController.abort(
					options?.signal?.reason ??
						new AbortError("fanout channel opening aborted"),
				);
			};
			if (options?.signal?.aborted) {
				abortFromCaller();
			} else {
				options?.signal?.addEventListener("abort", abortFromCaller, {
					once: true,
				});
			}

			const opening = this.ensureFanoutChannelOnce(
				topic,
				{ ...options, signal: abortController.signal },
				lifecycleRevision,
				candidateGeneration,
			);
			let resolveSettled!: () => void;
			const settled = new Promise<void>((resolve) => {
				resolveSettled = resolve;
			});
			const inFlight = {
				abortController,
				candidateGeneration,
				lifecycleRevision,
				opening,
				settled,
			};
			this.ensureFanoutChannelInFlight.set(topic, inFlight);
			try {
				await opening;
				this.assertTopicControlPlaneActive(lifecycleRevision);
				if (candidateGeneration !== this.getTopicRootCandidateGeneration()) {
					continue;
				}
				return;
			} catch (error) {
				if (options?.signal?.aborted) throw error;
				this.assertTopicControlPlaneActive(lifecycleRevision);
				if (abortController.signal.aborted) {
					if (this.ensureFanoutChannelInFlight.get(topic) === inFlight) {
						const staleJoin = this.fanoutChannels
							.get(topic)
							?.join.catch(() => {});
						if (staleJoin) await withAbort(staleJoin, options?.signal);
					}
					continue;
				}
				if (candidateGeneration !== this.getTopicRootCandidateGeneration()) {
					continue;
				}
				throw error;
			} finally {
				options?.signal?.removeEventListener("abort", abortFromCaller);
				if (this.ensureFanoutChannelInFlight.get(topic) === inFlight) {
					this.ensureFanoutChannelInFlight.delete(topic);
				}
				resolveSettled();
			}
		}
	}

	private async ensureFanoutChannelOnce(
		shardTopic: string,
		options: EnsureFanoutChannelOptions | undefined,
		lifecycleRevision: number,
		candidateGeneration: string,
	): Promise<void> {
		this.assertTopicControlPlaneActive(lifecycleRevision);
		if (candidateGeneration !== this.getTopicRootCandidateGeneration()) return;
		const t = shardTopic.toString();
		const pin = options?.pin === true;
		const wantEphemeral = options?.ephemeral === true;

		// Allow callers that already resolved the shard root (e.g. hostShardRootsNow)
		// to pass it through to avoid a race where the candidate set changes between
		// two resolve calls, causing an unnecessary (and potentially slow) join.
		const suppliedRoot = options?.root;
		const suppliedRootGeneration = options?.rootCandidateGeneration;
		if (
			suppliedRoot !== undefined &&
			suppliedRootGeneration !== candidateGeneration
		) {
			return;
		}
		this.assertTopicControlPlaneActive(lifecycleRevision);
		let resolvedGeneration = candidateGeneration;
		let root = suppliedRoot;
		const existing = this.fanoutChannels.get(t);
		if (existing) {
			if (!root) {
				const resolved = await this.resolveShardRootState(t, {
					signal: options?.signal,
				});
				this.assertTopicControlPlaneActive(lifecycleRevision);
				root = resolved.root;
				resolvedGeneration = resolved.candidateGeneration;
			}
			if (
				resolvedGeneration !== candidateGeneration ||
				candidateGeneration !== this.getTopicRootCandidateGeneration()
			) {
				return;
			}
			if (root === existing.root) {
				existing.lastUsedAt = Date.now();
				if (existing.ephemeral && !wantEphemeral) {
					existing.ephemeral = false;
					this.clearFanoutIdleClose(existing);
				} else if (existing.ephemeral) {
					this.scheduleFanoutIdleClose(t);
				}
				if (pin) this.pinnedShards.add(t);
				await withAbort(existing.join, options?.signal);
				this.assertTopicControlPlaneActive(lifecycleRevision);
				if (
					resolvedGeneration !== candidateGeneration ||
					candidateGeneration !== this.getTopicRootCandidateGeneration()
				) {
					return;
				}
				return;
			}

			// Root mapping changed (candidate set updated): migrate to the new
			// overlay.
			await withAbort(
				this.closeFanoutChannel(t, { force: true }),
				options?.signal,
			);
			this.assertTopicControlPlaneActive(lifecycleRevision);
			if (candidateGeneration !== this.getTopicRootCandidateGeneration()) {
				return;
			}
		}

		if (!root) {
			const resolved = await this.resolveShardRootState(t, {
				signal: options?.signal,
			});
			this.assertTopicControlPlaneActive(lifecycleRevision);
			root = resolved.root;
			resolvedGeneration = resolved.candidateGeneration;
		}
		root = await this.confirmDirectShardRoot(
			t,
			root,
			resolvedGeneration,
			lifecycleRevision,
			options?.signal,
		);
		this.assertTopicControlPlaneActive(lifecycleRevision);
		if (
			resolvedGeneration !== candidateGeneration ||
			candidateGeneration !== this.getTopicRootCandidateGeneration()
		) {
			return;
		}
		const channel = new FanoutChannel(this.fanout, { topic: t, root });

		const onPayload = (payload: Uint8Array) => {
			let dm: DataMessage;
			try {
				dm = DataMessage.from(new Uint8ArrayList(payload));
			} catch {
				return;
			}
			if (!dm?.data) return;
			if (!dm.header.signatures?.signatures?.length) return;

			const signedBySelf =
				dm.header.signatures?.publicKeys.some((x) =>
					x.equals(this.publicKey),
				) ?? false;
			if (signedBySelf) return;

			let pubsubMessage: PubSubMessage;
			try {
				pubsubMessage = this.decodePubSubMessage(dm.data);
			} catch {
				return;
			}

			// Fast filter before hashing/verifying.
			if (pubsubMessage instanceof PubSubData) {
				const forMe = pubsubMessage.topics.some((x) =>
					this.subscriptions.has(x),
				);
				if (!forMe) return;
			} else if (
				pubsubMessage instanceof Subscribe ||
				pubsubMessage instanceof Unsubscribe
			) {
				const relevant = pubsubMessage.topics.some((x) =>
					this.isTrackedTopic(x),
				);
				const needRespond =
					pubsubMessage instanceof Subscribe && pubsubMessage.requestSubscribers
						? pubsubMessage.topics.some((x) => this.subscriptions.has(x))
						: false;
				if (!relevant && !needRespond) return;
			} else if (pubsubMessage instanceof GetSubscribers) {
				const overlap = pubsubMessage.topics.some((x) =>
					this.subscriptions.has(x),
				);
				if (!overlap) return;
			} else if (pubsubMessage instanceof PeerUnavailable) {
				const relevant =
					pubsubMessage.topics.length > 0
						? pubsubMessage.topics.some((x) => this.isTrackedTopic(x))
						: [...this.topics.keys()].some(
								(topic) => this.getShardTopicForUserTopic(topic) === t,
							);
				if (!relevant) return;
			} else {
				return;
			}

			void (async () => {
				const msgId = await getMsgId(payload);
				const seen = this.seenCache.get(msgId);
				this.seenCache.add(msgId, seen ? seen + 1 : 1);
				if (seen) return;

				// fanout-delivered frames bypass the inbound stream decode, so
				// the nested envelope is verified natively here when possible
				this.seedNativeWireVerification(dm, payload);
				if ((await this.verifyAndProcess(dm)) === false) {
					return;
				}
				const sender = dm.header.signatures!.signatures[0]!.publicKey!;
				await this.processShardPubSubMessage({
					pubsubMessage,
					message: dm,
					from: sender,
					shardTopic: t,
				});
			})();
		};

		const onData = (ev?: CustomEvent<FanoutTreeDataEvent>) => {
			const detail = ev?.detail as FanoutTreeDataEvent | undefined;
			if (!detail) return;
			onPayload(detail.payload);
		};
		const onUnicast = (ev?: any) => {
			const detail = ev?.detail as any | undefined;
			if (!detail) return;
			if (detail.to && detail.to !== this.publicKeyHash) return;
			onPayload(detail.payload);
		};
		channel.addEventListener("data", onData as any);
		channel.addEventListener("unicast", onUnicast as any);

		const join = (async () => {
			try {
				if (root === this.publicKeyHash) {
					channel.openAsRoot(this.fanoutRootChannelOptions);
					return;
				}
				// Joining by root hash is much more reliable if the fanout protocol
				// stream is already established (especially in small test nets without
				// trackers/bootstraps). Best-effort only: join can still succeed via
				// trackers/other routing if this times out.
					try {
						await this.fanout.waitFor(root, {
							target: "neighbor",
							// Best-effort pre-check only: do not block subscribe/publish setup
							// for long periods if the root is not yet a direct stream neighbor.
							timeout: 1_000,
						});
					} catch {
						// ignore
					}
				const joinOpts = options?.signal
					? { ...(this.fanoutJoinOptions ?? {}), signal: options.signal }
					: this.fanoutJoinOptions;
				await channel.join(this.fanoutNodeChannelOptions, joinOpts);
			} catch (error) {
				try {
					channel.removeEventListener("data", onData as any);
				} catch {
					// ignore
				}
				try {
					channel.removeEventListener("unicast", onUnicast as any);
				} catch {
					// ignore
				}
				try {
					await channel.leave({ notifyParent: false, kickChildren: false });
				} catch {
					channel.close();
				}
				throw error;
			}
		})();

		const lastUsedAt = Date.now();
		if (pin) this.pinnedShards.add(t);
		const channelState = {
			root,
			channel,
			join,
			onData,
			onUnicast,
			ephemeral: wantEphemeral,
			lastUsedAt,
		};
		this.fanoutChannels.set(t, channelState);
		join.catch(() => {
			if (this.fanoutChannels.get(t) === channelState) {
				this.fanoutChannels.delete(t);
			}
		});
		join
			.then(() => {
				const st = this.fanoutChannels.get(t);
				if (st === channelState && st.ephemeral) {
					this.scheduleFanoutIdleClose(t);
				}
			})
			.catch(() => {
				// ignore
			});
		if (wantEphemeral) {
			this.evictEphemeralFanoutChannels(t);
		}
		await withAbort(join, options?.signal);
		this.assertTopicControlPlaneActive(lifecycleRevision);
	}

	private async closeFanoutChannel(
		shardTopic: string,
		options?: { force?: boolean },
	): Promise<void> {
		const t = shardTopic.toString();
		if (!options?.force && this.pinnedShards.has(t)) return;
		if (options?.force) this.pinnedShards.delete(t);
		const st = this.fanoutChannels.get(t);
		if (!st) return;
		this.fanoutChannels.delete(t);
		this.clearFanoutIdleClose(st);
		try {
			st.channel.removeEventListener("data", st.onData as any);
		} catch {
			// ignore
		}
		try {
			st.channel.removeEventListener("unicast", st.onUnicast as any);
		} catch {
			// ignore
		}
		try {
			await st.channel.leave({ notifyParent: true });
		} catch {
			try {
				st.channel.close();
			} catch {
				// ignore
			}
		}
	}

	public async hostShardRootsNow(options?: TopicRootResolutionOptions) {
		if (!this.started) throw new NotStartedError();
		const lifecycleSignal = this.topicRootResolutionAbortController.signal;
		return this.withTopicRootCandidateResolution(
			async ({ candidateGeneration, signal }) => {
				const joins: Promise<void>[] = [];
				try {
					for (let i = 0; i < this.shardCount; i++) {
						throwIfAborted(signal);
						const shardTopic = `${this.shardTopicPrefix}${i}`;
						const resolved = await this.resolveShardRootState(shardTopic, {
							signal,
						});
						if (resolved.root !== this.publicKeyHash) continue;
						const joining = this.ensureFanoutChannel(shardTopic, {
							pin: true,
							root: resolved.root,
							rootCandidateGeneration: resolved.candidateGeneration,
							signal,
						});
						void joining.catch(() => {});
						joins.push(joining);
					}
					await Promise.all(joins);
				} catch (error) {
					await Promise.allSettled(joins);
					throw error;
				}
			},
			[lifecycleSignal, options?.signal],
		);
	}

	async subscribe(topic: string) {
		this.pendingSubscriptions.add(topic);
		// `subscribe()` is debounced; start tracking immediately to avoid dropping
		// inbound subscription traffic during the debounce window.
		this.initializeTopic(topic);
		return this.debounceSubscribeAggregator.add({ key: topic });
	}

	private async _subscribe(topics: { key: string; counter: number }[]) {
		if (!this.started) throw new NotStartedError();
		if (topics.length === 0) return;

		const byShard = new Map<string, string[]>();
		const joins: Promise<void>[] = [];
		for (const { key: topic, counter } of topics) {
			let prev = this.subscriptions.get(topic);
			if (prev) {
				prev.counter += counter;
				this.pendingSubscriptions.delete(topic);
				continue;
			}
			this.subscriptions.set(topic, { counter });
			this.initializeTopic(topic);
			this.pendingSubscriptions.delete(topic);

			const shardTopic = this.getShardTopicForUserTopic(topic);
			byShard.set(shardTopic, [...(byShard.get(shardTopic) ?? []), topic]);
			this.shardRefCounts.set(
				shardTopic,
				(this.shardRefCounts.get(shardTopic) ?? 0) + 1,
			);
			joins.push(this.ensureFanoutChannel(shardTopic));
		}

		await Promise.all(joins);

		// Announce subscriptions per shard overlay.
		await Promise.all(
			[...byShard.entries()].map(async ([shardTopic, userTopics]) => {
				if (userTopics.length === 0) return;
				const msg = new Subscribe({
					topics: userTopics,
					requestSubscribers: true,
				});
				const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
					mode: new AnyWhere(),
					priority: 1,
					skipRecipientValidation: true,
				} as any);
				const st = this.fanoutChannels.get(shardTopic);
				if (!st)
					throw new Error(`Fanout channel missing for shard: ${shardTopic}`);
				await st.channel.publish(toUint8Array(embedded.bytes()));
				this.touchFanoutChannel(shardTopic);
			}),
		);
	}

	async unsubscribe(
		topic: string,
		options?: {
			force?: boolean;
			data?: Uint8Array;
		},
	) {
		this.pendingSubscriptions.delete(topic);

		if (this.debounceSubscribeAggregator.has(topic)) {
			this.debounceSubscribeAggregator.delete(topic);
			if (!this.subscriptions.has(topic)) {
				this.untrackTopic(topic);
			}
			return false;
		}

		const sub = this.subscriptions.get(topic);
		if (!sub) return false;

		if (options?.force) {
			sub.counter = 0;
		} else {
			sub.counter -= 1;
		}
		if (sub.counter > 0) return true;

		// Remove local subscription immediately so `publish()`/delivery paths observe
		// the change without waiting for batched control-plane announces.
		this.subscriptions.delete(topic);
		this.untrackTopic(topic);

		// Update shard refcount immediately. The debounced announcer will close the
		// channel if this was the last local subscription for that shard.
		const shardTopic = this.getShardTopicForUserTopic(topic);
		const next = (this.shardRefCounts.get(shardTopic) ?? 0) - 1;
		if (next <= 0) {
			this.shardRefCounts.delete(shardTopic);
		} else {
			this.shardRefCounts.set(shardTopic, next);
		}

		// Best-effort: do not block callers on network I/O (can hang under teardown).
		void this.debounceUnsubscribeAggregator.add({ key: topic }).catch(logErrorIfStarted);
		return true;
	}

	private async _announceUnsubscribe(topics: { key: string; counter: number }[]) {
		if (!this.started) throw new NotStartedError();

		const byShard = new Map<string, string[]>();
		for (const { key: topic } of topics) {
			// If the topic got re-subscribed before this debounced batch ran, skip.
			if (this.subscriptions.has(topic)) continue;
			const shardTopic = this.getShardTopicForUserTopic(topic);
			byShard.set(shardTopic, [...(byShard.get(shardTopic) ?? []), topic]);
		}

		await Promise.all(
			[...byShard.entries()].map(async ([shardTopic, userTopics]) => {
				if (userTopics.length === 0) return;

				// Announce first.
				try {
					const msg = new Unsubscribe({ topics: userTopics });
					const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
						mode: new AnyWhere(),
						priority: 1,
						skipRecipientValidation: true,
					} as any);
					const st = this.fanoutChannels.get(shardTopic);
					if (st) {
						// Best-effort: do not let a stuck proxy publish stall teardown.
						void st.channel
							.publish(toUint8Array(embedded.bytes()))
							.catch(() => {});
						this.touchFanoutChannel(shardTopic);
					}
				} catch {
					// best-effort
				}

				// Close shard overlay if no local topics remain.
				if ((this.shardRefCounts.get(shardTopic) ?? 0) <= 0) {
					try {
						// Shutdown should be bounded and not depend on network I/O.
						await this.closeFanoutChannel(shardTopic);
					} catch {
						// best-effort
					}
				}
			}),
		);
	}

	private async announcePeerUnavailable(
		publicKeyHash: string,
		batches: { session: bigint; timestamp: bigint; topics: string[] }[],
	) {
		if (!this.started) throw new NotStartedError();

		const byShard = new Map<
			string,
			{ session: bigint; timestamp: bigint; topics: string[] }
		>();
		for (const batch of batches) {
			for (const topic of batch.topics) {
				if (!this.isTrackedTopic(topic)) {
					continue;
				}
				const shardTopic = this.getShardTopicForUserTopic(topic);
				const key = `${shardTopic}:${batch.session}:${batch.timestamp}`;
				const existing = byShard.get(key);
				if (existing) {
					existing.topics.push(topic);
				} else {
					byShard.set(key, {
						session: batch.session,
						timestamp: batch.timestamp,
						topics: [topic],
					});
				}
			}
		}

		await Promise.all(
			[...byShard.entries()].map(async ([key, batch]) => {
				const [shardTopic] = key.split(":");
				if (!shardTopic || batch.topics.length === 0) {
					return;
				}
				try {
					const msg = new PeerUnavailable({
						publicKeyHash,
						session: batch.session,
						timestamp: batch.timestamp,
						topics: batch.topics,
					});
					const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
						mode: new AnyWhere(),
						priority: 1,
						skipRecipientValidation: true,
					} as any);
					await this.ensureFanoutChannel(shardTopic, { ephemeral: true });
					const st = this.fanoutChannels.get(shardTopic);
					if (st) {
						void st.channel.publishMaybe(toUint8Array(embedded.bytes()));
						this.touchFanoutChannel(shardTopic);
					}
				} catch {
					// best-effort
				}
			}),
		);
	}

	private async announcePeerUnavailableOnShard(
		publicKeyHash: string,
		shardTopic: string,
	) {
		if (!this.started) throw new NotStartedError();
		try {
			const msg = new PeerUnavailable({
				publicKeyHash,
				session: 0n,
				timestamp: 0n,
				topics: [],
			});
			const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
				mode: new AnyWhere(),
				priority: 1,
				skipRecipientValidation: true,
			} as any);
			await this.ensureFanoutChannel(shardTopic, { ephemeral: true });
			const st = this.fanoutChannels.get(shardTopic);
			if (st) {
				void st.channel.publishMaybe(toUint8Array(embedded.bytes()));
				this.touchFanoutChannel(shardTopic);
			}
		} catch {
			// best-effort
		}
	}

	private onFanoutPeerUnreachable(
		ev: CustomEvent<{ topic: string; root: string; publicKeyHash: string }>,
	) {
		void this
			.announcePeerUnavailableOnShard(ev.detail.publicKeyHash, ev.detail.topic)
			.catch(logErrorIfStarted);
	}

	getSubscribers(topic: string): PublicSignKey[] | undefined {
		const t = topic.toString();
		const remote = this.topics.get(t);
		const includeSelf = this.subscriptions.has(t);
		if (!remote || remote.size == 0) {
			return includeSelf ? [this.publicKey] : undefined;
		}
		const ret: PublicSignKey[] = [];
		for (const v of remote.values()) ret.push(v.publicKey);
		if (includeSelf) ret.push(this.publicKey);
		return ret;
	}

	/**
	 * Returns best-effort route hints for a target peer by combining:
	 * - DirectStream ACK-learned routes
	 * - Fanout route tokens for the topic's shard overlay
	 */
	getUnifiedRouteHints(topic: string, targetHash: string): RouteHint[] {
		const hints: RouteHint[] = [];
		const directHint = this.getBestRouteHint(targetHash);
		if (directHint) {
			hints.push(directHint);
		}

		const topicString = topic.toString();
		const shardTopic = topicString.startsWith(this.shardTopicPrefix)
			? topicString
			: this.getShardTopicForUserTopic(topicString);
		const shard = this.fanoutChannels.get(shardTopic);
		if (!shard) {
			return hints;
		}

		const fanoutHint = this.fanout.getRouteHint(
			shardTopic,
			shard.root,
			targetHash,
		);
		if (fanoutHint) {
			hints.push(fanoutHint);
		}

		return hints;
	}

	async requestSubscribers(
		topic: string | string[],
		to?: PublicSignKey,
	): Promise<void> {
		if (!this.started) throw new NotStartedError();
		if (topic == null) throw new Error("ERR_NOT_VALID_TOPIC");
		if (topic.length === 0) return;

		const topicsAll = (typeof topic === "string" ? [topic] : topic).map((t) =>
			t.toString(),
		);
		for (const t of topicsAll) this.initializeTopic(t);

		const byShard = new Map<string, string[]>();
		for (const t of topicsAll) {
			const shardTopic = this.getShardTopicForUserTopic(t);
			byShard.set(shardTopic, [...(byShard.get(shardTopic) ?? []), t]);
		}

		await Promise.all(
			[...byShard.entries()].map(async ([shardTopic, userTopics]) => {
				const msg = new GetSubscribers({ topics: userTopics });
				const directPeer = to ? this.peers.get(to.hashcode()) : undefined;
				if (directPeer) {
					await this.sendDirectControlMessage(directPeer, msg);
					return;
				}

				const persistent = (this.shardRefCounts.get(shardTopic) ?? 0) > 0;
				await this.ensureFanoutChannel(shardTopic, { ephemeral: !persistent });

				const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
					mode: new AnyWhere(),
					priority: 1,
					skipRecipientValidation: true,
				} as any);
				const payload = toUint8Array(embedded.bytes());

				const st = this.fanoutChannels.get(shardTopic);
				if (!st)
					throw new Error(`Fanout channel missing for shard: ${shardTopic}`);

				if (to) {
					try {
						await st.channel.unicastToAck(to.hashcode(), payload, {
							timeoutMs: 5_000,
						});
					} catch {
						await st.channel.publishMaybe(payload);
					}
				} else {
					await st.channel.publish(payload);
				}
				this.touchFanoutChannel(shardTopic);
			}),
		);
	}

	async publish(
		data: Uint8Array | undefined,
		options?: {
			topics: string[];
		} & { client?: string } & {
			mode?: SilentDelivery | AcknowledgeDelivery;
		} & PriorityOptions &
			ResponsePriorityOptions &
			ExpiresAtOptions &
			IdOptions &
			WithExtraSigners & { signal?: AbortSignal },
	): Promise<Uint8Array | undefined> {
		if (!this.started) throw new NotStartedError();

		const topicsAll =
			(options as { topics: string[] }).topics?.map((x) => x.toString()) || [];

		const hasExplicitTOs =
			options?.mode && deliveryModeHasReceiver(options.mode);

		// Explicit recipients: use DirectStream delivery (no shard broadcast).
		if (hasExplicitTOs || !data) {
			const msg = data
				? new PubSubData({ topics: topicsAll, data, strict: true })
				: undefined;
			const message = await this.createMessage(
				msg ? this.encodePubSubMessage(msg) : undefined,
				{
					...options,
					skipRecipientValidation: this.dispatchEventOnSelfPublish,
				},
			);

			if (msg) {
				this.dispatchEvent(
					new CustomEvent("publish", {
						detail: new PublishEvent({
							client: options?.client,
							data: msg,
							message,
						}),
					}),
				);
			}

			const silentDelivery =
				options?.mode != null && isSilentDeliveryMode(options.mode);
			try {
				await this.publishMessage(
					this.publicKey,
					message,
					undefined,
					undefined,
					options?.signal,
				);
			} catch (error) {
				if (error instanceof DeliveryError && silentDelivery !== false) {
					return message.id;
				}
				throw error;
			}
			return message.id;
		}

		if (this.fanoutPublishRequiresSubscribe) {
			for (const t of topicsAll) {
				if (!this.subscriptions.has(t)) {
					throw new Error(
						`Cannot publish to topic ${t} without subscribing (fanoutPublishRequiresSubscribe=true)`,
					);
				}
			}
		}

		const msg = new PubSubData({ topics: topicsAll, data, strict: false });
		const embedded = await this.createMessage(this.encodePubSubMessage(msg), {
			mode: new AnyWhere(),
			priority: options?.priority,
			responsePriority: options?.responsePriority,
			expiresAt: options?.expiresAt,
			id: options?.id,
			extraSigners: options?.extraSigners,
			skipRecipientValidation: true,
		} as any);

		this.dispatchEvent(
			new CustomEvent("publish", {
				detail: new PublishEvent({
					client: options?.client,
					data: msg,
					message: embedded,
				}),
			}),
		);

		const byShard = new Map<string, string[]>();
		for (const t of topicsAll) {
			const shardTopic = this.getShardTopicForUserTopic(t);
			byShard.set(shardTopic, [...(byShard.get(shardTopic) ?? []), t]);
		}

		for (const shardTopic of byShard.keys()) {
			const persistent = (this.shardRefCounts.get(shardTopic) ?? 0) > 0;
			await this.ensureFanoutChannel(shardTopic, {
				ephemeral: !persistent,
				signal: options?.signal,
			});
		}

		const payload = toUint8Array(embedded.bytes());
		await Promise.all(
			[...byShard.keys()].map(async (shardTopic) => {
				if (options?.signal?.aborted) {
					throw new AbortError("Publish was aborted");
				}
				const st = this.fanoutChannels.get(shardTopic);
				if (!st) {
					throw new Error(`Fanout channel missing for shard: ${shardTopic}`);
				}
				await withAbort(st.channel.publish(payload), options?.signal);
				this.touchFanoutChannel(shardTopic);
			}),
		);

		if (
			this.fanoutPublishIdleCloseMs == 0 ||
			this.fanoutPublishMaxEphemeralChannels == 0
		) {
			for (const shardTopic of byShard.keys()) {
				const st = this.fanoutChannels.get(shardTopic);
				if (st?.ephemeral) await this.closeFanoutChannel(shardTopic);
			}
		}

		return embedded.id;
	}

	/**
	 * Publish a pre-encoded `PubSubData` payload to explicit recipients.
	 *
	 * `payload` must be the exact borsh serialization of
	 * `new PubSubData({ topics, data, strict: true })` — callers that encode
	 * the payload elsewhere (e.g. the shared-log fused send path serializing
	 * inside wasm) hand the finished bytes here so the wrapping `PubSubData`
	 * copy of the regular `publish` path is skipped. Everything else matches
	 * the explicit-recipient branch of {@link publish}: the payload becomes a
	 * signed `DataMessage` delivered over DirectStream.
	 */
	async publishPreEncodedData(
		payload: Uint8Array,
		properties: {
			topics: string[];
		},
		options: {
			mode: SilentDelivery | AcknowledgeDelivery;
		} & { client?: string } & PriorityOptions &
			ResponsePriorityOptions &
			ExpiresAtOptions &
			IdOptions &
			WithExtraSigners & { signal?: AbortSignal },
	): Promise<Uint8Array | undefined> {
		if (!this.started) throw new NotStartedError();
		if (!options?.mode || !deliveryModeHasReceiver(options.mode)) {
			throw new Error(
				"publishPreEncodedData requires a delivery mode with explicit recipients",
			);
		}

		const message = await this.createMessage(payload, {
			...options,
			skipRecipientValidation: this.dispatchEventOnSelfPublish,
		});

		this.dispatchEvent(
			new CustomEvent("publish", {
				detail: new PublishEvent({
					client: options?.client,
					data: new PubSubData({
						topics: properties.topics,
						data: payload.subarray(
							preEncodedPubSubDataOffset(properties.topics),
						),
						strict: true,
					}),
					message,
				}),
			}),
		);

		const silentDelivery = isSilentDeliveryMode(options.mode);
		try {
			await this.publishMessage(
				this.publicKey,
				message,
				undefined,
				undefined,
				options?.signal,
			);
		} catch (error) {
			if (error instanceof DeliveryError && silentDelivery !== false) {
				return message.id;
			}
			throw error;
		}
		return message.id;
	}

	public onPeerSession(key: PublicSignKey, _session: number): void {
		if (!Number.isFinite(_session) || _session < 0) {
			return;
		}
		this.removeSubscriptionsBeforeSession(
			key,
			BigInt(Math.trunc(_session)),
			"peer-session-reset",
		);
	}

	public override onPeerUnreachable(publicKeyHash: string) {
		super.onPeerUnreachable(publicKeyHash);
		const key = this.peerKeyHashToPublicKey.get(publicKeyHash);
		if (!key) {
			return;
		}

		const removed = this.collectSubscriptionState(publicKeyHash);
		const changed = this.removeSubscriptions(key, "peer-unreachable");
		if (changed.length === 0) {
			return;
		}

		const changedSet = new Set(changed);
		const batches = removed
			.map((batch) => ({
				...batch,
				topics: batch.topics.filter((topic) => changedSet.has(topic)),
			}))
			.filter((batch) => batch.topics.length > 0);
		if (batches.length > 0) {
			void this
				.announcePeerUnavailable(publicKeyHash, batches)
				.catch(logErrorIfStarted);
		}
	}

	private collectSubscriptionState(peerHash: string) {
		const peerTopics = this.peerToTopic.get(peerHash);
		if (!peerTopics) {
			return [];
		}

		const grouped = new Map<string, { session: bigint; timestamp: bigint; topics: string[] }>();
		for (const topic of peerTopics) {
			const existing = this.topics.get(topic)?.get(peerHash);
			if (!existing) {
				continue;
			}
			const key = `${existing.session}:${existing.timestamp}`;
			const batch = grouped.get(key);
			if (batch) {
				batch.topics.push(topic);
			} else {
				grouped.set(key, {
					session: existing.session,
					timestamp: existing.timestamp,
					topics: [topic],
				});
			}
		}
		return [...grouped.values()];
	}

	private removeSubscriptionsBeforeSession(
		publicKey: PublicSignKey,
		session: bigint,
		reason: UnsubscriptionReason,
	) {
		const peerHash = publicKey.hashcode();
		const peerTopics = this.peerToTopic.get(peerHash);
		const changed: string[] = [];
		if (peerTopics) {
			for (const topic of [...peerTopics]) {
				const peers = this.topics.get(topic);
				const existing = peers?.get(peerHash);
				if (!existing || existing.session >= session) {
					continue;
				}
				if (peers?.delete(peerHash)) {
					changed.push(topic);
					peerTopics.delete(topic);
					this.lastSubscriptionMessages.get(peerHash)?.delete(topic);
				}
			}
		}
		if (!peerTopics?.size) {
			this.peerToTopic.delete(peerHash);
			this.lastSubscriptionMessages.delete(peerHash);
		}

		if (changed.length > 0) {
			this.dispatchEvent(
				new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
					detail: new UnsubcriptionEvent(publicKey, changed, reason),
				}),
			);
		}
	}

	private removeSubscriptions(
		publicKey: PublicSignKey,
		reason: UnsubscriptionReason,
	) {
		const peerHash = publicKey.hashcode();
		const peerTopics = this.peerToTopic.get(peerHash);
		const changed: string[] = [];
		if (peerTopics) {
			for (const topic of peerTopics) {
				const peers = this.topics.get(topic);
				if (!peers) continue;
				if (peers.delete(peerHash)) {
					changed.push(topic);
				}
			}
		}
		this.peerToTopic.delete(peerHash);
		this.lastSubscriptionMessages.delete(peerHash);

		if (changed.length > 0) {
			this.dispatchEvent(
				new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
					detail: new UnsubcriptionEvent(publicKey, changed, reason),
				}),
			);
		}

		return changed;
	}

	private subscriptionStateIsLatest(
		subscriberKey: string,
		session: bigint,
		timestamp: bigint,
		relevantTopics: string[],
	) {
		if (this.nativeTopicControl) {
			const lasts: bigint[] = [];
			for (const topic of relevantTopics) {
				const last = this.lastSubscriptionMessages
					.get(subscriberKey)
					?.get(topic);
				if (last) {
					lasts.push(last.session, last.timestamp);
				}
			}
			if (
				!this.nativeTopicControl.subscriptionIsLatest(
					BigUint64Array.from(lasts),
					session,
					timestamp,
				)
			) {
				return false;
			}
		} else {
			for (const topic of relevantTopics) {
				const last = this.lastSubscriptionMessages
					.get(subscriberKey)
					?.get(topic);
				if (!last) {
					continue;
				}
				if (last.session > session) {
					return false;
				}
				if (
					timestamp !== 0n &&
					last.session === session &&
					last.timestamp > timestamp
				) {
					return false;
				}
			}
		}

		for (const topic of relevantTopics) {
			if (!this.lastSubscriptionMessages.has(subscriberKey)) {
				this.lastSubscriptionMessages.set(subscriberKey, new Map());
			}
			this.lastSubscriptionMessages
				.get(subscriberKey)!
				.set(topic, {
					session,
					timestamp,
				});
		}
		return true;
	}

	/**
	 * `Subscribe` replaces tracked subscription data for new subscribers or
	 * strictly newer sessions; equal/older sessions only refresh cache order.
	 */
	private subscribeShouldReplace(
		existing: SubscriptionData | undefined,
		session: bigint,
	): boolean {
		if (this.nativeTopicControl) {
			return this.nativeTopicControl.subscribeShouldReplace(
				existing?.session,
				session,
			);
		}
		return !existing || existing.session < session;
	}

	private subscriptionMessageIsLatest(
		message: DataMessage,
		_pubsubMessage: Subscribe | Unsubscribe,
		relevantTopics: string[],
	) {
		const subscriber = message.header.signatures!.signatures[0].publicKey!;
		return this.subscriptionStateIsLatest(
			subscriber.hashcode(),
			message.header.session,
			message.header.timestamp,
			relevantTopics,
		);
	}

	/**
	 * Answer Subscribe{requestSubscribers} announces without letting bursts go
	 * quadratic (#983). Leading edge: the first announcer is answered
	 * immediately with a targeted unicast — a lone joiner sees no added
	 * latency. Announcers arriving within the trailing window that this opens
	 * are batched and answered with ONE broadcast, instead of S(S-1) routed
	 * unicasts whose cold route resolutions flood the shard tree.
	 */
	private queueSubscriberResponse(
		shardTopic: string,
		targetHash: string,
		topics: string[],
	) {
		let entry = this.pendingSubscriberResponses.get(shardTopic);
		if (!entry) {
			const created = {
				targets: new Set<string>(),
				topics: new Set<string>(),
				timer: setTimeout(
					() => {
						this.pendingSubscriberResponses.delete(shardTopic);
						if (created.targets.size === 0) return;
						void this.flushSubscriberResponses(shardTopic, created).catch(
							logErrorIfStarted,
						);
					},
					150 + Math.floor(Math.random() * 150),
				),
			};
			this.pendingSubscriberResponses.set(shardTopic, created);
			// Leading edge: respond to the burst opener right away.
			void this.flushSubscriberResponses(shardTopic, {
				targets: new Set([targetHash]),
				topics: new Set(topics),
			}).catch(logErrorIfStarted);
			return;
		}
		entry.targets.add(targetHash);
		for (const t of topics) entry.topics.add(t);
	}

	private async flushSubscriberResponses(
		shardTopic: string,
		entry: { targets: Set<string>; topics: Set<string> },
	) {
		if (!this.started) return;
		// Re-filter: our subscriptions may have changed within the window.
		const topics = this.getSubscriptionOverlap([...entry.topics]);
		if (topics.length === 0) return;
		const response = new Subscribe({
			topics,
			requestSubscribers: false,
		});
		const embedded = await this.createMessage(
			this.encodePubSubMessage(response),
			{
				mode: new AnyWhere(),
				priority: 1,
				skipRecipientValidation: true,
			} as any,
		);
		const payload = toUint8Array(embedded.bytes());
		const [firstTarget] = entry.targets;
		if (entry.targets.size === 1 && firstTarget) {
			await this.sendFanoutUnicastOrBroadcast(shardTopic, firstTarget, payload);
			return;
		}
		const st = this.fanoutChannels.get(shardTopic);
		if (!st) return;
		await st.channel.publishMaybe(payload);
	}

	private async sendFanoutUnicastOrBroadcast(
		shardTopic: string,
		targetHash: string,
		payload: Uint8Array,
	) {
		const st = this.fanoutChannels.get(shardTopic);
		if (!st) return;
		const hints = this.getUnifiedRouteHints(shardTopic, targetHash);
		const fanoutHint = hints.find(
			(hint): hint is Extract<RouteHint, { kind: "fanout-token" }> =>
				hint.kind === "fanout-token",
		);
		if (fanoutHint) {
			try {
				await st.channel.unicastAck(fanoutHint.route, payload, {
					timeoutMs: 5_000,
				});
				return;
			} catch {
				// ignore and fall back
			}
		}
		try {
			await st.channel.unicastToAck(targetHash, payload, { timeoutMs: 5_000 });
			return;
		} catch {
			// ignore and fall back
		}
			await st.channel.publishMaybe(payload);
	}

	private async processDirectPubSubMessage(input: {
		pubsubMessage: PubSubMessage;
		message: DataMessage;
		from: PublicSignKey;
		stream: PeerStreams;
	}): Promise<void> {
		const { pubsubMessage, message, from, stream } = input;
		const isDirectRootControl =
			pubsubMessage instanceof TopicRootCandidates ||
			pubsubMessage instanceof TopicRootCandidateClaims ||
			pubsubMessage instanceof TopicRootQuery ||
			pubsubMessage instanceof TopicRootQueryResponse;
		const directRootSigner = isDirectRootControl
			? message.header.signatures?.publicKeys[0]
			: undefined;
		if (
			isDirectRootControl &&
			(!directRootSigner || !directRootSigner.equals(stream.publicKey))
		) {
			return;
		}

		if (pubsubMessage instanceof TopicRootCandidates) {
			if (stream.protocol !== TOPIC_CONTROL_PLANE_PROTOCOL_V2_0) {
				return;
			}
			// `addPeer()` already admitted the authenticated direct stream identity.
			// The advertised list is intentionally ignored.
			return;
		}

		if (pubsubMessage instanceof TopicRootCandidateClaims) {
			if (
				stream.protocol !== TOPIC_CONTROL_PLANE_PROTOCOL_V2_1 ||
				!this.autoTopicRootCandidates
			) {
				return;
			}
			const imported: Uint8Array[] = [];
			const outerPeerHash = stream.publicKey.hashcode();
			for (const claim of pubsubMessage.claims) {
				if (
					await this.importSignedTopicRootCandidateClaim(claim, outerPeerHash)
				) {
					imported.push(claim);
				}
			}
			if (imported.length > 0) {
				this.rebuildAutoTopicRootCandidatesFromClaims();
				// Relay the exact nested claims; no relay signature is substituted for
				// their origin signatures.
				void this.sendSignedTopicRootCandidateClaims(
					imported,
					[...this.peers.values()].filter(
						(peer) => peer.publicKey.hashcode() !== outerPeerHash,
					),
				).catch(() => {});
			}
			return;
		}

		if (pubsubMessage instanceof TopicRootQuery) {
			const root = await this.resolveQueryableTopicRoot(pubsubMessage.topic);
			await this.sendDirectControlMessage(
				stream,
				new TopicRootQueryResponse({
					requestId: pubsubMessage.requestId,
					topic: pubsubMessage.topic,
					root,
				}),
			).catch(() => {});
			return;
		}

		if (pubsubMessage instanceof TopicRootQueryResponse) {
			const senderHash = directRootSigner!.hashcode();
			this.resolvePendingTopicRootQuery(pubsubMessage, senderHash);
			return;
		}

		if (pubsubMessage instanceof Subscribe) {
			const sender = from;
			const senderKey = sender.hashcode();
			const relevantTopics = pubsubMessage.topics.filter((t) =>
				this.isTrackedTopic(t),
			);

			if (
				relevantTopics.length > 0 &&
				this.subscriptionMessageIsLatest(message, pubsubMessage, relevantTopics)
			) {
				const changed: string[] = [];
				for (const topic of relevantTopics) {
					const peers = this.topics.get(topic);
					if (!peers) continue;
					this.initializePeer(sender);

					const existing = peers.get(senderKey);
					if (this.subscribeShouldReplace(existing, message.header.session)) {
						peers.delete(senderKey);
						peers.set(
							senderKey,
							new SubscriptionData({
								session: message.header.session,
								timestamp: message.header.timestamp,
								publicKey: sender,
							}),
						);
						changed.push(topic);
					} else {
						peers.delete(senderKey);
						peers.set(senderKey, existing!);
					}

					if (!existing) {
						this.peerToTopic.get(senderKey)!.add(topic);
					}
					this.pruneTopicSubscribers(topic);
				}

				if (changed.length > 0) {
					this.dispatchEvent(
						new CustomEvent<SubscriptionEvent>("subscribe", {
							detail: new SubscriptionEvent(sender, changed),
						}),
					);
				}
			}

			if (pubsubMessage.requestSubscribers) {
				const overlap = this.getSubscriptionOverlap(pubsubMessage.topics);
				if (overlap.length > 0) {
					await this.sendDirectControlMessage(
						stream,
						new Subscribe({
							topics: overlap,
							requestSubscribers: false,
						}),
					);
				}
			}
			return;
		}

		if (pubsubMessage instanceof GetSubscribers) {
			const overlap = this.getSubscriptionOverlap(pubsubMessage.topics);
			if (overlap.length === 0) return;
			await this.sendDirectControlMessage(
				stream,
				new Subscribe({
					topics: overlap,
					requestSubscribers: false,
				}),
			);
			return;
		}
		if (pubsubMessage instanceof PubSubData) {
			this.dispatchEvent(
				new CustomEvent("data", {
					detail: new DataEvent({
						data: pubsubMessage,
						message,
					}),
				}),
			);
			return;
		}
	}

	private async processShardPubSubMessage(input: {
		pubsubMessage: PubSubMessage;
		message: DataMessage;
		from: PublicSignKey;
		shardTopic: string;
	}): Promise<void> {
		const { pubsubMessage, message, from, shardTopic } = input;

		if (pubsubMessage instanceof PubSubData) {
			this.dispatchEvent(
				new CustomEvent("data", {
					detail: new DataEvent({
						data: pubsubMessage,
						message,
					}),
				}),
			);
			return;
		}

		if (pubsubMessage instanceof Subscribe) {
			const sender = from;
			const senderKey = sender.hashcode();
			const relevantTopics = pubsubMessage.topics.filter((t) =>
				this.isTrackedTopic(t),
			);

			if (
				relevantTopics.length > 0 &&
				this.subscriptionMessageIsLatest(message, pubsubMessage, relevantTopics)
			) {
				const changed: string[] = [];
				for (const topic of relevantTopics) {
					const peers = this.topics.get(topic);
					if (!peers) continue;
					this.initializePeer(sender);

					const existing = peers.get(senderKey);
					if (this.subscribeShouldReplace(existing, message.header.session)) {
						peers.delete(senderKey);
						peers.set(
							senderKey,
							new SubscriptionData({
								session: message.header.session,
								timestamp: message.header.timestamp,
								publicKey: sender,
							}),
						);
						changed.push(topic);
					} else {
						peers.delete(senderKey);
						peers.set(senderKey, existing!);
					}

					if (!existing) {
						this.peerToTopic.get(senderKey)!.add(topic);
					}
					this.pruneTopicSubscribers(topic);
				}

				if (changed.length > 0) {
					this.dispatchEvent(
						new CustomEvent<SubscriptionEvent>("subscribe", {
							detail: new SubscriptionEvent(sender, changed),
						}),
					);
				}
			}

			if (pubsubMessage.requestSubscribers) {
				const overlap = this.getSubscriptionOverlap(pubsubMessage.topics);
				if (overlap.length > 0) {
					this.queueSubscriberResponse(shardTopic, senderKey, overlap);
				}
			}
			return;
		}

		if (pubsubMessage instanceof Unsubscribe) {
			const sender = from;
			const senderKey = sender.hashcode();
			const relevantTopics = pubsubMessage.topics.filter((t) =>
				this.isTrackedTopic(t),
			);

			if (
				relevantTopics.length > 0 &&
				this.subscriptionMessageIsLatest(message, pubsubMessage, relevantTopics)
			) {
				const changed: string[] = [];
				for (const topic of relevantTopics) {
					const peers = this.topics.get(topic);
					if (!peers) continue;
					if (peers.delete(senderKey)) {
						changed.push(topic);
						this.peerToTopic.get(senderKey)?.delete(topic);
					}
				}
				if (!this.peerToTopic.get(senderKey)?.size) {
					this.peerToTopic.delete(senderKey);
					this.lastSubscriptionMessages.delete(senderKey);
				}
				if (changed.length > 0) {
					this.dispatchEvent(
						new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
							detail: new UnsubcriptionEvent(
								sender,
								changed,
								"remote-unsubscribe",
							),
						}),
					);
				}
			}
			return;
		}

		if (pubsubMessage instanceof PeerUnavailable) {
			const peerHash = pubsubMessage.publicKeyHash;
			// Relay-originated shard deltas are keyed only by shard membership, not by
			// per-topic subscription watermarks. They are emitted immediately when the
			// relay loses a child so downstream peers can shed stale membership without
			// waiting for the slower shared-log liveness sweep.
			const isShardFastPath =
				pubsubMessage.topics.length === 0 && pubsubMessage.timestamp === 0n;
			const relevantTopics =
				pubsubMessage.topics.length > 0
					? pubsubMessage.topics.filter((topic) => {
							if (!this.isTrackedTopic(topic)) {
								return false;
							}
							return this.topics.get(topic)?.has(peerHash) ?? false;
						})
					: [...this.topics.keys()].filter(
							(topic) =>
								this.getShardTopicForUserTopic(topic) === shardTopic &&
								(this.topics.get(topic)?.has(peerHash) ?? false),
						);
			const shouldApply =
				relevantTopics.length > 0 &&
				(isShardFastPath ||
					this.subscriptionStateIsLatest(
						peerHash,
						pubsubMessage.session,
						pubsubMessage.timestamp,
						relevantTopics,
					));
			if (shouldApply) {
				const changed: string[] = [];
				let publicKey: PublicSignKey | undefined;
				for (const topic of relevantTopics) {
					const peers = this.topics.get(topic);
					if (!peers) continue;
					const existing = peers.get(peerHash);
					if (!existing) continue;
					publicKey = publicKey ?? existing.publicKey;
					if (peers.delete(peerHash)) {
						changed.push(topic);
						this.peerToTopic.get(peerHash)?.delete(topic);
					}
				}
				if (!this.peerToTopic.get(peerHash)?.size) {
					this.peerToTopic.delete(peerHash);
					this.lastSubscriptionMessages.delete(peerHash);
				}
				if (changed.length > 0 && publicKey) {
					this.dispatchEvent(
						new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
							detail: new UnsubcriptionEvent(
								publicKey,
								changed,
								"peer-unreachable",
							),
						}),
					);
				}
			}
			return;
		}

		if (pubsubMessage instanceof GetSubscribers) {
			const sender = from;
			const senderKey = sender.hashcode();
			const overlap = this.getSubscriptionOverlap(pubsubMessage.topics);
			if (overlap.length === 0) return;

			const response = new Subscribe({
				topics: overlap,
				requestSubscribers: false,
			});
			const embedded = await this.createMessage(
				this.encodePubSubMessage(response),
				{
					mode: new AnyWhere(),
					priority: 1,
					skipRecipientValidation: true,
				} as any,
			);
			const payload = toUint8Array(embedded.bytes());
			await this.sendFanoutUnicastOrBroadcast(shardTopic, senderKey, payload);
			return;
		}
	}

	public override async onDataMessage(
		from: PublicSignKey,
		stream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		if (!message.data || message.data.length === 0) {
			return super.onDataMessage(from, stream, message, seenBefore);
		}
		if (this.shouldIgnore(message, seenBefore)) return false;

		let pubsubMessage: PubSubMessage;
		try {
			pubsubMessage = this.decodePubSubMessage(message.data);
		} catch {
			return super.onDataMessage(from, stream, message, seenBefore);
		}

		// DirectStream supports targeted pubsub data plus a small set of control
		// messages used for topic-root discovery and direct subscriber snapshots.
		if (
			!(pubsubMessage instanceof PubSubData) &&
			!(pubsubMessage instanceof TopicRootCandidates) &&
			!(pubsubMessage instanceof TopicRootCandidateClaims) &&
			!(pubsubMessage instanceof TopicRootQuery) &&
			!(pubsubMessage instanceof GetSubscribers) &&
			!(pubsubMessage instanceof Subscribe) &&
			!(pubsubMessage instanceof TopicRootQueryResponse)
		) {
			return true;
		}

		// Determine if this node should process it.
		let isForMe = false;
		if (deliveryModeHasReceiver(message.header.mode)) {
			isForMe = message.header.mode.to.includes(this.publicKeyHash);
		} else if (
			isAnyWhereDeliveryMode(message.header.mode) ||
			isAcknowledgeAnyWhereDeliveryMode(message.header.mode)
		) {
			isForMe = true;
		}

		if (pubsubMessage instanceof PubSubData) {
			const wantsTopic = pubsubMessage.topics.some((t) =>
				this.subscriptions.has(t) || this.pendingSubscriptions.has(t),
			);
			isForMe = pubsubMessage.strict ? isForMe && wantsTopic : wantsTopic;
		}

		if (isForMe) {
			if ((await this.verifyAndProcess(message)) === false) return false;
			await this.maybeAcknowledgeMessage(stream, message, seenBefore);
			if (seenBefore === 0) {
				await this.processDirectPubSubMessage({
					pubsubMessage,
					message,
					from,
					stream,
				});
			}
		}

		// Forward direct PubSubData only (subscription control lives on fanout shards).
		if (!(pubsubMessage instanceof PubSubData)) {
			return true;
		}

		if (
			isSilentDeliveryMode(message.header.mode) ||
			isAcknowledgeDeliveryMode(message.header.mode)
		) {
			if (
				message.header.mode.to.length === 1 &&
				message.header.mode.to[0] === this.publicKeyHash
			) {
				return true;
			}
		}

		const shouldForward =
			seenBefore === 0 ||
			((isAcknowledgeDeliveryMode(message.header.mode) ||
				isAcknowledgeAnyWhereDeliveryMode(message.header.mode)) &&
				seenBefore < message.header.mode.redundancy);
		if (shouldForward) {
			this.relayMessage(from, message).catch(logErrorIfStarted);
		}
		return true;
	}
}

export const waitForSubscribers = async (
	libp2p: { services: { pubsub: PubSub } },
	peersToWait:
		| PeerId
		| PeerId[]
		| { peerId: Libp2pPeerId }
		| { peerId: Libp2pPeerId }[]
		| string
		| string[],
	topic: string,
	options?: { signal?: AbortSignal; timeout?: number },
) => {
	const peersToWaitArr = Array.isArray(peersToWait)
		? peersToWait
		: [peersToWait];

	const peerIdsToWait: string[] = peersToWaitArr.map((peer) => {
		if (typeof peer === "string") {
			return peer;
		}
		const id: PublicSignKey | Libp2pPeerId = (peer as any)["peerId"] || peer;
		if (typeof id === "string") {
			return id;
		}
		return id instanceof PublicSignKey
			? id.hashcode()
			: getPublicKeyFromPeerId(id).hashcode();
	});

	return new Promise<void>((resolve, reject) => {
		if (peerIdsToWait.length === 0) {
			resolve();
			return;
		}

		let settled = false;
		let timeout: ReturnType<typeof setTimeout> | undefined = undefined;
		let interval: ReturnType<typeof setInterval> | undefined = undefined;
		let pollInFlight = false;
		const wanted = new Set(peerIdsToWait);
		const seen = new Set<string>();
		const pubsub = libp2p.services.pubsub;
		const shouldRejectWithTimeoutError = options?.timeout != null;

		const clear = () => {
			if (timeout) {
				clearTimeout(timeout);
				timeout = undefined;
			}
			if (interval) {
				clearInterval(interval);
				interval = undefined;
			}
			options?.signal?.removeEventListener("abort", onAbort);
			try {
				pubsub.removeEventListener("subscribe", onSubscribe);
				pubsub.removeEventListener("unsubscribe", onUnsubscribe);
			} catch {
				// ignore
			}
		};

		const resolveOnce = () => {
			if (settled) return;
			settled = true;
			clear();
			resolve();
		};

		const rejectOnce = (error: unknown) => {
			if (settled) return;
			settled = true;
			clear();
			reject(error);
		};

		const onAbort = () => {
			rejectOnce(new AbortError("waitForSubscribers was aborted"));
		};

		const updateSeen = (hash?: string, isSubscribed?: boolean) => {
			if (!hash) return false;
			if (!wanted.has(hash)) return false;
			if (isSubscribed) {
				seen.add(hash);
			} else {
				seen.delete(hash);
			}
			return seen.size === wanted.size;
		};

		const reconcileFromSubscribers = (peers?: PublicSignKey[]) => {
			const current = new Set<string>();
			for (const peer of peers || []) current.add(peer.hashcode());
			for (const hash of wanted) {
				if (current.has(hash)) seen.add(hash);
				else seen.delete(hash);
			}
			if (seen.size === wanted.size) resolveOnce();
		};

		const onSubscribe = (ev: any) => {
			const detail = ev?.detail as SubscriptionEvent | undefined;
			if (!detail) return;
			if (!detail.topics || detail.topics.length === 0) return;
			if (!detail.topics.includes(topic)) return;
			const hash = detail.from?.hashcode?.();
			if (updateSeen(hash, true)) {
				resolveOnce();
			}
		};

		const onUnsubscribe = (ev: any) => {
			const detail = ev?.detail as UnsubcriptionEvent | undefined;
			if (!detail) return;
			if (!detail.topics || detail.topics.length === 0) return;
			if (!detail.topics.includes(topic)) return;
			const hash = detail.from?.hashcode?.();
			updateSeen(hash, false);
		};

		if (options?.signal?.aborted) {
			rejectOnce(new AbortError("waitForSubscribers was aborted"));
			return;
		}

		options?.signal?.addEventListener("abort", onAbort);

		// Preserve previous behavior: without an explicit timeout, fail after ~20s.
		const timeoutMs = Math.max(0, Math.floor(options?.timeout ?? 20_000));
		if (timeoutMs > 0) {
			timeout = setTimeout(() => {
				rejectOnce(
					shouldRejectWithTimeoutError
						? new TimeoutError("waitForSubscribers timed out")
						: new Error(
								"Failed to find expected subscribers for topic: " + topic,
							),
				);
			}, timeoutMs);
		}

		// Observe new subscriptions.
		try {
			void pubsub.addEventListener("subscribe", onSubscribe);
			void pubsub.addEventListener("unsubscribe", onUnsubscribe);
		} catch (e) {
			rejectOnce(e);
			return;
		}

		const poll = () => {
			if (settled) return;
			if (pollInFlight) return;
			pollInFlight = true;
			void Promise.resolve(pubsub.getSubscribers(topic))
				.then((peers) => {
					if (settled) return;
					reconcileFromSubscribers(peers || []);
				})
				.catch((e) => rejectOnce(e))
				.finally(() => {
					pollInFlight = false;
				});
		};

		// Polling is a fallback for cases where no event is emitted (e.g. local subscribe completion),
		// and keeps behavior stable across implementations.
		poll();
		interval = setInterval(poll, 200);
	});
};
