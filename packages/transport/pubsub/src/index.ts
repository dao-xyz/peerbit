import {
	type Connection,
	type PeerId as Libp2pPeerId,
} from "@libp2p/interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	DataEvent,
	GetSubscribers,
	type PubSub,
	PubSubData,
	type PubSubEvents,
	PubSubMessage,
	PublishEvent,
	Subscribe,
	SubscriptionData,
	SubscriptionEvent,
	TopicRootCandidates,
	UnsubcriptionEvent,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import {
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
	type PeerStreams,
	dontThrowIfDeliveryError,
} from "@peerbit/stream";
import {
	AcknowledgeAnyWhere,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	DeliveryError,
	type IdOptions,
	MessageHeader,
	NotStartedError,
	type PriorityOptions,
	type RouteHint,
	SilentDelivery,
	type WithExtraSigners,
	deliveryModeHasReceiver,
	getMsgId,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import { Uint8ArrayList } from "uint8arraylist";
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
import { TopicRootControlPlane } from "./topic-root-control-plane.js";

export * from "./fanout-tree.js";
export * from "./fanout-channel.js";
export * from "./topic-root-control-plane.js";

export const toUint8Array = (arr: Uint8ArrayList | Uint8Array) =>
	arr instanceof Uint8ArrayList ? arr.subarray() : arr;

export const logger = loggerFn("peerbit:transport:topic-control-plane");
const warn = logger.newScope("warn");
const logError = (e?: { message: string }) => {
	logger.error(e?.message);
};
const logErrorIfStarted = (e?: { message: string }) => {
	e instanceof NotStartedError === false && logError(e);
};

const withAbort = async <T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> => {
	if (!signal) return promise;
	if (signal.aborted) {
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

const SUBSCRIBER_CACHE_MAX_ENTRIES_HARD_CAP = 100_000;
const SUBSCRIBER_CACHE_DEFAULT_MAX_ENTRIES = 4_096;
const DEFAULT_FANOUT_PUBLISH_IDLE_CLOSE_MS = 60_000;
const DEFAULT_FANOUT_PUBLISH_MAX_EPHEMERAL_CHANNELS = 64;
const DEFAULT_PUBSUB_SHARD_COUNT = 256;
const PUBSUB_SHARD_COUNT_HARD_CAP = 16_384;
const DEFAULT_PUBSUB_SHARD_TOPIC_PREFIX = "/peerbit/pubsub-shard/1/";
const AUTO_TOPIC_ROOT_CANDIDATES_MAX = 64;

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

export type TopicControlPlaneComponents = DirectStreamComponents;

export type PeerId = Libp2pPeerId | PublicSignKey;

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
	public lastSubscriptionMessages: Map<string, Map<string, bigint>> = new Map();
	public dispatchEventOnSelfPublish: boolean;
	public readonly topicRootControlPlane: TopicRootControlPlane;
	public readonly subscriberCacheMaxEntries: number;
	public readonly fanout: FanoutTree;

	private debounceSubscribeAggregator: DebouncedAccumulatorCounterMap;
	private debounceUnsubscribeAggregator: DebouncedAccumulatorCounterMap;

	private readonly shardCount: number;
	private readonly shardTopicPrefix: string;
	private readonly hostShards: boolean;
	private readonly shardRootCache = new Map<string, string>();
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
	private reconcileShardOverlaysInFlight?: Promise<void>;
	private autoCandidatesBroadcastTimers: Array<ReturnType<typeof setTimeout>> =
		[];
	private autoCandidatesGossipInterval?: ReturnType<typeof setInterval>;
	private autoCandidatesGossipUntil = 0;

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

	constructor(
		components: TopicControlPlaneComponents,
		props?: TopicControlPlaneOptions,
	) {
		super(components, ["/peerbit/topic-control-plane/2.0.0"], props);
		this.subscriptions = new Map();
		this.pendingSubscriptions = new Set();
		this.topics = new Map();
		this.peerToTopic = new Map();

		this.topicRootControlPlane =
			props?.topicRootControlPlane || new TopicRootControlPlane();
		this.dispatchEventOnSelfPublish =
			props?.dispatchEventOnSelfPublish || false;

		if (!props?.fanout) {
			throw new Error(
				"TopicControlPlane requires a FanoutTree instance (options.fanout)",
			);
		}
		this.fanout = props.fanout;

		// Default to a local-only shard-root candidate set so standalone peers can
		// subscribe/publish without explicit bootstraps. We'll expand candidates
		// opportunistically as neighbours connect.
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
		);
		// NOTE: Unsubscribe should update local state immediately and batch only the
		// best-effort network announcements to avoid teardown stalls (program close).
		this.debounceUnsubscribeAggregator = debouncedAccumulatorSetCounter(
			(set) => this._announceUnsubscribe([...set.values()]),
			props?.subscriptionDebounceDelay ?? 50,
		);
	}

	/**
	 * Configure deterministic topic-root candidates and disable the pubsub "auto"
	 * candidate mode.
	 *
	 * Auto mode is a convenience for small ad-hoc networks where no bootstraps/
	 * routers are configured. When an explicit candidate set is provided (e.g.
	 * from bootstraps or a test harness), we must stop mutating/gossiping the
	 * candidate set; otherwise shard root resolution can diverge and overlays can
	 * partition (especially in sparse graphs).
	 */
	public setTopicRootCandidates(candidates: string[]) {
		this.topicRootControlPlane.setTopicRootCandidates(candidates);

		// Disable auto mode and stop its background gossip/timers.
		this.autoTopicRootCandidates = false;
		this.autoTopicRootCandidateSet = undefined;
		for (const t of this.autoCandidatesBroadcastTimers) clearTimeout(t);
		this.autoCandidatesBroadcastTimers = [];
		if (this.autoCandidatesGossipInterval) {
			clearInterval(this.autoCandidatesGossipInterval);
			this.autoCandidatesGossipInterval = undefined;
		}
		this.autoCandidatesGossipUntil = 0;

		// Re-resolve roots under the new mapping.
		this.shardRootCache.clear();
		// Only candidates can become deterministic roots. Avoid doing a full shard
		// scan on non-candidates in large sessions.
		if (candidates.includes(this.publicKeyHash)) {
			void this.hostShardRootsNow().catch(() => {});
		}
		this.scheduleReconcileShardOverlays();
	}

	public override async start() {
		await this.fanout.start();
		await super.start();

		if (this.hostShards) {
			await this.hostShardRootsNow();
		}
	}

	public override async stop() {
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
		for (const t of this.autoCandidatesBroadcastTimers) clearTimeout(t);
		this.autoCandidatesBroadcastTimers = [];
		if (this.autoCandidatesGossipInterval) {
			clearInterval(this.autoCandidatesGossipInterval);
			this.autoCandidatesGossipInterval = undefined;
		}
		this.autoCandidatesGossipUntil = 0;

		this.subscriptions.clear();
		this.pendingSubscriptions.clear();
		this.topics.clear();
		this.peerToTopic.clear();
		this.lastSubscriptionMessages.clear();
		this.shardRootCache.clear();
		this.shardTopicCache.clear();
		this.shardRefCounts.clear();
		this.pinnedShards.clear();

		this.debounceSubscribeAggregator.close();
		this.debounceUnsubscribeAggregator.close();
		return super.stop();
	}

	public override async onPeerConnected(
		peerId: Libp2pPeerId,
		connection: Connection,
	) {
		await super.onPeerConnected(peerId, connection);

		// If we're in auto-candidate mode, expand the deterministic shard-root
		// candidate set as neighbours connect, then reconcile shard overlays and
		// re-announce subscriptions so membership knowledge converges.
		if (!this.autoTopicRootCandidates) return;
		let peerHash: string;
		try {
			peerHash = getPublicKeyFromPeerId(peerId).hashcode();
		} catch {
			return;
		}
		void this.maybeUpdateAutoTopicRootCandidates(peerHash);
	}

	// Ensure auto-candidate mode converges even when libp2p topology callbacks
	// are delayed or only fire for one side of a connection. `addPeer()` runs for
	// both inbound + outbound protocol streams once the remote public key is known.
	public override addPeer(
		peerId: Libp2pPeerId,
		publicKey: PublicSignKey,
		protocol: string,
		connId: string,
	): PeerStreams {
		const peer = super.addPeer(peerId, publicKey, protocol, connId);
		if (this.autoTopicRootCandidates) {
			void this.maybeUpdateAutoTopicRootCandidates(publicKey.hashcode());
			this.scheduleAutoTopicRootCandidatesBroadcast([peer]);
		}
		return peer;
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
		this.shardRootCache.clear();

		// Ensure we host any shard roots we're now responsible for. This is important
		// in tests where candidates may be configured before protocol streams have
		// fully started; earlier `hostShardRootsNow()` attempts can be skipped,
		// leading to join timeouts.
		void this.hostShardRootsNow().catch(() => {});
		this.scheduleReconcileShardOverlays();
		return true;
	}

	private maybeUpdateAutoTopicRootCandidates(peerHash: string) {
		if (!this.autoTopicRootCandidates) return;
		if (!peerHash || peerHash === this.publicKeyHash) return;

		if (this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured())
			return;

		const current = this.topicRootControlPlane.getTopicRootCandidates();
		const managed = this.autoTopicRootCandidateSet;

		if (current.includes(peerHash)) return;

		managed?.add(peerHash);
		const next = this.normalizeAutoTopicRootCandidates(
			managed ? [...managed] : [...current, peerHash],
		);
		this.autoTopicRootCandidateSet = new Set(next);
		this.topicRootControlPlane.setTopicRootCandidates(next);
		this.shardRootCache.clear();
		this.scheduleReconcileShardOverlays();

		// In auto-candidate mode, shard roots are selected deterministically across
		// *all* connected peers (not just those currently subscribed to a shard).
		// That means a peer can be selected as root for shards it isn't using yet.
		// Ensure we proactively host the shard roots we're responsible for so other
		// peers can join without timing out in small ad-hoc networks.
		void this.hostShardRootsNow().catch(() => {});

		// Share the updated candidate set so other peers converge on the same
		// deterministic mapping even in partially connected topologies.
		this.scheduleAutoTopicRootCandidatesBroadcast();
	}

	private normalizeAutoTopicRootCandidates(candidates: string[]): string[] {
		const unique = new Set<string>();
		for (const c of candidates) {
			if (!c) continue;
			unique.add(c);
		}
		unique.add(this.publicKeyHash);
		const sorted = [...unique].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
		return sorted.slice(0, AUTO_TOPIC_ROOT_CANDIDATES_MAX);
	}

	private scheduleAutoTopicRootCandidatesBroadcast(targets?: PeerStreams[]) {
		if (!this.autoTopicRootCandidates) return;
		if (!this.started || this.stopping) return;

		if (targets && targets.length > 0) {
			void this.sendAutoTopicRootCandidates(targets).catch(() => {});
			return;
		}

		for (const t of this.autoCandidatesBroadcastTimers) clearTimeout(t);
		this.autoCandidatesBroadcastTimers = [];

		// Burst a few times to survive early "stream not writable yet" races.
		const delays = [25, 500, 2_000];
		for (const delayMs of delays) {
			const t = setTimeout(() => {
				void this.sendAutoTopicRootCandidates().catch(() => {});
			}, delayMs);
			t.unref?.();
			this.autoCandidatesBroadcastTimers.push(t);
		}

		// Keep gossiping for a while after changes so partially connected topologies
		// converge even under slow stream negotiation.
		this.autoCandidatesGossipUntil = Date.now() + 60_000;
		this.ensureAutoCandidatesGossipInterval();
	}

	private ensureAutoCandidatesGossipInterval() {
		if (!this.autoTopicRootCandidates) return;
		if (!this.started || this.stopping) return;
		if (this.autoCandidatesGossipInterval) return;
		this.autoCandidatesGossipInterval = setInterval(() => {
			if (!this.started || this.stopping || !this.autoTopicRootCandidates)
				return;
			if (
				this.autoCandidatesGossipUntil > 0 &&
				Date.now() > this.autoCandidatesGossipUntil
			) {
				if (this.autoCandidatesGossipInterval) {
					clearInterval(this.autoCandidatesGossipInterval);
					this.autoCandidatesGossipInterval = undefined;
				}
				return;
			}
			void this.sendAutoTopicRootCandidates().catch(() => {});
		}, 2_000);
		this.autoCandidatesGossipInterval.unref?.();
	}

	private async sendAutoTopicRootCandidates(targets?: PeerStreams[]) {
		if (!this.started) throw new NotStartedError();
		const streams = targets ?? [...this.peers.values()];
		if (streams.length === 0) return;

		const candidates = this.topicRootControlPlane.getTopicRootCandidates();
		if (candidates.length === 0) return;

		const msg = new TopicRootCandidates({ candidates });
		const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
			mode: new AnyWhere(),
			priority: 1,
			skipRecipientValidation: true,
		} as any);
		await this.publishMessage(this.publicKey, embedded, streams).catch(
			dontThrowIfDeliveryError,
		);
	}

	private mergeAutoTopicRootCandidatesFromPeer(candidates: string[]): boolean {
		if (!this.autoTopicRootCandidates) return false;
		if (this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured())
			return false;
		const managed = this.autoTopicRootCandidateSet;
		if (!managed) return false;

		const before = this.topicRootControlPlane.getTopicRootCandidates();
		for (const c of candidates) {
			if (!c) continue;
			managed.add(c);
		}
		const next = this.normalizeAutoTopicRootCandidates([...managed]);
		if (
			before.length === next.length &&
			before.every((c, i) => c === next[i])
		) {
			return false;
		}

		this.autoTopicRootCandidateSet = new Set(next);
		this.topicRootControlPlane.setTopicRootCandidates(next);
		this.shardRootCache.clear();
		this.scheduleReconcileShardOverlays();
		void this.hostShardRootsNow().catch(() => {});
		this.scheduleAutoTopicRootCandidatesBroadcast();
		return true;
	}

	private scheduleReconcileShardOverlays() {
		if (this.reconcileShardOverlaysInFlight) return;
		this.reconcileShardOverlaysInFlight = this.reconcileShardOverlays()
			.catch(() => {
				// best-effort retry: fanout streams/roots might not be ready yet.
				if (!this.started || this.stopping) return;
				const t = setTimeout(() => this.scheduleReconcileShardOverlays(), 250);
				t.unref?.();
			})
			.finally(() => {
				this.reconcileShardOverlaysInFlight = undefined;
			});
	}

	private async reconcileShardOverlays() {
		if (!this.started) return;

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
				const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
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
		const index = topicHash32(t) % this.shardCount;
		const shardTopic = `${this.shardTopicPrefix}${index}`;
		this.shardTopicCache.set(t, shardTopic);
		return shardTopic;
	}

	private async resolveShardRoot(shardTopic: string): Promise<string> {
		// If someone configured topic-root candidates externally (e.g. TestSession router
		// selection or Peerbit.bootstrap) after this peer entered auto mode, disable auto
		// mode before we cache any roots based on a stale candidate set.
		if (this.autoTopicRootCandidates) {
			this.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured();
		}

		const cached = this.shardRootCache.get(shardTopic);
		if (cached) return cached;
		const resolved =
			await this.topicRootControlPlane.resolveTopicRoot(shardTopic);
		if (!resolved) {
			throw new Error(
				`No root resolved for shard topic ${shardTopic}. Configure TopicRootControlPlane candidates/resolver/trackers.`,
			);
		}
		this.shardRootCache.set(shardTopic, resolved);
		return resolved;
	}

	private async ensureFanoutChannel(
		shardTopic: string,
		options?: {
			ephemeral?: boolean;
			pin?: boolean;
			root?: string;
			signal?: AbortSignal;
		},
	): Promise<void> {
		const t = shardTopic.toString();
		const pin = options?.pin === true;
		const wantEphemeral = options?.ephemeral === true;

		// Allow callers that already resolved the shard root (e.g. hostShardRootsNow)
		// to pass it through to avoid a race where the candidate set changes between
		// two resolve calls, causing an unnecessary (and potentially slow) join.
		let root: string | undefined = options?.root;
		const existing = this.fanoutChannels.get(t);
		if (existing) {
			root = root ?? (await this.resolveShardRoot(t));
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
				return;
			}

			// Root mapping changed (candidate set updated): migrate to the new overlay.
			await withAbort(this.closeFanoutChannel(t, { force: true }), options?.signal);
		}

		root = root ?? (await this.resolveShardRoot(t));
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
				pubsubMessage = PubSubMessage.from(dm.data);
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
			} else {
				return;
			}

			void (async () => {
				const msgId = await getMsgId(payload);
				const seen = this.seenCache.get(msgId);
				this.seenCache.add(msgId, seen ? seen + 1 : 1);
				if (seen) return;

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
					channel.close();
				} catch {
					// ignore
				}
				throw error;
			}
		})();

		const lastUsedAt = Date.now();
		if (pin) this.pinnedShards.add(t);
		this.fanoutChannels.set(t, {
			root,
			channel,
			join,
			onData,
			onUnicast,
			ephemeral: wantEphemeral,
			lastUsedAt,
		});
		join.catch(() => {
			this.fanoutChannels.delete(t);
		});
		join
			.then(() => {
				const st = this.fanoutChannels.get(t);
				if (st?.ephemeral) this.scheduleFanoutIdleClose(t);
			})
			.catch(() => {
				// ignore
			});
		if (wantEphemeral) {
			this.evictEphemeralFanoutChannels(t);
		}
		await withAbort(join, options?.signal);
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

	public async hostShardRootsNow() {
		if (!this.started) throw new NotStartedError();
		const joins: Promise<void>[] = [];
		for (let i = 0; i < this.shardCount; i++) {
			const shardTopic = `${this.shardTopicPrefix}${i}`;
			const root = await this.resolveShardRoot(shardTopic);
			if (root !== this.publicKeyHash) continue;
			joins.push(this.ensureFanoutChannel(shardTopic, { pin: true, root }));
		}
		await Promise.all(joins);
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
				const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
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
					const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
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
				const persistent = (this.shardRefCounts.get(shardTopic) ?? 0) > 0;
				await this.ensureFanoutChannel(shardTopic, { ephemeral: !persistent });

				const msg = new GetSubscribers({ topics: userTopics });
				const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
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
						await st.channel.publish(payload);
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
			const message = await this.createMessage(msg?.bytes(), {
				...options,
				skipRecipientValidation: this.dispatchEventOnSelfPublish,
			});

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

			const silentDelivery = options?.mode instanceof SilentDelivery;
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
		const embedded = await this.createMessage(toUint8Array(msg.bytes()), {
			mode: new AnyWhere(),
			priority: options?.priority,
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

	public onPeerSession(key: PublicSignKey, _session: number): void {
		this.removeSubscriptions(key);
	}

	public override onPeerUnreachable(publicKeyHash: string) {
		super.onPeerUnreachable(publicKeyHash);
		const key = this.peerKeyHashToPublicKey.get(publicKeyHash);
		if (key) this.removeSubscriptions(key);
	}

	private removeSubscriptions(publicKey: PublicSignKey) {
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
					detail: new UnsubcriptionEvent(publicKey, changed),
				}),
			);
		}
	}

	private subscriptionMessageIsLatest(
		message: DataMessage,
		pubsubMessage: Subscribe | Unsubscribe,
		relevantTopics: string[],
	) {
		const subscriber = message.header.signatures!.signatures[0].publicKey!;
		const subscriberKey = subscriber.hashcode();
		const messageTimestamp = message.header.timestamp;

		for (const topic of relevantTopics) {
			const lastTimestamp = this.lastSubscriptionMessages
				.get(subscriberKey)
				?.get(topic);
			if (lastTimestamp != null && lastTimestamp > messageTimestamp) {
				return false;
			}
		}

		for (const topic of relevantTopics) {
			if (!this.lastSubscriptionMessages.has(subscriberKey)) {
				this.lastSubscriptionMessages.set(subscriberKey, new Map());
			}
			this.lastSubscriptionMessages
				.get(subscriberKey)!
				.set(topic, messageTimestamp);
		}
		return true;
	}

	private async sendFanoutUnicastOrBroadcast(
		shardTopic: string,
		targetHash: string,
		payload: Uint8Array,
	) {
		const st = this.fanoutChannels.get(shardTopic);
		if (!st) return;
		try {
			await st.channel.unicastToAck(targetHash, payload, { timeoutMs: 5_000 });
			return;
		} catch {
			// ignore and fall back
		}
		try {
			await st.channel.publish(payload);
		} catch {
			// ignore
		}
	}

	private async processDirectPubSubMessage(input: {
		pubsubMessage: PubSubMessage;
		message: DataMessage;
	}): Promise<void> {
		const { pubsubMessage, message } = input;

		if (pubsubMessage instanceof TopicRootCandidates) {
			// Used only to converge deterministic shard-root candidates in auto mode.
			this.mergeAutoTopicRootCandidatesFromPeer(pubsubMessage.candidates);
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
					if (!existing || existing.session < message.header.session) {
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
						peers.set(senderKey, existing);
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
					const response = new Subscribe({
						topics: overlap,
						requestSubscribers: false,
					});
					const embedded = await this.createMessage(
						toUint8Array(response.bytes()),
						{
							mode: new AnyWhere(),
							priority: 1,
							skipRecipientValidation: true,
						} as any,
					);
					const payload = toUint8Array(embedded.bytes());
					await this.sendFanoutUnicastOrBroadcast(
						shardTopic,
						senderKey,
						payload,
					);
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
							detail: new UnsubcriptionEvent(sender, changed),
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
				toUint8Array(response.bytes()),
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
			pubsubMessage = PubSubMessage.from(message.data);
		} catch {
			return super.onDataMessage(from, stream, message, seenBefore);
		}

		// DirectStream only supports targeted pubsub data and a small set of utility
		// messages. All membership/control traffic is shard-only.
		if (
			!(pubsubMessage instanceof PubSubData) &&
			!(pubsubMessage instanceof TopicRootCandidates)
		) {
			return true;
		}

		// Determine if this node should process it.
		let isForMe = false;
		if (deliveryModeHasReceiver(message.header.mode)) {
			isForMe = message.header.mode.to.includes(this.publicKeyHash);
		} else if (
			message.header.mode instanceof AnyWhere ||
			message.header.mode instanceof AcknowledgeAnyWhere
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
				await this.processDirectPubSubMessage({ pubsubMessage, message });
			}
		}

		// Forward direct PubSubData only (subscription control lives on fanout shards).
		if (!(pubsubMessage instanceof PubSubData)) {
			return true;
		}

		if (
			message.header.mode instanceof SilentDelivery ||
			message.header.mode instanceof AcknowledgeDelivery
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
			((message.header.mode instanceof AcknowledgeDelivery ||
				message.header.mode instanceof AcknowledgeAnyWhere) &&
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
