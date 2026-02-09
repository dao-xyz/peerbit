import { type PeerId as Libp2pPeerId } from "@libp2p/interface";
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
	SilentDelivery,
	deliveryModeHasReceiver,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import { Uint8ArrayList } from "uint8arraylist";
import {
	type DebouncedAccumulatorCounterMap,
	debouncedAccumulatorSetCounter,
} from "./debounced-set.js";
import { TopicRootControlPlane } from "./topic-root-control-plane.js";

export * from "./fanout-tree.js";
export * from "./fanout-channel.js";
export * from "./topic-root-control-plane.js";

export const toUint8Array = (arr: Uint8ArrayList | Uint8Array) =>
	arr instanceof Uint8ArrayList ? arr.subarray() : arr;

export const logger = loggerFn("peerbit:transport:lazysub");
const warn = logger.newScope("warn");
const logError = (e?: { message: string }) => {
	logger.error(e?.message);
};
const logErrorIfStarted = (e?: { message: string }) => {
	e instanceof NotStartedError === false && logError(e);
};

const SUBSCRIBER_CACHE_MAX_ENTRIES_HARD_CAP = 100_000;
const SUBSCRIBER_CACHE_DEFAULT_MAX_ENTRIES = 4_096;

export type TopicControlPlaneOptions = DirectStreamOptions & {
	dispatchEventOnSelfPublish?: boolean;
	subscriptionDebounceDelay?: number;
	topicRootControlPlane?: TopicRootControlPlane;
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

/**
 * Runtime control-plane implementation for pubsub topic membership + forwarding.
 */
export class TopicControlPlane
	extends DirectStream<PubSubEvents>
	implements PubSub
{
	public topics: Map<string, Map<string, SubscriptionData>>; // topic -> peers --> Uint8Array subscription metadata (the latest received)
	public peerToTopic: Map<string, Set<string>>; // peer -> topics
	public topicsToPeers: Map<string, Set<string>>; // topic -> peers
	public subscriptions: Map<string, { counter: number }>; // topic -> subscription ids
	public lastSubscriptionMessages: Map<string, Map<string, bigint>> = new Map();
	public dispatchEventOnSelfPublish: boolean;
	public readonly topicRootControlPlane: TopicRootControlPlane;
	public readonly subscriberCacheMaxEntries: number;

	private debounceSubscribeAggregator: DebouncedAccumulatorCounterMap;
	private debounceUnsubscribeAggregator: DebouncedAccumulatorCounterMap;

	constructor(
		components: TopicControlPlaneComponents,
		props?: TopicControlPlaneOptions,
	) {
		super(components, ["/lazysub/0.0.1"], props);
		this.subscriptions = new Map();
		this.topics = new Map();
		this.topicsToPeers = new Map();
		this.peerToTopic = new Map();
		this.topicRootControlPlane =
			props?.topicRootControlPlane || new TopicRootControlPlane();
		this.dispatchEventOnSelfPublish =
			props?.dispatchEventOnSelfPublish || false;
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
		this.debounceUnsubscribeAggregator = debouncedAccumulatorSetCounter(
			(set) => this._unsubscribe([...set.values()]),
			props?.subscriptionDebounceDelay ?? 50,
		);
	}

	stop() {
		this.subscriptions.clear();
		this.topics.clear();
		this.peerToTopic.clear();
		this.topicsToPeers.clear();
		this.lastSubscriptionMessages.clear();
		this.debounceSubscribeAggregator.close();
		this.debounceUnsubscribeAggregator.close();
		return super.stop();
	}

	private initializeTopic(topic: string) {
		this.topics.get(topic) || this.topics.set(topic, new Map());
		this.topicsToPeers.get(topic) || this.topicsToPeers.set(topic, new Set());
	}

	private initializePeer(publicKey: PublicSignKey) {
		this.peerToTopic.get(publicKey.hashcode()) ||
			this.peerToTopic.set(publicKey.hashcode(), new Set());
	}

	async subscribe(topic: string) {
		// this.debounceUnsubscribeAggregator.delete(topic);
		return this.debounceSubscribeAggregator.add({ key: topic });
	}

	/**
	 * Subscribes to a given topic.
	 */
	async _subscribe(topics: { key: string; counter: number }[]) {
		if (!this.started) {
			throw new NotStartedError();
		}

		if (topics.length === 0) {
			return;
		}

		const newTopicsForTopicData: string[] = [];
		for (const { key: topic, counter } of topics) {
			let prev = this.subscriptions.get(topic);
			if (prev) {
				prev.counter += counter;
			} else {
				prev = {
					counter: counter,
				};
				this.subscriptions.set(topic, prev);

				newTopicsForTopicData.push(topic);
				this.listenForSubscribers(topic);
			}
		}

		if (newTopicsForTopicData.length > 0) {
			const message = new DataMessage({
				data: toUint8Array(
					new Subscribe({
						topics: newTopicsForTopicData,
						requestSubscribers: true,
					}).bytes(),
				),
				header: new MessageHeader({
					priority: 1,
					// Flood but require ACKs so routes become populated (enables routed fanout vs blind flooding).
					mode: new AcknowledgeAnyWhere({ redundancy: 1 }),
					session: this.session,
				}),
			});

			await this.publishMessage(this.publicKey, await message.sign(this.sign));
		}
	}

	async unsubscribe(topic: string) {
		if (this.debounceSubscribeAggregator.has(topic)) {
			this.debounceSubscribeAggregator.delete(topic); // cancel subscription before it performed
			return false;
		}
		const subscriptions = this.subscriptions.get(topic);
		await this.debounceUnsubscribeAggregator.add({ key: topic });
		return !!subscriptions;
	}

	async _unsubscribe(
		topics: { key: string; counter: number }[],
		options?: { force: boolean },
	) {
		if (!this.started) {
			throw new NotStartedError();
		}

		let topicsToUnsubscribe: string[] = [];
		for (const { key: topic, counter } of topics) {
			if (counter <= 0) {
				continue;
			}
			const subscriptions = this.subscriptions.get(topic);

			logger.trace(
				`unsubscribe from ${topic} - am subscribed with subscriptions ${JSON.stringify(subscriptions)}`,
			);

			if (!subscriptions) {
				// Not subscribed (any longer). Treat as local cache cleanup.
				const peersOnTopic = this.topicsToPeers.get(topic);
				if (peersOnTopic) {
					for (const peer of [...peersOnTopic]) {
						const last = this.lastSubscriptionMessages.get(peer);
						last?.delete(topic);
						if (last && last.size === 0)
							this.lastSubscriptionMessages.delete(peer);
						this.deletePeerFromTopic(topic, peer);
					}
				}
				this.topics.delete(topic);
				this.topicsToPeers.delete(topic);
				continue;
			}

			if (subscriptions?.counter && subscriptions?.counter >= 0) {
				subscriptions.counter -= counter;
			}

			if (!subscriptions.counter || options?.force) {
				const peersOnTopic = this.topicsToPeers.get(topic);
				if (peersOnTopic) {
					for (const peer of [...peersOnTopic]) {
						const last = this.lastSubscriptionMessages.get(peer);
						last?.delete(topic);
						if (last && last.size === 0)
							this.lastSubscriptionMessages.delete(peer);
						this.deletePeerFromTopic(topic, peer);
					}
				}

				topicsToUnsubscribe.push(topic);
				this.subscriptions.delete(topic);
				this.topics.delete(topic);
				this.topicsToPeers.delete(topic);
			}
		}

		if (topicsToUnsubscribe.length > 0) {
			await this.publishMessage(
				this.publicKey,
				await new DataMessage({
					header: new MessageHeader({
						mode: new AnyWhere(), // TODO make this better
						session: this.session,
						priority: 1,
					}),
					data: toUint8Array(
						new Unsubscribe({
							topics: topicsToUnsubscribe,
						}).bytes(),
					),
				}).sign(this.sign),
			);
		}
	}

	getSubscribers(topic: string): PublicSignKey[] | undefined {
		const remote = this.topics.get(topic.toString());

		if (!remote) {
			return undefined;
		}
		const ret: PublicSignKey[] = [];
		for (const v of remote.values()) {
			ret.push(v.publicKey);
		}
		if (this.subscriptions.get(topic)) {
			ret.push(this.publicKey);
		}
		return ret;
	}

	private listenForSubscribers(topic: string) {
		this.initializeTopic(topic);
	}

	async requestSubscribers(
		topic: string | string[],
		to?: PublicSignKey,
	): Promise<void> {
		if (!this.started) {
			throw new NotStartedError();
		}

		if (topic == null) {
			throw new Error("ERR_NOT_VALID_TOPIC");
		}

		if (topic.length === 0) {
			return;
		}

		const topics = typeof topic === "string" ? [topic] : topic;
		for (const topic of topics) {
			this.listenForSubscribers(topic);
		}

		const stream = to ? this.peers.get(to.hashcode()) : undefined;
		const mode = stream
			? new AcknowledgeDelivery({ to: [to!], redundancy: 1 })
			: // If we don't have a direct stream yet, targeted delivery may not be routable.
				// Fall back to a route-learning broadcast so subscribers can respond and routes can form.
				new AcknowledgeAnyWhere({ redundancy: 1 });

		const message = await new DataMessage({
			data: toUint8Array(new GetSubscribers({ topics }).bytes()),
			header: new MessageHeader({
				// Route-learning broadcast: flood but require ACKs so DirectStream can populate routes
				// (used by pubsub routing to avoid forwarding to non-subscribers).
				mode,
				session: this.session,
				priority: 1,
			}),
		}).sign(this.sign);

		return this.publishMessage(
			this.publicKey,
			message,
			stream ? [stream] : undefined,
		);
	}

	getPeersOnTopics(topics: string[]): Set<string> {
		const newPeers: Set<string> = new Set();
		if (topics?.length) {
			for (const topic of topics) {
				const peersOnTopic = this.topicsToPeers.get(topic);
				if (peersOnTopic) {
					peersOnTopic.forEach((peer) => {
						newPeers.add(peer);
					});
				}
			}
		}
		return newPeers;
	}

	private shouldSendMessage(tos?: string[] | Set<string>) {
		if (
			Array.isArray(tos) &&
			(tos.length === 0 || (tos.length === 1 && tos[0] === this.publicKeyHash))
		) {
			// skip this one
			return false;
		}

		if (
			tos instanceof Set &&
			(tos.size === 0 || (tos.size === 1 && tos.has(this.publicKeyHash)))
		) {
			// skip this one
			return false;
		}

		return true;
	}

	async publish(
		data: Uint8Array | undefined,
		options?: {
			topics: string[];
		} & { client?: string } & {
			mode?: SilentDelivery | AcknowledgeDelivery;
		} & PriorityOptions &
			IdOptions & { signal?: AbortSignal },
	): Promise<Uint8Array | undefined> {
		if (!this.started) {
			throw new NotStartedError();
		}

		const topics =
			(options as { topics: string[] }).topics?.map((x) => x.toString()) || [];

		const hasExplicitTOs =
			options?.mode && deliveryModeHasReceiver(options.mode);
		let tos = hasExplicitTOs ? options.mode?.to : this.getPeersOnTopics(topics);

		// Bootstrap publish (best-effort):
		// If the caller publishes on a topic it is NOT subscribed to, we may have zero
		// subscriber knowledge for that topic yet (cache cold), even though subscribers
		// exist. In that situation, send to a small bounded set of already connected peers
		// so the message has a chance to reach a subscriber and warm the cache.
		//
		// Important: when publishing on topics we *are* subscribed to, keep the legacy
		// behavior: if we only know about ourselves (no known remote subscribers), treat
		// it as a no-op unless `dispatchEventOnSelfPublish` is enabled.
		const canBootstrapToConnectedPeers =
			topics.length > 0 && topics.every((t) => !this.subscriptions.has(t));
		if (
			canBootstrapToConnectedPeers &&
			!hasExplicitTOs &&
			tos instanceof Set &&
			tos.size === 0
		) {
			const bootstrap = new Set<string>();
			const push = (hash?: string) => {
				if (!hash) return;
				if (hash === this.publicKeyHash) return;
				if (bootstrap.has(hash)) return;
				bootstrap.add(hash);
			};

			for (const h of this.peers.keys()) {
				push(h);
				if (bootstrap.size >= 32) break;
			}
			if (bootstrap.size < 32) {
				for (const conn of this.components.connectionManager.getConnections()) {
					try {
						push(getPublicKeyFromPeerId(conn.remotePeer).hashcode());
					} catch {
						// ignore unexpected key types
					}
					if (bootstrap.size >= 32) break;
				}
			}

			if (bootstrap.size > 0) {
				tos = bootstrap;
			}
		}

		// Embedd topic info before the data so that peers/relays can also use topic info to route messages efficiently
		const dataMessage = data
			? new PubSubData({
					topics: topics.map((x) => x.toString()),
					data,
					strict: hasExplicitTOs,
				})
			: undefined;

		const bytes = dataMessage?.bytes();
		const silentDelivery = options?.mode instanceof SilentDelivery;

		// do send check before creating and signing the message
		if (!this.dispatchEventOnSelfPublish && !this.shouldSendMessage(tos)) {
			return;
		}

		const message = await this.createMessage(bytes, {
			...options,
			to: tos,
			skipRecipientValidation: this.dispatchEventOnSelfPublish,
		});

		if (dataMessage) {
			this.dispatchEvent(
				new CustomEvent("publish", {
					detail: new PublishEvent({
						client: options?.client,
						data: dataMessage,
						message,
					}),
				}),
			);
		}

		// for emitSelf we do this check here, since we don't want to send the message to ourselves
		if (this.dispatchEventOnSelfPublish && !this.shouldSendMessage(tos)) {
			return message.id;
		}

		// send to all the other peers
		try {
			await this.publishMessage(
				this.publicKey,
				message,
				undefined,
				undefined,
				options?.signal,
			);
		} catch (error) {
			if (error instanceof DeliveryError) {
				if (silentDelivery === false) {
					// If we are not in silent mode, we should throw the error
					throw error;
				}
				return message.id;
			}
			throw error;
		}

		return message.id;
	}

	private deletePeerFromTopic(topic: string, publicKeyHash: string) {
		const peers = this.topics.get(topic);
		let change: SubscriptionData | undefined = undefined;
		if (peers) {
			change = peers.get(publicKeyHash);
		}

		this.topics.get(topic)?.delete(publicKeyHash);

		this.peerToTopic.get(publicKeyHash)?.delete(topic);
		if (!this.peerToTopic.get(publicKeyHash)?.size) {
			this.peerToTopic.delete(publicKeyHash);
			this.lastSubscriptionMessages.delete(publicKeyHash);
		}

		this.topicsToPeers.get(topic)?.delete(publicKeyHash);

		return change;
	}

	private pruneTopicSubscribers(topic: string) {
		const peers = this.topics.get(topic);
		if (!peers) return;

		while (peers.size > this.subscriberCacheMaxEntries) {
			const oldest = peers.keys().next().value as string | undefined;
			if (!oldest) break;
			this.deletePeerFromTopic(topic, oldest);
		}
	}

	private getSubscriptionOverlap(topics?: string[]) {
		const subscriptions: string[] = [];
		if (topics) {
			for (const topic of topics) {
				const subscription = this.subscriptions.get(topic);
				if (subscription) {
					subscriptions.push(topic);
				}
			}
		} else {
			for (const [topic, _subscription] of this.subscriptions) {
				subscriptions.push(topic);
			}
		}
		return subscriptions;
	}

	public onPeerSession(key: PublicSignKey, session: number): void {
		// reset subs, the peer has restarted
		this.removeSubscriptions(key);
	}

	public async onPeerReachable(publicKey: PublicSignKey) {
		// Aggregate subscribers for my topics through this new peer because if we don't do this we might end up with a situtation where
		// we act as a relay and relay messages for a topic, but don't forward it to this new peer because we never learned about their subscriptions

		const resp = super.onPeerReachable(publicKey);
		const stream = this.peers.get(publicKey.hashcode());

		if (this.subscriptions.size > 0) {
			// Tell the newly reachable peer about our subscriptions. This also acts as a
			// catch-up mechanism when we learn about new peers via routed ACKs (not only
			// direct neighbours).
			this.publishMessage(
				this.publicKey,
				await new DataMessage({
					data: toUint8Array(
						new Subscribe({
							topics: this.getSubscriptionOverlap(), // TODO: protocol efficiency; do we really need to share *everything*?
							requestSubscribers: true,
						}).bytes(),
					),
					header: new MessageHeader({
						priority: 1,
						// If this is a direct neighbour, seed subscription gossip via that connection
						// (and let it relay further). If it is only reachable via routes, deliver a
						// unicast catch-up to that peer.
						mode: stream
							? new AnyWhere()
							: new SilentDelivery({ to: [publicKey], redundancy: 1 }),
						session: this.session,
					}),
				}).sign(this.sign),
				stream ? [stream] : undefined,
			).catch(dontThrowIfDeliveryError); // peer might have become unreachable immediately
		}

		return resp;
	}

	public onPeerUnreachable(publicKeyHash: string) {
		super.onPeerUnreachable(publicKeyHash);
		this.removeSubscriptions(this.peerKeyHashToPublicKey.get(publicKeyHash)!);
	}

	private removeSubscriptions(publicKey: PublicSignKey) {
		const peerTopics = this.peerToTopic.get(publicKey.hashcode());

		const changed: string[] = [];
		if (peerTopics) {
			for (const topic of peerTopics) {
				const change = this.deletePeerFromTopic(topic, publicKey.hashcode());
				if (change) {
					changed.push(topic);
				}
			}
		}
		this.lastSubscriptionMessages.delete(publicKey.hashcode());

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
	) {
		const subscriber = message.header.signatures!.signatures[0].publicKey!;
		const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing
		const messageTimestamp = message.header.timestamp;

		for (const topic of pubsubMessage.topics) {
			const lastTimestamp = this.lastSubscriptionMessages
				.get(subscriberKey)
				?.get(topic);
			if (lastTimestamp != null && lastTimestamp > messageTimestamp) {
				return false; // message is old
			}
		}

		for (const topic of pubsubMessage.topics) {
			if (!this.lastSubscriptionMessages.has(subscriberKey)) {
				this.lastSubscriptionMessages.set(subscriberKey, new Map());
			}
			this.lastSubscriptionMessages
				.get(subscriberKey)
				?.set(topic, messageTimestamp);
		}
		return true;
	}

	private addPeersOnTopic(message: DataMessage, topics: string[]) {
		if (!deliveryModeHasReceiver(message.header.mode)) {
			return;
		}
		const existingPeers: Set<string> = new Set(message.header.mode.to);
		const allPeersOnTopic = this.getPeersOnTopics(topics);

		for (const existing of existingPeers) {
			allPeersOnTopic.add(existing);
		}

		allPeersOnTopic.delete(this.publicKeyHash);
		message.header.mode.to = [...allPeersOnTopic];
	}

	async onDataMessage(
		from: PublicSignKey,
		stream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		if (!message.data || message.data.length === 0) {
			return super.onDataMessage(from, stream, message, seenBefore);
		}

		if (this.shouldIgnore(message, seenBefore)) {
			return false;
		}

		const pubsubMessage = PubSubMessage.from(message.data);
		if (pubsubMessage instanceof PubSubData) {
			if (message.header.mode instanceof AnyWhere) {
				throw new Error("Unexpected mode for PubSubData messages");
			}

			const meInTOs =
				deliveryModeHasReceiver(message.header.mode) &&
				!!message.header.mode.to?.find((x) => this.publicKeyHash === x);

			let isForMe: boolean;
			if (pubsubMessage.strict) {
				isForMe =
					!!pubsubMessage.topics.find((topic) =>
						this.subscriptions.has(topic),
					) && meInTOs;
			} else {
				isForMe =
					!!pubsubMessage.topics.find((topic) =>
						this.subscriptions.has(topic),
					) ||
					(pubsubMessage.topics.length === 0 && meInTOs);
			}

			if (isForMe) {
				if ((await this.verifyAndProcess(message)) === false) {
					warn("Recieved message that did not verify PubSubData");
					return false;
				}
			}

			await this.maybeAcknowledgeMessage(stream, message, seenBefore);

			if (isForMe) {
				if (seenBefore === 0) {
					this.dispatchEvent(
						new CustomEvent("data", {
							detail: new DataEvent({
								data: pubsubMessage,
								message,
							}),
						}),
					);
				}
			}

			// Forward
			if (!pubsubMessage.strict) {
				this.addPeersOnTopic(message, pubsubMessage.topics);
			}

			// Only relay if we got additional receivers
			// or we are NOT subscribing ourselves (if we are not subscribing ourselves we are)
			// If we are not subscribing ourselves, then we don't have enough information to "stop" message propagation here
			if (
				(deliveryModeHasReceiver(message.header.mode) &&
					message.header.mode.to?.length) ||
				!pubsubMessage.topics.find((topic) => this.topics.has(topic))
			) {
				// DONT await this since it might introduce a dead-lock
				this.relayMessage(from, message).catch(logErrorIfStarted);
			}
		} else {
			if ((await this.verifyAndProcess(message)) === false) {
				warn("Recieved message that did not verify Unsubscribe");
				return false;
			}

			if (message.header.signatures!.signatures.length === 0) {
				warn("Recieved subscription message with no signers");
				return false;
			}

			await this.maybeAcknowledgeMessage(stream, message, seenBefore);

			const sender = message.header.signatures!.signatures[0].publicKey!;
			const senderKey = sender.hashcode(); // Assume first signature is the one who is signing

			if (pubsubMessage instanceof Subscribe) {
				if (
					seenBefore === 0 &&
					this.subscriptionMessageIsLatest(message, pubsubMessage) &&
					pubsubMessage.topics.length > 0
				) {
					const changed: string[] = [];
					pubsubMessage.topics.forEach((topic) => {
						const peers = this.topics.get(topic);
						if (peers == null) {
							return;
						}

						this.initializePeer(sender);

						// if no subscription data, or new subscription has data (and is newer) then overwrite it.
						// subscription where data is undefined is not intended to replace existing data
						const existingSubscription = peers.get(senderKey);

						if (
							!existingSubscription ||
							existingSubscription.session < message.header.session
						) {
							// LRU touch
							peers.delete(senderKey);
							peers.set(
								senderKey,
								new SubscriptionData({
									session: message.header.session,
									timestamp: message.header.timestamp, // TODO update timestamps on all messages?
									publicKey: sender,
								}),
							);

							changed.push(topic);
						} else if (existingSubscription) {
							// LRU touch
							peers.delete(senderKey);
							peers.set(senderKey, existingSubscription);
						}

						if (!existingSubscription) {
							this.topicsToPeers.get(topic)?.add(senderKey);
							this.peerToTopic.get(senderKey)?.add(topic);
						}

						this.pruneTopicSubscribers(topic);
					});

					if (changed.length > 0) {
						this.dispatchEvent(
							new CustomEvent<SubscriptionEvent>("subscribe", {
								detail: new SubscriptionEvent(sender, changed),
							}),
						);
					}

					if (pubsubMessage.requestSubscribers) {
						// respond if we are subscribing
						const mySubscriptions = this.getSubscriptionOverlap(
							pubsubMessage.topics,
						);
						if (mySubscriptions.length > 0) {
							const response = new DataMessage({
								data: toUint8Array(
									new Subscribe({
										topics: mySubscriptions,
										requestSubscribers: false,
									}).bytes(),
								),
								header: new MessageHeader({
									session: this.session,
									priority: 1,
									mode: new AnyWhere(),
								}),
							});

							this.publishMessage(
								this.publicKey,
								await response.sign(this.sign),
								[stream],
							).catch(dontThrowIfDeliveryError);
						}
					}
				}

				// Forward
				// DONT await this since it might introduce a dead-lock
				if (seenBefore === 0) {
					this.relayMessage(from, message).catch(logErrorIfStarted);
				}
			} else if (pubsubMessage instanceof Unsubscribe) {
				if (this.subscriptionMessageIsLatest(message, pubsubMessage)) {
					const changed: string[] = [];

					for (const unsubscription of pubsubMessage.topics) {
						const change = this.deletePeerFromTopic(unsubscription, senderKey);
						if (change) {
							changed.push(unsubscription);
						}
					}

					if (changed.length > 0 && seenBefore === 0) {
						this.dispatchEvent(
							new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
								detail: new UnsubcriptionEvent(sender, changed),
							}),
						);
					}
				}

				// DONT await this since it might introduce a dead-lock
				if (seenBefore === 0) {
					this.relayMessage(from, message).catch(logErrorIfStarted);
				}
			} else if (pubsubMessage instanceof GetSubscribers) {
				const subscriptionsToSend: string[] = this.getSubscriptionOverlap(
					pubsubMessage.topics,
				);
				if (subscriptionsToSend.length > 0) {
					// respond
					this.publishMessage(
						this.publicKey,
						await new DataMessage({
							data: toUint8Array(
								new Subscribe({
									topics: subscriptionsToSend,
									requestSubscribers: false,
								}).bytes(),
							),
							header: new MessageHeader({
								priority: 1,
								mode: new AnyWhere(),
								session: this.session,
							}),
						}).sign(this.sign),
						[stream],
					).catch(dontThrowIfDeliveryError); // send back to same stream
				}

				// Forward
				// DONT await this since it might introduce a dead-lock
				if (seenBefore === 0) {
					this.relayMessage(from, message).catch(logErrorIfStarted);
				}
			}
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
