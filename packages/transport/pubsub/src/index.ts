import type { PeerId as Libp2pPeerId } from "@libp2p/interface/peer-id";
import { logger as logFn } from "@peerbit/logger";

import { DataMessage } from "@peerbit/stream-interface";
import {
	DirectStream,
	DirectStreamComponents,
	DirectStreamOptions,
	PeerStreams
} from "@peerbit/stream";

import { CodeError } from "@libp2p/interface/errors";
import {
	PubSubMessage,
	Subscribe,
	PubSubData,
	toUint8Array,
	Unsubscribe,
	GetSubscribers,
	Subscription,
	UnsubcriptionEvent,
	SubscriptionEvent,
	PubSub,
	DataEvent,
	SubscriptionData
} from "@peerbit/pubsub-interface";
import { getPublicKeyFromPeerId, PublicSignKey } from "@peerbit/crypto";
import { CustomEvent } from "@libp2p/interface/events";
import { waitFor } from "@peerbit/time";
import { Connection } from "@libp2p/interface/connection";
import { equals, startsWith } from "@peerbit/uint8arrays";
import { PubSubEvents } from "@peerbit/pubsub-interface";

export const logger = logFn({ module: "direct-sub", level: "warn" });

export interface PeerStreamsInit {
	id: Libp2pPeerId;
	protocol: string;
}

export type DirectSubOptions = {
	aggregate: boolean; // if true, we will collect topic/subscriber info for all traffic
};

export type DirectSubComponents = DirectStreamComponents;

export type PeerId = Libp2pPeerId | PublicSignKey;

export class DirectSub extends DirectStream<PubSubEvents> implements PubSub {
	public topics: Map<string, Map<string, SubscriptionData>>; // topic -> peers --> Uint8Array subscription metadata (the latest received)
	public peerToTopic: Map<string, Set<string>>; // peer -> topics
	public topicsToPeers: Map<string, Set<string>>; // topic -> peers
	public subscriptions: Map<string, { counter: number; data?: Uint8Array }>; // topic -> subscription ids
	public lastSubscriptionMessages: Map<string, Map<string, DataMessage>> =
		new Map();

	constructor(components: DirectSubComponents, props?: DirectStreamOptions) {
		super(components, ["pubsub/0.0.0"], props);
		this.subscriptions = new Map();
		this.topics = new Map();
		this.topicsToPeers = new Map();
		this.peerToTopic = new Map();
	}

	stop() {
		this.subscriptions.clear();
		this.topics.clear();
		this.peerToTopic.clear();
		this.topicsToPeers.clear();
		return super.stop();
	}

	public async onPeerReachable(publicKey: PublicSignKey) {
		// Aggregate subscribers for my topics through this new peer because if we don't do this we might end up with a situtation where
		// we act as a relay and relay messages for a topic, but don't forward it to this new peer because we never learned about their subscriptions
		await this.requestSubscribers([...this.topics.keys()], publicKey);
		return super.onPeerReachable(publicKey);
	}

	public async onPeerDisconnected(peerId: Libp2pPeerId, conn?: Connection) {
		return super.onPeerDisconnected(peerId, conn);
	}

	private initializeTopic(topic: string) {
		this.topics.get(topic) || this.topics.set(topic, new Map());
		this.topicsToPeers.get(topic) || this.topicsToPeers.set(topic, new Set());
	}

	private initializePeer(publicKey: PublicSignKey) {
		this.peerToTopic.get(publicKey.hashcode()) ||
			this.peerToTopic.set(publicKey.hashcode(), new Set());
	}

	/**
	 * Subscribes to a given topic.
	 */
	/**
	 * @param topic,
	 * @param data, metadata associated with the subscription, shared with peers
	 */
	async subscribe(topic: string | string[], options?: { data?: Uint8Array }) {
		if (!this.started) {
			throw new Error("Pubsub has not started");
		}

		topic = typeof topic === "string" ? [topic] : topic;

		const newTopicsForTopicData: string[] = [];
		for (const t of topic) {
			const prev = this.subscriptions.get(t);
			if (prev) {
				const difference =
					!!prev.data != !!options?.data ||
					(prev.data && options?.data && !equals(prev.data, options?.data));
				prev.counter += 1;

				if (difference) {
					prev.data = options?.data;
					newTopicsForTopicData.push(t);
				}
			} else {
				this.subscriptions.set(t, {
					counter: 1,
					data: options?.data
				});

				newTopicsForTopicData.push(t);
				this.listenForSubscribers(t);
			}
		}

		if (newTopicsForTopicData.length > 0) {
			const message = new DataMessage({
				data: toUint8Array(
					new Subscribe({
						subscriptions: newTopicsForTopicData.map(
							(x) => new Subscription(x, options?.data)
						)
					}).bytes()
				)
			});

			await this.publishMessage(
				this.components.peerId,
				await message.sign(this.sign)
			);
		}
	}

	/**
	 *
	 * @param topic
	 * @param force
	 * @returns true unsubscribed completely
	 */
	async unsubscribe(
		topic: string,
		options?: { force: boolean; data: Uint8Array }
	) {
		if (!this.started) {
			throw new Error("Pubsub is not started");
		}

		const subscriptions = this.subscriptions.get(topic);

		logger.debug(
			`unsubscribe from ${topic} - am subscribed with subscriptions ${subscriptions}`
		);

		if (subscriptions?.counter && subscriptions?.counter >= 0) {
			subscriptions.counter -= 1;
		}

		const peersOnTopic = this.topicsToPeers.get(topic);
		if (peersOnTopic) {
			for (const peer of peersOnTopic) {
				this.lastSubscriptionMessages.delete(peer);
			}
		}
		if (!subscriptions?.counter || options?.force) {
			this.subscriptions.delete(topic);
			this.topics.delete(topic);
			this.topicsToPeers.delete(topic);

			await this.publishMessage(
				this.components.peerId,
				await new DataMessage({
					data: toUint8Array(new Unsubscribe({ topics: [topic] }).bytes())
				}).sign(this.sign)
			);
			return true;
		}
		return false;
	}

	getSubscribers(topic: string): Map<string, SubscriptionData> | undefined {
		if (!this.started) {
			throw new CodeError("not started yet", "ERR_NOT_STARTED_YET");
		}

		if (topic == null) {
			throw new CodeError("topic is required", "ERR_NOT_VALID_TOPIC");
		}

		return this.topics.get(topic.toString());
	}

	getSubscribersWithData(
		topic: string,
		data: Uint8Array,
		options?: { prefix: boolean }
	): string[] | undefined {
		const map = this.topics.get(topic);
		if (map) {
			const results: string[] = [];
			for (const [peer, info] of map.entries()) {
				if (!info.data) {
					continue;
				}
				if (options?.prefix) {
					if (!startsWith(info.data, data)) {
						continue;
					}
				} else {
					if (!equals(info.data, data)) {
						continue;
					}
				}
				results.push(peer);
			}
			return results;
		}
		return;
	}

	listenForSubscribers(topic: string) {
		this.initializeTopic(topic);
	}

	async requestSubscribers(
		topic: string | string[],
		from?: PublicSignKey
	): Promise<void> {
		if (!this.started) {
			throw new CodeError("not started yet", "ERR_NOT_STARTED_YET");
		}

		if (topic == null) {
			throw new CodeError("topic is required", "ERR_NOT_VALID_TOPIC");
		}

		if (topic.length === 0) {
			return;
		}

		const topics = typeof topic === "string" ? [topic] : topic;
		for (const topic of topics) {
			this.listenForSubscribers(topic);
		}

		return this.publishMessage(
			this.components.peerId,
			await new DataMessage({
				to: from ? [from.hashcode()] : [],
				data: toUint8Array(new GetSubscribers({ topics }).bytes())
			}).sign(this.sign)
		);
	}

	getPeersWithTopics(topics: string[], otherPeers?: string[]): Set<string> {
		const peers: Set<string> = otherPeers ? new Set(otherPeers) : new Set();
		if (topics?.length) {
			for (const topic of topics) {
				const peersOnTopic = this.topicsToPeers.get(topic.toString());
				if (peersOnTopic) {
					peersOnTopic.forEach((peer) => {
						peers.add(peer);
					});
				}
			}
		}
		return peers;
	}

	/* getStreamsWithTopics(topics: string[], otherPeers?: string[]): PeerStreams[] {
		const peers = this.getNeighboursWithTopics(topics, otherPeers);
		return [...this.peers.values()].filter((s) =>
			peers.has(s.publicKey.hashcode())
		);
	} */

	async publish(
		data: Uint8Array,
		options:
			| {
					topics?: string[];
					to?: (string | PeerId)[];
					strict?: false;
			  }
			| {
					topics: string[];
					to: (string | PeerId)[];
					strict: true;
			  }
	): Promise<Uint8Array> {
		if (!this.started) {
			throw new Error("Not started");
		}

		const topics =
			(options as { topics: string[] }).topics?.map((x) => x.toString()) || [];
		const tos =
			options?.to?.map((x) =>
				x instanceof PublicSignKey
					? x.hashcode()
					: typeof x === "string"
					? x
					: getPublicKeyFromPeerId(x).hashcode()
			) || [];
		// Embedd topic info before the data so that peers/relays can also use topic info to route messages efficiently
		const dataMessage = new PubSubData({
			topics: topics.map((x) => x.toString()),
			data,
			strict: options.strict
		});

		const bytes = dataMessage.bytes();

		const message = await this.createMessage(bytes, options);

		if (this.emitSelf) {
			super.dispatchEvent(
				new CustomEvent("data", {
					detail: new DataEvent(dataMessage, message)
				})
			);
		}

		// send to all the other peers
		await this.publishMessage(this.components.peerId, message, undefined);
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
		}

		this.topicsToPeers.get(topic)?.delete(publicKeyHash);

		return change;
	}
	public onPeerUnreachable(publicKey: PublicSignKey) {
		super.onPeerUnreachable(publicKey);

		const publicKeyHash = publicKey.hashcode();
		const peerTopics = this.peerToTopic.get(publicKeyHash);

		const changed: Subscription[] = [];
		if (peerTopics) {
			for (const topic of peerTopics) {
				const change = this.deletePeerFromTopic(topic, publicKeyHash);
				if (change) {
					changed.push(new Subscription(topic, change.data));
				}
			}
		}
		this.lastSubscriptionMessages.delete(publicKeyHash);

		if (changed.length > 0) {
			this.dispatchEvent(
				new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
					detail: new UnsubcriptionEvent(publicKey, changed)
				})
			);
		}
	}

	private subscriptionMessageIsLatest(
		message: DataMessage,
		pubsubMessage: Subscribe | Unsubscribe
	) {
		const subscriber = message.signatures.signatures[0].publicKey!;
		const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

		for (const topic of pubsubMessage.topics) {
			const lastTimestamp = this.lastSubscriptionMessages
				.get(subscriberKey)
				?.get(topic)?.header.timetamp;
			if (lastTimestamp != null && lastTimestamp > message.header.timetamp) {
				return false; // message is old
			}
		}

		for (const topic of pubsubMessage.topics) {
			if (!this.lastSubscriptionMessages.has(subscriberKey)) {
				this.lastSubscriptionMessages.set(subscriberKey, new Map());
			}
			this.lastSubscriptionMessages.get(subscriberKey)?.set(topic, message);
		}
		return true;
	}

	async onDataMessage(
		from: Libp2pPeerId,
		stream: PeerStreams,
		message: DataMessage
	) {
		const pubsubMessage = PubSubMessage.from(message.data);
		if (pubsubMessage instanceof PubSubData) {
			/**
			 * See if we know more subscribers of the message topics. If so, add aditional end receivers of the message
			 */
			let verified: boolean | undefined = undefined;

			const isFromSelf = this.components.peerId.equals(from);
			if (!isFromSelf || this.emitSelf) {
				let isForMe: boolean;
				if (pubsubMessage.strict) {
					isForMe =
						!!pubsubMessage.topics.find((topic) =>
							this.subscriptions.has(topic)
						) && !!message.to.find((x) => this.publicKeyHash === x);
				} else {
					isForMe =
						!!pubsubMessage.topics.find((topic) =>
							this.subscriptions.has(topic)
						) ||
						(pubsubMessage.topics.length === 0 &&
							!!message.to.find((x) => this.publicKeyHash === x));
				}
				if (isForMe) {
					if (verified === undefined) {
						verified = await message.verify(
							this.signaturePolicy === "StictSign" ? true : false
						);
					}
					if (!verified) {
						logger.warn("Recieved message that did not verify PubSubData");
						return false;
					}
					this.dispatchEvent(
						new CustomEvent("data", {
							detail: new DataEvent(pubsubMessage, message)
						})
					);
				}
			}

			// Forward
			if (!pubsubMessage.strict) {
				const newTos = this.getPeersWithTopics(
					pubsubMessage.topics,
					message.to
				);
				newTos.delete(this.publicKeyHash);
				message.to = [...newTos];
			}

			// Only relay if we got additional receivers
			// or we are NOT subscribing ourselves (if we are not subscribing ourselves we are)
			// If we are not subscribing ourselves, then we don't have enough information to "stop" message propagation here
			if (
				message.to.length > 0 ||
				!pubsubMessage.topics.find((topic) => this.topics.has(topic))
			) {
				await this.relayMessage(from, message);
			}
		} else if (pubsubMessage instanceof Subscribe) {
			if (!(await message.verify(true))) {
				logger.warn("Recieved message that did not verify Subscribe");
				return false;
			}

			if (message.signatures.signatures.length === 0) {
				logger.warn("Recieved subscription message with no signers");
				return false;
			}

			if (pubsubMessage.subscriptions.length === 0) {
				logger.info("Recieved subscription message with no topics");
				return false;
			}

			if (!this.subscriptionMessageIsLatest(message, pubsubMessage)) {
				logger.trace("Recieved old subscription message");
				return false;
			}

			const subscriber = message.signatures.signatures[0].publicKey!;
			const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

			this.initializePeer(subscriber);

			const changed: Subscription[] = [];
			pubsubMessage.subscriptions.forEach((subscription) => {
				const peers = this.topics.get(subscription.topic);
				if (peers == null) {
					return;
				}

				// if no subscription data, or new subscription has data (and is newer) then overwrite it.
				// subscription where data is undefined is not intended to replace existing data
				const existingSubscription = peers.get(subscriberKey);

				if (
					!existingSubscription ||
					(existingSubscription.timestamp < message.header.timetamp &&
						subscription.data)
				) {
					peers.set(
						subscriberKey,
						new SubscriptionData({
							timestamp: message.header.timetamp, // TODO update timestamps on all messages?
							data: subscription.data,
							publicKey: subscriber
						})
					);
					if (
						!existingSubscription?.data ||
						!equals(existingSubscription.data, subscription.data)
					) {
						changed.push(subscription);
					}
				}

				this.topicsToPeers.get(subscription.topic)?.add(subscriberKey);
				this.peerToTopic.get(subscriberKey)?.add(subscription.topic);
			});
			if (changed.length > 0) {
				this.dispatchEvent(
					new CustomEvent<SubscriptionEvent>("subscribe", {
						detail: new SubscriptionEvent(subscriber, changed)
					})
				);
			}

			// Forward
			await this.relayMessage(from, message);
		} else if (pubsubMessage instanceof Unsubscribe) {
			if (!(await message.verify(true))) {
				logger.warn("Recieved message that did not verify Unsubscribe");
				return false;
			}

			if (message.signatures.signatures.length === 0) {
				logger.warn("Recieved subscription message with no signers");
				return false;
			}

			if (!this.subscriptionMessageIsLatest(message, pubsubMessage)) {
				logger.trace("Recieved old subscription message");
				return false;
			}

			const changed: Subscription[] = [];
			const subscriber = message.signatures.signatures[0].publicKey!;
			const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

			for (const unsubscription of pubsubMessage.unsubscriptions) {
				const change = this.deletePeerFromTopic(
					unsubscription.topic,
					subscriberKey
				);
				if (change) {
					changed.push(new Subscription(unsubscription.topic, change.data));
				}
			}

			if (changed.length > 0) {
				this.dispatchEvent(
					new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
						detail: new UnsubcriptionEvent(subscriber, changed)
					})
				);
			}

			// Forward
			await this.relayMessage(from, message);
		} else if (pubsubMessage instanceof GetSubscribers) {
			if (!(await message.verify(true))) {
				logger.warn("Recieved message that did not verify Unsubscribe");
				return false;
			}

			const subscriptionsToSend: Subscription[] = [];
			for (const topic of pubsubMessage.topics) {
				const subscription = this.subscriptions.get(topic);
				if (subscription) {
					subscriptionsToSend.push(new Subscription(topic, subscription.data));
				}
			}

			if (subscriptionsToSend.length > 0) {
				// respond
				if (!stream.isWritable) {
					try {
						await waitFor(() => stream.isWritable);
					} catch (error) {
						logger.warn(
							`Failed to respond to GetSubscribers request to ${from.toString()} stream is not writable`
						);
						return false;
					}
				}
				this.publishMessage(
					this.components.peerId,
					await new DataMessage({
						data: toUint8Array(
							new Subscribe({
								subscriptions: subscriptionsToSend
							}).bytes()
						)
					}).sign(this.sign),
					[stream]
				); // send back to same stream
			}

			// Forward
			await this.relayMessage(from, message);
		}
		return true;
	}
}

export const waitForSubscribers = async (
	libp2p: { services: { pubsub: DirectSub } },
	peersToWait:
		| PeerId
		| PeerId[]
		| { peerId: Libp2pPeerId }
		| { peerId: Libp2pPeerId }[]
		| string
		| string[],
	topic: string
) => {
	const peersToWaitArr = Array.isArray(peersToWait)
		? peersToWait
		: [peersToWait];

	const peerIdsToWait: string[] = peersToWaitArr.map((peer) => {
		if (typeof peer === "string") {
			return peer;
		}
		const id: PublicSignKey | Libp2pPeerId = peer["peerId"] || peer;
		if (typeof id === "string") {
			return id;
		}
		return id instanceof PublicSignKey
			? id.hashcode()
			: getPublicKeyFromPeerId(id).hashcode();
	});

	await libp2p.services.pubsub.requestSubscribers(topic);
	return new Promise<void>((resolve, reject) => {
		let counter = 0;
		const interval = setInterval(async () => {
			counter += 1;
			if (counter > 100) {
				clearInterval(interval);
				reject(
					new Error("Failed to find expected subscribers for topic: " + topic)
				);
			}
			try {
				const peers = await libp2p.services.pubsub.getSubscribers(topic);
				const hasAllPeers =
					peerIdsToWait
						.map((e) => peers && peers.has(e))
						.filter((e) => e === false).length === 0;

				// FIXME: Does not fail on timeout, not easily fixable
				if (hasAllPeers) {
					clearInterval(interval);
					resolve();
				}
			} catch (e) {
				clearInterval(interval);
				reject(e);
			}
		}, 200);
	});
};
