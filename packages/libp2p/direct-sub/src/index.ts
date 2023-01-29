import type { PeerId } from "@libp2p/interface-peer-id";
import { logger as logFn } from "@dao-xyz/peerbit-logger";
import { Libp2p } from "libp2p";
import {
	DataMessage,
	DirectStream,
	DirectStreamOptions,
	Message,
	PeerStreams,
} from "@dao-xyz/libp2p-direct-stream";
import { CodeError } from "@libp2p/interfaces/errors";
import {
	PubSubMessage,
	Subscribe,
	PubSubData,
	toUint8Array,
	Unsubscribe,
	GetSubscribers,
	Subscription,
} from "./messages.js";
import { Uint8ArrayList } from "uint8arraylist";
import { getPublicKeyFromPeerId, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { CustomEvent } from "@libp2p/interfaces/events";
import type { Connection } from "@libp2p/interface-connection";
import { waitFor } from "@dao-xyz/peerbit-time";
import { Goodbye } from "@dao-xyz/libp2p-direct-stream";
import { PeerEvents } from "@dao-xyz/libp2p-direct-stream";
export {
	PubSubMessage,
	PubSubData,
	toUint8Array,
	GetSubscribers,
	Subscription,
};
import { equals } from "uint8arrays";
export const logger = logFn({ module: "direct-sub", level: "warn" });

export interface PeerStreamsInit {
	id: PeerId;
	protocol: string;
}

interface From {
	from: PublicSignKey;
}

export interface SubscriptionEvent extends From {
	subscriptions: Subscription[];
}

export interface UnsubcriptionEvent extends From {
	unsubscriptions: Subscription[];
}

export interface PubSubEvents extends PeerEvents {
	data: CustomEvent<PubSubData>;
	subscribe: CustomEvent<SubscriptionEvent>;
	unsubscribe: CustomEvent<UnsubcriptionEvent>;
	message: CustomEvent<Message>;
}
export type DirectSubOptions = {
	aggregate: boolean; // if true, we will collect topic/subscriber info for all traffic
};

export type SubscriptionData = { timestamp: bigint; data?: Uint8Array };

export class DirectSub extends DirectStream<PubSubEvents> {
	public topics: Map<string, Map<string, SubscriptionData>>; // topic -> peers --> Uint8Array subscription metadata (the latest recieved)
	public peerToTopic: Map<string, Set<string>>; // peer -> topics
	public topicsToStreams: Map<string, Set<string>>; // topic -> neighbour peers that are subscribing, or neighbour peers that are connected to other peers that are subscribing
	public subscriptions: Map<string, { counter: number; data?: Uint8Array }>; // topic -> subscription ids

	constructor(libp2p: Libp2p, props?: DirectStreamOptions) {
		super(libp2p, ["directsub/0.0.0"], props);
		this.subscriptions = new Map();
		this.topics = new Map();
		this.topicsToStreams = new Map();
		this.peerToTopic = new Map();
	}

	stop() {
		this.subscriptions.clear();
		this.topics.clear();
		this.peerToTopic.clear();
		this.topicsToStreams.clear();
		return super.stop();
	}

	public async onPeerConnected(peerId: PeerId, conn: Connection) {
		const ret = await super.onPeerConnected(peerId, conn);
		//	this.streamToTopics.set(peerId.toString(), new Set());

		// Aggregate subscribers for my topics through this new connection because if we don't do this we might end up with a situtation where
		// we act as a relay and relay messages for a topic, but don't forward it to this new peer because we never learned about their subscriptions
		const stream = this.peers.get(getPublicKeyFromPeerId(peerId).hashcode());
		if (stream) {
			await this.requestSubscribers([...this.topics.keys()], [stream]);
		}

		return ret;
	}

	public async onPeerDisconnected(peerId: PeerId) {
		const ret = await super.onPeerDisconnected(peerId);
		return ret;
	}

	private initializeTopic(topic: string) {
		this.topics.get(topic) || this.topics.set(topic, new Map());
		this.topicsToStreams.get(topic) ||
			this.topicsToStreams.set(topic, new Set());
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
					data: options?.data,
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
						),
					}).serialize()
				),
			});

			await this.publishMessage(
				this.libp2p.peerId,
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

		logger.info(
			`unsubscribe from ${topic} - am subscribed with subscriptions ${subscriptions}`
		);

		if (subscriptions?.counter && subscriptions?.counter >= 0) {
			subscriptions.counter -= 1;
		}

		if (!subscriptions?.counter || options?.force) {
			this.subscriptions.delete(topic);
			this.topics.delete(topic);
			this.topicsToStreams.delete(topic);
			await this.publishMessage(
				this.libp2p.peerId,
				await new DataMessage({
					data: toUint8Array(new Unsubscribe({ topics: [topic] }).serialize()),
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
			outer: for (const [peer, info] of map.entries()) {
				if (
					!info.data ||
					info.data.length < data.length ||
					(!options?.prefix && info.data.length !== data.length)
				) {
					continue;
				}

				for (let i = 0; i < data.length; i++) {
					if (data[i] !== info.data[i]) {
						continue outer;
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
		streams?: PeerStreams[]
	): Promise<void> {
		if (!this.started) {
			throw new CodeError("not started yet", "ERR_NOT_STARTED_YET");
		}

		if (topic == null) {
			throw new CodeError("topic is required", "ERR_NOT_VALID_TOPIC");
		}
		const topics = typeof topic === "string" ? [topic] : topic;
		for (const topic of topics) {
			this.listenForSubscribers(topic);
		}

		return this.publishMessage(
			this.libp2p.peerId,
			await new DataMessage({
				data: toUint8Array(new GetSubscribers({ topics }).serialize()),
			}).sign(this.sign),
			streams
		);
	}

	getNeighboursWithTopics(
		topics: string[],
		otherPeers?: string[]
	): Set<string> {
		const peers: Set<string> = otherPeers ? new Set(otherPeers) : new Set();
		if (topics?.length) {
			for (const topic of topics) {
				const peersOnTopic = this.topicsToStreams.get(topic.toString());
				if (peersOnTopic) {
					peersOnTopic.forEach((peer) => {
						peers.add(peer);
					});
				}
			}
		}
		return peers;
	}

	getStreamsWithTopics(topics: string[], otherPeers?: string[]): PeerStreams[] {
		const peers = this.getNeighboursWithTopics(topics, otherPeers);
		return [...this.peers.values()].filter((s) =>
			peers.has(s.publicKey.hashcode())
		);
	}

	async publish(
		data: Uint8Array,
		options:
			| { topics: string[]; to?: (string | PeerId | PublicSignKey)[] }
			| { to: (string | PeerId | PublicSignKey)[] }
	): Promise<void> {
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
		const message = new PubSubData({
			topics: topics.map((x) => x.toString()),
			data,
		});
		const bytes = message.serialize();
		await super.publish(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			{ to: this.getNeighboursWithTopics(topics, tos) }
		);
	}

	async onGoodbye(from: PeerId, peerStream: PeerStreams, message: Goodbye) {
		const processed = await super.onGoodbye(from, peerStream, message);
		if (message.early) {
			return true;
		}
		const senderKey = message.sender.hashcode();
		const topicsFromSender = this.peerToTopic.get(senderKey);
		if (topicsFromSender) {
			for (const topic of topicsFromSender) {
				this.topicsToStreams
					.get(topic)
					?.delete(getPublicKeyFromPeerId(from).hashcode());
			}
		}
		return processed;
	}

	deletePeerFromTopic(
		topic: string,
		publicKeyHash: string,
		fromStream?: PeerId
	) {
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

		const fromStreams = fromStream
			? [getPublicKeyFromPeerId(fromStream).hashcode()]
			: this.peers.keys();
		for (const fromPublicKeyHash of fromStreams) {
			this.topicsToStreams.get(topic)?.delete(fromPublicKeyHash);
		}
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
		if (changed.length > 0) {
			this.dispatchEvent(
				new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
					detail: { from: publicKey, unsubscriptions: changed },
				})
			);
		}
	}

	async onDataMessage(from: PeerId, stream: PeerStreams, message: DataMessage) {
		const pubsubMessage = PubSubMessage.deserialize(
			message.data instanceof Uint8Array
				? new Uint8ArrayList(message.data)
				: message.data
		);
		if (pubsubMessage instanceof PubSubData) {
			/**
			 * See if we know more subscribers of the message topics. If so, add aditional end recievers of the message
			 */
			let verified: boolean | undefined = undefined;

			const isFromSelf = this.libp2p.peerId.equals(from);
			if (!isFromSelf || this.emitSelf) {
				//	const isForAll = message.to.length === 0;
				const isForMe =
					pubsubMessage.topics.find((topic) =>
						this.subscriptions.has(topic)
					) /*  && !isForAll */ ||
					message.to.find((x) => this.publicKeyHash === x);
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
							detail: pubsubMessage,
						})
					);
				}
			}

			// Forward
			const streamToSendTO = this.getStreamsWithTopics(
				pubsubMessage.topics,
				message.to
			);
			await this.relayMessage(
				from,
				message,
				streamToSendTO.length > 0 ? streamToSendTO : undefined
			); // if not find any stream, send to all
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

			const subscriber = message.signatures.signatures[0].publicKey!;
			const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

			const fromPublic = getPublicKeyFromPeerId(from);
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
					peers.set(subscriberKey, {
						timestamp: message.header.timetamp, // TODO update timestamps on all messages?
						data: subscription.data,
					});
					changed.push(subscription);
				}

				this.topicsToStreams
					.get(subscription.topic)
					?.add(fromPublic.hashcode()); // "from" is perhaps actually not the subscriber, but we will reach the subscriber if we send for this topic through it connection
				this.peerToTopic.get(subscriberKey)?.add(subscription.topic);
			});
			if (changed.length > 0) {
				const subscriptionEvent: SubscriptionEvent = {
					from: subscriber,
					subscriptions: changed,
				};
				this.dispatchEvent(
					new CustomEvent<SubscriptionEvent>("subscribe", {
						detail: subscriptionEvent,
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
			const changed: Subscription[] = [];
			const subscriber = message.signatures.signatures[0].publicKey!;
			const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

			pubsubMessage.unsubscriptions.forEach((unsubscription) => {
				const change = this.deletePeerFromTopic(
					unsubscription.topic,
					subscriberKey
				);
				if (change) {
					changed.push(new Subscription(unsubscription.topic, change.data));
				}
			});

			if (changed.length > 0) {
				this.dispatchEvent(
					new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
						detail: { from: subscriber, unsubscriptions: changed },
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
					this.libp2p.peerId,
					await new DataMessage({
						data: toUint8Array(
							new Subscribe({
								subscriptions: subscriptionsToSend,
							}).serialize()
						),
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
