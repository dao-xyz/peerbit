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
import { CodeError } from '@libp2p/interfaces/errors'
import {
	PubSubMessage,
	Subscribe,
	PubSubData,
	toUint8Array,
	Unsubscribe,
	GetSubscribers,
} from "./messages.js";
import { Uint8ArrayList } from "uint8arraylist";
import { getPublicKeyFromPeerId, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { CustomEvent } from "@libp2p/interfaces/events";
import type { Connection } from "@libp2p/interface-connection";
import { waitFor } from "@dao-xyz/peerbit-time";
import { Goodbye } from "@dao-xyz/libp2p-direct-stream";
export {
	PubSubMessage,
	Subscribe,
	PubSubData,
	toUint8Array,
	Unsubscribe,
	GetSubscribers,
};
export const logger = logFn({ module: "direct-sub", level: "warn" });

export interface PeerStreamsInit {
	id: PeerId;
	protocol: string;
}

export interface PubSubEvents {
	data: CustomEvent<PubSubData>;
	subscribe: CustomEvent<Subscribe>;
	unsubscribe: CustomEvent<Unsubscribe>;
	message: CustomEvent<Message>;
}
export type DirectSubOptions = {
	aggregate: boolean; // if true, we will collect topic/subscriber info for all traffic
};

export class DirectSub extends DirectStream<PubSubEvents> {
	public topics: Map<string, Set<string>>; // topic -> peers
	public peerToTopic: Map<string, Set<string>>;  // peer -> topics

	//public streamToTopics: Map<string, Set<string>>; // topic -> neighbour peers that are subscribing, or neighbour peers that are connected to other peers that are subscribing
	public topicsToStreams: Map<string, Set<string>>; // topic -> neighbour peers that are subscribing, or neighbour peers that are connected to other peers that are subscribing

	public subscriptions: Set<string>;

	constructor(libp2p: Libp2p, props?: DirectStreamOptions) {
		super(libp2p, ["directsub/0.0.0"], props);
		this.subscriptions = new Set();
		this.topics = new Map();
		this.topicsToStreams = new Map();
		this.peerToTopic = new Map()
	}

	stop() {
		this.subscriptions.clear();
		this.topics.clear();
		this.peerToTopic.clear()
		//this.streamToTopics.clear();
		this.topicsToStreams.clear();
		return super.stop();
	}

	public async onPeerConnected(peerId: PeerId, conn: Connection, existing?: boolean) {
		const ret = await super.onPeerConnected(peerId, conn, existing);
		//	this.streamToTopics.set(peerId.toString(), new Set());

		// Aggregate subscribers for my topics through this new connection because if we don't do this we might end up with a situtation where
		// we act as a relay and relay messages for a topic, but don't forward it to this new peer because we never learned about their subscriptions
		const stream = this.peers.get(getPublicKeyFromPeerId(peerId).hashcode());
		if (stream) {
			await this.requestSubscribers(
				[...this.topics.keys()],
				[stream]
			);
		}

		return ret;
	}

	public async onPeerDisconnected(peerId: PeerId) {
		const ret = await super.onPeerDisconnected(peerId);
		//this.streamToTopics.delete(peerId.toString());
		// TODO also modify topics?
		return ret;
	}



	private initializeTopic(topic: string) {
		this.topics.get(topic) || this.topics.set(topic, new Set());
		this.topicsToStreams.get(topic) || this.topicsToStreams.set(topic, new Set());
	}

	private initializePeer(publicKey: PublicSignKey) {
		this.peerToTopic.get(publicKey.hashcode()) || this.peerToTopic.set(publicKey.hashcode(), new Set());
	}




	/**
	 * Subscribes to a given topic.
	 */
	subscribe(topic: string) {
		if (!this.started) {
			throw new Error("Pubsub has not started");
		}

		logger.info("subscribe to topic: " + topic);

		if (!this.subscriptions.has(topic)) {
			this.subscriptions.add(topic);
			this.initializeTopic(topic);
			this.publishMessage(
				this.libp2p.peerId,
				new DataMessage({
					data: toUint8Array(
						new Subscribe({ topics: [topic] }).serialize()
					),
				}).sign(this.sign)
			);
		}
	}

	/**
	 * Unsubscribe from the given topic
	 */
	unsubscribe(topic: string) {
		if (!this.started) {
			throw new Error("Pubsub is not started");
		}

		const wasSubscribed = this.subscriptions.has(topic);

		logger.info(
			`unsubscribe from ${topic} - am subscribed ${wasSubscribed}`
		);

		if (wasSubscribed) {
			this.subscriptions.delete(topic);
			this.topics.delete(topic);
			this.topicsToStreams.delete(topic);
			this.publishMessage(
				this.libp2p.peerId,
				new DataMessage({
					data: toUint8Array(
						new Unsubscribe({ topics: [topic] }).serialize()
					),
				}).sign(this.sign)
			);
		}
	}

	getSubscribers(topic: string): Set<string> | undefined {
		if (!this.started) {
			throw new CodeError("not started yet", "ERR_NOT_STARTED_YET");
		}

		if (topic == null) {
			throw new CodeError(
				"topic is required",
				"ERR_NOT_VALID_TOPIC"
			);
		}

		return this.topics.get(topic.toString());

	}

	requestSubscribers(
		topic: string | string[],
		streams?: PeerStreams[]
	): Promise<void> {
		if (!this.started) {
			throw new CodeError("not started yet", "ERR_NOT_STARTED_YET");
		}

		if (topic == null) {
			throw new CodeError(
				"topic is required",
				"ERR_NOT_VALID_TOPIC"
			);
		}
		const topics = typeof topic === "string" ? [topic] : topic;
		for (const topic of topics) {
			this.initializeTopic(topic);
		}

		return this.publishMessage(
			this.libp2p.peerId,
			new DataMessage({
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

	getStreamsWithTopics(
		topics: string[],
		otherPeers?: string[]
	): PeerStreams[] {
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
			(options as { topics: string[] }).topics?.map((x) =>
				x.toString()
			) || [];
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
		const processed = await super.onGoodbye(from, peerStream, message)
		if (message.early) {
			return true;
		}
		const senderKey = message.sender?.hashcode()!;
		const topicsFromSender = this.peerToTopic.get(senderKey);
		if (topicsFromSender) {
			for (const topic of topicsFromSender) {
				this.topicsToStreams.get(topic)?.delete(getPublicKeyFromPeerId(from).hashcode())
			}
		}
		return processed;
	}

	public onPeerUnreachable(publicKey: PublicSignKey) {
		super.onPeerUnreachable(publicKey)
		const publicKeyHash = publicKey.hashcode();
		let peerTopics = this.peerToTopic.get(publicKeyHash)
		if (peerTopics) {
			for (const topic of peerTopics) {
				this.topics.get(topic)?.delete(publicKeyHash)
			}
		}
	}

	async onDataMessage(
		from: PeerId,
		stream: PeerStreams,
		message: DataMessage
	) {
		const pubsubMessage = PubSubMessage.deserialize(
			message.data instanceof Uint8Array
				? new Uint8ArrayList(message.data)
				: message.data
		);
		if (pubsubMessage instanceof PubSubData) {
			/**
			 * See if we know more subscribers of the message topics. If so, add aditional end recievers of the message
			 */
			const topics = pubsubMessage.topics;
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
						verified = message.verify(
							this.signaturePolicy === "StictSign" ? true : false
						);
					}
					if (!verified) {
						logger.warn(
							"Recieved message that did not verify PubSubData"
						);
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

			if (!message.verify(true)) {
				logger.warn("Recieved message that did not verify Subscribe");
				return false;
			}

			if (message.signatures.signatures.length === 0) {
				logger.warn("Recieved subscription message with no signers");
				return false;
			}

			if (pubsubMessage.topics.length === 0) {
				logger.info("Recieved subscription message with no topics");
				return false;
			}

			const subscriber = message.signatures.signatures[0].publicKey!;
			const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing

			const fromPublic =
				getPublicKeyFromPeerId(from)

			this.initializePeer(subscriber)

			pubsubMessage.topics.forEach((topic) => {
				const peers = this.topics.get(topic);
				if (peers == null) {
					return;
				}

				peers.add(subscriberKey); // "from" is perhaps actually not the subscriber, but we will reach the subscriber if we send for this topic through it connection

				//this.streamToTopics.get(from.toString())!.add(topic);
				this.topicsToStreams.get(topic)?.add(fromPublic.hashcode());
				this.peerToTopic.get(subscriberKey)?.add(topic);

			});

			this.dispatchEvent(
				new CustomEvent("subscribe", { detail: pubsubMessage })
			);

			// Forward
			await this.relayMessage(from, message);
		} else if (pubsubMessage instanceof Unsubscribe) {
			if (!message.verify(true)) {
				logger.warn("Recieved message that did not verify Unsubscribe");
				return false;
			}

			if (message.signatures.signatures.length === 0) {
				logger.warn("Recieved subscription message with no signers");
				return false;
			}

			pubsubMessage.topics.forEach((topic) => {
				const peers = this.topics.get(topic);
				if (peers == null) {
					return;
				}
				const subscriber = message.signatures.signatures[0].publicKey!;
				const subscriberKey = subscriber.hashcode(); // Assume first signature is the one who is signing
				peers.delete(subscriberKey);

				const fromPublicKeyHash =
					getPublicKeyFromPeerId(from).hashcode();

				this.topicsToStreams.get(topic)!.delete(fromPublicKeyHash);
				this.peerToTopic.get(subscriberKey)?.delete(topic);
				if (!this.peerToTopic.get(subscriberKey)?.size) {
					this.peerToTopic.delete(subscriberKey)
				}
			});

			this.dispatchEvent(
				new CustomEvent("unsubscribe", { detail: pubsubMessage })
			);

			// Forward
			await this.relayMessage(from, message);
		} else if (pubsubMessage instanceof GetSubscribers) {

			if (!message.verify(true)) {
				logger.warn("Recieved message that did not verify Unsubscribe");
				return false;
			}

			const myTopics = pubsubMessage.topics.filter((topic) =>
				this.subscriptions.has(topic)
			);
			if (myTopics.length > 0) {
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
					new DataMessage({
						data: toUint8Array(
							new Subscribe({ topics: myTopics }).serialize()
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
