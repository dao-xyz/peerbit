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
import errcode from "err-code";
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
export {
    PubSubMessage,
    Subscribe,
    PubSubData,
    toUint8Array,
    Unsubscribe,
    GetSubscribers,
};
const logger = logFn({ module: "direct-sub", level: "warn" });

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
    //public streamToTopics: Map<string, Set<string>>; // topic -> neighbour peers that are subscribing, or neighbour peers that are connected to other peers that are subscribing
    public topicsToStreams: Map<string, Set<string>>; // topic -> neighbour peers that are subscribing, or neighbour peers that are connected to other peers that are subscribing

    public subscriptions: Set<string>;

    constructor(libp2p: Libp2p, props?: DirectStreamOptions) {
        super(libp2p, ["directsub/0.0.0"], props);
        this.subscriptions = new Set();
        this.topics = new Map();
        //this.streamToTopics = new Map();
        this.topicsToStreams = new Map();
    }

    stop() {
        this.subscriptions.clear();
        this.topics.clear();
        //this.streamToTopics.clear();
        this.topicsToStreams.clear();
        return super.stop();
    }

    public async onPeerConnected(peerId: PeerId, conn: Connection) {
        const ret = await super.onPeerConnected(peerId, conn);
        //	this.streamToTopics.set(peerId.toString(), new Set());

        // Aggregate subscribers for my topics through this new connection because if we don't do this we might end up with a situtation where
        // we act as a relay and relay messages for a topic, but don't forward it to this new peer because we never learned about their subscriptions
        this.requestSubscribers(
            [...this.topics.keys()],
            [this.peers.get(getPublicKeyFromPeerId(peerId).hashcode())!]
        );
        return ret;
    }

    public async onPeerDisconnected(peerId: PeerId) {
        const ret = await super.onPeerDisconnected(peerId);
        //this.streamToTopics.delete(peerId.toString());
        // TODO also modify topics?
        return ret;
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
            this.topics.set(topic, new Set());
            this.topicsToStreams.set(topic, new Set());
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

    getSubscribers(topic: string): Set<string> {
        if (!this.started) {
            throw errcode(new Error("not started yet"), "ERR_NOT_STARTED_YET");
        }

        if (topic == null) {
            throw errcode(
                new Error("topic is required"),
                "ERR_NOT_VALID_TOPIC"
            );
        }

        const peersInTopic = this.topics.get(topic.toString());
        if (peersInTopic == null) {
            return new Set();
        }
        return peersInTopic;
    }

    requestSubscribers(
        topic: string | string[],
        streams?: PeerStreams[]
    ): Promise<void> {
        if (!this.started) {
            throw errcode(new Error("not started yet"), "ERR_NOT_STARTED_YET");
        }

        if (topic == null) {
            throw errcode(
                new Error("topic is required"),
                "ERR_NOT_VALID_TOPIC"
            );
        }
        const topics = typeof topic === "string" ? [topic] : topic;
        for (const topic of topics) {
            if (!this.topics.has(topic)) {
                this.topics.set(topic, new Set());
            }
            if (!this.topicsToStreams.has(topic)) {
                this.topicsToStreams.set(topic, new Set());
            }
        }

        return this.publishMessage(
            this.libp2p.peerId,
            new DataMessage({
                data: toUint8Array(new GetSubscribers({ topics }).serialize()),
            }).sign(this.sign),
            streams
        );
    }

    getPeersOnTopic(topic: string): Set<string> | undefined {
        return this.topics.get(topic.toString());
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
                        return;
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
                return;
            }

            if (message.signatures.signatures.length === 0) {
                logger.warn("Recieved subscription message with no signers");
                return;
            }

            pubsubMessage.topics.forEach((topic) => {
                const peers = this.topics.get(topic);
                if (peers == null) {
                    return;
                }

                const subscriber = message.signatures.signatures[0].publicKey!;
                const key = subscriber.hashcode(); // Assume first signature is the one who is signing
                peers.add(key); // "from" is perhaps actually not the subscriber, but we will reach the subscriber if we send for this topic through it connection

                const fromPublicKeyHash =
                    getPublicKeyFromPeerId(from).hashcode();
                //this.streamToTopics.get(from.toString())!.add(topic);
                this.topicsToStreams.get(topic)!.add(fromPublicKeyHash);
            });

            this.dispatchEvent(
                new CustomEvent("subscribe", { detail: pubsubMessage })
            );

            // Forward
            await this.relayMessage(from, message);
        } else if (pubsubMessage instanceof Unsubscribe) {
            if (!message.verify(true)) {
                logger.warn("Recieved message that did not verify Unsubscribe");
                return;
            }

            if (message.signatures.signatures.length === 0) {
                logger.warn("Recieved subscription message with no signers");
                return;
            }

            pubsubMessage.topics.forEach((topic) => {
                const peers = this.topics.get(topic);
                if (peers == null) {
                    return;
                }
                const subscriber = message.signatures.signatures[0].publicKey!;
                const key = subscriber.hashcode(); // Assume first signature is the one who is signing
                peers.delete(key);

                const fromPublicKeyHash =
                    getPublicKeyFromPeerId(from).hashcode();
                //this.streamToTopics.get(from.toString())!.delete(topic);
                this.topicsToStreams.get(topic)!.delete(fromPublicKeyHash);
            });

            this.dispatchEvent(
                new CustomEvent("unsubscribe", { detail: pubsubMessage })
            );

            // Forward
            await this.relayMessage(from, message);
        } else if (pubsubMessage instanceof GetSubscribers) {
            if (!message.verify(true)) {
                logger.warn("Recieved message that did not verify Unsubscribe");
                return;
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
                        return;
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
    }

    /**
     * Publishes messages to all peers
     */
    /* async publish(data: Uint8Array, topics: string[]): Promise<void> {
		if (!this.started) {
			throw new Error('Not started')
		}

		const msgId = this.getMsgId(data);
		this.seenCache.set(msgId, true);


		// dispatch the event if we are interested
		const message = new DataMessage({ data, to: options?.to });
		if (this.emitSelf) {
			super.dispatchEvent(new CustomEvent('data', {
				detail: message
			}))

		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, message)

	} */

    /* private async publishMessage(from: PeerId, message: Message, topics: string[]): Promise<void> {

		let peers: PeerStreams[] | PeerMap<PeerStreams>;
		if (!to) {
			if (message instanceof DataMessage && message.to.length > 0) {
				peers = [];
				for (const to of message.to) {
					try {
						const path = this.routes.getPath(this.peerIdStr, to);
						if (path && path.length > 0) {
							const stream = this.peers.get(path[1].id.toString());
							if (stream) {
								peers.push(stream)
								continue
							}
						}

					} catch (error) {
						// Can't find path
					}

					// we can't find path, send message to all peers
					peers = this.peers;
					break;
				}
			}
			else {
				peers = this.peers;
			}

		}
		else {
			peers = to;
		}
		if (peers == null || peers.length === 0) {
			logger.info('no peers are subscribed')
			return
		}

		const bytes = message.serialize()
		peers.forEach(_id => {
			let id = _id as PeerStreams;
			if (this.libp2p.peerId.equals(id.id)) {
				logger.info('not sending message to myself')
				return
			}

			if (id.id.equals(from)) {
				logger.info('not sending messageto sender', id.id)
				return
			}

			logger.info('publish msgs on %p', id)
			if (!id.isWritable) {
				logger.error('Cannot send RPC to %p as there is no open stream to it available', id.id)
				return
			}

			id.write(bytes)
		})
	} */
}
