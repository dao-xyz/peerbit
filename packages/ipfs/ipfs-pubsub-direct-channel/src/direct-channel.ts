import { waitForPeers } from "./wait-for-peers.js";
import { v1 as PROTOCOL } from "./protocol.js";
import { Libp2p } from "libp2p";
import { IpfsPubsubPeerMonitor } from "@dao-xyz/ipfs-pubsub-peer-monitor";
import type { Message } from "@libp2p/interface-pubsub";
import type { EventHandler } from "@libp2p/interfaces/events";
import type { PeerId } from "@libp2p/interface-peer-id";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "direct-channel" });
/**
 * Communication channel over Pubsub between two IPFS nodes
 */
export class DirectChannel {
    _id: string;
    _libp2p: Libp2p;
    _closed: boolean;
    _isClosed: () => boolean;
    _receiverID: string;
    _senderID: string;
    _peers: string[];
    _handler: EventHandler<CustomEvent<Message>>;
    _monitor: IpfsPubsubPeerMonitor;

    constructor(libp2p: Libp2p, receiverID: string) {
        // IPFS instance to use internally
        this._libp2p = libp2p;

        this._closed = false;
        this._isClosed = () => this._closed;
        this._receiverID = receiverID;

        if (!this._receiverID) {
            throw new Error("Receiver ID was undefined");
        }
        // See _setup() for more state initialization
    }

    /**
     * Channel ID
     * @return {[String]} Channel's ID
     */
    get id() {
        return this._id;
    }

    get recieverId() {
        return this._receiverID;
    }

    get senderId() {
        return this._senderID;
    }

    /**
     * Peers participating in this channel
     * @return {[Array]} Array of peer IDs participating in this channel
     */
    get peers() {
        return this._peers;
    }

    async connect(options?: { isClosed: () => boolean }): Promise<boolean> {
        return waitForPeers(
            this._libp2p,
            [this._receiverID.toString()],
            this._id,
            () =>
                this._isClosed() || (!!options?.isClosed && options.isClosed())
        );
    }

    /**
     * Send a message to the other peer
     * @param  {[Any]} message Payload
     */
    async send(message: Uint8Array) {
        if (this._closed) return;
        logger.debug(
            `Send message on channel: ${this._id} with size ${message.byteLength}`
        );
        await this._libp2p.pubsub.publish(this._id, message);
    }

    /**
     * Close the channel
     */
    async close(options?: { subscription: boolean }): Promise<void> {
        this._closed = true;
        this._monitor?.stop();
        if (options?.subscription) {
            await this._libp2p.pubsub.unsubscribe(this._id); // because the topic could be shared
        }
        return this._libp2p.pubsub.removeEventListener(
            "message",
            this._handler
        );
    }

    async _setup() {
        this._senderID = this._libp2p.peerId.toString();

        // Channel's participants
        this._peers = Array.from([this._senderID, this._receiverID]);

        // ID of the channel is "<peer1 id>/<peer 2 id>""
        this._id = DirectChannel.getTopic(this._peers);

        // Function to use to handle incoming messages
        /* this._messageHandler = (message: {
      data: Buffer
      from: string
      key: Buffer
      receivedFrom: string
      seqno: Buffer
      signature: Uint8Array
      topicIDs: string[]
    }) => {

      // Make sure the message is coming from the correct peer
      const isValid = message && message.from === this._receiverID

      // Filter out all messages that didn't come from the second peer

      if (isValid) {
        this.emit('message', message)
      }
    } */
    }

    async _openChannel(
        onMessageCallback: EventHandler<CustomEvent<Message>>,
        monitor?: {
            onNewPeerCallback?: (channel: DirectChannel) => void;
            onPeerLeaveCallback?: (channel: DirectChannel) => void;
        }
    ) {
        this._closed = false;
        await this._setup();
        this._handler = onMessageCallback;
        await this._libp2p.pubsub.subscribe(this._id);
        this._libp2p.pubsub.addEventListener("message", onMessageCallback);
        if (monitor?.onNewPeerCallback || monitor?.onPeerLeaveCallback) {
            const topicMonitor = new IpfsPubsubPeerMonitor(
                this._libp2p.pubsub,
                this._id,
                {
                    onJoin: (peer) => {
                        if (peer === this._receiverID) {
                            logger.debug(
                                `Peer joined direct channel ${this.id}`
                            );
                            logger.debug(peer);
                            if (monitor.onNewPeerCallback) {
                                monitor.onNewPeerCallback(this);
                            }
                        }
                    },
                    onLeave: (peer) => {
                        if (peer === this._receiverID) {
                            logger.debug(`Peer ${peer} left ${this.id}`);
                            if (monitor.onPeerLeaveCallback) {
                                monitor.onPeerLeaveCallback(this);
                            }
                        }
                    },
                    onError: (e) => {
                        logger.error(e);
                    },
                }
            );
            this._monitor = topicMonitor;
        }
    }
    _messageHandler(
        messageCallback: (message: Message) => void
    ): EventHandler<CustomEvent<Message>> {
        return (evt: CustomEvent<Message>) => {
            logger.debug(
                `Recieved message on channel: ${this._id} with size ${evt.detail.data.byteLength}`
            );
            const message = evt.detail;
            if (message.type === "signed" && this._id === message.topic) {
                if (message.from.equals(this._receiverID)) {
                    let data: Uint8Array = message.data;
                    if (data.constructor !== Uint8Array) {
                        if (data instanceof Uint8Array) {
                            data = new Uint8Array(data);
                        } else {
                            throw new Error("Unexpected data format");
                        }
                    }
                    messageCallback(message);
                }
            }
        };
    }

    static async open(
        libp2p: Libp2p,
        receiverID: string | PeerId,
        onMessageCallback: (message: Message) => void,
        monitor?: {
            onNewPeerCallback?: (channel: DirectChannel) => void;
            onPeerLeaveCallback?: (channel: DirectChannel) => void;
        }
    ): Promise<DirectChannel> {
        const channel = new DirectChannel(libp2p, receiverID.toString());
        const handler = channel._messageHandler(onMessageCallback);
        await channel._openChannel(handler, monitor);
        return channel;
    }

    static getTopic(peers: (string | PeerId)[]): string {
        return (
            "/" +
            PROTOCOL +
            "/" +
            peers
                .map((p) => (typeof p === "string" ? p : p.toString()))
                .sort()
                .join("/")
        );
    }
}
