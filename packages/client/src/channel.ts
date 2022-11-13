import { IpfsPubsubPeerMonitor } from "@dao-xyz/ipfs-pubsub-peer-monitor";
import { IPFS } from "ipfs-core-types";
import type { Message, SignedMessage } from "@libp2p/interface-pubsub";
import type { EventHandler } from "@libp2p/interfaces/events";
import type { PeerId } from "@libp2p/interface-peer-id";

interface Closable {
    close: () => Promise<void>;
}
export class SharedChannel<T extends Closable> {
    _channel: T;
    dependencies: Set<string>;
    constructor(channel: T, dependencies?: Set<string>) {
        this._channel = channel;
        this.dependencies = dependencies || new Set();
    }

    get channel(): T {
        return this._channel;
    }
    async close(dependency?: string): Promise<boolean> {
        if (dependency) {
            this.dependencies.delete(dependency);
            if (this.dependencies.size === 0) {
                await this._channel.close();
                return true;
            }
            return false;
        }
        await this._channel.close();
        return true;
    }
}

export class SharedIPFSChannel implements Closable {
    _ipfs: IPFS;
    _topic: string;
    _handler: EventHandler<Message>;
    _monitor?: IpfsPubsubPeerMonitor;
    _id: PeerId;
    constructor(
        ipfs: IPFS,
        id: PeerId,
        topic: string,
        handler: (message: Message) => void,
        monitor?: IpfsPubsubPeerMonitor
    ) {
        this._ipfs = ipfs;
        this._topic = topic;
        this._handler = this._messageHandler(handler);
        this._monitor = monitor;
        this._id = id;
    }
    /**
     * Compatibility wrapper
     * @param messageCallback
     * @returns MessageHandlerFn
     */
    _messageHandler(
        messageCallback: (message: Message) => void
    ): EventHandler<Message> {
        return (message: Message) => {
            if (message.type === "signed") {
                const signedMessage = message as SignedMessage;
                if (signedMessage.from.equals(this._id)) {
                    return;
                }

                messageCallback(message);
            } else {
                // unsigned message
            }
        };
    }

    async start(): Promise<SharedIPFSChannel> {
        await this._ipfs.pubsub.subscribe(this._topic, this._handler);
        return this;
    }
    async close() {
        await this._monitor?.stop();
        return this._ipfs.pubsub.unsubscribe(this._topic);
    }
}
