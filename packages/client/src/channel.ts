import { IpfsPubsubPeerMonitor } from "@dao-xyz/libp2p-pubsub-peer-monitor";
import { Libp2p } from "libp2p";
import type { Message, SignedMessage } from "@libp2p/interface-pubsub";
import type { EventHandler } from "@libp2p/interfaces/events";
import type { PeerId } from "@libp2p/interface-peer-id";

interface Closable {
    close: (options?: { subscription: boolean }) => Promise<void>;
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
                await this._channel.close({ subscription: true });
                return true;
            }
            return false;
        }
        await this._channel.close();
        return true;
    }
}

export class SharedIPFSChannel implements Closable {
    _libp2p: Libp2p;
    _topic: string;
    _handler: EventHandler<CustomEvent<Message>>;
    _monitor?: IpfsPubsubPeerMonitor;
    _id: PeerId;
    constructor(
        libp2p: Libp2p,
        id: PeerId,
        topic: string,
        handler: (message: Message) => void,
        monitor?: IpfsPubsubPeerMonitor
    ) {
        this._libp2p = libp2p;
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
    ): EventHandler<CustomEvent<Message>> {
        return (evt: CustomEvent<Message>) => {
            const message = evt.detail;
            if (message.topic !== this._topic) {
                return;
            }

            if (message.type === "signed") {
                const signedMessage = message as SignedMessage;
                if (signedMessage.from.equals(this._id)) {
                    return;
                }
                if (signedMessage.topic !== this._topic) {
                    return;
                }

                messageCallback(message);
            } else {
                // unsigned message
            }
        };
    }

    async start(): Promise<SharedIPFSChannel> {
        await this._libp2p.pubsub.subscribe(this._topic);
        this._libp2p.pubsub.addEventListener("message", this._handler);
        return this;
    }
    async close() {
        await this._monitor?.stop();
        this._libp2p.pubsub.removeEventListener("message", this._handler);
        await this._libp2p.pubsub.unsubscribe(this._topic);
    }
}
