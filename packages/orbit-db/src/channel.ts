import { IpfsPubsubPeerMonitor } from '@dao-xyz/ipfs-pubsub-peer-monitor'
import { IPFS } from 'ipfs-core-types/src'
import { MessageHandlerFn } from 'ipfs-core-types/src/pubsub'

/* export const getOrCreateChannel = async (pubsub: SharedPubSub, peer: string, getDirectConnection: (peer: string) => DirectChannel, onMessage: (message: { data: Uint8Array }) => void, monitor?: {
    onNewPeerCallback?: (channel: DirectChannel, peer: string) => void,
    onPeerLeaveCallback?: (channel: DirectChannel, peer: string) => void
}): Promise<DirectChannel> => {

   

} */

interface Closable { close: () => Promise<void> };
export class SharedChannel<T extends Closable>
{
    _channel: T
    dependencies: Set<string>
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
    _handler: MessageHandlerFn;
    _monitor: IpfsPubsubPeerMonitor
    _id: string;
    constructor(ipfs: IPFS, id: string, topic: string, handler: (topic: string, content: Buffer, peer: string) => void, monitor?: IpfsPubsubPeerMonitor) {
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
    _messageHandler(messageCallback: (topic: string, data: Buffer, from: string) => void): MessageHandlerFn {
        return (message: {
            data: Buffer
            from: string
            key: Buffer
            receivedFrom: string
            seqno: Buffer
            signature: Uint8Array
            topic: string,
            topicIDs: string[]
        }) => {
            if (message.from === this._id) {
                return;
            }

            const topicId = message.topic ? message.topic : message.topicIDs[0]
            messageCallback(topicId, message.data, message.from)

        }
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