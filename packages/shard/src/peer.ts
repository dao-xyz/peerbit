import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { ResultSource } from "@dao-xyz/bquery";
import BN from "bn.js";
import { PublicKey } from "./key";
import { Shard } from "./shard";
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { delay } from "@dao-xyz/time";


/* @variant([0, 3])
export class Peer extends ResultSource {

    @field({ type: PublicKey })
    key: PublicKey

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({ type: 'u64' })
    timestamp: BN

    @field({ type: 'u64' })
    memoryBudget: BN // how much memory left to space

    constructor(obj?: {
        key: PublicKey,
        addresses: string[],
        timestamp: BN,
        memoryBudget: BN
    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }
}
 */

@variant("check")
export class PeerCheck {

    @field({ type: 'String' })
    responseTopic: string

    constructor(obj?: { responseTopic: string }) {
        if (obj) {
            Object.assign(this, obj);
        }

    }

}

@variant("info")
export class PeerInfo {

    @field({ type: PublicKey })
    key: PublicKey

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({ type: 'u64' })
    memoryBudget: BN

    constructor(obj?: {
        key: PublicKey,
        addresses: string[],
        memoryBudget: BN
    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

}

export class ShardPeerInfo {
    _shard: Shard<any>
    constructor(shard: Shard<any>) {
        this._shard = shard;
    }


    /**
     * 
     * Start to "support" the shard
     * by responding to peer healthcheck requests
     */
    async emitHealthcheck(): Promise<void> {
        if (this._shard.peer.options.isServer) {
            this._shard.peer.node.pubsub.publish(this.peerHealthTopic, serialize(new PeerInfo({
                key: PublicKey.from(this._shard.peer.orbitDB.identity),
                addresses: (await this._shard.peer.node.id()).addresses.map(x => x.toString()),
                memoryBudget: new BN(this._shard.peer.options.replicationCapacity)
            })))
        }
    }
    get peerHealthTopic(): string {
        return this._shard.getQueryTopic('health')
    }


    async getPeers(maxAggregationTime: number = 5 * 1000): Promise<PeerInfo[]> {
        let peers: Map<string, PeerInfo> = new Map();
        const ids = new Set();
        await this._shard.peer.node.pubsub.subscribe(this.peerHealthTopic, (message: Message) => {
            const p = deserialize(Buffer.from(message.data), PeerInfo);
            peers.set(p.key.toString(), p); // TODO check verify responses are valid
        })

        await delay(maxAggregationTime);
        return [...peers.values()]
    }

    close(): Promise<void> {
        return this._shard.peer.node.pubsub.unsubscribe(this._shard.getQueryTopic('peer'))
    }
}