import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import BN from "bn.js";
import { Shard } from "./shard";
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { delay } from "@dao-xyz/time";
import { createHash } from "crypto";
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";

export const EMIT_HEALTHCHECK_INTERVAL = 5000;


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

    @field({ type: IdentitySerializable })
    key: IdentitySerializable

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({ type: 'u64' })
    memoryBudget: BN

    constructor(obj?: {
        key: IdentitySerializable,
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


    async getShardPeerInfo(): Promise<PeerInfo> {
        return new PeerInfo({
            key: this._shard.peer.orbitDB.identity.toSerializable(),
            addresses: (await this._shard.peer.node.id()).addresses.map(x => x.toString()),
            memoryBudget: new BN(this._shard.peer.options.replicationCapacity)
        })
    }

    /**
     * 
     * Start to "support" the shard
     * by responding to peer healthcheck requests
     */
    async emitHealthcheck(): Promise<void> {
        if (this._shard.peer.options.isServer) {
            this._shard.peer.node.pubsub.publish(this.peerHealthTopic, serialize(await this.getShardPeerInfo()))
        }
    }
    get peerHealthTopic(): string {
        return this._shard.getQueryTopic('health')
    }


    async getPeers(): Promise<PeerInfo[]> {
        let peers: Map<string, PeerInfo> = new Map();
        const ids = new Set();
        await this._shard.peer.node.pubsub.subscribe(this.peerHealthTopic, (message: Message) => {
            const p = deserialize(Buffer.from(message.data), PeerInfo);
            peers.set(p.key.toString(), p); // TODO check verify responses are valid
        })

        await delay(EMIT_HEALTHCHECK_INTERVAL + 3000); // add some extra padding to make sure all nodes have emitted
        return [...peers.values()]
    }

    /**
     * An intentionally imperfect leader rotation routine
     * @param slot, some time measure
     * @returns 
     */
    async isLeader(slot: number): Promise<boolean> {
        // Hash the time, and find the closest peer id to this hash
        const h = (h: string) => createHash('sha1').update(h).digest('hex');
        const slotHash = h(slot.toString())


        const hashToPeer: Map<string, PeerInfo> = new Map();
        const peers: PeerInfo[] = [...await this.getPeers()];
        if (peers.length == 0) {
            return false;
        }

        const peerHashed: string[] = [];
        peers.forEach((peer) => {
            const peerHash = h(peer.key.toString());
            hashToPeer.set(peerHash, peer);
            peerHashed.push(peerHash);
        })
        peerHashed.push(slotHash);
        // TODO make more efficient
        peerHashed.sort((a, b) => a.localeCompare(b)) // sort is needed, since "getPeers" order is not deterministic
        let slotIndex = peerHashed.findIndex(x => x === slotHash);
        // we only step forward 1 step (ignoring that step backward 1 could be 'closer')
        // This does not matter, we only have to make sure all nodes running the code comes to somewhat the 
        // same conclusion (are running the same leader selection algorithm)
        let nextIndex = slotIndex + 1;
        if (nextIndex >= peerHashed.length)
            nextIndex = 0;
        return hashToPeer.get(peerHashed[nextIndex]).key.id === this._shard.peer.orbitDB.identity.id

        // better alg, 
        // convert slot into hash, find most "probable peer" 
    }

    close(): Promise<void> {
        return this._shard.peer.node.pubsub.unsubscribe(this._shard.getQueryTopic('peer'))
    }
}