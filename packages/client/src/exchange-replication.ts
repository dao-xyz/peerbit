import { variant, field, vec, option, serialize } from '@dao-xyz/borsh';
import { delay } from '@dao-xyz/time';
import { ProtocolMessage } from './message.js';
// @ts-ignore
import isNode from 'is-node';
import { MaybeSigned, PublicSignKey } from '@dao-xyz/peerbit-crypto';
import { DecryptedThing, PublicKeyEncryption } from "@dao-xyz/peerbit-crypto";
import { Address, Store, StoreLike } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from './orbit-db.js';
import { StringSetSerializer } from '@dao-xyz/borsh-utils';
// @ts-ignore
import { v4 as uuid } from 'uuid';
let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

export const WAIT_FOR_PEERS_TIME = 5000;


@variant([2, 0])
export class ReplicatorInfo extends ProtocolMessage {

    @field({ type: option('string') })
    fromId?: string;

    @field({ type: 'string' })
    replicationTopic: string

    @field({ type: 'string' })
    store: string // address

    @field({ type: option(StringSetSerializer) })
    heads?: Set<string> // address
    /* 
        @field({ type: 'bool' })
        allowForks: boolean
     */
    @field({ type: 'u64' })
    memoryLeft: bigint

    constructor(props?: {
        fromId?: string;
        replicationTopic: string,
        store: string,
        memoryLeft: bigint,
/*         allowForks: boolean
 */        heads?: Set<string> | string[]
    }) {
        super();
        if (props) {
            this.fromId = props.fromId;
            this.replicationTopic = props.replicationTopic;
            this.store = props.store;
            this.memoryLeft = props.memoryLeft;
            this.heads = Array.isArray(props.heads) ? new Set(props.heads) : this.heads;
            /*  this.allowForks = props.allowForks; */
        }
    }
}

@variant([2, 1])
export class RequestReplicatorInfo extends ProtocolMessage {

    @field({ type: 'string' })
    id: string;

    @field({ type: 'string' })
    replicationTopic: string

    @field({ type: 'string' })
    address: string // address

    @field({ type: vec('string') })
    heads: string[]

    constructor(props?: {
        replicationTopic: string,
        address: Address | string,
        heads: string[]
    }) {
        super();
        if (props) {
            this.id = uuid();
            this.replicationTopic = props.replicationTopic;
            this.address = typeof props.address === 'string' ? props.address : props.address.toString()
            this.heads = props.heads;
        }
    }

}


export interface PeerInfoWithMeta {
    peerInfo: ReplicatorInfo
    publicKey: PublicSignKey
}
/* return new PeerInfo({
    key: this._shard.peer.orbitDB.identity,
    addresses: (await this._shard.peer.node.id()).addresses.map(x => x.toString()),
    memoryLeft: v8.getHeapStatistics().total_available_size//v8
}) */

/* export const createEmitHealthCheckJob = (properties: { stores: () => string[] | undefined, subscribingForReplication: (topic: string) => boolean }, replicationTopic: string, publish: (topic: string, message: Uint8Array) => Promise<void>, isOnline: () => boolean, controller: AbortController, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>, encryption: PublicKeyEncryption) => {

    const emitHealthcheck = async (): Promise<void> => {
        const s = properties.stores();
        if (!s || s.length === 0) {
            return;
        }
        const signedMessage = await new MaybeSigned({
            data: serialize(new PeerInfo({
                replicationTopic,
                stores: s,
                subscribingForReplication: properties.subscribingForReplication(replicationTopic),
                memoryLeft: v8.getHeapStatistics().total_available_size//v8

            }))
        }).sign(sign)
        const decryptedMessage = new DecryptedThing({
            data: serialize(signedMessage)
        })// TODO add encryption  .init(encryption).encrypt(lala)

        return publish(replicationTopic, serialize(decryptedMessage))
    }

    const task = async () => {
        await emitHealthcheck();
    }

    const cron = async () => {
        let stop = false;
        let promise: Promise<any> = undefined;
        let delayStopper: () => void | undefined = undefined;
        controller.signal.addEventListener("abort", async () => {
            stop = true;
            if (delayStopper)
                delayStopper();
            await promise;
        });
        while (isOnline() && !stop) { // 
            promise = task();
            await promise;
            await delay(EMIT_HEALTHCHECK_INTERVAL, { stopperCallback: (stopper) => { delayStopper = stopper } }); // some delay
        }
    }
    return cron;
}
 */

export const requestPeerInfo = async (serializedRequest: Uint8Array, replicationTopic: string, publish: (topic: string, message: Uint8Array) => Promise<void>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicSignKey }>) => {

    const signedMessage = await new MaybeSigned({
        data: serializedRequest
    }).sign(sign)
    const decryptedMessage = new DecryptedThing({
        data: serialize(signedMessage)
    })// TODO add encryption  .init(encryption).encrypt(lala)

    return publish(replicationTopic, serialize(decryptedMessage))
}

export const exchangePeerInfo = async (fromId: string, replicationTopic: string, store: StoreLike<any>, heads: string[] | undefined, publish: (message: Uint8Array) => Promise<void>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicSignKey }>) => {

    const signedMessage = await new MaybeSigned({
        data: serialize(new ReplicatorInfo({
            fromId,
            replicationTopic,
            store: store.address.toString(),
            /*   allowForks: store.allowForks, */
            heads,
            memoryLeft: v8.getHeapStatistics().total_available_size //v8
        }))
    }).sign(sign)

    const decryptedMessage = new DecryptedThing({
        data: serialize(signedMessage)
    })// TODO add encryption  .init(encryption).encrypt(lala)

    return publish(serialize(decryptedMessage))
}

export class ResourceRequirement {

    async ok(_orbitdb: OrbitDB): Promise<boolean> {
        throw new Error("Not implemented")
    }
}

@variant(0)
export class NoResourceRequirement extends ResourceRequirement { }

/* @variant(1)
export class HeapSizeRequirement extends ResourceRequirement {

    @field({ type: 'u64' })
    heapSize: bigint

    constructor(properties?: { heapSize: bigint }) {
        super();
        if (properties) {
            this.heapSize = properties.heapSize;
        }
    }

    async ok(orbitdb: OrbitDB): Promise<boolean> {
        if (!v8 || typeof orbitdb.heapsizeLimitForForks !== 'number') {
            return true;
        }
        const usedHeap: number = v8.getHeapStatistics().used_heap_size;
        return BigInt(usedHeap) + this.heapSize < orbitdb.heapsizeLimitForForks;
    }


} */

