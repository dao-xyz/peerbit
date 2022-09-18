import { variant, field, vec, serialize } from '@dao-xyz/borsh';
import { delay } from '@dao-xyz/time';
import { Message } from './message';
import isNode from 'is-node';
import { MaybeSigned, PublicKey } from '@dao-xyz/identity';
import { DecryptedThing, PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { Store } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from './orbit-db';


let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

export const EMIT_HEALTHCHECK_INTERVAL = 5000;


@variant([2, 0])
export class PeerInfo extends Message {

    @field({ type: 'string' })
    replicationTopic: string

    @field({ type: 'string' })
    store: string // address

    @field({ type: 'u64' })
    memoryLeft: bigint

    constructor(props?: {
        replicationTopic: string,
        store: string,
        memoryLeft: bigint
    }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic;
            this.store = props.store;
            this.memoryLeft = props.memoryLeft;
        }
    }
}
@variant([2, 1])
export class RequestPeerInfo extends Message {

    @field({ type: 'string' })
    replicationTopic: string

    @field({ type: 'string' })
    store: string // address

    @field({ type: 'string' })
    peer: string

    constructor(props?: {
        replicationTopic: string,
        store: string,
        peer: string
    }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic;
            this.store = props.store;
            this.peer = props.peer;
        }
    }
}


export interface PeerInfoWithMeta {
    peerInfo: PeerInfo
    publicKey: PublicKey
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

export const requestPeerInfo = async (replicationTopic: string, store: string, peer: string, publish: (topic: string, message: Uint8Array) => Promise<void>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>) => {

    const signedMessage = await new MaybeSigned({
        data: serialize(new RequestPeerInfo({
            replicationTopic,
            store,
            peer
        }))

    }).sign(sign)
    const decryptedMessage = new DecryptedThing({
        data: serialize(signedMessage)
    })// TODO add encryption  .init(encryption).encrypt(lala)

    return publish(replicationTopic, serialize(decryptedMessage))
}

export const exchangePeerInfo = async (replicationTopic: string, store: string, publish: (topic: string, message: Uint8Array) => Promise<void>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>) => {

    const signedMessage = await new MaybeSigned({
        data: serialize(new PeerInfo({
            replicationTopic,
            store,
            memoryLeft: v8.getHeapStatistics().total_available_size//v8

        }))

    }).sign(sign)
    const decryptedMessage = new DecryptedThing({
        data: serialize(signedMessage)
    })// TODO add encryption  .init(encryption).encrypt(lala)

    return publish(replicationTopic, serialize(decryptedMessage))
}

export class ResourceRequirement {

    async ok(_orbitdb: OrbitDB): Promise<boolean> {
        throw new Error("Not implemented")
    }
}

@variant(0)
export class NoResourceRequirement extends ResourceRequirement { }

@variant(1)
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
        if (!v8) {
            return true;
        }
        const usedHeap: number = v8.getHeapStatistics().used_heap_size;
        return BigInt(usedHeap) + this.heapSize < orbitdb.heapSizeLimit;
    }


}


@variant([2, 2])
export class RequestReplication extends Message {

    @field({ type: 'string' })
    replicationTopic: string

    @field({ type: Store })
    store: Store<any> // address

    @field({ type: vec(ResourceRequirement) })
    resourceRequirements: ResourceRequirement[];

    constructor(props?: {
        resourceRequirements: ResourceRequirement[],
        replicationTopic: string,
        store: Store<any>
    }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic;
            this.store = props.store;
            this.resourceRequirements = props.resourceRequirements;
        }
    }
}
