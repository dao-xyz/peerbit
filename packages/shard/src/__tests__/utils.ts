import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { AnyPeer, PeerOptions } from '../peer';
import { RecursiveShardDBInterface } from '../interface';
import { SingleDBInterface, DBInterface, } from '@dao-xyz/orbit-db-store-interface';
import BN from 'bn.js';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore, BinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryFeedStoreOptions, BinaryFeedStore } from '@dao-xyz/orbit-db-bfeedstore';
import { v4 as uuid } from 'uuid';
import { NoResourceRequirements, ResourceRequirements, Shard } from '../shard';
import { getPeer as getPeerTest, getConnectedPeers as getConnectedPeersTest, Peer } from '@dao-xyz/peer-test-utils';
import { P2PTrust } from '@dao-xyz/orbit-db-trust-web';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
export const getPeer = async (identity?: Identity, isServer?: boolean, peerCapacity?: number) => getPeerTest(identity).then((peer) => createAnyPeer(peer, isServer, peerCapacity));
export const getConnectedPeers = async (amountOf: number, identity?: Identity, isServer?: boolean, peerCapacity?: number) => getConnectedPeersTest(amountOf, identity).then(peers => Promise.all(peers.map(peer => createAnyPeer(peer, isServer, peerCapacity))));
const createAnyPeer = async (peer: Peer, isServer: boolean = true, peerCapacity: number = 1000 * 1000 * 1000): Promise<AnyPeer> => {
    const anyPeer = new AnyPeer(peer.id);
    let options = new PeerOptions({
        directoryId: peer.id,
        heapSizeLimit: peerCapacity,
        isServer
    });

    await anyPeer.create({ options, orbitDB: peer.orbitDB });
    return anyPeer;

}
export class Document {
    @field({ type: 'String' })
    id: string;
    constructor(opts?: { id: string }) {
        if (opts) {
            this.id = opts.id;
        }

    }
}

/* const testBehaviours: TypedBehaviours = {

    typeMap: {
        [Document.name]: Document
    }
} */

@variant([1, 0])
export class BinaryFeedStoreInterface extends SingleDBInterface<Document, BinaryFeedStore<Document>> {

    /* @field({ type: SingleDBInterface })
    db: ;

    constructor(opts?: { db: SingleDBInterface<Document, BinaryFeedStore<Document>> }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get initialized(): boolean {
        return !!this.db?.db && !!this.db._peer;
    }

    get loaded(): boolean {
        return this.db.loaded
    }

    close() {
        this.db.db = undefined;
    }

    async init(peer: AnyPeer, options: IStoreOptions<Document, any>): Promise<void> {
        await this.db.init(peer, options);
    }

    async load(): Promise<void> {
        await this.db.load();
    } */
}

export const feedStoreShard = async<T>(clazz: Constructor<T>, trust?: P2PTrust) => new Shard({
    id: uuid(),
    cluster: 'x',
    resourceRequirements: new NoResourceRequirements(),
    interface: new BinaryFeedStoreInterface({
        name: 'feed',
        storeOptions: new BinaryFeedStoreOptions({
            objectType: clazz.name
        })
    }),
    trust
})


@variant([1, 1])
export class DocumentStoreInterface extends SingleDBInterface<Document, BinaryDocumentStore<Document>> {
    init(orbitDB: OrbitDB, options: IStoreOptions<Document, any>): Promise<void> {
        return super.init(orbitDB, {
            ...options, typeMap: {
                [Document.name]: Document
            }
        })
    }
}

export const documentStoreShard = async <T>(trust?: P2PTrust, indexBy: string = 'id') => new Shard({
    id: uuid(),
    cluster: 'x',
    resourceRequirements: new NoResourceRequirements(),
    interface: new DocumentStoreInterface({
        name: 'documents',
        storeOptions: new BinaryDocumentStoreOptions({
            indexBy,
            objectType: Document.name
        }),
    }),
    trust
})


export const shardStoreShard = async <T extends DBInterface>(trust?: P2PTrust) => new Shard<RecursiveShardDBInterface<T>>({
    id: uuid(),
    cluster: 'x',
    resourceRequirements: new NoResourceRequirements(),
    interface: new RecursiveShardDBInterface({
        name: 'shards'
    }),
    trust
})
