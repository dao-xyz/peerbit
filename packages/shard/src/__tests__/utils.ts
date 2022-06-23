import fs from 'mz/fs';
import { Identity } from 'orbit-db-identity-provider';
import { TypedBehaviours } from '..';
import { create } from 'ipfs';
import { AnyPeer, IPFSInstanceExtended, PeerOptions } from '../node';
import { SingleDBInterface, DBInterface, RecursiveShardDBInterface } from '../interface';
import BN from 'bn.js';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore, BinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryFeedStoreOptions, BinaryFeedStore } from '@dao-xyz/orbit-db-bfeedstore';
import { v4 as uuid } from 'uuid';
import { Shard } from '../shard';
import { getPeer as getPeerTest, getConnectedPeers as getConnectedPeersTest, Peer } from '@dao-xyz/peer-test-utils';
export const getPeer = async (identity?: Identity, isServer?: boolean, peerCapacity?: number) => getPeerTest(identity).then((peer) => createAnyPeer(peer, isServer, peerCapacity));
export const getConnectedPeers = async (amountOf: number, identity?: Identity, isServer?: boolean, peerCapacity?: number) => getConnectedPeersTest(amountOf, identity).then(peers => Promise.all(peers.map(peer => createAnyPeer(peer, isServer, peerCapacity))));
const createAnyPeer = async (peer: Peer, isServer: boolean = true, peerCapacity: number = 1000 * 1000 * 1000): Promise<AnyPeer> => {
    const anyPeer = new AnyPeer(peer.id);
    let options = new PeerOptions({
        behaviours: {
            typeMap: {}
        },
        directoryId: peer.id,
        replicationCapacity: peerCapacity,
        isServer,
        peersRecycle: {
            maxOplogLength: 20,
            cutOplogToLength: 10
        }
    });

    await anyPeer.create({ options, orbitDB: peer.orbitDB });
    anyPeer.options.behaviours.typeMap[Document.name] = Document
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
export class BinaryFeedStoreInterface extends DBInterface {

    @field({ type: SingleDBInterface })
    db: SingleDBInterface<Document, BinaryFeedStore<Document>>;

    constructor(opts?: { db: SingleDBInterface<Document, BinaryFeedStore<Document>> }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get initialized(): boolean {
        return !!this.db?.db && !!this.db._shard;
    }

    get loaded(): boolean {
        return this.db.loaded
    }

    close() {
        this.db.db = undefined;
    }

    async init(shard: Shard<any>): Promise<void> {
        await this.db.init(shard);
    }

    async load(): Promise<void> {
        await this.db.load();
    }
}

export const feedStoreShard = async<T>(clazz: Constructor<T>) => new Shard({
    id: uuid(),
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new BinaryFeedStoreInterface({
        db: new SingleDBInterface({
            name: 'feed',
            storeOptions: new BinaryFeedStoreOptions({
                objectType: clazz.name
            })
        })

    }),
})


@variant([1, 1])
export class DocumentStoreInterface<T> extends DBInterface {

    @field({ type: SingleDBInterface })
    db: SingleDBInterface<T, BinaryDocumentStore<T>>;

    constructor(opts?: { db: SingleDBInterface<T, BinaryDocumentStore<T>> }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get initialized(): boolean {
        return this.db.initialized;
    }

    get loaded(): boolean {
        return this.db.loaded
    }

    close() {
        this.db.close();
    }

    async init(shard: Shard<any>): Promise<void> {
        await this.db.init(shard);
    }

    async load(waitForReplicationEventsCount = 0): Promise<void> {
        await this.db.load(waitForReplicationEventsCount);
    }
}

export const documentStoreShard = async <T>(clazz: Constructor<T>, indexBy: string = 'id') => new Shard({
    id: uuid(),
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new DocumentStoreInterface<T>({
        db: new SingleDBInterface({
            name: 'documents',
            storeOptions: new BinaryDocumentStoreOptions<T>({
                indexBy,
                objectType: clazz.name
            })
        })
    })
})

export const shardStoreShard = async <T extends DBInterface>() => new Shard<RecursiveShardDBInterface<T>>({
    id: uuid(),
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new RecursiveShardDBInterface({
        db: new SingleDBInterface({
            name: 'shards',
            storeOptions: new BinaryDocumentStoreOptions<Shard<T>>({
                indexBy: 'cid',
                objectType: Shard.name
            })
        })
    })
})
