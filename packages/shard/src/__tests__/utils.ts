import fs from 'mz/fs';
import { Identity } from 'orbit-db-identity-provider';
import { TypedBehaviours } from '..';
import { generateUUID } from '../id';
import * as IPFS from 'ipfs';
import { AnyPeer, createOrbitDBInstance, IPFSInstanceExtended, ServerOptions } from '../node';
import { SingleDBInterface, DBInterface, RecursiveShardDBInterface } from '../interface';
import FeedStore from 'orbit-db-feedstore';
import { BinaryDocumentStoreOptions, FeedStoreOptions } from '../stores';
import BN from 'bn.js';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { QueryRequestV0 } from '../query';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { Shard } from '../shard';



export const clean = (id?: string) => {
    let suffix = id ? id + '/' : '';
    try {
        fs.rmSync('./ipfs/' + suffix, { recursive: true, force: true });
        fs.rmSync('./orbitdb/' + suffix, { recursive: true, force: true });
        fs.rmSync('./orbit-db/' + suffix, { recursive: true, force: true });
        fs.rmSync('./orbit-db-stores/' + suffix, { recursive: true, force: true });
    } catch (error) {

    }
}

const testBehaviours: TypedBehaviours = {

    typeMap: {}
}

export const getPeer = async (rootAddress: string = 'root', behaviours: TypedBehaviours = testBehaviours, identity?: Identity, peerCapacity: number = 1000 * 1000 * 1000): Promise<AnyPeer> => {
    let id = generateUUID();
    await clean(id);
    const peer = new AnyPeer(id);
    let options = new ServerOptions({
        behaviours,
        id,
        replicationCapacity: peerCapacity
    });
    let node = await createIPFSNode(false, './ipfs/' + id + '/');
    let orbitDB = await createOrbitDBInstance(node, id, identity);
    await peer.create({ rootAddress, options, orbitDB });
    return peer;
}
export const disconnectPeers = async (peers: AnyPeer[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
    await Promise.all(peers.map(peer => peer.id ? clean(peer.id) : () => { }));

}

export const createIPFSNode = (local: boolean = false, repo: string = './ipfs'): Promise<IPFSInstanceExtended> => {
    // Create IPFS instance
    const ipfsOptions = local ? {
        preload: { enabled: false },
        repo: repo,
        EXPERIMENTAL: { pubsub: true },
        config: {
            Bootstrap: [],
            Addresses: { Swarm: [] }
        }
    } : {
        relay: { enabled: true, hop: { enabled: true, active: true } },
        repo: repo,
        EXPERIMENTAL: { pubsub: true },
        config: {
            Addresses: {
                Swarm: [
                    `/ip4/0.0.0.0/tcp/0`,
                    `/ip4/127.0.0.1/tcp/0/ws`
                ]
            }

        },
    }
    return IPFS.create(ipfsOptions)

}

@variant(122)
export class FeedStoreInterface extends DBInterface {

    @field({ type: SingleDBInterface })
    db: SingleDBInterface<string, FeedStore<string>>;

    constructor(opts?: { db: SingleDBInterface<string, FeedStore<string>> }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get initialized(): boolean {
        return !!this.db?.db && !!this.db._shard;
    }

    close() {
        this.db.db = undefined;
    }

    async init(shard: Shard<any>) {
        await this.db.init(shard);
    }

    async load(): Promise<void> {
        await this.db.load();
    }


    async query(q: QueryRequestV0): Promise<string[]> {
        return this.db.query(q);
    }
}

export const feedStoreShard = async () => new Shard({
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new FeedStoreInterface({
        db: new SingleDBInterface({
            name: 'feed',
            storeOptions: new FeedStoreOptions()
        })

    }),
})


@variant(123)
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

    close() {
        this.db.close();
    }

    async init(shard: Shard<any>) {
        await this.db.init(shard);
    }

    async load(waitForReplicationEventsCount = 0): Promise<void> {
        await this.db.load(waitForReplicationEventsCount);
    }


    async query(q: QueryRequestV0): Promise<T[]> {
        return this.db.query(q);
    }
}

export const documentStoreShard = async <T>(clazz: Constructor<T>, indexBy: string = 'id') => new Shard({
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
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new RecursiveShardDBInterface({
        db: new SingleDBInterface({
            name: 'shards',
            storeOptions: new BinaryDocumentStoreOptions<Shard<T>>({
                indexBy: 'id',
                objectType: Shard.name
            })
        })
    })
})
