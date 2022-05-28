import { Connection } from '@solana/web3.js';
import fs from 'mz/fs';
import { Identity } from 'orbit-db-identity-provider';
import os from 'os';
import path from 'path';
import yaml from 'yaml';
import { TypedBehaviours } from '..';
import { generateUUID } from '../id';
import * as IPFS from 'ipfs';
import { AnyPeer, createOrbitDBInstance, IPFSInstanceExtended, ServerOptions } from '../node';
import { DB, DBInterface, RecursiveShardDBInterface, Shard } from '../shard';
import FeedStore from 'orbit-db-feedstore';
import { BinaryDocumentStoreOptions, FeedStoreOptions } from '../stores';
import BN from 'bn.js';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { query, QueryRequestV0 } from '../query';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import Store from 'orbit-db-store';
/**
 * @private
 */
async function getConfig(): Promise<any> {
    // Path to Solana CLI config file
    const CONFIG_FILE_PATH = path.resolve(os.homedir(), '.config', 'solana', 'cli', 'config.yml');
    const configYml = await fs.readFile(CONFIG_FILE_PATH, { encoding: 'utf8' });
    return yaml.parse(configYml);
}

/**
 * Load and parse the Solana CLI config file to determine which RPC url to use
 */
export async function getRpcUrl(): Promise<string> {
    try {
        const config = await getConfig();
        if (!config.json_rpc_url) throw new Error('Missing RPC URL');
        return config.json_rpc_url;
    } catch (err) {
        console.warn('Failed to read RPC url from CLI config file, falling back to localhost');
        return 'http://localhost:8899';
    }
}

export const establishConnection = async (rpcUrl?: string): Promise<Connection> => {
    if (!rpcUrl) rpcUrl = await getRpcUrl();
    const connection = new Connection(rpcUrl, 'confirmed');
    return connection;
};
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

export const getPeer = async (rootAddress: string = 'root', behaviours: TypedBehaviours = testBehaviours, identity?: Identity, peerCapacity: number = 50 * 1000): Promise<AnyPeer> => {
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

    @field({ type: DB })
    db: DB<FeedStore<string>>;

    constructor(opts?: { db: DB<FeedStore<string>> }) {
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

    async load(): Promise<FeedStore<string>> {
        return this.db.load();
    }

    isLoaded(): boolean {
        return this.initialized;
    }

    async query(q: QueryRequestV0): Promise<string[]> {
        return query<string>(q, this.db.db)
    }
}

export const feedStoreShard = async () => new Shard({
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new FeedStoreInterface({
        db: new DB({
            name: 'feed',
            storeOptions: new FeedStoreOptions()
        })
    }),
})


@variant(123)
export class DocumentStoreInterface<T> extends DBInterface {

    @field({ type: DB })
    db: DB<BinaryDocumentStore<T>>;

    constructor(opts?: { db: DB<BinaryDocumentStore<T>> }) {
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

    async load(waitForReplicationEventsCount = 0): Promise<BinaryDocumentStore<T>> {
        return this.db.load(waitForReplicationEventsCount);
    }

    isLoaded(): boolean {
        return this.initialized;
    }

    async query(q: QueryRequestV0): Promise<T[]> {
        return query<T>(q, this.db.db)
    }
}

export const documentStoreShard = async <T>(clazz: Constructor<T>, indexBy: string = 'id') => new Shard({
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new DocumentStoreInterface<T>({
        db: new DB({
            name: 'documents',
            storeOptions: new BinaryDocumentStoreOptions<T>({
                indexBy,
                objectType: clazz.name
            })
        })
    })
})

export const shardStoreShard = async <T extends DBInterface>(id?: string) => new Shard<RecursiveShardDBInterface<T>>({
    id,
    cluster: 'x',
    shardSize: new BN(500 * 1000),
    interface: new RecursiveShardDBInterface({
        db: new DB({
            name: 'shards',
            storeOptions: new BinaryDocumentStoreOptions<Shard<T>>({
                indexBy: 'id',
                objectType: Shard.name
            })
        })
    })
})
