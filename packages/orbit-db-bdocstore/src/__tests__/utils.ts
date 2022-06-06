import fs from 'mz/fs';
import { Identity } from 'orbit-db-identity-provider';
import * as IPFS from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import OrbitDB from 'orbit-db';
import { randomUUID } from 'crypto';
import { BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore } from '../document-store';

OrbitDB.addDatabaseType(BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore as any)

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


export const createOrbitDBInstance = (node: IPFSInstance | any, id: string, identity?: Identity) => OrbitDB.createInstance(node,
    {
        identity: identity,
        directory: './orbit-db/' + id
    })

export class Peer {
    id: string
    node: IPFSInstance
    orbitDB: OrbitDB
}
export const getPeer = async (): Promise<Peer> => {
    let id = randomUUID();
    await clean(id);
    let node = await createIPFSNode(false, './ipfs/' + id + '/');
    let orbitDB = await createOrbitDBInstance(node, id);
    return {
        id,
        node,
        orbitDB
    };
}
export const disconnectPeers = async (peers: Peer[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.orbitDB.disconnect()));
    await Promise.all(peers.map(peer => peer.id ? clean(peer.id) : () => { }));

}

export const createIPFSNode = (local: boolean = false, repo: string = './ipfs'): Promise<IPFSInstance> => {
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



export const delay = (ms: number) => new Promise(res => setTimeout(res, ms));
export const waitFor = async (fn: () => boolean, timeout: number = 60 * 1000) => {

    let startTime = +new Date;
    while (+new Date - startTime < timeout) {
        if (fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")

};
