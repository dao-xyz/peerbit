import fs from 'mz/fs';
import { Identity } from 'orbit-db-identity-provider';
import { create } from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import OrbitDB from 'orbit-db';
import { v4 as uuid } from 'uuid';
import PubSub from '@dao-xyz/orbit-db-pubsub'

export interface IPFSInstanceExtended extends IPFSInstance {
    libp2p: any
}


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
        directory: './orbit-db/' + id,
        broker: PubSub
    })

export interface Peer {
    id: string
    node: IPFSInstanceExtended
    orbitDB: OrbitDB,
    disconnect: () => Promise<void>
}
export const getPeer = async (identity?: Identity): Promise<Peer> => {
    require('events').EventEmitter.prototype._maxListeners = 100;
    require('events').defaultMaxListeners = 100;

    let id = uuid();
    await clean(id);

    let node = await createIPFSNode('./ipfs/' + id + '/');
    let orbitDB = await createOrbitDBInstance(node, id, identity);

    return {
        id,
        node,
        orbitDB,
        disconnect: async () => {
            try {
                await orbitDB.disconnect();
                await node.stop();
            } catch (error) {

            }
        }
    };
}
export const getConnectedPeers = async (numberOf: number, identity?: Identity): Promise<Peer[]> => {

    const peersPromises: Promise<Peer>[] = [];
    for (let i = 0; i < numberOf; i++) {
        peersPromises.push(getPeer(identity));
    }
    const peers = await Promise.all(peersPromises);
    const connectPromises = [];
    for (let i = 1; i < numberOf; i++) {
        connectPromises.push(connectPeers(peers[i - 1], peers[i]))
    }
    await Promise.all(connectPromises);
    return peers;
}

export const connectPeers = async (a: Peer, b: Peer): Promise<void> => {
    let addr = (await a.node.id()).addresses[0];
    await b.node.swarm.connect(addr);
}



export const disconnectPeers = async (peers: Peer[]): Promise<void> => {
    //await Promise.all(peers.map(peer => peer.node.libp2p.dialer.destroy()));
    await Promise.all(peers.map(peer => peer.disconnect()));
    await Promise.all(peers.map(peer => peer.id ? clean(peer.id) : () => { }));
}

export const createIPFSNode = (repo: string = './ipfs'): Promise<IPFSInstanceExtended> => {
    // Create IPFS instance
    const ipfsOptions = {
        relay: { enabled: false, hop: { enabled: false, active: false } },
        /*  relay: { enabled: false, hop: { enabled: false, active: false } }, */
        preload: { enabled: false },
        offline: true,
        repo: repo,
        EXPERIMENTAL: { pubsub: true },
        config: {
            Addresses: {
                Swarm: [
                    `/ip4/0.0.0.0/tcp/0`/* ,
                    `/ip4/127.0.0.1/tcp/0/ws` */
                ]
            }
        },
        libp2p:
        {
            autoDial: false
        }
    }
    return create(ipfsOptions)

}