import fs from 'mz/fs';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { OrbitDB } from '@dao-xyz/orbit-db';
import { v4 as uuid } from 'uuid';
import PubSub from '@dao-xyz/orbit-db-pubsub'
import Ctl, { Controller } from 'ipfsd-ctl'
import * as ipfs from 'ipfs';

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
    node: IPFSInstance
    orbitDB: OrbitDB,
    disconnect: () => Promise<void>
}
export const getPeer = async (identity?: Identity): Promise<Peer> => {
    require('events').EventEmitter.prototype._maxListeners = 100;
    require('events').defaultMaxListeners = 100;

    let id = uuid();
    await clean(id);

    let controller = await createIPFSNode('./ipfs/' + id + '/');
    let node = await controller.api;
    /*     let node = await createIPFSNode('./ipfs/' + id + '/');
     */
    let orbitDB = await createOrbitDBInstance(node, id, identity);
    return {
        id,
        node: node,
        orbitDB,
        disconnect: async () => {
            try {
                await orbitDB.disconnect();
                await controller.stop();
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
    let addrA = (await a.node.id()).addresses[0];
    await b.node.swarm.connect(addrA);
    let addrB = (await b.node.id()).addresses[0];
    await a.node.swarm.connect(addrB);
}



export const disconnectPeers = async (peers: Peer[]): Promise<void> => {
    //await Promise.all(peers.map(peer => peer.node.libp2p.dialer.destroy()));
    await Promise.all(peers.map(peer => peer.disconnect()));
    await Promise.all(peers.map(peer => peer.id ? clean(peer.id) : () => { }));
}

export const createIPFSNode = (repo: string = './ipfs'): Promise<Controller> => {
    // Create IPFS instance
    const ipfsOptions = {
        relay: { enabled: false, hop: { enabled: false, active: false } },
        preload: { enabled: false },
        repo: repo,
        EXPERIMENTAL: { pubsub: true },
        config: {
            Addresses: {
                Swarm: [
                    `/ip4/0.0.0.0/tcp/0`
                ],
                Bootstrap: []
            },
        },
        libp2p:
        {
            autoDial: false
        }
    }

    try {
        const ipfsd = Ctl.createController({
            type: 'proc',
            test: true,
            disposable: true,
            ipfsModule: ipfs,
            ipfsOptions: ipfsOptions as any
        })
        return ipfsd
    } catch (err) {
        throw new Error(err)
    }

}