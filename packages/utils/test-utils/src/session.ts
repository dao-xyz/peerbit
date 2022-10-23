import { IPFS } from "ipfs";
import type { PeerId } from "@libp2p/interface-peer-id";
import { connectPeers, nodeConfig } from "./index.js";
import { startIpfs } from "./start-ipfs.js";
import getIpfsPeerId from "./get-ipfs-peer-id.js";
// @ts-ignore
import { v4 as uuid } from 'uuid';
import { Controller } from "ipfsd-ctl";

export interface Peer {
    ipfsd: any,
    id: PeerId,
    ipfs: IPFS
}
export class Session {
    peers: Peer[]

    constructor(peers: Peer[]) {
        this.peers = peers;
    }

    static async connected(n: number, api: 'js-ipfs' | 'go-ipfs' | string = 'js-ipfs', config?: any, connectFilter?: { filter: (addrs: string) => boolean }) {
        const promises: Promise<Controller>[] = [];
        for (let i = 0; i < n; i++) {
            promises.push(startIpfs(api, { ...(config || nodeConfig.defaultIpfsConfig), repo: './tmp/ipfs/repo-' + uuid() }))
        }
        const ipfsd = await Promise.all(promises);
        const connectPromises: Promise<any>[] = []
        const ids = await Promise.all(ipfsd.map(d => getIpfsPeerId(d.api)))
        const ipfs = ipfsd.map(ipfsd => ipfsd.api);

        for (let i = 0; i < n - 1; i++) {
            for (let j = i + 1; j < n; j++) {
                connectPromises.push(connectPeers(ipfsd[i].api, ipfsd[j].api, connectFilter))
            }

        }
        await Promise.all(connectPromises);
        const peers: Peer[] = []
        for (let i = 0; i < ipfsd.length; i++) {
            peers.push(
                {
                    id: ids[i],
                    ipfs: ipfsd[i].api,
                    ipfsd: ipfsd[i]
                }
            )
        }
        return new Session(peers)
    }

    stop(): Promise<any> {
        return Promise.all(this.peers.map(p => p.ipfsd.stop()))
    }
}