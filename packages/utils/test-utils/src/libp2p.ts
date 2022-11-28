import { createLibp2p, Libp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { floodsub } from "@libp2p/floodsub";

export class LSession {
    peers: Libp2p[];

    constructor(peers: Libp2p[]) {
        this.peers = peers;
    }

    static async connected(n: number) {
        const promises: Promise<Libp2p>[] = [];
        for (let i = 0; i < n; i++) {
            const result = async () => {
                const node = await createLibp2p({
                    addresses: {
                        listen: ["/ip4/0.0.0.0/tcp/0"],
                    },
                    transports: [tcp()],
                    streamMuxers: [mplex()],
                    connectionEncryption: [noise()],
                    pubsub: floodsub(),
                });
                await node.start();
                return node;
            };
            promises.push(result());
        }

        const ipfsd = await Promise.all(promises);
        const connectPromises: Promise<any>[] = [];

        for (let i = 0; i < n - 1; i++) {
            for (let j = i + 1; j < n; j++) {
                await ipfsd[i].peerStore.addressBook.set(
                    ipfsd[j].peerId,
                    ipfsd[j].getMultiaddrs()
                );
                connectPromises.push(ipfsd[i].dial(ipfsd[j].peerId));
            }
        }

        await Promise.all(connectPromises);

        const peers: Libp2p[] = [];
        for (let i = 0; i < ipfsd.length; i++) {
            peers.push(ipfsd[i]);
        }
        return new LSession(peers);
    }

    stop(): Promise<any> {
        return Promise.all(this.peers.map((p) => p.stop()));
    }
}
