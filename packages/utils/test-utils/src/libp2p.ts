import { createLibp2p, Libp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import waitForPeers from "./wait-for-peers";
import { setMaxListeners } from "events";
import { PubSub } from "@libp2p/interface-pubsub";
import { GossipsubEvents } from "@chainsafe/libp2p-gossipsub";

export class LSession {
    peers: (Libp2p & { pubsub: PubSub<GossipsubEvents> })[];

    constructor(peers: (Libp2p & { pubsub: PubSub<GossipsubEvents> })[]) {
        this.peers = peers;
    }

    static async connected(n: number, pubsubTopics?: string[]) {
        // Allow more than 11 listneers
        setMaxListeners(Infinity);

        const libs = (await LSession.disconnected(n)).peers;

        // Connect the nodes
        const connectPromises: Promise<any>[] = [];
        for (let i = 0; i < n - 1; i++) {
            for (let j = i + 1; j < n; j++) {
                await libs[i].peerStore.addressBook.set(
                    libs[j].peerId,
                    libs[j].getMultiaddrs()
                );
                connectPromises.push(libs[i].dial(libs[j].peerId));
            }
        }

        await Promise.all(connectPromises);
        const peers: Libp2p[] = [];
        for (let i = 0; i < libs.length; i++) {
            peers.push(libs[i]);
        }

        // Subscribe to initial topics
        if (pubsubTopics) {
            for (const topic of pubsubTopics) {
                for (const lib of libs) {
                    lib.pubsub.subscribe(topic);
                }
                for (let i = 0; i < n - 1; i++) {
                    for (let j = i + 1; j < n; j++) {
                        await waitForPeers(libs[i], libs[j], topic);
                    }
                }
            }
        }
        return new LSession(peers);
    }

    static async disconnected(n: number) {
        // Allow more than 11 listneers
        setMaxListeners(Infinity);

        // create nodes
        const promises: Promise<Libp2p>[] = [];
        for (let i = 0; i < n; i++) {
            const result = async () => {
                let msgCounter = 0;
                const node = await createLibp2p({
                    connectionManager: {
                        autoDial: false,
                    },
                    addresses: {
                        listen: ["/ip4/127.0.0.1/tcp/0"],
                    },
                    transports: [tcp()],
                    connectionEncryption: [noise()],
                    streamMuxers: [mplex()],
                    pubsub: gossipsub({
                        emitSelf: false,
                        globalSignaturePolicy: "StrictNoSign",
                    }),
                });
                await node.start();
                return node;
            };
            promises.push(result());
        }

        const libs = await Promise.all(promises);
        return new LSession(libs);
    }

    stop(): Promise<any> {
        return Promise.all(this.peers.map((p) => p.stop()));
    }
}
