import { createLibp2p, Libp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";

export const createNode = async () => {
    const node = await createLibp2p({
        connectionManager: {
            autoDial: false,
        },
        addresses: {
            listen: ["/ip4/127.0.0.1/tcp/8001", "/ip4/127.0.0.1/tcp/8002/ws"],
        },
        transports: [tcp(), webSockets()],
        connectionEncryption: [noise()],
        streamMuxers: [mplex()],
        pubsub: gossipsub(),
    });
    await node.start();
    return node;
};
