import { createLibp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { webSockets } from "@libp2p/websockets";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";

export const createNode = async () => {
    const node = await createLibp2p({
        connectionManager: {
            autoDial: false,
        },
        addresses: {
            listen: ["/ip4/127.0.0.1/tpc/tcp/8081/ws"],
        },
        transports: [webSockets()],
        connectionEncryption: [noise()],
        streamMuxers: [mplex()],
        pubsub: gossipsub(),
    });
    await node.start();
    return node;
};
