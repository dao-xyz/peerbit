import { DirectBlock } from "../libp2p";
import { waitFor, delay } from "@dao-xyz/peerbit-time";

export const waitForPeers = async (...stores: DirectBlock[]) => {
    for (let i = 0; i < stores.length; i++) {
        await waitFor(() => {
            for (let j = 0; j < stores.length; j++) {
                if (i === j) {
                    continue;
                }
                if (!stores[i].peers.has(stores[j].publicKeyHash)) {
                    return false;
                }
            }
            return true;
        });
        await waitFor(() => {
            const peers = stores[i].peers;
            for (const peer of peers.values()) {
                if (!peer.isReadable || !peer.isWritable) {
                    return false;
                }
            }
            return true;
        });
    }
};
