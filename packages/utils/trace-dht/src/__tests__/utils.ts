import { LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";


export const waitForPeers = async (...stores: Blocks[]) => {
    for (let i = 0; i < stores.length; i++) {
        await waitFor(() => (stores[i]._store as LibP2PBlockStore)._blockSub.peers.size === stores.length - 1);
        const peers = (stores[i]._store as LibP2PBlockStore)._blockSub.peers;
        for (const peer of peers.values()) {
            await waitFor(() => peer.isWritable)
        }
    }
}
