import { LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";
import { waitFor, delay } from "@dao-xyz/peerbit-time";


export const waitForPeers = async (...stores: Blocks[]) => {
	for (let i = 0; i < stores.length; i++) {
		await waitFor(() => {

			for (let j = 0; j < stores.length; j++) {
				if (i === j) {
					continue;
				}
				if (!(stores[i]._store as LibP2PBlockStore)._blockSub.peers.has((stores[j]._store as LibP2PBlockStore)._blockSub.libp2p.peerId)) {
					return false;
				}
			}
			return true;
		});
		const peers = (stores[i]._store as LibP2PBlockStore)._blockSub.peers;
		for (const peer of peers.values()) {
			await waitFor(() => peer.isReadable && peer.isWritable)
		}
	}
}
