import { waitFor, delay } from "@dao-xyz/peerbit-time";
import { DirectStream } from "..";

export const waitForPeers = async (...libs: DirectStream<any>[]) => {
	for (let i = 0; i < libs.length; i++) {
		await waitFor(() => {

			for (let j = 0; j < libs.length; j++) {
				if (i === j) {
					continue;
				}
				if (!libs[i].peers.has(libs[j].libp2p.peerId)) {
					return false;
				}
			}
			return true;
		});
		const peers = libs[i].peers;
		for (const peer of peers.values()) {
			await waitFor(() => peer.isReadable && peer.isWritable)
		}
	}
}
