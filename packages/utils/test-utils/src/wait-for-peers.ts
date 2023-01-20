import type { PeerId } from "@libp2p/interface-peer-id";
import { Libp2p } from "libp2p";
import { Libp2pExtended } from "./session";
import { getPublicKeyFromPeerId } from "@dao-xyz/peerbit-crypto";
const waitForPeers = async (
	libp2p: Libp2pExtended,
	peersToWait: (PeerId | Libp2p)[] | PeerId | Libp2p,
	topic: string
) => {
	const peersToWaitArr = Array.isArray(peersToWait)
		? peersToWait
		: [peersToWait];

	const peerIdsToWait = peersToWaitArr.map((peer) =>
		(peer as Libp2p).peerId
			? getPublicKeyFromPeerId((peer as Libp2p).peerId).hashcode()
			: getPublicKeyFromPeerId(peer as PeerId).hashcode()
	);

	await libp2p.directsub.requestSubscribers(topic);
	return new Promise<void>((resolve, reject) => {
		let counter = 0;
		const interval = setInterval(async () => {
			counter += 1;
			if (counter > 100) {
				clearInterval(interval);
				reject()
			}
			try {
				const peers = libp2p.directsub.getSubscribers(topic);
				const hasAllPeers =
					peerIdsToWait
						.map((e) => peers && peers.has(e))
						.filter((e) => e === false).length === 0;

				// FIXME: Does not fail on timeout, not easily fixable
				if (hasAllPeers) {
					clearInterval(interval);
					resolve();
				}
			} catch (e) {
				clearInterval(interval);
				reject(e);
			}
		}, 200);
	});
};

export default waitForPeers;
