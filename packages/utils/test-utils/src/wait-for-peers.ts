import type { PeerId } from "@libp2p/interface-peer-id";
import { Libp2p } from "libp2p";
import { PubSub } from "@libp2p/interface-pubsub";
import { GossipsubEvents } from "@chainsafe/libp2p-gossipsub";
const waitForPeers = (
    libp2p: (Libp2p & { pubsub: PubSub<GossipsubEvents> }) | any,
    peersToWait: (PeerId | string | Libp2p)[] | PeerId | string | Libp2p,
    topic: string
) => {
    const peersToWaitArr = Array.isArray(peersToWait)
        ? peersToWait
        : [peersToWait];
    return new Promise<void>((resolve, reject) => {
        const interval = setInterval(async () => {
            try {
                const peers = !!(libp2p as Libp2p).pubsub.getSubscribers
                    ? (libp2p as Libp2p).pubsub.getSubscribers(topic)
                    : await (libp2p as any).pubsub.peers(topic);
                const peerIds = peers.map((peer) => peer.toString());
                const peerIdsToWait = peersToWaitArr.map((peer) =>
                    (peer as Libp2p).peerId
                        ? (peer as Libp2p).peerId.toString()
                        : peer.toString()
                );

                const hasAllPeers =
                    peerIdsToWait
                        .map((e) => peerIds.includes(e))
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
