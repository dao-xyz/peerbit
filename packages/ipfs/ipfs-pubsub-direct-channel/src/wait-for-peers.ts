import type { PeerId } from "@libp2p/interface-peer-id";
import { Libp2p } from "libp2p";

export const waitForPeers2 = (
    libp2p: Libp2p,
    peersToWait: (PeerId | string | Libp2p)[] | PeerId | string | Libp2p,
    topic: string,
    isClosed: () => boolean
) => {
    const peersToWaitArr = Array.isArray(peersToWait)
        ? peersToWait
        : [peersToWait];
    return new Promise<void>((resolve, reject) => {
        const interval = setInterval(async () => {
            try {
                const peers = libp2p.pubsub.getSubscribers(topic);
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

export const waitForPeers = async (
    libp2p: Libp2p,
    peersToWait: (PeerId | string | Libp2p)[] | PeerId | string | Libp2p,
    topic: string,
    isClosed: () => boolean
) => {
    const peersToWaitArr = Array.isArray(peersToWait)
        ? peersToWait
        : [peersToWait];
    const peerIdsToWaitMapped = peersToWaitArr.map((peer) =>
        (peer as Libp2p).peerId
            ? (peer as Libp2p).peerId.toString()
            : peer.toString()
    );

    const checkPeers = async () => {
        const peers = (await libp2p.pubsub.getSubscribers(topic)).toString();
        const hasAllPeers =
            peerIdsToWaitMapped
                .map((e) => peers.includes(e))
                .filter((e) => e === false).length === 0;
        return hasAllPeers;
    };

    if (await checkPeers()) {
        return Promise.resolve(false);
    }

    return new Promise<boolean>((resolve, reject) => {
        const interval = setInterval(async () => {
            try {
                if (isClosed()) {
                    clearInterval(interval);
                } else if (await checkPeers()) {
                    clearInterval(interval);
                    resolve(true);
                }
            } catch (e) {
                reject(e);
            }
        }, 100);
    });
};
