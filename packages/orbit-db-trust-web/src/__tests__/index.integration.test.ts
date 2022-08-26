import { getConnectedPeers, disconnectPeers } from '@dao-xyz/peer-test-utils'
import { P2PTrust } from '..';
import { PublicKey } from '@dao-xyz/identity';

const defaultStoreOptions = (l: P2PTrust, isServer: boolean = true, directory: string = undefined) => {
    return l.getStoreOptions({ replicate: isServer, directory })
}

describe('isTrusted', () => {

    it('trusted by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);
        const l0a = new P2PTrust({
            rootTrust: PublicKey.from(peer.orbitDB.identity)
        });
        await l0a.init(peer.orbitDB, defaultStoreOptions(l0a));

        let peer2Key = peer2.orbitDB.identity;
        await l0a.addTrust(peer2Key);

        let l0b = await P2PTrust.loadFromCID(l0a.cid, peer2.node)
        await l0b.init(peer2.orbitDB, defaultStoreOptions(l0b));
        await l0b.load(1);

        let peer3Key = peer3.orbitDB.identity;
        await l0b.addTrust(peer3Key);

        // now check if peer3 is trusted from peer perspective
        await l0a.load(2);
        expect(l0a.isTrusted(peer3Key));
        await disconnectPeers([peer, peer2, peer3]);

    })

    it('untrusteed by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);

        let l0a = new P2PTrust({
            rootTrust: peer.orbitDB.identity
        });

        await l0a.init(peer.orbitDB, defaultStoreOptions(l0a));

        let l0b = await P2PTrust.loadFromCID(l0a.cid, peer2.node);
        await l0b.init(peer2.orbitDB, defaultStoreOptions(l0b));

        let peer3Key = peer3.orbitDB.identity

        // Can not append peer3Key since its not trusted by the root
        await expect(l0b.addTrust(peer3Key)).rejects.toBeInstanceOf(Error);
        await disconnectPeers([peer, peer2, peer3]);

    })
})