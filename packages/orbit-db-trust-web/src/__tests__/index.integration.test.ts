import { getConnectedPeers, disconnectPeers } from '@dao-xyz/peer-test-utils'
import { PublicKey } from '@dao-xyz/identity';
import { TrustWebAccessController } from '..';
import { waitFor } from '@dao-xyz/time';

describe('isTrusted', () => {

    it('trusted by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);
        const l0a = new TrustWebAccessController({
            rootTrust: PublicKey.from(peer.orbitDB.identity)
        });
        await l0a.init(peer.orbitDB._ipfs, peer.orbitDB.identity, { replicate: true });

        let peer2Key = peer2.orbitDB.identity;
        await l0a.addTrust(peer2Key);

        let l0b: TrustWebAccessController = await TrustWebAccessController.load(peer2.node, l0a.address) as any
        await l0b.init(peer2.orbitDB._ipfs, peer2.orbitDB.identity, { replicate: true });

        await waitFor(() => Object.keys(l0b.store._index).length == 1)

        let peer3Key = peer3.orbitDB.identity;
        await l0b.addTrust(peer3Key);

        // now check if peer3 is trusted from peer perspective
        await waitFor(() => Object.keys(l0a.store._index).length == 2)
        expect(l0a.isTrusted(peer3Key));
        await disconnectPeers([peer, peer2, peer3]);

    })

    it('untrusteed by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);

        let l0a = new TrustWebAccessController({
            rootTrust: peer.orbitDB.identity
        });

        await l0a.init(peer.orbitDB._ipfs, peer.orbitDB.identity, { replicate: true });

        let l0b: TrustWebAccessController = await TrustWebAccessController.load(peer2.node, l0a.address) as any
        await l0b.init(peer2.orbitDB._ipfs, peer2.orbitDB.identity, { replicate: true });

        let peer3Key = peer3.orbitDB.identity

        // Can not append peer3Key since its not trusted by the root
        await expect(l0b.addTrust(peer3Key)).rejects.toBeInstanceOf(Error);
        await disconnectPeers([peer, peer2, peer3]);

    })
}) 