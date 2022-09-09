import { getConnectedPeers, disconnectPeers } from '@dao-xyz/peer-test-utils'
import { PublicKey } from '@dao-xyz/identity';
import { TrustWebAccessController } from '..';
import { waitFor } from '@dao-xyz/time';
import { AccessError } from '@dao-xyz/encryption-utils';

describe('isTrusted', () => {

    it('trusted by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);
        const l0a = new TrustWebAccessController({
            rootTrust: PublicKey.from(peer.orbitDB.identity)
        });

        await peer.orbitDB.open(l0a);

        let peer2Key = peer2.orbitDB.identity;
        await l0a.addTrust(peer2Key);

        let l0b: TrustWebAccessController = await TrustWebAccessController.load(peer2.node, l0a.address) as any
        await peer2.orbitDB.open(l0b);

        await waitFor(() => Object.keys(l0b.store._index._index).length == 1)

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

        await peer.orbitDB.open(l0a);

        let l0b: TrustWebAccessController = await TrustWebAccessController.load(peer2.node, l0a.address) as any
        await peer2.orbitDB.open(l0b);

        let peer3Key = peer3.orbitDB.identity

        // Can not append peer3Key since its not trusted by the root
        await expect(l0b.addTrust(peer3Key)).rejects.toBeInstanceOf(AccessError);
        await disconnectPeers([peer, peer2, peer3]);

    })
}) 