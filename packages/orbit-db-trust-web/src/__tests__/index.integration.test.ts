import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { getConnectedPeers, disconnectPeers } from '@dao-xyz/peer-test-utils'
import { P2PTrust } from '..';

const defaultStoreOptions: IQueryStoreOptions<any, any> = {
    nameResolver: (n) => n,
    cache: undefined,
    create: true,
    typeMap: {},
    replicate: true,
    subscribeToQueries: true
};
describe('isTrusted', () => {

    test('trusted by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);
        const l0a = new P2PTrust({
            rootTrust: peer.orbitDB.identity
        });
        await l0a.init(peer.orbitDB, defaultStoreOptions);

        let peer2Key = peer2.orbitDB.identity;
        await l0a.addTrust(peer2Key);

        let l0b = await P2PTrust.loadFromCID(l0a.cid, peer2.node)
        await l0b.init(peer2.orbitDB, defaultStoreOptions);
        await l0b.load(1);

        let peer3Key = peer3.orbitDB.identity;
        await l0b.addTrust(peer3Key);

        // now check if peer3 is trusted from peer perspective
        await l0a.load(2);
        expect(l0a.isTrusted(peer3Key));
        await disconnectPeers([peer, peer2, peer3]);

    })

    test('untrusteed by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);

        let l0a = new P2PTrust({
            rootTrust: peer.orbitDB.identity
        });

        await l0a.init(peer.orbitDB, defaultStoreOptions);

        let l0b = await P2PTrust.loadFromCID(l0a.cid, peer2.node);
        await l0b.init(peer2.orbitDB, defaultStoreOptions);

        let peer3Key = peer3.orbitDB.identity

        // Can not append peer3Key since its not trusted by the root
        await expect(l0b.addTrust(peer3Key)).rejects.toBeInstanceOf(Error);
        await disconnectPeers([peer, peer2, peer3]);

    })
})