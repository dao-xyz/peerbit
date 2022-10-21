
import { Session } from '../session.js'
import waitForPeers from '../wait-for-peers.js';
import { delay } from '@dao-xyz/peerbit-time';
describe(`Session`, function () {
    let session: Session;
    beforeEach(async () => {
        session = await Session.connected(3, 'go-ipfs');
    })
    it('starts and stops two connected nodes', async () => {
        expect(session.peers).toHaveLength(3);
        for (const peer of session.peers) {
            expect(peer.id).toBeDefined();
            expect(peer.ipfs).toBeDefined();
            expect(peer.ipfsd).toBeDefined();
            expect((await peer.ipfsd.api.swarm.peers()).length).toEqual(2)
        }

        await session.peers[0].ipfs.pubsub.subscribe('x', (message) => {

            const x = 123;
        })
        await waitForPeers(session.peers[1].ipfs, [session.peers[0].id], 'x')
        await session.peers[1].ipfs.pubsub.publish('x', new Uint8Array([1, 2, 3]));
        const x = await session.peers[1].ipfs.pubsub.peers('x');
        await delay(10000);
        const y = 123;
    })

    afterEach(async () => {
        await session.stop()
    })
})
