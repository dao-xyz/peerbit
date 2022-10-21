//
import rmrf from 'rimraf'

import { OrbitDB } from '../orbit-db'

import { EventStore } from './utils/stores/event-store'
import { jest } from '@jest/globals';


// Include test utilities
import {
    nodeConfig as config,
    testAPIs,
    Session,
    connectPeers
} from '@dao-xyz/peerbit-test-utils'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network'
import { waitFor } from '@dao-xyz/time'

const orbitdbPath1 = './orbitdb/tests/discovery/1'
const orbitdbPath2 = './orbitdb/tests/discovery/2'
const orbitdbPath3 = './orbitdb/tests/discovery/3'

const dbPath1 = './orbitdb/tests/discovery/1/db1'
const dbPath2 = './orbitdb/tests/discovery/2/db2'
const dbPath3 = './orbitdb/tests/discovery/3/db3'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - discovery`, function () {

        jest.setTimeout(config.timeout * 2)

        let session1: Session, session2: Session;
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB


        beforeAll(async () => {
            session1 = await Session.connected(2);
            session2 = await Session.connected(1);
        })

        afterAll(async () => {
            await session1.stop();
            await session2.stop();
        })

        beforeEach(async () => {

            rmrf.sync(orbitdbPath1)
            rmrf.sync(orbitdbPath2)
            rmrf.sync(orbitdbPath3)

            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)

            orbitdb1 = await OrbitDB.createInstance(session1.peers[0].ipfs, { directory: orbitdbPath1, localNetwork: true })
            orbitdb2 = await OrbitDB.createInstance(session1.peers[1].ipfs, { directory: orbitdbPath2, localNetwork: true })
            orbitdb3 = await OrbitDB.createInstance(session2.peers[0].ipfs, { directory: orbitdbPath3, localNetwork: true })


        })

        afterEach(async () => {


            if (orbitdb1)
                await orbitdb1.stop()

            if (orbitdb2)
                await orbitdb2.stop()

            if (orbitdb3)
                await orbitdb3.stop()
        })


        it('will connect to network with swarm exchange', async () => {
            const network = await orbitdb1.openNetwork(new TrustedNetwork({ name: 'network-tests', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })
            await orbitdb1.joinNetwork(network);

            // trust client 2
            await network.add(orbitdb2.id) // we have to trust peer because else other party will not exchange heads
            await network.add(orbitdb2.identity.publicKey) // will have to trust identity because else this can t add more idenetities

            // trust client 3
            await network.add(orbitdb3.id) // we have to trust peer because else other party will not exchange heads
            await network.add(orbitdb3.identity.publicKey) // will have to trust identity because else this can t add more idenetities
            await waitFor(() => Object.keys((orbitdb1.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 5)

            await orbitdb2.openNetwork(network.address, { directory: dbPath2 })

            // Connect client 1 with 3, but try to connect 2 to 3 by swarm messages
            await connectPeers(session1.peers[0].ipfs, session2.peers[0].ipfs)
            await orbitdb3.openNetwork(network.address, { directory: dbPath3 })
            await waitFor(() => orbitdb3._directConnections.size === 2);
            expect(orbitdb3._directConnections.has(orbitdb1.id.toString())).toBeTrue()
            expect(orbitdb3._directConnections.has(orbitdb2.id.toString())).toBeTrue()

        })
    })
})
