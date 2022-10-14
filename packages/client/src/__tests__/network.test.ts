//
import rmrf from 'rimraf'

import { DirectChannel } from '@dao-xyz/ipfs-pubsub-direct-channel'
import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore } from './utils/stores/event-store'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// @ts-ignore 
import { v4 as uuid } from 'uuid';

// Include test utilities
import {
    nodeConfig as config,
    testAPIs,
    Session
} from '@dao-xyz/orbit-db-test-utils'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network'
import { delay, waitFor } from '@dao-xyz/time'
import { AccessError, Ed25519Keypair } from '@dao-xyz/peerbit-crypto'

const orbitdbPath1 = './orbitdb/tests/leader/1'
const orbitdbPath2 = './orbitdb/tests/leader/2'
const orbitdbPath3 = './orbitdb/tests/leader/3'

const dbPath1 = './orbitdb/tests/leader/1/db1'
const dbPath2 = './orbitdb/tests/leader/2/db2'
const dbPath3 = './orbitdb/tests/leader/3/db3'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - network`, function () {

        jest.setTimeout(config.timeout * 2)

        let session: Session;
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>


        beforeAll(async () => {
            session = await Session.connected(3);
        })

        afterAll(async () => {
            await session.stop();
        })

        beforeEach(async () => {

            rmrf.sync(orbitdbPath1)
            rmrf.sync(orbitdbPath2)
            rmrf.sync(orbitdbPath3)

            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)

            orbitdb1 = await OrbitDB.createInstance(session.peers[0].ipfs, { directory: orbitdbPath1 })
            orbitdb2 = await OrbitDB.createInstance(session.peers[1].ipfs, { directory: orbitdbPath2 })
            orbitdb3 = await OrbitDB.createInstance(session.peers[2].ipfs, { directory: orbitdbPath3 })


        })

        afterEach(async () => {

            if (db1)
                await db1.drop()

            if (db2)
                await db2.drop()

            if (db3)
                await db3.drop()

            if (orbitdb1)
                await orbitdb1.stop()

            if (orbitdb2)
                await orbitdb2.stop()

            if (orbitdb3)
                await orbitdb3.stop()
        })


        it('will not recieved heads if not trusted', async () => {
            const network = await orbitdb1.openNetwork(new TrustedNetwork({ name: 'network-tests', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })
            await orbitdb1.joinNetwork(network);

            // trust client 3
            await network.add(orbitdb3.id) // we have to trust peer because else other party will not exchange heads
            await network.add(orbitdb3.identity.publicKey) // will have to trust identity because else this can t add more idenetities

            // but only partially trust client 2
            await network.add(orbitdb2.identity.publicKey) // omitt adding trust to orbitdb2 peer id, so we can test that it does not recieve heads
            await waitFor(() => Object.keys((orbitdb1.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 4)

            await orbitdb2.openNetwork(network.address, { directory: dbPath2 })
            await orbitdb3.openNetwork(network.address, { directory: dbPath3 })
            await waitFor(() => orbitdb1._directConnections.size === 2);

            await waitFor(() => Object.keys((orbitdb3.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 4)

            expect(Object.keys((orbitdb2.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index)).toHaveLength(0); // because peer id is not trusted so it will not recieve heads
            await orbitdb3.joinNetwork(network); // will add relation form client 3 to peer id 3 (it also exist another relation from client 1 to peer id 3 btw, but these are not the same)


            expect(() => orbitdb2.joinNetwork(network)).rejects.toThrow(AccessError)


            // Do two additional writes from trusted client 1 and 3
            await (orbitdb1.getNetwork(network.address) as TrustedNetwork).add((await Ed25519Keypair.create()).publicKey)
            await (orbitdb3.getNetwork(network.address) as TrustedNetwork).add((await Ed25519Keypair.create()).publicKey)

            await waitFor(() => Object.keys((orbitdb1.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 7)
            await waitFor(() => Object.keys((orbitdb3.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 7)

            await delay(2000); // arb. delay
            expect(Object.keys((orbitdb2.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index)).toHaveLength(0); // because peer id is not trusted so it will not recieve heads

        })

        it('it will try to connect to new peers', async () => {
            const network = await orbitdb1.openNetwork(new TrustedNetwork({ name: 'connect', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })

        })
    })
})
