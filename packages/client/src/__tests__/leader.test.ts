
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
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
    waitForPeers,
    Session,
} from '@dao-xyz/orbit-db-test-utils'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network'
import { delay, waitFor } from '@dao-xyz/time'

const orbitdbPath1 = './orbitdb/tests/leader/1'
const orbitdbPath2 = './orbitdb/tests/leader/2'
const orbitdbPath3 = './orbitdb/tests/leader/3'

const dbPath1 = './orbitdb/tests/leader/1/db1'
const dbPath2 = './orbitdb/tests/leader/2/db2'
const dbPath3 = './orbitdb/tests/leader/3/db3'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - leaders`, function () {
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

        it('will use trusted network for filtering', async () => {
            const network = await orbitdb1.openNetwork(new TrustedNetwork({ name: 'network', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })
            await orbitdb1.joinNetwork(network);

            // make client 2 trusted
            await network.add(orbitdb2.id);
            await network.add(orbitdb2.identity.publicKey);
            await orbitdb2.openNetwork(network.address, { directory: dbPath2 });
            await waitFor(() => Object.keys((orbitdb2.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 3);
            await orbitdb2.joinNetwork(network);

            // but dont trust client 3
            // however open direct channels so client 3 could perhaps be a leader anyway (?)
            orbitdb1.getChannel(orbitdb3.id.toString(), network.address.toString());
            orbitdb3.getChannel(orbitdb1.id.toString(), network.address.toString());
            await waitFor(() => orbitdb1._directConnections.size === 2); // to 2 and 3
            await waitFor(() => orbitdb2._directConnections.size === 1); // to 1
            await waitFor(() => orbitdb3._directConnections.size === 1); // to 1
            await waitFor(() => Object.keys((orbitdb2.getNetwork(network.address) as TrustedNetwork).trustGraph._index._index).length === 4) // 1. identiy -> peer id, 1. -> 2 identity, 1. -> 2. peer id and 2. identity -> peer id, 

            // now find 3 leaders from the network with 2 trusted participants (should return 2 leaders if trust control works correctly)
            const leadersFrom1 = await orbitdb1.findLeaders(network.address.toString(), true, "", 3);
            const leadersFrom2 = await orbitdb2.findLeaders(network.address.toString(), true, "", 3);
            expect(leadersFrom1).toEqual(leadersFrom2);
            expect(leadersFrom1).toHaveLength(2);
            expect(leadersFrom1).toContainAllValues([orbitdb1.id.toString(), orbitdb2.id.toString()]);
        })

        it('select leaders for one or two peers', async () => {

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api



            const replicationTopic = uuid();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() }), replicationTopic
                , { directory: dbPath1 })

            const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, 123, 1));
            expect(isLeaderAOneLeader);
            const isLeaderATwoLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, 123, 2));
            expect(isLeaderATwoLeader);

            db2 = await orbitdb2.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath2 })

            await waitForPeers(session.peers[0].ipfs, [orbitdb2.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))
            await waitForPeers(session.peers[1].ipfs, [orbitdb1.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))

            // leader rotation is kind of random, so we do a sequence of tests
            for (let slot = 0; slot < 3; slot++) {
                // One leader
                const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 1));
                const isLeaderBOneLeader = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 1));
                expect([isLeaderAOneLeader, isLeaderBOneLeader]).toContainAllValues([false, true])

                // Two leaders
                const isLeaderATwoLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 2));
                const isLeaderBTwoLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 2));
                expect([isLeaderATwoLeaders, isLeaderBTwoLeaders]).toContainAllValues([true, true])
            }
        })

        it('leader are selected from 1 replicating peer', async () => {


            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api



            const replicationTopic = uuid()
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() }), replicationTopic
                , { replicate: false, directory: dbPath1 })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath2 })

            // One leader
            const slot = 0;


            // Two leaders, but only one will be leader since only one is replicating
            const isLeaderA = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 2));
            const isLeaderB = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 2));
            expect(!isLeaderA) // because replicate is false
            expect(isLeaderB)


        })

        it('leader are selected from 2 replicating peers', async () => {

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api



            const replicationTopic = uuid();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() }), replicationTopic
                , { replicate: false, directory: dbPath1 })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath2 })
            db3 = await orbitdb3.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath3 })

            await waitForPeers(session.peers[1].ipfs, [orbitdb3.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))
            await waitForPeers(session.peers[2].ipfs, [orbitdb2.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))

            expect(orbitdb1._directConnections.size).toEqual(0);
            // One leader
            const slot = 0;


            // Two leaders, but only one will be leader since only one is replicating
            const isLeaderA = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 3));
            const isLeaderB = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 3));
            const isLeaderC = orbitdb3.isLeader(await orbitdb3.findLeaders(replicationTopic, true, slot, 3));

            expect(!isLeaderA) // because replicate is false
            expect(isLeaderB)
            expect(isLeaderC)


        })


        it('select leaders for three peers', async () => {



            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api



            const replicationTopic = uuid();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() }), replicationTopic
                , { directory: dbPath1 })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath2 })
            db3 = await orbitdb3.open<EventStore<string>>(db1.address, replicationTopic, { directory: dbPath3 })

            await waitForPeers(session.peers[0].ipfs, [orbitdb2.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))
            await waitForPeers(session.peers[2].ipfs, [orbitdb1.id], DirectChannel.getTopic([orbitdb1.id, orbitdb3.id]))
            await waitForPeers(session.peers[1].ipfs, [orbitdb3.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))

            // One leader
            const slot = 0;

            const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 1));
            const isLeaderBOneLeader = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 1));
            const isLeaderCOneLeader = orbitdb3.isLeader(await orbitdb3.findLeaders(replicationTopic, true, slot, 1));
            expect([isLeaderAOneLeader, isLeaderBOneLeader, isLeaderCOneLeader]).toContainValues([false, false, true])

            // Two leaders
            const isLeaderATwoLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 2));
            const isLeaderBTwoLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 2));
            const isLeaderCTwoLeaders = orbitdb3.isLeader(await orbitdb3.findLeaders(replicationTopic, true, slot, 2));
            expect([isLeaderATwoLeaders, isLeaderBTwoLeaders, isLeaderCTwoLeaders]).toContainValues([false, true, true])

            // Three leders
            const isLeaderAThreeLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(replicationTopic, true, slot, 3));
            const isLeaderBThreeLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(replicationTopic, true, slot, 3));
            const isLeaderCThreeLeaders = orbitdb3.isLeader(await orbitdb3.findLeaders(replicationTopic, true, slot, 3));
            expect([isLeaderAThreeLeaders, isLeaderBThreeLeaders, isLeaderCThreeLeaders]).toContainValues([true, true, true])
        })

    })
})
