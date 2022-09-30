
import assert from 'assert'
const mapSeries = require('p-each-series')
import rmrf from 'rimraf'

import { DirectChannel } from '@dao-xyz/ipfs-pubsub-direct-channel'
import { delay, waitFor, waitForAsync } from '@dao-xyz/time'

import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore, Operation } from './utils/stores/event-store'

// Include test utilities
const {
    config,
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
    waitForPeers,
} = require('@dao-xyz/orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/leader/1'
const orbitdbPath2 = './orbitdb/tests/leader/2'
const orbitdbPath3 = './orbitdb/tests/leader/3'

const dbPath1 = './orbitdb/tests/leader/1/db1'
const dbPath2 = './orbitdb/tests/leader/2/db2'
const dbPath3 = './orbitdb/tests/leader/3/db3'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - leaders`, function () {
        jest.setTimeout(config.timeout * 2)

        let ipfsd1, ipfsd2, ipfsd3, ipfs1, ipfs2, ipfs3
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>


        beforeAll(async () => {
            ipfsd1 = await startIpfs(API, config.daemon1)
            ipfsd2 = await startIpfs(API, config.daemon2)
            ipfsd3 = await startIpfs(API, config.daemon2)

            ipfs1 = ipfsd1.api
            ipfs2 = ipfsd2.api
            ipfs3 = ipfsd3.api

            // Connect the peers manually to speed up test times
            const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
            console.log("Peers connected")
        })

        afterAll(async () => {
            if (ipfsd1)
                await stopIpfs(ipfsd1)

            if (ipfsd2)
                await stopIpfs(ipfsd2)

            if (ipfsd3)
                await stopIpfs(ipfsd3)
        })

        beforeEach(async () => {

            rmrf.sync(orbitdbPath1)
            rmrf.sync(orbitdbPath2)
            rmrf.sync(orbitdbPath3)

            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)

            orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: orbitdbPath1 })
            orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2 })
            orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: orbitdbPath3 })


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


        it('select leaders for one or two peers', async () => {

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api
            console.log("Waiting for peers to connect")

            const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })

            const replicationTopicFn = () => 'x';
            const replicationTopic = replicationTopicFn();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
                , { directory: dbPath1, replicationTopic })

            const waitForPeersTime = 3000;
            const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, 123, 1));
            expect(isLeaderAOneLeader);
            const isLeaderATwoLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, 123, 2));
            expect(isLeaderATwoLeader);

            db2 = await orbitdb2.open<EventStore<string>>(db1.address, { directory: dbPath2, replicationTopic: replicationTopicFn })

            await waitForPeers(ipfs1, [orbitdb2.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))
            await waitForPeers(ipfs2, [orbitdb1.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))

            // leader rotation is kind of random, so we do a sequence of tests
            for (let slot = 0; slot < 3; slot++) {
                // One leader
                const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 1));
                const isLeaderBOneLeader = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 1));
                expect([isLeaderAOneLeader, isLeaderBOneLeader]).toContainAllValues([false, true])

                // Two leaders
                const isLeaderATwoLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 2));
                const isLeaderBTwoLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 2));
                expect([isLeaderATwoLeaders, isLeaderBTwoLeaders]).toContainAllValues([true, true])
            }
        })

        it('leader are selected from 1 replicating peer', async () => {

            const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api
            console.log("Waiting for peers to connect")


            const replicationTopicFn = () => 'x';
            const replicationTopic = replicationTopicFn();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
                , { replicate: false, directory: dbPath1, replicationTopic })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, { directory: dbPath2, replicationTopic: replicationTopicFn })

            // One leader
            const slot = 0;


            // Two leaders, but only one will be leader since only one is replicating
            const isLeaderA = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 2));
            const isLeaderB = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 2));
            expect(!isLeaderA) // because replicate is false
            expect(isLeaderB)


        })

        it('leader are selected from 2 replicating peers', async () => {

            const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
            await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api
            console.log("Waiting for peers to connect")


            const replicationTopicFn = () => 'x';
            const replicationTopic = replicationTopicFn();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
                , { replicate: false, directory: dbPath1, replicationTopic })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, { directory: dbPath2, replicationTopic: replicationTopicFn })
            db3 = await orbitdb3.open<EventStore<string>>(db1.address, { directory: dbPath3, replicationTopic: replicationTopicFn })

            await waitForPeers(ipfs2, [orbitdb3.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))
            await waitForPeers(ipfs3, [orbitdb2.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))

            expect(orbitdb1._directConnections.size).toEqual(0);
            // One leader
            const slot = 0;


            // Two leaders, but only one will be leader since only one is replicating
            const isLeaderA = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 3));
            const isLeaderB = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 3));
            const isLeaderC = orbitdb3.isLeader(await orbitdb3.findLeaders(db1.replicationTopic, true, slot, 3));

            expect(!isLeaderA) // because replicate is false
            expect(isLeaderB)
            expect(isLeaderC)


        })


        it('select leaders for three peers', async () => {

            const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
            await connectPeers(ipfs1, ipfs3, { filter: isLocalhostAddress })
            await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api
            console.log("Waiting for peers to connect")


            const replicationTopicFn = () => 'x';
            const replicationTopic = replicationTopicFn();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
                , { directory: dbPath1, replicationTopic })
            db2 = await orbitdb2.open<EventStore<string>>(db1.address, { directory: dbPath2, replicationTopic: replicationTopicFn })
            db3 = await orbitdb3.open<EventStore<string>>(db1.address, { directory: dbPath3, replicationTopic: replicationTopicFn })

            await waitForPeers(ipfs1, [orbitdb2.id], DirectChannel.getTopic([orbitdb1.id, orbitdb2.id]))
            await waitForPeers(ipfs3, [orbitdb1.id], DirectChannel.getTopic([orbitdb1.id, orbitdb3.id]))
            await waitForPeers(ipfs2, [orbitdb3.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))

            // One leader
            const slot = 0;

            const isLeaderAOneLeader = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 1));
            const isLeaderBOneLeader = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 1));
            const isLeaderCOneLeader = orbitdb3.isLeader(await orbitdb3.findLeaders(db1.replicationTopic, true, slot, 1));
            expect([isLeaderAOneLeader, isLeaderBOneLeader, isLeaderCOneLeader]).toContainValues([false, false, true])

            // Two leaders
            const isLeaderATwoLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 2));
            const isLeaderBTwoLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 2));
            const isLeaderCTwoLeaders = orbitdb3.isLeader(await orbitdb3.findLeaders(db1.replicationTopic, true, slot, 2));
            expect([isLeaderATwoLeaders, isLeaderBTwoLeaders, isLeaderCTwoLeaders]).toContainValues([false, true, true])

            // Three leders
            const isLeaderAThreeLeaders = orbitdb1.isLeader(await orbitdb1.findLeaders(db1.replicationTopic, true, slot, 3));
            const isLeaderBThreeLeaders = orbitdb2.isLeader(await orbitdb2.findLeaders(db1.replicationTopic, true, slot, 3));
            const isLeaderCThreeLeaders = orbitdb3.isLeader(await orbitdb3.findLeaders(db1.replicationTopic, true, slot, 3));
            expect([isLeaderAThreeLeaders, isLeaderBThreeLeaders, isLeaderCThreeLeaders]).toContainValues([true, true, true])
        })

    })
})
