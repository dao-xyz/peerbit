
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { delay, waitFor, waitForAsync } from '@dao-xyz/time'
import { EMIT_HEALTHCHECK_INTERVAL } from '../exchange-replication'

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
} = require('orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/replication/1'
const orbitdbPath2 = './orbitdb/tests/replication/2'
const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 2)

        let ipfsd1, ipfsd2, ipfs1, ipfs2
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>


        beforeAll(async () => {
            ipfsd1 = await startIpfs(API, config.daemon1)
            ipfsd2 = await startIpfs(API, config.daemon2)
            ipfs1 = ipfsd1.api
            ipfs2 = ipfsd2.api
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
        })

        beforeEach(async () => {

            rmrf.sync(orbitdbPath1)
            rmrf.sync(orbitdbPath2)
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)

            orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: orbitdbPath1 })
            orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2 })


        })

        afterEach(async () => {

            if (db1)
                await db1.drop()

            if (db2)
                await db2.drop()

            if (orbitdb1)
                await orbitdb1.stop()

            if (orbitdb2)
                await orbitdb2.stop()
        })


        it('one leader for two peers', async () => {

            // TODO fix test timeout, isLeader is too slow as we need to wait for peers
            // perhaps do an event based get peers using the pubsub peers api
            console.log("Waiting for peers to connect")


            const replicationTopicFn = () => 'x';
            const replicationTopic = replicationTopicFn();
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
                , { directory: dbPath1, replicationTopic })


            const isLeaderA = await orbitdb1.isLeader(db1, 123);
            expect(isLeaderA); // since only 1 peer

            const options = { directory: dbPath2, sync: true }
            db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options, replicationTopic: replicationTopicFn })

            await waitForAsync(async () => (await orbitdb1._ipfs.pubsub.ls()).length == 2)
            expect(await orbitdb1._ipfs.pubsub.ls()).toContain(replicationTopic)
            const ls2 = await orbitdb2._ipfs.pubsub.ls();
            expect(ls2).toContain(replicationTopic)
            expect(ls2).toHaveLength(2)

            await delay(EMIT_HEALTHCHECK_INTERVAL);

            // leader rotation is kind of random, so we do a sequence of tests
            for (let slot = 0; slot < 3; slot++) {
                const isLeaderA = await orbitdb1.isLeader(db1, slot);
                const isLeaderB = await orbitdb2.isLeader(db1, slot);
                expect(typeof isLeaderA).toEqual('boolean');
                expect(typeof isLeaderB).toEqual('boolean');
                expect(isLeaderA).toEqual(!isLeaderB);
            }

        })


    })
})
