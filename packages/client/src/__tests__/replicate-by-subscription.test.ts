
import rmrf from 'rimraf'
import { OrbitDB } from '../orbit-db'
import { EventStore } from './utils/stores/event-store'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// Include test utilities
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
    waitForPeers,
} from '@dao-xyz/orbit-db-test-utils'

const orbitdbPath1 = './orbitdb/tests/replication/1'
const orbitdbPath2 = './orbitdb/tests/replication/2'
const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'

Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 2)

        let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db: EventStore<string>


        beforeAll(async () => {
            ipfsd1 = await startIpfs(API, config.daemon1)
            ipfsd2 = await startIpfs(API, config.daemon2)
            ipfs1 = ipfsd1.api
            ipfs2 = ipfsd2.api
            // Connect the peers manually to speed up test times
            const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
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

            if (db)
                await db.drop()

            if (orbitdb1)
                await orbitdb1.stop()

            if (orbitdb2)
                await orbitdb2.stop()
        })



        /*  it('replicate by request no heads', async () => {
 
             const waitForPeersTime = 1000;
             const replicationTopic = 'x';
             const store = new EventStore<string>({ name: 'replication-tests' });
             await orbitdb2.subscribeForReplicationStart(replicationTopic);
 
             await orbitdb1.requestReplication(store, { replicationTopic, waitForPeersTime });
 
             const replicatedStore = Object.values(orbitdb2.stores[replicationTopic])[0];
             expect(replicatedStore).toBeDefined();
 
             await waitForPeers(ipfs1, [orbitdb2.id], replicationTopic)
             const ls2 = await orbitdb2._ipfs.pubsub.ls();
             expect(ls2).toContain(replicationTopic)
             expect(ls2).toHaveLength(2)
             const peersFrom1 = await orbitdb1.getPeers(replicationTopic, replicatedStore.address, { waitForPeersTime });
             expect(peersFrom1).toHaveLength(1);
             expect(peersFrom1[0].peerInfo.memoryLeft).toBeDefined();
 
         }) */




    })
})
