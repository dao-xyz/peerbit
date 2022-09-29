
import { Entry } from "@dao-xyz/ipfs-log-entry"
import { delay, waitFor } from "@dao-xyz/time"
import { OrbitDB } from "../orbit-db"
import { SimpleAccessController } from "./utils/access"
import { EventStore, Operation } from "./utils/stores/event-store"
import { KeyValueStore } from "./utils/stores/key-value-store"
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')

// Include test utilities
const {
    config,
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
} = require('orbit-db-test-utils')

const dbPath1 = './orbitdb/tests/sharding/1'
const dbPath2 = './orbitdb/tests/sharding/2'
const dbPath3 = './orbitdb/tests/sharding/3'
const v8 = require('v8');
Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - Automatic Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 3)

        let ipfsd1, ipfsd2, ipfsd3, ipfs1, ipfs2, ipfs3
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>

        beforeEach(async () => {
            rmrf.sync('./orbitdb')
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            ipfsd1 = await startIpfs(API, config.daemon1)
            ipfsd2 = await startIpfs(API, config.daemon2)
            ipfsd3 = await startIpfs(API, config.daemon2)

            ipfs1 = ipfsd1.api
            ipfs2 = ipfsd2.api
            orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: dbPath1 })
            orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: dbPath2 })

            let options: any = {}
            // Set write access for both clients


            options = Object.assign({}, options)
            db1 = await orbitdb1.open(new EventStore<string>({ name: 'replicate-automatically-tests', accessController: new SimpleAccessController() })
                , options)
        })

        afterEach(async () => {
            if (orbitdb1) {
                await orbitdb1.stop()
            }

            if (orbitdb2) {
                await orbitdb2.stop()
            }
            if (orbitdb3) {
                await orbitdb3.stop()
            }
            if (ipfsd1) {
                await stopIpfs(ipfsd1)
            }

            if (ipfs2) {
                await stopIpfs(ipfsd2)
            }
            if (ipfs3) {
                await stopIpfs(ipfs3)
            }
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)

        })

        /*     it('can control forking behaviour with `allowForks`', async () => {
    
                const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
                await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
                console.log('Peers connected')
                const entryCount = 2
    
                // Create the entries in the first database
                let prev: Entry<any> = undefined;
                for (let i = 0; i < entryCount; i++) {
                    prev = await db1.add('hello' + i, { refs: prev ? [prev.hash] : undefined });
    
                }
    
                // Open the second database
                db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {})
                await waitFor(() => db2.oplog.values.length === entryCount);
    
                db2.allowForks = false; // Only allow "changes"
    
                const _forkEntry = await db1.add('fork entry', { refs: [] }); // to reject since it is not referencing any prior logs
                const lastEntry = await db1.add('chained entry', { refs: [prev.hash] });
                await waitFor(() => db2.oplog.values.length > entryCount);
                expect(db2.oplog.values.length).toEqual(3);
                expect(db2.oplog.values[db2.oplog.values.length - 1].hash).toEqual(lastEntry.hash);
            }) */

        /*   it('will reject forks when reaching memory limit', async () => {
              const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
              await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
              console.log('Peers connected')
  
  
  
              // Create the entries in the first database
              let prev: Entry<any> = undefined;
              const entryCount = 2
  
  
              // Open the second database and set a heap size limit and assume this heap size limit is set in the opened store
              // Now check whether this heap size limit makes `allowForks` false when we start to write alot of data
              const heapsizeLimitForForks = 30000 + v8.getHeapStatistics().used_heap_size;
              orbitdb3 = await OrbitDB.createInstance(ipfs2, { directory: dbPath3, heapsizeLimitForForks })
              db2 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), {})
              expect(db2.options.resourceOptions.heapSizeLimit()).toEqual(heapsizeLimitForForks);
              let i = 0;
              expect(db2.allowForks);
              while (db2.allowForks && i < 100) {
                  for (let i = 0; i < entryCount; i++) {
                      prev = await db1.add('hello' + i, { refs: prev ? [prev.hash] : undefined });
                  }
                  i++;
              }
              expect(!db2.allowForks);
          }) */
    })
})
