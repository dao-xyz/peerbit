
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
import { Entry, LamportClock } from '@dao-xyz/ipfs-log-entry'
import { BoxKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { delay, waitFor } from '@dao-xyz/time'
import { OrbitDB } from '../orbit-db'
import { EventStore, EVENT_STORE_TYPE, Operation } from './utils/stores/event-store'

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

        let timer
        let options

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
            clearInterval(timer)

            rmrf.sync(orbitdbPath1)
            rmrf.sync(orbitdbPath2)
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)

            orbitdb1 = await OrbitDB.createInstance(ipfs1, {
                directory: orbitdbPath1, canAccessKeys: (requester, _keyToAccess) => {
                    const comp = Buffer.compare(requester.getBuffer(), orbitdb2.identity.publicKey.getBuffer());
                    return Promise.resolve(comp === 0) // allow orbitdb1 to share keys with orbitdb2
                }, waitForKeysTimout: 1000
            })
            orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2 })

            options = {
                // Set write access for both clients
                accessController: {
                    write: [
                        orbitdb1.identity.id,
                        orbitdb2.identity.id,
                    ]
                }
            }

            options = Object.assign({}, options, { directory: dbPath1, encryption: orbitdb1.replicationTopicEncryption() })
            db1 = await orbitdb1.create('replication-tests', EVENT_STORE_TYPE, options)
        })

        afterEach(async () => {
            clearInterval(timer)
            options = {}

            if (db1)
                await db1.drop()

            if (db2)
                await db2.drop()

            if (orbitdb1)
                await orbitdb1.stop()

            if (orbitdb2)
                await orbitdb2.stop()
        })

        it('replicates database of 1 entry', async () => {
            console.log("Waiting for peers to connect")
            await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
            options = Object.assign({}, options, { create: true, type: EVENT_STORE_TYPE, directory: dbPath2, sync: true, replicate: false, encryption: undefined })
            db2 = await orbitdb2.open(db1.address.toString(), options)
            let finished = false
            await db1.add('hello');
            await waitFor(() => db2._oplog.clock.time > 0);
            await db2.add('world');

            await new Promise((resolve, reject) => {
                let replicatedEventCount = 0
                db1.events.on('replicated', (address, length) => {
                    replicatedEventCount++
                    const all = db1.iterator({ limit: -1 }).collect().length
                    finished = (all === 2) // the thing I added + the thing db2 added
                })

                db2.events.on('replicated', (address, length) => {
                    reject(); // should not happen since db2 is write only (no reads from peers)
                })

                timer = setInterval(() => {
                    if (finished) {
                        clearInterval(timer)
                        const entries1: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
                        try {
                            assert.equal(entries1.length, 2)
                            assert.equal(entries1[0].clock.time, 1n)
                            assert.equal(entries1[0].payload.value.value, 'hello')
                            assert.equal(entries1[1].clock.time, 2n)
                            assert.equal(entries1[1].payload.value.value, 'world')
                            assert.equal(replicatedEventCount, 1)
                        } catch (error) {
                            reject(error)
                        }

                        const entries2: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
                        try {
                            assert.equal(entries2.length, 1)
                        } catch (error) {
                            reject(error)
                        }
                        resolve(true)
                    }
                }, 100)
            })
        })

        it('replicates database of 1 entry encrypted', async () => {
            console.log("Waiting for peers to connect")
            await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
            const encryptionKey = await orbitdb1.keystore.createKey('encryption key', BoxKeyWithMeta, db1.replicationTopic);
            options = Object.assign({}, options, { create: true, type: EVENT_STORE_TYPE, directory: dbPath2, sync: true, replicate: false, encryption: orbitdb2.replicationTopicEncryption() })
            db2 = await orbitdb2.open(db1.address.toString(), options)
            let finished = false
            await db1.add('hello', {
                reciever: {
                    clock: encryptionKey.publicKey,
                    id: encryptionKey.publicKey,
                    identity: encryptionKey.publicKey,
                    payload: encryptionKey.publicKey,
                    signature: encryptionKey.publicKey
                }
            });
            await waitFor(() => db2._oplog.clock.time > 0);

            // Now the db2 will request sync clocks even though it does not replicate any content
            await db2.add('world');

            await new Promise((resolve, reject) => {
                let replicatedEventCount = 0
                db1.events.on('replicated', (address, length) => {
                    replicatedEventCount++
                    const all = db1.iterator({ limit: -1 }).collect().length
                    finished = (all === 2) // the thing I added + the thing db2 added
                })

                db2.events.on('replicated', (address, length) => {
                    reject(); // should not happen since db2 is write only (no reads from peers)
                })

                timer = setInterval(() => {
                    if (finished) {
                        clearInterval(timer)
                        const entries1: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
                        try {
                            assert.equal(entries1.length, 2)
                            assert.equal(entries1[0].clock.time, 1n)
                            assert.equal(entries1[0].payload.value.value, 'hello')
                            assert.equal(entries1[1].clock.time, 2n)
                            assert.equal(entries1[1].payload.value.value, 'world')
                            assert.equal(replicatedEventCount, 1)
                        } catch (error) {
                            reject(error)
                        }

                        const entries2: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
                        try {
                            assert.equal(entries2.length, 1)
                        } catch (error) {
                            reject(error)
                        }
                        resolve(true)
                    }
                }, 100)
            })
        })
    })
})
