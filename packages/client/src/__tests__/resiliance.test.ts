
import assert from 'assert'
const mapSeries = require('p-each-series')
import rmrf from 'rimraf'
import { OrbitDB } from '../orbit-db'
import { EventStore } from './utils/stores/event-store'

// Include test utilities
const {
    config,
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
    waitForPeers,
} = require('@dao-xyz/orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/resiliance/1'
const orbitdbPath2 = './orbitdb/tests/resiliance/2'
const dbPath1 = './orbitdb/tests/resiliance/1/db1'
const dbPath2 = './orbitdb/tests/resiliance/2/db2'

const API = 'js-ipfs';
describe(`orbit-db - Resiliance (ipfs-js)`, function () {
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




})

