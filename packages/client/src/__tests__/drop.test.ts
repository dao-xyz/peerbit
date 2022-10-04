
import assert from 'assert'
const fs = require('fs')
import path from 'path'
import rmrf from 'rimraf'

import { Address } from '@dao-xyz/orbit-db-store'
import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore } from './utils/stores'

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
} = require('@dao-xyz/orbit-db-test-utils')

const dbPath = './orbitdb/tests/drop'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Drop Database (${API})`, function () {
    jest.setTimeout(config.timeout)

    let ipfsd: Controller, ipfs: IPFS, orbitdb: OrbitDB, db: EventStore<string>
    let localDataPath

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api
      orbitdb = await OrbitDB.createInstance(ipfs, { directory: dbPath })
    })

    afterAll(async () => {
      if (orbitdb)
        await orbitdb.stop()

      if (ipfsd)
        await stopIpfs(ipfsd)

      rmrf.sync(dbPath)
    })

    describe('Drop', function () {
      beforeAll(async () => {
        db = await orbitdb.open(new EventStore({ name: 'first', accessController: new SimpleAccessController() }))
        localDataPath = path.join(dbPath)
        expect(fs.existsSync(localDataPath)).toEqual(true)
      })

      it('removes local database cache', async () => {
        await db.drop()
        await db._cache.open()
        expect(await db._cache.get(db.localHeadsPath)).toEqual(undefined)
        expect(await db._cache.get(db.remoteHeadsPath)).toEqual(undefined)
        expect(await db._cache.get(db.snapshotPath)).toEqual(undefined)
        expect(await db._cache.get(db.queuePath)).toEqual(undefined)
        expect(await db._cache.get(db.manifestPath)).toEqual(undefined)
        await db._cache.close()
      })
    })
  })
})
