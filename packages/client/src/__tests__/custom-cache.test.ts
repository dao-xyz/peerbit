
import assert from 'assert'
import rmrf from 'rimraf'
import path from 'path'
import { OrbitDB } from '../orbit-db'
import { createStore } from './storage.js'
import CustomCache from '@dao-xyz/orbit-db-cache'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";

// Include test utilities
import {
  nodeConfig as config,
  startIpfs,
  stopIpfs,
  testAPIs,
} from '@dao-xyz/orbit-db-test-utils'

import {
  databases
} from './utils';

const dbPath = './orbitdb/tests/customKeystore'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Use a Custom Cache (${API})`, function () {
    jest.setTimeout(20000)

    let ipfsd: Controller, ipfs: IPFS, orbitdb1: OrbitDB, store

    beforeAll(async () => {
      store = await createStore("local")
      const cache = new CustomCache(store)

      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api
      orbitdb1 = await OrbitDB.createInstance(ipfs, {
        directory: path.join(dbPath, '1'),
        cache: cache
      })
    })

    afterAll(async () => {
      await orbitdb1.stop()
      await stopIpfs(ipfsd)
    })

    describe('allows orbit to use a custom cache with different store types', function () {
      for (let database of databases) {
        it(database.type + ' allows custom cache', async () => {
          const db1 = await database.create(orbitdb1, 'custom-keystore')
          await database.tryInsert(db1)

          assert.deepEqual(database.getTestValue(db1), database.expectedValue)
          await db1.store.close()
        })
      }
    })
  })
})
