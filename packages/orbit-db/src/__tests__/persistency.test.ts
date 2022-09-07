
const assert = require('assert')
const mapSeries = require('p-map-series')
const rmrf = require('rimraf')
const path = require('path')

import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore, Operation } from './utils/stores/event-store'
const Cache = require('@dao-xyz/orbit-db-cache')

const localdown = require('localstorage-down')

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs
} = require('orbit-db-test-utils')

const dbPath = './orbitdb/tests/persistency'

const tests = [
  {
    title: 'Persistency',
    type: undefined,
    orbitDBConfig: { directory: path.join(dbPath, '1') }
  }/* ,
  {
    title: 'Persistency with custom cache',
    type: "custom",
    orbitDBConfig: { directory: path.join(dbPath, '2') }
  } */
]
const API = 'js-ipfs';
const test = tests[0];
/* tests.forEach(test => {*/
describe(`orbit-db - Persistency (js-ipfs)`, function () { //${test.title}
  jest.setTimeout(config.timeout)

  const entryCount = 65

  let ipfsd, ipfs, orbitdb1: OrbitDB, db: EventStore<string>, address

  beforeAll(async () => {
    const options: any = Object.assign({}, test.orbitDBConfig)
    rmrf.sync(dbPath)
    ipfsd = await startIpfs(API, config.daemon1)
    ipfs = ipfsd.api
    orbitdb1 = await OrbitDB.createInstance(ipfs, options)
  })

  afterAll(async () => {
    if (orbitdb1)
      await orbitdb1.stop()

    if (ipfsd)
      await stopIpfs(ipfsd)
  })

  describe('load', function () {
    beforeEach(async () => {
      const dbName = new Date().getTime().toString()
      const entryArr = []

      for (let i = 0; i < entryCount; i++)
        entryArr.push(i)

      db = await orbitdb1.create(new EventStore<string>({ name: dbName, accessController: new SimpleAccessController() }))
      address = db.address.toString()
      await mapSeries(entryArr, (i) => db.add('hello' + i))
      await db.close()
      db = null
    })

    afterEach(async () => {
      await db?.drop()
    })

    it('loads database from local cache', async () => {
      db = await orbitdb1.open(address)
      await db.load()
      const items = db.iterator({ limit: -1 }).collect()
      assert.equal(items.length, entryCount)
      assert.equal(items[0].payload.value.value, 'hello0')
      assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount - 1))
    })

    it('loads database partially', async () => {
      const amount = 33
      db = await orbitdb1.open(address)
      await db.load(amount)
      const items = db.iterator({ limit: -1 }).collect()
      assert.equal(items.length, amount)
      assert.equal(items[0].payload.value.value, 'hello' + (entryCount - amount))
      assert.equal(items[1].payload.value.value, 'hello' + (entryCount - amount + 1))
      assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount - 1))
    })

    it('load and close several times', async () => {
      const amount = 8
      for (let i = 0; i < amount; i++) {
        db = await orbitdb1.open(address)
        await db.load()
        const items = db.iterator({ limit: -1 }).collect()
        assert.equal(items.length, entryCount)
        assert.equal(items[0].payload.value.value, 'hello0')
        assert.equal(items[1].payload.value.value, 'hello1')
        assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount - 1))
        await db.close()
      }
    })

    /* it('closes database while loading', async () => { TODO fix
      db = await orbitdb1.open(address, { type: EVENT_STORE_TYPE, replicationConcurrency: 1 })
      return new Promise(async (resolve, reject) => {
        // don't wait for load to finish
        db.load()
          .then(() => reject("Should not finish loading?"))
          .catch(e => {
            if (e.toString() !== 'ReadError: Database is not open') {
              reject(e)
            } else {
              assert.equal(db._cache._store, null)
              resolve(true)
            }
          })
        await db.close()
      })
    }) */

    it('load, add one, close - several times', async () => {
      const amount = 8
      for (let i = 0; i < amount; i++) {
        db = await orbitdb1.open(address)
        await db.load()
        await db.add('hello' + (entryCount + i))
        const items = db.iterator({ limit: -1 }).collect()
        assert.equal(items.length, entryCount + i + 1)
        assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount + i))
        await db.close()
      }
    })

    it('loading a database emits \'ready\' event', async () => {
      db = await orbitdb1.open(address)
      return new Promise(async (resolve) => {
        db.events.on('ready', () => {
          const items = db.iterator({ limit: -1 }).collect()
          assert.equal(items.length, entryCount)
          assert.equal(items[0].payload.value.value, 'hello0')
          assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount - 1))
          resolve(true)
        })
        await db.load()
      })
    })

    it('loading a database emits \'load.progress\' event', async () => {
      db = await orbitdb1.open(address)
      return new Promise(async (resolve, reject) => {
        let count = 0
        db.events.on('load.progress', (address, hash, entry) => {
          count++
          try {
            assert.equal(address, db.address.toString())

            const { progress, max } = db.replicationStatus
            assert.equal(max, entryCount)
            assert.equal(progress, count)

            assert.notEqual(hash, null)
            assert.notEqual(entry, null)

            if (progress === BigInt(entryCount) && count === entryCount) {
              setTimeout(() => {
                resolve(true)
              }, 200)
            }
          } catch (e) {
            reject(e)
          }
        })
        // Start loading the database
        await db.load()
      })
    })
  })

  describe('load from empty snapshot', function () {
    it('loads database from an empty snapshot', async () => {
      db = await orbitdb1.create(new EventStore<string>({ name: 'empty-snapshot', accessController: new SimpleAccessController() }))
      address = db.address.toString()
      await db.saveSnapshot()
      await db.close()

      db = await orbitdb1.open(address)
      await db.loadFromSnapshot()
      const items = db.iterator({ limit: -1 }).collect()
      assert.equal(items.length, 0)
    })
  })

  describe('load from snapshot', function () {
    beforeEach(async () => {
      const dbName = new Date().getTime().toString()
      const entryArr = []

      for (let i = 0; i < entryCount; i++)
        entryArr.push(i)

      db = await orbitdb1.create(new EventStore<string>({ name: dbName, accessController: new SimpleAccessController() }))
      address = db.address.toString()
      await mapSeries(entryArr, (i) => db.add('hello' + i))
      await db.saveSnapshot()
      await db.close()
      db = null
    })

    afterEach(async () => {
      await db.drop()
    })

    it('loads database from snapshot', async () => {
      db = await orbitdb1.open(address)
      await db.loadFromSnapshot()
      const items = db.iterator({ limit: -1 }).collect()
      assert.equal(items.length, entryCount)
      assert.equal(items[0].payload.value.value, 'hello0')
      assert.equal(items[entryCount - 1].payload.value.value, 'hello' + (entryCount - 1))
    })

    it('load, add one and save snapshot several times', async () => {
      const amount = 4
      for (let i = 0; i < amount; i++) {
        db = await orbitdb1.open(address)
        await db.loadFromSnapshot()
        await db.add('hello' + (entryCount + i))
        const items = db.iterator({ limit: -1 }).collect()
        assert.equal(items.length, entryCount + i + 1)
        assert.equal(items[0].payload.value.value, 'hello0')
        assert.equal(items[items.length - 1].payload.value.value, 'hello' + (entryCount + i))
        await db.saveSnapshot()
        await db.close()
      }
    })

    it('throws an error when trying to load a missing snapshot', async () => {
      db = await orbitdb1.open(address)
      await db.drop()
      db = null
      db = await orbitdb1.open(address)

      let err
      try {
        await db.loadFromSnapshot()
      } catch (e) {
        err = e.toString()
      }
      assert.equal(err, `Error: Snapshot for ${address} not found!`)
    })

    it('loading a database emits \'ready\' event', async () => {
      db = await orbitdb1.open(address)
      return new Promise(async (resolve) => {
        db.events.on('ready', () => {
          const items = db.iterator({ limit: -1 }).collect()
          assert.equal(items.length, entryCount)
          assert.equal(items[0].payload.value.value, 'hello0')
          assert.equal(items[entryCount - 1].payload.value.value, 'hello' + (entryCount - 1))
          resolve(true)
        })
        await db.loadFromSnapshot()
      })
    })

    it('loading a database emits \'load.progress\' event', async () => {
      db = await orbitdb1.open(address)
      return new Promise(async (resolve, reject) => {
        let count = 0
        db.events.on('load.progress', (address, hash, entry) => {
          count++
          try {
            assert.equal(address, db.address.toString())

            const { progress, max } = db.replicationStatus
            assert.equal(max, entryCount)
            assert.equal(progress, count)

            assert.notEqual(hash, null)
            assert.notEqual(entry, null)
            if (progress === BigInt(entryCount) && count === entryCount) {
              resolve(true)
            }
          } catch (e) {
            reject(e)
          }
        })
        // Start loading the database
        await db.loadFromSnapshot()
      })
    })
  })
})
/* }) */
