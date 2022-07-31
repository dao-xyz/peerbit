
const assert = require('assert')
const fs = require('fs-extra')
const path = require('path')
const rmrf = require('rimraf')
const Zip = require('adm-zip')
import { OrbitDB } from '../orbit-db'
import { KeyValueStore } from './utils/stores/key-value-store'
import io from '@dao-xyz/orbit-db-io'
import { FEED_STORE_TYPE } from './utils/stores'
import { OrbitDBAddress } from '../orbit-db-address'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
} = require('orbit-db-test-utils')

const dbPath = path.join('./orbitdb', 'tests', 'create-open')
const migrationFixturePath = path.join('./packages/orbit-db/src/__tests__', 'fixtures', 'migration', 'cache-schema-test')
const ipfsFixturesDir = path.join('./packages/orbit-db/src/__tests__', 'fixtures', 'ipfs')

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Create & Open (${API})`, function () {
    let ipfsFixtures = path.join('./packages/orbit-db/src/__tests__', 'fixtures', `${API}.zip`)

    jest.retryTimes(1) // windows...
    jest.setTimeout(config.timeout)

    let ipfsd, ipfs, orbitdb, address
    let localDataPath

    const filterFunc = (src, dest) => {
      // windows has problems copying these files...
      return !(src.includes('LOG') || src.includes('LOCK'))
    }

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api
      const zip = new Zip(ipfsFixtures)
      await zip.extractAllToAsync(path.join('./packages/orbit-db/src/__tests__', 'fixtures'), true)
      await fs.copy(path.join(ipfsFixturesDir, 'blocks'), path.join(ipfsd.path, 'blocks'))
      await fs.copy(path.join(ipfsFixturesDir, 'datastore'), path.join(ipfsd.path, 'datastore'), { filter: filterFunc })
      orbitdb = await OrbitDB.createInstance(ipfs, { directory: dbPath })
    })

    afterAll(async () => {
      if (orbitdb)
        await orbitdb.stop()

      if (ipfsd)
        await stopIpfs(ipfsd)

      rmrf.sync(ipfsFixturesDir)
    })

    describe('Create', function () {
      describe('Errors', function () {
        it('throws an error if given an invalid database type', async () => {
          let err
          try {
            await orbitdb.create('first', 'invalid-type')
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, 'Error: Invalid database type \'invalid-type\'')
        })

        it('throws an error if given an address instead of name', async () => {
          let err
          try {
            await orbitdb.create('/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first', FEED_STORE_TYPE)
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, 'Error: Given database name is an address. Please give only the name of the database!')
        })

        it('throws an error if database already exists', async () => {
          let err, db
          try {
            db = await orbitdb.create('first', FEED_STORE_TYPE, { replicate: false })
            const db2 = await orbitdb.create('first', FEED_STORE_TYPE, { replicate: false })
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, `Error: Database '${db.address}' already exists!`)
          await db.close()
        })


        it('throws an error if database type doesn\'t match', async () => {
          let err, log, kv
          try {
            log = await orbitdb.kvstore('keyvalue', { replicate: false })
            kv = await orbitdb.eventlog(log.address.toString())
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, `Error: Database '${log.address}' is type 'keyvalue' but was opened as 'eventlog'`)
        })
      })

      describe('Success', function () {
        let db: KeyValueStore;

        beforeAll(async () => {
          db = await orbitdb.create('second', FEED_STORE_TYPE, { replicate: false })
          localDataPath = path.join(dbPath, orbitdb.id, 'cache')
          await db.close()
        })

        it('creates a feed database', async () => {
          assert.notEqual(db, null)
        })

        it('database has the correct address', async () => {
          assert.equal(db.address.toString().indexOf('/orbitdb'), 0)
          assert.equal(db.address.toString().indexOf('zd'), 9)
          assert.equal(db.address.toString().indexOf('second'), 59)
        })

        it('saves the database locally', async () => {
          assert.equal(fs.existsSync(localDataPath), true)
        })

        it('saves database manifest reference locally', async () => {
          const address = db.id
          const manifestHash = address.split('/')[2]
          await db._cache._store.open()
          const value = await db._cache.get(path.join(address, '/_manifest'))
          assert.equal(value, manifestHash)
        })

        it('saves database manifest file locally', async () => {
          const manifestHash = db.id.split('/')[2]
          const manifest = await io.read(ipfs, manifestHash)
          assert.notEqual(manifest, false)
          assert.equal(manifest.name, 'second')
          assert.equal(manifest.type, FEED_STORE_TYPE)
          assert.notEqual(manifest.accessController, null)
          assert.equal(manifest.accessController.indexOf('/ipfs'), 0)
        })

        it('can pass local database directory as an option', async () => {
          const dir = './orbitdb/tests/another-feed'
          const db2 = await orbitdb.create('third', FEED_STORE_TYPE, { directory: dir })
          assert.equal(fs.existsSync(dir), true)
          await db2.close()
        })

        it('loads cache from previous version of orbit-db', async () => {
          const dbName = 'cache-schema-test'

          db = await orbitdb.create(dbName, 'keyvalue', { overwrite: true })
          const manifestHash = db.address.root
          const migrationDataPath = path.join(dbPath, manifestHash, dbName)

          await db.load()
          assert.equal((await db.get('key')), undefined)
          await db.drop()

          await fs.copy(migrationFixturePath, migrationDataPath, { filter: filterFunc })
          db = await orbitdb.create(dbName, 'keyvalue')
          await db.load()

          assert.equal(manifestHash, db.address.root)
          assert.equal((await db.get('key')), 'value')
        })

        it('loads cache from previous version of orbit-db with the directory option', async () => {
          const dbName = 'cache-schema-test2'
          const directory = path.join(dbPath, "some-other-place")

          await fs.copy(migrationFixturePath, directory, { filter: filterFunc })
          db = await orbitdb.create(dbName, 'keyvalue', { directory })
          await db.load()

          assert.equal((await db.get('key')), 'value')
        })

        describe('Access Controller', function () {
          beforeAll(async () => {
            if (db) {
              await db.drop()
            }
          })

          afterEach(async () => {
            if (db) {
              await db.drop()
            }
          })

          it('creates an access controller and adds ourselves as writer by default', async () => {
            db = await orbitdb.create('fourth', FEED_STORE_TYPE)
            assert.deepEqual(db.access.write, [orbitdb.identity.id])
          })

          it('creates an access controller and adds writers', async () => {
            db = await orbitdb.create('fourth', FEED_STORE_TYPE, {
              accessController: {
                write: ['another-key', 'yet-another-key', orbitdb.identity.id]
              }
            })
            assert.deepEqual(db.access.write, ['another-key', 'yet-another-key', orbitdb.identity.id])
          })

          it('creates an access controller and doesn\'t add read access keys', async () => {
            db = await orbitdb.create('seventh', FEED_STORE_TYPE, { read: ['one', 'two'] })
            assert.deepEqual(db.access.write, [orbitdb.identity.id])
          })
        })
        describe('Meta', function () {
          beforeAll(async () => {
            if (db) {
              await db.close()
              await db.drop()
            }
          })

          afterEach(async () => {
            if (db) {
              await db.close()
              await db.drop()
            }
          })

          it('creates a manifest with no meta field', async () => {
            db = await orbitdb.create('no-meta', FEED_STORE_TYPE)
            const manifest = await io.read(ipfs, db.address.root)
            assert.strictEqual(manifest.meta, undefined)
            assert.deepStrictEqual(Object.keys(manifest).filter(k => k === 'meta'), [])
          })

          it('creates a manifest with a meta field', async () => {
            const meta = { test: 123 }
            db = await orbitdb.create('meta', FEED_STORE_TYPE, { meta })
            const manifest = await io.read(ipfs, db.address.root)
            assert.deepStrictEqual(manifest.meta, meta)
            assert.deepStrictEqual(Object.keys(manifest).filter(k => k === 'meta'), ['meta'])
          })
        })
      })
    })

    describe('determineAddress', function () {
      describe('Errors', function () {
        it('throws an error if given an invalid database type', async () => {
          let err
          try {
            await orbitdb.determineAddress('first', 'invalid-type')
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, 'Error: Invalid database type \'invalid-type\'')
        })

        it('throws an error if given an address instead of name', async () => {
          let err
          try {
            await orbitdb.determineAddress('/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first', FEED_STORE_TYPE)
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, 'Error: Given database name is an address. Please give only the name of the database!')
        })
      })

      describe('Success', function () {
        beforeAll(async () => {
          address = await orbitdb.determineAddress('third', FEED_STORE_TYPE, { replicate: false })
          localDataPath = path.join(dbPath, address.root, address.path)
        })

        it('does not save the address locally', async () => {
          assert.equal(fs.existsSync(localDataPath), false)
        })

        it('returns the address that would have been created', async () => {
          const db = await orbitdb.create('third', FEED_STORE_TYPE, { replicate: false })
          assert.equal(address.toString().indexOf('/orbitdb'), 0)
          assert.equal(address.toString().indexOf('zd'), 9)
          assert.equal(address.toString(), db.address.toString())
          await db.close()
        })
      })
    })

    describe('Open', function () {
      it('throws an error if trying to open a database with name only and \'create\' is not set to \'true\'', async () => {
        let err
        try {
          let db = await orbitdb.open('XXX', { create: false })
        } catch (e) {
          err = e.toString()
        }
        assert.equal(err, "Error: 'options.create' set to 'false'. If you want to create a database, set 'options.create' to 'true'.")
      })

      it('throws an error if trying to open a database with name only and \'create\' is not set to true', async () => {
        let err
        try {
          let db = await orbitdb.open('YYY', { create: true })
        } catch (e) {
          err = e.toString()
        }
        assert.equal(err, `Error: Database type not provided! Provide a type with 'options.type' (${OrbitDB.databaseTypes.join('|')})`)
      })

      it('opens a database - name only', async () => {
        const db = await orbitdb.open('abc', { create: true, type: FEED_STORE_TYPE, overwrite: true })
        assert.equal(db.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db.address.toString().indexOf('zd'), 9)
        assert.equal(db.address.toString().indexOf('abc'), 59)
        await db.drop()
      })

      it('opens a database - with a different identity', async () => {
        const identity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore: orbitdb.keystore })
        const db = await orbitdb.open('abc', { create: true, type: FEED_STORE_TYPE, overwrite: true, identity })
        assert.equal(db.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db.address.toString().indexOf('zd'), 9)
        assert.equal(db.address.toString().indexOf('abc'), 59)
        assert.equal(db.identity, identity)
        await db.drop()
      })

      it('opens the same database - from an address', async () => {
        const identity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore: orbitdb.keystore })
        const db = await orbitdb.open('abc', { create: true, type: FEED_STORE_TYPE, overwrite: true, identity })
        const db2 = await orbitdb.open(db.address)
        assert.equal(db2.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db2.address.toString().indexOf('zd'), 9)
        assert.equal(db2.address.toString().indexOf('abc'), 59)
        await db.drop()
        await db2.drop()
      })

      it('opens a database and adds the creator as the only writer', async () => {
        const db = await orbitdb.open('abc', { create: true, type: FEED_STORE_TYPE, overwrite: true })
        assert.equal(db.access.write.length, 1)
        assert.equal(db.access.write[0], db.identity.id)
        await db.drop()
      })

      it('doesn\'t open a database if we don\'t have it locally', async () => {
        const db = await orbitdb.open('abcabc', { create: true, type: FEED_STORE_TYPE, overwrite: true })
        const address = new OrbitDBAddress(db.address.root.slice(0, -1) + 'A', 'non-existent')
        await db.drop()
        return new Promise((resolve, reject) => {
          setTimeout(resolve, 900)
          orbitdb.open(address)
            .then(() => reject(new Error('Shouldn\'t open the database')))
            .catch(reject)
        })
      })

      it('throws an error if trying to open a database locally and we don\'t have it', async () => {
        const db = await orbitdb.open('abc', { create: true, type: FEED_STORE_TYPE, overwrite: true })
        const address = new OrbitDBAddress(db.address.root.slice(0, -1) + 'A', 'second')
        await db.drop()
        return orbitdb.open(address, { localOnly: true })
          .then(() => new Error('Shouldn\'t open the database'))
          .catch(e => {
            assert.equal(e.toString(), `Error: Database '${address}' doesn't exist!`)
          })
      })

      it('open the database and it has the added entries', async () => {
        const db = await orbitdb.open('ZZZ', { create: true, type: FEED_STORE_TYPE })
        await db.add('hello1')
        await db.add('hello2')
        await db.close()

        const db2 = await orbitdb.open(db.address)

        await db.load()
        const res = db.iterator({ limit: -1 }).collect()

        assert.equal(res.length, 2)
        assert.equal(res[0].data.payload.value, 'hello1')
        assert.equal(res[1].data.payload.value, 'hello2')
        await db.drop()
        await db2.drop()
      })
    })

    describe("Close", function () {
      beforeAll(async () => {
        if (orbitdb) await orbitdb.stop()
        orbitdb = await OrbitDB.createInstance(ipfs, { directory: dbPath })
      })
      it('closes a custom store', async () => {
        const directory = path.join(dbPath, "custom-store")
        const db = await orbitdb.open('xyz', { create: true, type: FEED_STORE_TYPE, directory })
        await db.close()
        assert.strictEqual(db._cache._store._db.status, 'closed')
      })

      it("close load close sets status to 'closed'", async () => {
        const directory = path.join(dbPath, "custom-store")
        const db = await orbitdb.open('xyz', { create: true, type: FEED_STORE_TYPE, directory })
        await db.close()
        await db.load()
        await db.close()
        assert.strictEqual(db._cache._store._db.status, 'closed')
      })

      it('successfully manages multiple caches', async () => {
        // Cleaning up cruft from other tests
        const directory = path.join(dbPath, "custom-store")
        const directory2 = path.join(dbPath, "custom-store2")

        const db1 = await orbitdb.open('xyz1', { create: true, type: FEED_STORE_TYPE, })
        const db2 = await orbitdb.open('xyz2', { create: true, type: FEED_STORE_TYPE, directory })
        const db3 = await orbitdb.open('xyz3', { create: true, type: FEED_STORE_TYPE, directory })
        const db4 = await orbitdb.open('xyz4', { create: true, type: FEED_STORE_TYPE, directory: directory2 })
        const db5 = await orbitdb.open('xyz5', { create: true, type: FEED_STORE_TYPE, })

        await db1.close()
        await db2.close()
        await db4.close()

        assert.strictEqual(orbitdb.cache._store._db.status, 'open')
        assert.strictEqual(db2._cache._store._db.status, 'open')
        assert.strictEqual(db3._cache._store._db.status, 'open')
        assert.strictEqual(db4._cache._store._db.status, 'closed')

        await db3.close()
        await db5.close()

        assert.strictEqual(orbitdb.cache._store._db.status, 'closed')
        assert.strictEqual(db2._cache._store._db.status, 'closed')
        assert.strictEqual(db3._cache._store._db.status, 'closed')
        assert.strictEqual(db4._cache._store._db.status, 'closed')
        assert.strictEqual(db5._cache._store._db.status, 'closed')
      })
    })
  })
})
