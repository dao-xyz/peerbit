
const assert = require('assert')
const fs = require('fs-extra')
const path = require('path')
const rmrf = require('rimraf')
const Zip = require('adm-zip')
import { OrbitDB } from '../orbit-db'
import { KeyValueStore } from './utils/stores/key-value-store'
import io from '@dao-xyz/orbit-db-io'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { SimpleAccessController } from './utils/access'
import { Address } from '@dao-xyz/orbit-db-store'
import { EventStore } from './utils/stores'
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

    let ipfsd, ipfs, orbitdb: OrbitDB, address
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




        it('throws an error if database already exists', async () => {
          let err, db
          try {
            db = await orbitdb.create(new EventStore({ name: 'first', accessController: new SimpleAccessController() })
              , { replicate: false })
            await orbitdb.create(new EventStore({ name: 'first', accessController: new SimpleAccessController() })
              , { replicate: false })
          } catch (e) {
            err = e.toString()
          }
          assert.equal(err, `Error: Database '${db.address}' already exists!`)
          await db.close()
        })

      })

      describe('Success', function () {
        let db: KeyValueStore<string>;

        beforeAll(async () => {
          db = await orbitdb.create(new KeyValueStore<string>({ name: 'second', accessController: new SimpleAccessController() })
            , { replicate: false })
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
          assert.notEqual(manifest.accessController, null)
          assert.equal(manifest.accessController.indexOf('/ipfs'), 0)
        })

        it('can pass local database directory as an option', async () => {
          const dir = './orbitdb/tests/another-feed'
          const db2 = await orbitdb.create(new EventStore({ name: 'third', accessController: new SimpleAccessController() })
            , { directory: dir })
          assert.equal(fs.existsSync(dir), true)
          await db2.close()
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

      it('opens a database - name only', async () => {
        const db = await orbitdb.open(new EventStore({ name: 'abc', accessController: new SimpleAccessController() }), { create: true, overwrite: true })
        assert.equal(db.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db.address.toString().indexOf('zd'), 9)
        assert.equal(db.address.toString().indexOf('abc'), 59)
        await db.drop()
      })

      it('opens a database - with a different identity', async () => {
        const identity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore: orbitdb.keystore })
        const db = await orbitdb.open(new EventStore({ name: 'abc', accessController: new SimpleAccessController() }), { create: true, overwrite: true, identity })
        assert.equal(db.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db.address.toString().indexOf('zd'), 9)
        assert.equal(db.address.toString().indexOf('abc'), 59)
        assert.equal(db.identity, identity)
        await db.drop()
      })

      it('opens the same database - from an address', async () => {
        const identity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore: orbitdb.keystore })
        const db = await orbitdb.open(new EventStore({ name: 'abc', accessController: new SimpleAccessController() }), { create: true, overwrite: true, identity })
        const db2 = await orbitdb.open(db.address)
        assert.equal(db2.address.toString().indexOf('/orbitdb'), 0)
        assert.equal(db2.address.toString().indexOf('zd'), 9)
        assert.equal(db2.address.toString().indexOf('abc'), 59)
        await db.drop()
        await db2.drop()
      })

      it('doesn\'t open a database if we don\'t have it locally', async () => {
        const db = await orbitdb.open(new EventStore({ name: 'abcabc', accessController: new SimpleAccessController() }), { create: true, overwrite: true })
        const address = new Address(db.address.root.slice(0, -1) + 'A', 'non-existent')
        await db.drop()
        return new Promise((resolve, reject) => {
          setTimeout(resolve, 900)
          orbitdb.open(address)
            .then(() => reject(new Error('Shouldn\'t open the database')))
            .catch(reject)
        })
      })

      it('throws an error if trying to open a database locally and we don\'t have it', async () => {
        const db = await orbitdb.open(new EventStore({ name: 'abc', accessController: new SimpleAccessController() }), { create: true, overwrite: true })
        const address = new Address(db.address.root.slice(0, -1) + 'A', 'second')
        await db.drop()
        return orbitdb.open(address, { localOnly: true })
          .then(() => new Error('Shouldn\'t open the database'))
          .catch(e => {
            assert.equal(e.toString(), `Error: Database '${address}' doesn't exist!`)
          })
      })

      it('open the database and it has the added entries', async () => {
        const db = await orbitdb.open(new EventStore({ name: 'ZZZ', accessController: new SimpleAccessController() }), { create: true })
        await db.add('hello1')
        await db.add('hello2')
        await db.close()

        const db2 = await orbitdb.open(db.address)

        await db.load()
        const res = db.iterator({ limit: -1 }).collect()

        assert.equal(res.length, 2)
        assert.equal(res[0].payload.value.value, 'hello1')
        assert.equal(res[1].payload.value.value, 'hello2')
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
        const db = await orbitdb.open(new EventStore({ name: 'xyz', accessController: new SimpleAccessController() }), { create: true, directory })
        await db.close()
        assert.strictEqual(db._cache._store.status, 'closed')
      })

      it("close load close sets status to 'closed'", async () => {
        const directory = path.join(dbPath, "custom-store")
        const db = await orbitdb.open(new EventStore({ name: 'xyz', accessController: new SimpleAccessController() }), { create: true, directory })
        await db.close()
        await db.load()
        await db.close()
        assert.strictEqual(db._cache._store.status, 'closed')
      })

      it('successfully manages multiple caches', async () => {
        // Cleaning up cruft from other tests
        const directory = path.join(dbPath, "custom-store")
        const directory2 = path.join(dbPath, "custom-store2")

        const db1 = await orbitdb.open(new EventStore({ name: 'xyz1', accessController: new SimpleAccessController() }), { create: true })
        const db2 = await orbitdb.open(new EventStore({ name: 'xyz2', accessController: new SimpleAccessController() }), { create: true, directory })
        const db3 = await orbitdb.open(new EventStore({ name: 'xyz3', accessController: new SimpleAccessController() }), { create: true, directory })
        const db4 = await orbitdb.open(new EventStore({ name: 'xyz4', accessController: new SimpleAccessController() }), { create: true, directory: directory2 })
        const db5 = await orbitdb.open(new EventStore({ name: 'xyz5', accessController: new SimpleAccessController() }), { create: true })

        await db1.close()
        await db2.close()
        await db4.close()

        assert.strictEqual(orbitdb.cache._store._db.status, 'open')
        assert.strictEqual(db2._cache._store.status, 'open')
        assert.strictEqual(db3._cache._store.status, 'open')
        assert.strictEqual(db4._cache._store.status, 'closed')

        await db3.close()
        await db5.close()

        assert.strictEqual(orbitdb.cache._store._db.status, 'closed')
        assert.strictEqual(db2._cache._store.status, 'closed')
        assert.strictEqual(db3._cache._store.status, 'closed')
        assert.strictEqual(db4._cache._store.status, 'closed')
        assert.strictEqual(db5._cache._store.status, 'closed')
      })
    })
  })
})
