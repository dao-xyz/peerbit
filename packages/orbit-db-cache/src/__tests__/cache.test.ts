import Cache from ".."

const assert = require('assert')
const Storage = require('orbit-db-storage-adapter')
const implementations = require('orbit-db-storage-adapter/test/implementations')
const timeout = 50000

implementations.forEach(implementation => {
  describe(`Cache - ${implementation.key}`, function () {
    this.timeout(timeout)

    let cache, storage, store

    const location = implementation.fileName
    const server = implementation.server

    const data = [
      { type: (typeof true), key: 'boolean', value: true },
      { type: (typeof 1.0), key: 'number', value: 9000 },
      { type: (typeof 'x'), key: 'strng', value: 'string value' },
      { type: (typeof []), key: 'array', value: [1, 2, 3, 4] },
      { type: (typeof {}), key: 'object', value: { object: 'object', key: 'key' } }
    ]

    beforeAll(async () => {
      const storageType = implementation.module
      if (server && server.start) await implementation.server.start({})
      storage = (Storage as any)(storageType)
      store = await storage.createStore(location, implementation.defaultOptions || {})
      cache = new Cache(store)
    })

    afterEach(async () => {
      if (server && server.afterEach) await implementation.server.afterEach()
    })

    afterAll(async () => {
      await store.close()
      await storage.destroy(store)
      if (server && server.stop) await implementation.server.stop()
    })

    data.forEach(d => {
      it(`sets and gets a ${d.key}`, async () => {
        await cache.set(d.key, d.value)
        const val = await cache.get(d.key)
        assert.deepStrictEqual(val, d.value)
        assert.strictEqual(typeof val, d.type)
      })

      it('throws an error trying to get an unknown key', async () => {
        try {
          await cache.get('fooKey')
        } catch (e) {
          assert.strictEqual(true, true)
        }
      })

      it('deletes properly', async () => {
        await cache.set(d.key, JSON.stringify(d.value))
        await cache.del(d.key)
        try {
          await store.get(d.key)
        } catch (e) {
          assert.strictEqual(true, true)
        }
      })

      it('throws an error trying to delete an unknown key', async () => {
        try {
          await cache.delete('fooKey')
        } catch (e) {
          assert.strictEqual(true, true)
        }
      })
    })
  })
})
