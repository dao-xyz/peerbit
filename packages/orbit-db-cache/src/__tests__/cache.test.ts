import { field } from "@dao-xyz/borsh"
import Cache from ".."

const assert = require('assert')
const timeout = 50000
import fs from 'fs';
import { Level } from 'level';
const prefixPath = 'packages/orbit-db-cache/src/__tests__/tmp/'
export const createStore = (name = 'keystore'): Level => {
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(prefixPath + name, { recursive: true })
  }
  return new Level(prefixPath + name, { valueEncoding: 'view' })
}

describe(`Cache - level`, function () {
  jest.setTimeout(timeout)

  let cache: Cache, storage: {}, store: Level

  const data = [
    { type: (typeof true), key: 'boolean', value: true },
    { type: (typeof 1.0), key: 'number', value: 9000 },
    { type: (typeof 'x'), key: 'strng', value: 'string value' },
    { type: (typeof []), key: 'array', value: [1, 2, 3, 4] },
    { type: (typeof {}), key: 'object', value: { object: 'object', key: 'key' } }
  ]

  beforeAll(async () => {
    try {
      store = await createStore('test')

    } catch (error) {
      const x = 123;
    }
    cache = new Cache(store)
  })


  afterAll(async () => {
    await store.close()
  })

  it(`set, get, delete`, async () => {
    for (const d of data) {
      await cache.set(d.key, d.value)
      const val = await cache.get(d.key)
      assert.deepStrictEqual(val, d.value)
      assert.strictEqual(typeof val, d.type)


      try {
        await cache.get('fooKey')
      } catch (e) {
        assert(true)
      }

      await cache.set(d.key, JSON.stringify(d.value))
      await cache.del(d.key)
      try {
        await store.get(d.key)
      } catch (e) {
        assert(true)
      }

      try {
        await cache.del('fooKey')
      } catch (e) {
        assert(true)
      }
    }
  })

  it(`set get binary`, async () => {
    class TestStruct {
      @field({ type: 'u32' })
      number: number
    }

    const obj = Object.assign(new TestStruct(), { number: 123 });
    await cache.setBinary('key', obj)
    const val = await cache.getBinary('key', TestStruct)
    assert.deepStrictEqual(val, obj)
  })


  /* data.forEach(d => {
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
        assert(true)
      }
    })

    it('deletes properly', async () => {
      await cache.set(d.key, JSON.stringify(d.value))
      await cache.del(d.key)
      try {
        await store.get(d.key)
      } catch (e) {
        assert(true)
      }
    })

    it('throws an error trying to delete an unknown key', async () => {
      try {
        await cache.delete('fooKey')
      } catch (e) {
        assert(true)
      }
    })
  }) */
})

