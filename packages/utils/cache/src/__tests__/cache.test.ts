import { field } from "@dao-xyz/borsh"
import Cache from "../index.js"

import assert from 'assert'
const timeout = 50000
import fs from 'fs';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path, { dirname } from 'path';
import { jest } from '@jest/globals';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);
const prefixPath = path.resolve(__dirname, "tmp");
export const createStore = (name = 'keystore'): Level => {
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path.resolve(prefixPath, name), { recursive: true })
  }
  return new Level(path.resolve(prefixPath, name), { valueEncoding: 'view' })
}

describe(`Cache - level`, function () {
  jest.setTimeout(timeout)

  let cache: Cache<any>, storage: {}, store: Level

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
      expect(typeof val).toEqual(d.type)


      try {
        await cache.get('fooKey')
      } catch (e: any) {
        fail();
      }

      await cache.set(d.key, JSON.stringify(d.value))
      await cache.del(d.key)
      try {
        await store.get(d.key)
        fail()
      } catch (e: any) {
        assert(true)
      }

      try {
        await cache.del('fooKey')
        fail()
      } catch (e: any) {
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
      expect(typeof val).toEqual(d.type)
    })

    it('throws an error trying to get an unknown key', async () => {
      try {
        await cache.get('fooKey')
      } catch (e: any) {
        assert(true)
      }
    })

    it('deletes properly', async () => {
      await cache.set(d.key, JSON.stringify(d.value))
      await cache.del(d.key)
      try {
        await store.get(d.key)
      } catch (e: any) {
        assert(true)
      }
    })

    it('throws an error trying to delete an unknown key', async () => {
      try {
        await cache.delete('fooKey')
      } catch (e: any) {
        assert(true)
      }
    })
  }) */
})

