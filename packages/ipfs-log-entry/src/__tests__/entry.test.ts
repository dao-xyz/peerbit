import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import assert from 'assert';
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Entry, EntryDataDecrypted } from '../entry';
import { Keystore } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs: any, testIdentity

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Entry', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
    let keystore, signingKeystore

    beforeAll(async () => {
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      testIdentity = await Identities.createIdentity({ id: 'id', keystore, signingKeystore })
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await keystore?.close()
      await signingKeystore?.close()
    })

    describe('create', () => {
      it('creates a an empty entry', async () => {
        const expectedHash = 'zdpuAvANoW5BBgbiZSxKuLtxmFbbBE1dsK5ciAXXj58YPhSjN'
        const entry = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello' })
        assert.strictEqual(entry.hash, expectedHash)
        assert.strictEqual(entry.data.id, 'A')
        assert.strictEqual(entry.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.data.clock.time, 0)
        assert.strictEqual(entry.data.payload, 'hello')
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
      })

      it('creates a entry with payload', async () => {
        const expectedHash = 'zdpuApKvTDtBXXLEKABqVk5vsYxrMT8SeeEUYvzZ9PYt6ZoTo'
        const payload = 'hello world'
        const entry = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload, next: [] })
        assert.strictEqual(entry.hash, expectedHash)
        assert.strictEqual(entry.data.payload, payload)
        assert.strictEqual(entry.data.id, 'A')
        assert.strictEqual(entry.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.data.clock.time, 0)
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
      })

      it('creates a entry with payload and next', async () => {
        const expectedHash = 'zdpuAm5GvbxL2CY7WLU2GezMSepLNvdRtxQP64N4mjkmnRENE'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        entry1.data.clock.tick()
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [entry1], clock: entry1.data.clock })
        assert.strictEqual(entry2.data.payload, payload2)
        assert.strictEqual(entry2.next.length, 1)
        assert.strictEqual(entry2.hash, expectedHash)
        assert.strictEqual(entry2.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry2.data.clock.time, 1)
      })

      it('`next` parameter can be an array of strings', async () => {
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello1', next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello2', next: [entry1.hash] })
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      it('`next` parameter can be an array of Entry instances', async () => {
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello1', next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello2', next: [entry1] })
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      it('`next` parameter can contain nulls and undefined objects', async () => {
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello1', next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello2', next: [entry1, null, undefined] })
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      it('throws an error if identity are not defined', async () => {
        let err
        try {
          await Entry.create({ ipfs, identity: null, logId: 'A', data: 'hello2', next: [] })
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Identity is required, cannot create entry')
      })

      it('throws an error if id is not defined', async () => {
        let err
        try {
          await Entry.create({ ipfs, identity: testIdentity, logId: null, data: 'hello', next: [] })
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Entry requires an id')
      })

      it('throws an error if data is not defined', async () => {
        let err
        try {
          await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: null, next: [] })
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Entry requires data')
      })

      it('throws an error if next is not an array', async () => {
        let err
        try {
          await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello', next: {} as any })
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, '\'next\' argument is not an array')
      })
    })

    describe('toMultihash', () => {
      it('returns an ipfs multihash', async () => {
        const expectedMultihash = 'zdpuAvANoW5BBgbiZSxKuLtxmFbbBE1dsK5ciAXXj58YPhSjN'
        const entry = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello', next: [] })
        const multihash = await Entry.toMultihash(ipfs, entry)
        assert.strictEqual(multihash, expectedMultihash)
      })

      it('throws an error if ipfs is not defined', async () => {
        let err
        try {
          await Entry.toMultihash(undefined, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Ipfs instance not defined')
      })

      it('throws an error if the object being passed is invalid', async () => {
        let err
        try {
          const entry = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello', next: [] })
          delete (entry.data as EntryDataDecrypted<any>)._clock
          await Entry.toMultihash(ipfs, entry)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Invalid object format, cannot generate entry hash')
      })
    })

    describe('fromMultihash', () => {
      it('creates a entry from ipfs hash', async () => {
        const expectedHash = 'zdpuAvc2tuTUucpU8N7MUGXReWVdivfx2SCfsAVtL8GvXX6Pt'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [entry1] })
        const final = await Entry.fromMultihash<string>(ipfs, entry2.hash)
        final.init(entry2);
        assert(final.equals(entry2));
        assert.strictEqual(final.data.id, 'A')
        assert.strictEqual(final.data.payload, payload2)
        assert.strictEqual(final.next.length, 1)
        assert.strictEqual(final.next[0], entry1.hash)
        assert.strictEqual(final.hash, expectedHash)
      })

      it('throws an error if ipfs is not present', async () => {
        let err
        try {
          await Entry.fromMultihash(undefined as any, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Ipfs instance not defined')
      })

      it('throws an error if hash is undefined', async () => {
        let err
        try {
          await Entry.fromMultihash(ipfs, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Invalid hash: undefined')
      })
    })

    describe('isParent', () => {
      it('returns true if entry has a child', async () => {
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [entry1] })
        assert.strictEqual(Entry.isParent(entry1, entry2), true)
      })

      it('returns false if entry does not have a child', async () => {
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [] })
        const entry3 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [entry2] })
        assert.strictEqual(Entry.isParent(entry1, entry2), false)
        assert.strictEqual(Entry.isParent(entry1, entry3), false)
        assert.strictEqual(Entry.isParent(entry2, entry3), true)
      })
    })

    describe('compare', () => {
      it('returns true if entries are the same', async () => {
        const payload1 = 'hello world'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        assert.strictEqual(Entry.isEqual(entry1, entry2), true)
      })

      it('returns true if entries are not the same', async () => {
        const payload1 = 'hello world1'
        const payload2 = 'hello world2'
        const entry1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload1, next: [] })
        const entry2 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: payload2, next: [] })
        assert.strictEqual(Entry.isEqual(entry1, entry2), false)
      })
    })

    describe('isEntry', () => {
      it('is an Entry', async () => {
        const entry = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'hello', next: [] })
        assert.strictEqual(Entry.isEntry(entry), true)
      })

      it('is not an Entry - no id', async () => {
        const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123, seq: 0 }, next: [], }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      it('is not an Entry - no seq', async () => {
        const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      it('is not an Entry - no next', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, payload: 123 }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      it('is not an Entry - no version', async () => {
        const fakeEntry = { data: { id: 'A', payload: 123, seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      it('is not an Entry - no hash', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, payload: 123, seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      it('is not an Entry - no payload', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })
    })
  })
})
