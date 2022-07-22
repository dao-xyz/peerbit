import { DefaultAccessController } from '../default-access-controller'
import { Entry } from '@dao-xyz/ipfs-log-entry';
export const io = require('orbit-db-io')
const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')

import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import * as v0Entries from './fixtures/v0-entries.fixture'
import * as v1Entries from './fixtures/v1-entries.fixture.json'
import { assertPayload } from './utils/assert'
const Keystore = require('orbit-db-keystore')

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Entry (' + IPFS + ')', function () {
    jest.setTimeout(config.timeout)

    const testACL = new DefaultAccessController()
    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      testIdentity = await Identities.createIdentity({ id: 'userA', keystore, signingKeystore })
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
      test('creates a an empty entry', async () => {
        const expectedHash = 'zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB'
        const entry = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')))
        assert.strictEqual(entry.hash, expectedHash)
        assert.strictEqual(entry.data.id, 'A')
        assert.strictEqual(entry.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.data.clock.time, 0)
        assertPayload(entry.data.payload, 'hello')
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
      })

      test('creates a entry with payload', async () => {
        const expectedHash = 'zdpuAyvJU3TS7LUdfRxwAnJorkz6NfpAWHGypsQEXLZxcCCRC'
        const payload = 'hello world'
        const entry = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload)), [])
        assertPayload(entry.data.payload, payload)
        assert.strictEqual(entry.data.id, 'A')
        assert.strictEqual(entry.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.data.clock.time, 0)
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
        assert.strictEqual(entry.hash, expectedHash)
      })

      test('creates a entry with payload and next', async () => {
        const expectedHash = 'zdpuAqsN9Py4EWSfrGYZS8tuokWuiTd9zhS8dhr9XpSGQajP2'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        entry1.data.clock.tick()
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [entry1], entry1.data.clock)
        assertPayload(entry2.data.payload, payload2)
        assert.strictEqual(entry2.next.length, 1)
        assert.strictEqual(entry2.hash, expectedHash)
        assert.strictEqual(entry2.data.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry2.data.clock.time, 1)
      })

      test('`next` parameter can be an array of strings', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello1')), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello2')), [entry1.hash])
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      test('`next` parameter can be an array of Entry instances', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello1')), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello2')), [entry1])
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      test('`next` parameter can contain nulls and undefined objects', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello1')), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello2')), [entry1, null, undefined])
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      test('throws an error if ipfs is not defined', async () => {
        let err
        try {
          await Entry.create(undefined, undefined, undefined, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Ipfs instance not defined')
      })

      test('throws an error if identity are not defined', async () => {
        let err
        try {
          await Entry.create(ipfs, null, 'A', new Uint8Array(Buffer.from('hello2')), [])
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Identity is required, cannot create entry')
      })

      test('throws an error if id is not defined', async () => {
        let err
        try {
          await Entry.create(ipfs, testIdentity, null, new Uint8Array(Buffer.from('hello')), [])
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Entry requires an id')
      })

      test('throws an error if data is not defined', async () => {
        let err
        try {
          await Entry.create(ipfs, testIdentity, 'A', null, [])
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Entry requires data')
      })

      test('throws an error if next is not an array', async () => {
        let err
        try {
          await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')), {} as any)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, '\'next\' argument is not an array')
      })
    })

    describe('toMultihash', () => {
      test('returns an ipfs multihash', async () => {
        const expectedMultihash = 'zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB'
        const entry = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')), [])
        const multihash = await Entry.toMultihash(ipfs, entry)
        assert.strictEqual(multihash, expectedMultihash)
      })

      test('returns the correct ipfs multihash for a v0 entry', async () => {
        const expectedMultihash = 'Qmc2DEiLirMH73kHpuFPbt3V65sBrnDWkJYSjUQHXXvghT'
        const entry = v0Entries.hello
        const multihash = await Entry.toMultihash(ipfs, entry)
        assert.strictEqual(multihash, expectedMultihash)
      })

      test('returns the correct ipfs multihash for a v1 entry', async () => {
        const entry = v1Entries[0]
        const expectedMultihash = 'zdpuAsJDrLKrAiU8M518eu6mgv9HzS3e1pfH5XC7LUsFgsK5c'
        const e = Entry.toEntry(entry as any)
        const multihash = await Entry.toMultihash(ipfs, e)
        assert.strictEqual(expectedMultihash, entry.hash)
        assert.strictEqual(multihash, expectedMultihash)
      })

      test('throws an error if ipfs is not defined', async () => {
        let err
        try {
          await Entry.toMultihash(undefined, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Ipfs instance not defined')
      })

      test('throws an error if the object being passed is invalid', async () => {
        let err
        try {
          const entry = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')), [])
          delete entry.data.clock
          await Entry.toMultihash(ipfs, entry)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Invalid object format, cannot generate entry hash')
      })
    })

    describe('fromMultihash', () => {
      test('creates a entry from ipfs hash', async () => {
        const expectedHash = 'zdpuAnRGWKPkMHqumqdkRJtzbyW6qAGEiBRv61Zj3Ts4j9tQF'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [entry1])
        const final = await Entry.fromMultihash(ipfs, entry2.hash)

        assert.deepStrictEqual({ ...entry2, identity: { ...entry2.data.identity, signatures: { ...entry2.data.identity.signatures } } }, final)
        assert.strictEqual(final.data.id, 'A')
        assertPayload(final.data.payload, payload2)
        assert.strictEqual(final.next.length, 1)
        assert.strictEqual(final.next[0], entry1.hash)
        assert.strictEqual(final.hash, expectedHash)
      })
      /* 
            test('creates a entry from ipfs multihash of v0 entries', async () => {
              const expectedHash = 'QmZ8va2fSjRufV1sD6x5mwi6E5GrSjXHx7RiKFVBzkiUNZ'
              const entry1Hash = await io.write(ipfs, 'dag-pb', Entry.toEntry(v0Entries.helloWorld))
              const entry2Hash = await io.write(ipfs, 'dag-pb', Entry.toEntry(v0Entries.helloAgain))
              const final = await Entry.fromMultihash(ipfs, entry2Hash)
      
              assert.strictEqual(final.data.id, 'A')
              assertPayload(final.data.payload, v0Entries.helloAgain.payload)
              assert.strictEqual(final.next.length, 1)
              assert.strictEqual(final.next[0], v0Entries.helloAgain.next[0])
              assert.strictEqual(final.next[0], entry1Hash)
              assert.strictEqual(final.data.v, 0)
              assert.strictEqual(final.hash, entry2Hash)
              assert.strictEqual(final.hash, expectedHash)
            })
      
            test('creates a entry from ipfs multihash of v1 entries', async () => {
              const expectedHash = 'zdpuAxgKyiM9qkP9yPKCCqrHer9kCqYyr7KbhucsPwwfh6JB3'
              const e1 = v1Entries[0]
              const e2 = v1Entries[1]
              const entry1Hash = await io.write(ipfs, 'dag-cbor', Entry.toEntry(e1 as any), { links: Entry.IPLD_LINKS })
              const entry2Hash = await io.write(ipfs, 'dag-cbor', Entry.toEntry(e2 as any), { links: Entry.IPLD_LINKS })
              const final = await Entry.fromMultihash(ipfs, entry2Hash)
              assert.strictEqual(final.data.id, 'A')
              assertPayload(final.data.payload, e2.payload)
              assert.strictEqual(final.next.length, 1)
              assert.strictEqual(final.next[0], e2.next[0])
              assert.strictEqual(final.next[0], entry1Hash)
              assert.strictEqual(final.data.v, 1)
              assert.strictEqual(final.hash, entry2Hash)
              assert.strictEqual(entry2Hash, expectedHash)
            }) */

      test('should return an entry interopable with older and newer versions', async () => {
        const expectedHashV1 = 'zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB'
        const entryV1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')), [])
        const finalV1 = await Entry.fromMultihash(ipfs, entryV1.hash)
        assert.strictEqual(finalV1.hash, expectedHashV1)
        assert.strictEqual(Object.assign({}, finalV1).hash, expectedHashV1)

        const expectedHashV0 = 'QmenUDpFksTa3Q9KmUJYjebqvHJcTF2sGQaCH7orY7bXKC'
        const entryHashV0 = await io.write(ipfs, 'dag-pb', v0Entries.helloWorld)
        const finalV0 = await Entry.fromMultihash(ipfs, entryHashV0)
        assert.strictEqual(finalV0.hash, expectedHashV0)
        assert.strictEqual(Object.assign({}, finalV0).hash, expectedHashV0)
      })

      test('throws an error if ipfs is not present', async () => {
        let err
        try {
          await Entry.fromMultihash(undefined as any, undefined)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Ipfs instance not defined')
      })

      test('throws an error if hash is undefined', async () => {
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
      test('returns true if entry has a child', async () => {
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [entry1])
        assert.strictEqual(Entry.isParent(entry1, entry2), true)
      })

      test('returns false if entry does not have a child', async () => {
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [])
        const entry3 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [entry2])
        assert.strictEqual(Entry.isParent(entry1, entry2), false)
        assert.strictEqual(Entry.isParent(entry1, entry3), false)
        assert.strictEqual(Entry.isParent(entry2, entry3), true)
      })
    })

    describe('compare', () => {
      test('returns true if entries are the same', async () => {
        const payload1 = 'hello world'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        assert.strictEqual(Entry.isEqual(entry1, entry2), true)
      })

      test('returns true if entries are not the same', async () => {
        const payload1 = 'hello world1'
        const payload2 = 'hello world2'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload1)), [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from(payload2)), [])
        assert.strictEqual(Entry.isEqual(entry1, entry2), false)
      })
    })

    describe('isEntry', () => {
      test('is an Entry', async () => {
        const entry = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('hello')), [])
        assert.strictEqual(Entry.isEntry(entry), true)
      })

      test('is an Entry (v0)', async () => {
        assert.strictEqual(Entry.isEntry(v0Entries.hello), true)
      })

      test('is not an Entry - no id', async () => {
        const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123, seq: 0 }, next: [], }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      test('is not an Entry - no seq', async () => {
        const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      test('is not an Entry - no next', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, payload: 123 }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      test('is not an Entry - no version', async () => {
        const fakeEntry = { data: { id: 'A', payload: 123, seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      test('is not an Entry - no hash', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, payload: 123, seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })

      test('is not an Entry - no payload', async () => {
        const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, next: [] }
        assert.strictEqual(Entry.isEntry(fakeEntry as any), false)
      })
    })
  })
})
