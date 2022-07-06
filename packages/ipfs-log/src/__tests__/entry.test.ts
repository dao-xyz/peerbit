import { DefaultAccessController } from '../default-access-controller'
import { Entry } from '../signable'
export const io = require('orbit-db-io')
const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')

const IdentityProvider = require('orbit-db-identity-provider')
import * as v0Entries from './fixtures/v0-entries.fixture'
import * as v1Entries from './fixtures/v1-entries.fixture.json'
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

      testIdentity = await IdentityProvider.createIdentity({ id: 'userA', keystore, signingKeystore })
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
        const expectedHash = 'zdpuAs2iZUuNRTHLRFQ5Mp82wUNYEXshAmjfqwQWj99PvMSaA'
        const entry = await Entry.create(ipfs, testIdentity, 'A', 'hello')
        assert.strictEqual(entry.hash, expectedHash)
        assert.strictEqual(entry.id, 'A')
        assert.strictEqual(entry.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.clock.time, 0)
        assert.strictEqual(entry.payload.toString(), 'hello')
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
      })

      test('creates a entry with payload', async () => {
        const expectedHash = 'zdpuAwcWMMEvk5TFVuwKS5Ymd89jfNPxBTto8A2sobgsEPYGu'
        const payload = 'hello world'
        const entry = await Entry.create(ipfs, testIdentity, 'A', payload, [])
        assert.strictEqual(entry.payload.toString(), payload)
        assert.strictEqual(entry.id, 'A')
        assert.strictEqual(entry.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry.clock.time, 0)
        assert.strictEqual(entry.next.length, 0)
        assert.strictEqual(entry.refs.length, 0)
        assert.strictEqual(entry.hash, expectedHash)
      })

      test('creates a entry with payload and next', async () => {
        const expectedHash = 'zdpuB1b5DipCQSkCYbJJvqX8zH9KkMqA29pYW3xAst3WnBotx'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        entry1.clock.tick()
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload2, [entry1], entry1.clock)
        assert.strictEqual(entry2.payload.toString(), payload2)
        assert.strictEqual(entry2.next.length, 1)
        assert.strictEqual(entry2.hash, expectedHash)
        assert.strictEqual(entry2.clock.id, testIdentity.publicKey)
        assert.strictEqual(entry2.clock.time, 1)
      })

      test('`next` parameter can be an array of strings', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', 'hello1', [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', 'hello2', [entry1.hash])
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      test('`next` parameter can be an array of Entry instances', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', 'hello1', [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', 'hello2', [entry1])
        assert.strictEqual(typeof entry2.next[0] === 'string', true)
      })

      test('`next` parameter can contain nulls and undefined objects', async () => {
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', 'hello1', [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', 'hello2', [entry1, null, undefined])
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
          await Entry.create(ipfs, null, 'A', 'hello2', [])
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Identity is required, cannot create entry')
      })

      test('throws an error if id is not defined', async () => {
        let err
        try {
          await Entry.create(ipfs, testIdentity, null, 'hello', [])
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
          await Entry.create(ipfs, testIdentity, 'A', 'hello', {} as any)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, '\'next\' argument is not an array')
      })
    })

    describe('toMultihash', () => {
      test('returns an ipfs multihash', async () => {
        const expectedMultihash = 'zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB'
        const entry = await Entry.create(ipfs, testIdentity, 'A', 'hello', [])
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
          const entry = await Entry.create(ipfs, testIdentity, 'A', 'hello', [])
          delete entry.clock
          await Entry.toMultihash(ipfs, entry)
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'Invalid object format, cannot generate entry hash')
      })
    })

    describe('fromMultihash', () => {
      test('creates a entry from ipfs hash', async () => {
        const expectedHash = 'zdpuAvW6MgeAHbD5mKz17mpeqxodRvCU3tYWa5TEvQDSiexPP'
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload2, [entry1])
        const final = await Entry.fromMultihash(ipfs, entry2.hash)

        assert.deepStrictEqual(entry2, final)
        assert.strictEqual(final.id, 'A')
        assert.strictEqual(Buffer.from(final.payload).toString(), payload2)
        assert.strictEqual(final.next.length, 1)
        assert.strictEqual(final.next[0], entry1.hash)
        assert.strictEqual(final.hash, expectedHash)
      })

      test('creates a entry from ipfs multihash of v0 entries', async () => {
        const expectedHash = 'QmUKMoRrmsYAzQg1nQiD7Fzgpo24zXky7jVJNcZGiSAdhc'
        const entry1Hash = await io.write(ipfs, 'dag-pb', Entry.toEntry(v0Entries.helloWorld))
        const entry2Hash = await io.write(ipfs, 'dag-pb', Entry.toEntry(v0Entries.helloAgain))
        const final = await Entry.fromMultihash(ipfs, entry2Hash)

        assert.strictEqual(final.id, 'A')
        assert.strictEqual(final.payload, v0Entries.helloAgain.payload)
        assert.strictEqual(final.next.length, 1)
        assert.strictEqual(final.next[0], v0Entries.helloAgain.next[0])
        assert.strictEqual(final.next[0], entry1Hash)
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
        assert.strictEqual(final.id, 'A')
        assert.strictEqual(final.payload, e2.payload)
        assert.strictEqual(final.next.length, 1)
        assert.strictEqual(final.next[0], e2.next[0])
        assert.strictEqual(final.next[0], entry1Hash)
        assert.strictEqual(final.hash, entry2Hash)
        assert.strictEqual(entry2Hash, expectedHash)
      })

      test('should return an entry interopable with older and newer versions', async () => {
        const expectedHashV1 = 'zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB'
        const entryV1 = await Entry.create(ipfs, testIdentity, 'A', 'hello', [])
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
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload2, [entry1])
        assert.strictEqual(Entry.isParent(entry1, entry2), true)
      })

      test('returns false if entry does not have a child', async () => {
        const payload1 = 'hello world'
        const payload2 = 'hello again'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload2, [])
        const entry3 = await Entry.create(ipfs, testIdentity, 'A', payload2, [entry2])
        assert.strictEqual(Entry.isParent(entry1, entry2), false)
        assert.strictEqual(Entry.isParent(entry1, entry3), false)
        assert.strictEqual(Entry.isParent(entry2, entry3), true)
      })
    })

    describe('compare', () => {
      test('returns true if entries are the same', async () => {
        const payload1 = 'hello world'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        assert.strictEqual(Entry.isEqual(entry1, entry2), true)
      })

      test('returns true if entries are not the same', async () => {
        const payload1 = 'hello world1'
        const payload2 = 'hello world2'
        const entry1 = await Entry.create(ipfs, testIdentity, 'A', payload1, [])
        const entry2 = await Entry.create(ipfs, testIdentity, 'A', payload2, [])
        assert.strictEqual(Entry.isEqual(entry1, entry2), false)
      })
    })

    /*  describe('isEntry', () => {
       test('is an Entry', async () => {
         const entry = await Entry.create(ipfs, testIdentity, 'A', 'hello', [])
         assert.strictEqual(Entry.isEntry(entry), true)
       })
 
       test('is an Entry (v0)', async () => {
         assert.strictEqual(Entry.isEntry(v0Entries.hello), true)
       })
 
       test('is not an Entry - no id', async () => {
         const fakeEntry = { next: [], v: 1, hash: 'Foo', payload: 123, seq: 0 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
 
       test('is not an Entry - no seq', async () => {
         const fakeEntry = { next: [], v: 1, hash: 'Foo', payload: 123 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
 
       test('is not an Entry - no next', async () => {
         const fakeEntry = { id: 'A', v: 1, hash: 'Foo', payload: 123, seq: 0 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
 
       test('is not an Entry - no version', async () => {
         const fakeEntry = { id: 'A', next: [], payload: 123, seq: 0 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
 
       test('is not an Entry - no hash', async () => {
         const fakeEntry = { id: 'A', v: 1, next: [], payload: 123, seq: 0 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
 
       test('is not an Entry - no payload', async () => {
         const fakeEntry = { id: 'A', v: 1, next: [], hash: 'Foo', seq: 0 }
         assert.strictEqual(Entry.isEntry(fakeEntry), false)
       })
     }) */
  })
})
