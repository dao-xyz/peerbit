import assert from 'assert';
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Entry, Payload } from '../entry.js';
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { deserialize, serialize } from '@dao-xyz/borsh';
import { Ed25519Keypair, Ed25519PublicKey, X25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import sodium from 'libsodium-wrappers';
// Test utils
import { jest } from '@jest/globals';
import {
  nodeConfig as config,
  Session
} from '@dao-xyz/orbit-db-test-utils'
import { IPFS } from 'ipfs-core-types';
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import path from 'path';
import { Identity } from '../identity.js';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

const identityFromSignKey = (key: KeyWithMeta<Ed25519Keypair>): Identity => {
  if (!key) {
    throw new Error("Key not defined");
  }
  return {
    ...key.keypair,
    sign: async (data: Uint8Array) => (await key.keypair.sign(data))
  }
}
const API = 'js-ipfs';

describe('Entry', function () {
  jest.setTimeout(config.timeout)
  let session: Session, ipfs: IPFS

  const { signingKeyFixtures, signingKeysPath } = config
  let keystore: Keystore, signKey: KeyWithMeta<Ed25519Keypair>

  beforeAll(async () => {
    await sodium.ready;
    session = await Session.connected(1, API, config.defaultIpfsConfig);
    ipfs = session.peers[0].ipfs;
    await sodium.ready;


    await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

    keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)));
    await keystore.waitForOpen();

    signKey = await keystore.getKey(new Uint8Array([0])) as KeyWithMeta<Ed25519Keypair>
  })

  afterAll(async () => {
    await session.stop();

    await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

    rmrf.sync(signingKeysPath(__filenameBase))
    await keystore?.close()

  })
  describe('endocing', () => {
    it('can serialize and deserialialize', async () => {

      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello'
      })
      deserialize(serialize(entry), Entry)
    })
  })

  describe('create', () => {
    it('creates a an empty entry', async () => {
      const expectedHash = 'zdpuAodEgpKQpFjAgCtJPYLDFjLrYJ56j69MvhS7gdrvEVisW'
      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello'
      })

      expect(entry.hash).toEqual(expectedHash)
      expect(entry.gid).toEqual(await sodium.crypto_generichash(32, 'A'))
      assert.deepStrictEqual(entry.clock.id, new Ed25519PublicKey({
        publicKey: signKey.keypair.publicKey.publicKey
      }).bytes);
      expect(entry.clock.time).toEqual(0n)
      expect(entry.payload.getValue()).toEqual('hello')
      expect(entry.next.length).toEqual(0)
    })

    it('creates a entry with payload', async () => {
      const expectedHash = 'zdpuAotgKm27uJe3j1dJTHi29P9Kunt1oKNDdcutcjY4gWY8k'
      const payload = 'hello world'
      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload, next: []
      })
      expect(entry.hash).toEqual(expectedHash)
      expect(entry.payload.getValue()).toEqual(payload)
      expect(entry.gid).toEqual(await sodium.crypto_generichash(32, 'A'))
      assert.deepStrictEqual(entry.clock.id, new Ed25519PublicKey({
        publicKey: signKey.keypair.publicKey.publicKey
      }).bytes);
      expect(entry.clock.time).toEqual(0n)
      expect(entry.next.length).toEqual(0)
    })

    it('creates a encrypted entry with payload', async () => {

      const payload = 'hello world'
      const senderKey = await keystore.createX25519Key({ id: 'sender', overwrite: true });
      const receiverKey = await keystore.createX25519Key({ id: 'reciever', overwrite: true });
      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload, next: [], encryption: {
          reciever: {
            clock: undefined,
            signature: undefined,
            payload: receiverKey.keypair.publicKey
          },
          options: {
            getEncryptionKeypair: () => Promise.resolve(senderKey.keypair as Ed25519Keypair | X25519Keypair),
            getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
              for (let i = 0; i < publicKeys.length; i++) {
                if (publicKeys[i].equals((senderKey.keypair as X25519Keypair).publicKey)) {
                  return {
                    index: i,
                    keypair: senderKey.keypair as Ed25519Keypair | X25519Keypair
                  }
                }
                if (publicKeys[i].equals((receiverKey.keypair as X25519Keypair).publicKey)) {
                  return {
                    index: i,
                    keypair: receiverKey.keypair as Ed25519Keypair | X25519Keypair
                  }
                }

              }
            }
          }
        }
      })
      assert(entry.payload instanceof Payload)
      expect(entry.payload.getValue()).toEqual(payload);

      // We can not have a hash check because nonce of encryption will always change
      expect(entry.gid).toEqual(await sodium.crypto_generichash(32, 'A'))
      assert.deepStrictEqual(entry.clock.id, (new Ed25519PublicKey({ publicKey: signKey.keypair.publicKey.publicKey })).bytes)
      expect(entry.clock.time).toEqual(0n)
      expect(entry.next.length).toEqual(0)
    })

    it('creates a entry with payload and next', async () => {
      const expectedHash = 'zdpuAvEB1kk7M6ZhrhV5eG85pKEiArEbHV2EFgA37qcuk5w1p'
      const payload1 = 'hello world'
      const payload2 = 'hello again'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const clock = entry1.clock;
      /* entry1.metadata = new MetadataSecure({
        metadata: new Metadata({
          id: await entry1.metadata.id,
          identity: await entry1.metadata.identity,
          signature: await entry1.metadata.signature
        })
      }) */
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: [entry1]
      })
      expect(entry2.payload.getValue()).toEqual(payload2)
      expect(entry2.next.length).toEqual(1)
      expect(entry2.maxChainLength).toEqual(2n); // because 1 next
      expect(entry2.hash).toEqual(expectedHash)
      assert.deepStrictEqual(entry2.clock.id, new Ed25519PublicKey({
        publicKey: signKey.keypair.publicKey.publicKey
      }).bytes);
      expect(entry2.clock.time).toEqual(1n)
    })

    it('`next` parameter can be an array of strings', async () => {
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello2', next: [entry1]
      })
      assert.strictEqual(typeof entry2.next[0] === 'string', true)
    })

    it('`next` parameter can be an array of Entry instances', async () => {
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello2', next: [entry1]
      })
      assert.strictEqual(typeof entry2.next[0] === 'string', true)
    })

    it('can calculate join gid from `next` max chain length', async () => {
      const entry0A = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
      })

      const entry1A = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: [entry0A]
      })

      const entry1B = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'B', clock: entry1A.clock, data: 'hello1', next: []
      })

      expect(entry1A.gid > entry1B.gid); // so that gid is not choosen because A has smaller gid
      expect(entry1A.clock.time).toEqual(entry1B.clock.time);

      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1A, entry1B]
      })
      expect(entry2.gid).toEqual(entry1A.gid) // because A has alonger chain
    })

    it('can calculate join gid from `next` max clock', async () => {

      const entry1A = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'B', data: 'hello1', next: []
      })

      const entry1B = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', clock: entry1A.clock.advance(), data: 'hello1', next: []
      })

      expect(entry1B.gid > entry1A.gid); // so that gid is not choosen because B has smaller gid
      expect(entry1A.maxChainLength).toEqual(entry1B.maxChainLength);
      expect(entry1A.clock.time).toEqual(entry1B.clock.time - 1n);

      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1A, entry1B]
      })
      expect(entry2.gid).toEqual(entry1B.gid) // because A has alonger chain
    })

    it('can calculate join gid from `next` gid comparison', async () => {

      const entry1A = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'B', data: 'hello1', next: []
      })

      const entry1B = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', clock: entry1A.clock, data: 'hello1', next: []
      })

      expect(entry1B.gid < entry1A.gid); // so that B is choosen because of gid
      expect(entry1A.maxChainLength).toEqual(entry1B.maxChainLength);
      expect(entry1A.clock.time).toEqual(entry1B.clock.time);

      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1A, entry1B]
      })
      expect(entry2.gid).toEqual(entry1B.gid) // because gid B < gid A
    })

    it('can calculate reuse gid from `next`', async () => {
      const entry1A = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
      })

      const entry1B = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gid: entry1A.gid, data: 'hello1', next: []
      })

      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1A, entry1B]
      })
      expect(entry2.gid).toEqual(entry1A.gid)
      expect(entry1A.gid).toEqual(entry1B.gid)
    })


    it('will use next for gid instaed of gidSeed', async () => {
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
      })


      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1]
      })
      expect(entry2.gid).toEqual(entry1.gid)

    })


    it('throws an error if data is not defined', async () => {
      let err: any
      try {
        await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: null, next: []
        })
      } catch (e: any) {
        err = e
      }
      expect(err.message).toEqual('Entry requires data')
    })

    it('throws an error if next is not an array', async () => {
      let err: any
      try {
        await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello', next: {} as any
        })
      } catch (e: any) {
        err = e
      }
      expect(err.message).toEqual('\'next\' argument is not an array')
    })
  })

  describe('toMultihash', () => {

    it('returns an ipfs multihash', async () => {
      const expectedMultihash = 'zdpuAodEgpKQpFjAgCtJPYLDFjLrYJ56j69MvhS7gdrvEVisW'
      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello', next: []
      })
      const multihash = await Entry.toMultihash(ipfs, entry)
      expect(multihash).toEqual(expectedMultihash)
    })


    /*  TODO what is the point of this test?
    
    it('throws an error if the object being passed is invalid', async () => {
      let err
      try {
        const entry = await Entry.create({ ipfs, identity: identityFromSignKey(signKey), gidSeed:   'A', data: 'hello', next: [] })
        delete ((entry.metadata as MetadataSecure)._metadata as DecryptedThing<Metadata>)
        await Entry.toMultihash(ipfs, entry)
      } catch (e: any) {
        err = e
      }
      expect(err.message).toEqual('Invalid object format, cannot generate entry hash')
    }) */
  })

  describe('fromMultihash', () => {
    it('creates a entry from ipfs hash', async () => {
      const expectedHash = 'zdpuAvEB1kk7M6ZhrhV5eG85pKEiArEbHV2EFgA37qcuk5w1p'
      const payload1 = 'hello world'
      const payload2 = 'hello again'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: [entry1]
      })
      const final = await Entry.fromMultihash<string>(ipfs, entry2.hash)
      final.init(entry2);
      assert(final.equals(entry2));
      expect(final.gid).toEqual((await sodium.crypto_generichash(32, 'A')))
      expect(final.payload.getValue()).toEqual(payload2)
      expect(final.next.length).toEqual(1)
      expect(final.next[0]).toEqual(entry1.hash)
      expect(final.hash).toEqual(expectedHash)
    })
  })

  describe('isParent', () => {
    it('returns true if entry has a child', async () => {
      const payload1 = 'hello world'
      const payload2 = 'hello again'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: [entry1]
      })
      expect(Entry.isDirectParent(entry1, entry2)).toEqual(true);
    })

    it('returns false if entry does not have a child', async () => {
      const payload1 = 'hello world'
      const payload2 = 'hello again'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: []
      })
      const entry3 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: [entry2]
      })
      expect(Entry.isDirectParent(entry1, entry2)).toEqual(false);
      expect(Entry.isDirectParent(entry1, entry1)).toEqual(false);
      expect(Entry.isDirectParent(entry2, entry3)).toEqual(true);

    })
  })

  describe('compare', () => {
    it('returns true if entries are the same', async () => {
      const payload1 = 'hello world'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      expect(Entry.isEqual(entry1, entry2)).toEqual(true);
    })

    it('returns true if entries are not the same', async () => {
      const payload1 = 'hello world1'
      const payload2 = 'hello world2'
      const entry1 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload1, next: []
      })
      const entry2 = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload2, next: []
      })
      expect(Entry.isEqual(entry1, entry2)).toEqual(false);
    })
  })

  describe('isEntry', () => {
    it('is an Entry', async () => {
      const entry = await Entry.create({
        ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello', next: []
      })
      expect(Entry.isEntry(entry)).toEqual(true)
    })

    it('is not an Entry - no id', async () => {
      const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123, seq: 0 }, next: [], }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })

    it('is not an Entry - no seq', async () => {
      const fakeEntry = { data: { v: 1, hash: 'Foo', payload: 123 }, next: [] }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })

    it('is not an Entry - no next', async () => {
      const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, payload: 123 }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })

    it('is not an Entry - no version', async () => {
      const fakeEntry = { data: { id: 'A', payload: 123, seq: 0 }, next: [] }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })

    it('is not an Entry - no hash', async () => {
      const fakeEntry = { data: { id: 'A', v: 1, payload: 123, seq: 0 }, next: [] }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })

    it('is not an Entry - no payload', async () => {
      const fakeEntry = { data: { id: 'A', v: 1, hash: 'Foo', seq: 0 }, next: [] }
      expect(Entry.isEntry(fakeEntry as any)).toEqual(false)
    })
  })
})