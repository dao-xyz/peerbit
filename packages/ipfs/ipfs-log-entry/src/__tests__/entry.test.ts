import assert from 'assert';
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Entry, Payload, Identity } from '../entry.js';
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { deserialize, serialize } from '@dao-xyz/borsh';
import { Ed25519Keypair, Ed25519PublicKey, X25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import sodium from 'libsodium-wrappers';
// Test utils
import { jest } from '@jest/globals';
import {
  nodeConfig as config,
  testAPIs,
  Session
} from '@dao-xyz/orbit-db-test-utils'
import { IPFS } from 'ipfs-core-types';
import { dirname } from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const identityFromSignKey = (key: KeyWithMeta<Ed25519Keypair>): Identity => {
  if (!key) {
    throw new Error("Key not defined");
  }
  return {
    publicKey: key.keypair.publicKey,
    sign: async (data: Uint8Array) => (await key.keypair.sign(data)).signature
  }
}
Object.keys(testAPIs).forEach((api) => {
  let session: Session, ipfs: IPFS
  describe('Entry', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
    let keystore: Keystore, signingKeystore: Keystore, signKey: KeyWithMeta<Ed25519Keypair>

    beforeAll(async () => {
      session = await Session.connected(1, api, config.defaultIpfsConfig);
      ipfs = session.peers[0].ipfs;
      await sodium.ready;

      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(await createStore(identityKeysPath))
      signingKeystore = new Keystore(await createStore(signingKeysPath))

      signKey = await signingKeystore.createEd25519Key({ id: new Uint8Array([0]) }) as KeyWithMeta<Ed25519Keypair>

      const signKeyLoaded = await signingKeystore.getKeyById(new Uint8Array([0])) as KeyWithMeta<Ed25519Keypair>
      const y = 123;
    })

    afterAll(async () => {
      await session.stop();
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await keystore?.close()
      await signingKeystore?.close()
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
        const expectedHash = 'zdpuAmgPgov4ACUDbjKY67jZZp3HNbPUbb8YYfHBkE38VPFBp'
        const entry = await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello'
        })
        expect(entry.hash).toEqual(expectedHash)
        expect(entry.gid).toEqual('A')
        assert.deepStrictEqual(entry.clock.id, new Ed25519PublicKey({
          publicKey: signKey.keypair.publicKey.publicKey
        }).bytes);
        expect(entry.clock.time).toEqual(0n)
        expect(entry.payload.value).toEqual('hello')
        expect(entry.next.length).toEqual(0)
      })

      it('creates a entry with payload', async () => {
        const expectedHash = 'zdpuAnFYGzmBbHLVtY9KuTeFLAy1e4xTBhj9tndVzWETRv6qk'
        const payload = 'hello world'
        const entry = await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: payload, next: []
        })
        expect(entry.hash).toEqual(expectedHash)
        expect(entry.payload.value).toEqual(payload)
        expect(entry.gid).toEqual('A')
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
              payload: receiverKey.keypair.publicKey,
              publicKey: receiverKey.keypair.publicKey
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
        expect(entry.payload.value).toEqual(payload);

        // We can not have a hash check because nonce of encryption will always change
        expect(entry.gid).toEqual('A')
        assert.deepStrictEqual(entry.clock.id, (new Ed25519PublicKey({ publicKey: signKey.keypair.publicKey.publicKey })).bytes)
        expect(entry.clock.time).toEqual(0n)
        expect(entry.next.length).toEqual(0)
      })

      it('creates a entry with payload and next', async () => {
        const expectedHash = 'zdpuAuRqSyYhfbaeQA9pqcvDNkKwe9MTgXLESw8WxAeMSwc3f'
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
        expect(entry2.payload.value).toEqual(payload2)
        expect(entry2.next.length).toEqual(1)
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

      it('can calculate join gid from `next`', async () => {
        const entry1A = await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'A', data: 'hello1', next: []
        })

        const entry1B = await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'B', data: 'hello1', next: []
        })

        const entry2 = await Entry.create({
          ipfs, identity: identityFromSignKey(signKey), gidSeed: 'Should not be used', data: 'hello2', next: [entry1A, entry1B]
        })
        expect(entry2.gid).toEqual((await Entry.createGid([entry1A.gid, entry1B.gid].sort().join())))
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
        } catch (e) {
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
        } catch (e) {
          err = e
        }
        expect(err.message).toEqual('\'next\' argument is not an array')
      })
    })

    describe('toMultihash', () => {

      it('returns an ipfs multihash', async () => {
        const expectedMultihash = 'zdpuArZfc1JLrXSuvAtDAUZQMX6nApFivKwpPoug4k8Yj57EX'
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
        } catch (e) {
          err = e
        }
        expect(err.message).toEqual('Invalid object format, cannot generate entry hash')
      }) */
    })

    describe('fromMultihash', () => {
      it('creates a entry from ipfs hash', async () => {
        const expectedHash = 'zdpuApQ8jABz9j2dzpLw57xKYxaQX7wLWzLfwDQpt13Pabrzi'
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
        expect(final.payload.value).toEqual(payload2)
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
        expect(Entry.isParent(entry1, entry2)).toEqual(true);
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
        expect(Entry.isParent(entry1, entry2)).toEqual(false);
        expect(Entry.isParent(entry1, entry1)).toEqual(false);
        expect(Entry.isParent(entry2, entry3)).toEqual(true);

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
        expect(Entry.isParent(entry1, entry2)).toEqual(true);
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
        expect(Entry.isParent(entry1, entry2)).toEqual(false);
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
})
