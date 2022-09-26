const assert = require('assert')
const rmrf = require('rimraf')
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { IPFSAccessController } from '../ipfs-access-controller'
import { Ed25519PublicKeyData } from '@dao-xyz/identity';

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')
const API = 'js-ipfs';
const dbPath1 = './orbitdb/tests/ipfs-access-controller/1'
const dbPath2 = './orbitdb/tests/ipfs-access-controller/2'

describe(`orbit-db - IPFSAccessController`, function () {
  jest.setTimeout(config.timeout)

  let ipfsd1, ipfsd2, ipfs1, ipfs2, signKey1: SignKeyWithMeta, signKey2: SignKeyWithMeta
  let orbitdb1: OrbitDB, orbitdb2: OrbitDB

  beforeAll(async () => {
    rmrf.sync(dbPath1)
    rmrf.sync(dbPath2)
    ipfsd1 = await startIpfs(API, config.daemon1)
    ipfsd2 = await startIpfs(API, config.daemon2)

    ipfs1 = ipfsd1.api
    ipfs2 = ipfsd2.api

    const keystore1 = new Keystore(dbPath1 + '/keys')
    const keystore2 = new Keystore(dbPath2 + '/keys')

    signKey1 = await keystore1.createKey(new Uint8Array([0]), SignKeyWithMeta)
    signKey2 = await keystore2.createKey(new Uint8Array([1]), SignKeyWithMeta)

    orbitdb1 = await OrbitDB.createInstance(ipfs1, {
      directory: dbPath1,
      publicKey: new Ed25519PublicKeyData({ publicKey: signKey1.publicKey }),
      sign: (data) => Keystore.sign(data, signKey1)
    })

    orbitdb2 = await OrbitDB.createInstance(ipfs2, {
      directory: dbPath2,
      publicKey: new Ed25519PublicKeyData({ publicKey: signKey2.publicKey }),
      sign: (data) => Keystore.sign(data, signKey2)
    })
  })

  afterAll(async () => {
    if (orbitdb1) {
      await orbitdb1.stop()
    }

    if (orbitdb2) {
      await orbitdb2.stop()
    }

    if (ipfsd1) {
      await stopIpfs(ipfsd1)
    }

    if (ipfsd2) {
      await stopIpfs(ipfsd2)
    }
  })

  describe('Constructor', function () {
    let accessController: IPFSAccessController<any>

    beforeAll(async () => {
      accessController = new IPFSAccessController({
        write: [signKey1.publicKey.toString('base64')]
      });
    })

    it('creates an access controller', () => {
      assert.notStrictEqual(accessController, null)
      assert.notStrictEqual(accessController, undefined)
    })

    it('sets the controller type', () => {
      assert(accessController instanceof IPFSAccessController)
    })

    it('has IPFS instance', async () => {
      const peerId1 = await accessController._ipfs.id()
      const peerId2 = await ipfs1.id()
      expect(peerId1.id).toEqual(peerId2.id)
    })

    it('sets default capabilities', async () => {
      assert.deepStrictEqual(accessController.write, [signKey1.publicKey.toString('base64')])
    })

    it('allows owner to append after creation', async () => {
      const mockEntry = {
        data: {
          publicKey: signKey1.publicKey
        }
        // ...
        // doesn't matter what we put here, only identity is used for the check
      }
      const canAppend = await accessController.canAppend(mockEntry.data as any, new DecryptedThing({
        value: new Ed25519PublicKeyData({
          publicKey: signKey1.publicKey
        })
      }))
      expect(canAppend).toEqual(true)
    })
  })

  describe('save and load', function () {
    let accessController, manifest

    beforeAll(async () => {
      accessController = new IPFSAccessController({
        write: ['A', 'B', signKey1.publicKey.toString('base64')]
      });
      await accessController.init(orbitdb1._ipfs, undefined, orbitdb1.identity, undefined);

      manifest = await accessController.save()
      await accessController.load(manifest.address)
    })

    it('has correct capabalities', async () => {
      assert.deepStrictEqual(accessController.write, ['A', 'B', signKey1.publicKey.toString('base64')])
    })
  })
})

