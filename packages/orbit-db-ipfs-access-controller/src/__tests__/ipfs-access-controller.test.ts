const assert = require('assert')
const rmrf = require('rimraf')
import { Identities as IdentityProvider, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { IPFSAccessController } from '../ipfs-access-controller'

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

  let ipfsd1, ipfsd2, ipfs1, ipfs2, id1: Identity, id2: Identity
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

    id1 = await IdentityProvider.createIdentity({ id: new Uint8Array([0]), keystore: keystore1 })
    id2 = await IdentityProvider.createIdentity({ id: new Uint8Array([1]), keystore: keystore2 })

    orbitdb1 = await OrbitDB.createInstance(ipfs1, {
      directory: dbPath1,
      identity: id1
    })

    orbitdb2 = await OrbitDB.createInstance(ipfs2, {
      directory: dbPath2,
      identity: id2
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
        write: [id1.id]
      });
      await accessController.init(orbitdb1._ipfs, orbitdb1.identity, {});
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
      assert.strictEqual(peerId1.id, peerId2.id)
    })

    it('sets default capabilities', async () => {
      assert.deepStrictEqual(accessController.write, [Buffer.from(id1.id).toString('base64')])
    })

    it('allows owner to append after creation', async () => {
      const mockEntry = {
        data: {
          identity: id1
        }
        // ...
        // doesn't matter what we put here, only identity is used for the check
      }
      const canAppend = await accessController.canAppend(mockEntry.data as any, new DecryptedThing({
        value: id1.toSerializable()
      }), id1.provider)
      assert.strictEqual(canAppend, true)
    })
  })

  describe('save and load', function () {
    let accessController, manifest

    beforeAll(async () => {
      accessController = new IPFSAccessController({
        write: ['A', 'B', id1.id]
      });
      await accessController.init(orbitdb1._ipfs, undefined, orbitdb1.identity, undefined);

      manifest = await accessController.save()
      await accessController.load(manifest.address)
    })

    it('has correct capabalities', async () => {
      assert.deepStrictEqual(accessController.write, ['A', 'B', Buffer.from(id1.id).toString('base64')])
    })
  })
})

