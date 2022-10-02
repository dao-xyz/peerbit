import rmrf from 'rimraf'
import { OrbitDB } from '@dao-xyz/orbit-db';
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import io from '@dao-xyz/io-utils'
import { EventStore } from './event-store';
import { IPFSAccessController } from '../ipfs-access-controller';
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto';
// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  connectPeers
} = require('@dao-xyz/orbit-db-test-utils')

const dbPath1 = './orbitdb/tests/orbitdb-access-controller-integration/1'
const dbPath2 = './orbitdb/tests/orbitdb-access-controller-integration/2'
const API = 'js-ipfs'

describe(`orbit-db - IPFSAccessController Integration`, function () {
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

    // Connect the peers manually to speed up test times
    const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
    await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })

    const keystore1 = new Keystore(dbPath1 + '/keys')
    const keystore2 = new Keystore(dbPath2 + '/keys')

    signKey1 = await keystore1.createKey(new Uint8Array([0]), SignKeyWithMeta)
    signKey2 = await keystore2.createKey(new Uint8Array([1]), SignKeyWithMeta)

    orbitdb1 = await OrbitDB.createInstance(ipfs1, {
      directory: dbPath1,
      publicKey: new Ed25519PublicKey({ publicKey: signKey1.publicKey }),
      sign: (data) => Keystore.sign(data, signKey1)
    })

    orbitdb2 = await OrbitDB.createInstance(ipfs2, {
      directory: dbPath2,
      publicKey: new Ed25519PublicKey({ publicKey: signKey2.publicKey }),
      sign: (data) => Keystore.sign(data, signKey2)
    })
  })

  afterAll(async () => {
    if (orbitdb1) { await orbitdb1.stop() }

    if (orbitdb2) { await orbitdb2.stop() }

    if (ipfsd1) { await stopIpfs(ipfsd1) }

    if (ipfsd2) { await stopIpfs(ipfsd2) }
  })

  describe('OrbitDB Integration', function () {
    let db: EventStore<string>, db2: EventStore<string>
    let dbManifest, acManifest

    beforeAll(async () => {
      db = await orbitdb1.open(new EventStore<string>({
        name: 'AABB',
        accessController: new IPFSAccessController({
          write: [signKey1.publicKey.toString('base64')]
        })
      }));

      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db.address))
      await db2.load()

      dbManifest = await io.read(ipfs1, db.address.root)
      const hash = dbManifest.accessController.split('/').pop()
      acManifest = await io.read(ipfs1, hash)
    })

    it('has the correct access rights after creating the database', async () => {
      expect((db.accessController as any as IPFSAccessController<string>).write).toEqual([signKey1.publicKey.toString('base64')])
    })

    it('makes database use the correct access controller', async () => {
      const { address } = await (db.accessController as IPFSAccessController<any>).save()
      expect(acManifest.params.address).toEqual(address)
    })

    it('saves database manifest file locally', async () => {
      expect(dbManifest)
    })

    it('saves access controller manifest file locally', async () => {
      expect(acManifest)
    })

    it('has correct type', async () => {
      expect(acManifest.type).toEqual('ipfs')
    })

    describe('database manifest', () => {
      it('has correct name', async () => {
        expect(dbManifest.name).toEqual('AABB')
      })

      it('has correct type', async () => {
        expect(dbManifest.type).toEqual('feed')
      })

      it('has correct address', async () => {
        expect(dbManifest.accessController)
        expect(dbManifest.accessController.indexOf('/ipfs')).toEqual(0)
      })
    })

    describe('access controls', () => {
      it('allows to write if user has write access', async () => {
        let err
        try {
          await db.add('hello?')
        } catch (e) {
          err = e.toString()
        }

        const res = await db.iterator().collect().map(e => e.payload.value.value)
        expect(err).toEqual(undefined)
        expect(res).toEqual(['hello?'])
      })

      it('doesn\'t allow to write without write access', async () => {
        let err
        try {
          await db2.add('hello!!')
          fail();
        } catch (e) {
          err = e
        }

        const res = await db2.iterator().collect().map(e => e.payload.value.value)
        expect(err.message).toEqual(`Could not append Entry<T>, key "${db2.publicKey}" is not allowed to write to the log`)
        expect(res.includes('hello!!')).toEqual(false)
      })
    })
  })
})
  // TODO: use two separate peers for testing the AC
  // TODO: add tests for revocation correctness with a database (integration tests)
