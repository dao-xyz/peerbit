import { OrbitDB } from "../orbit-db"

const fs = require('fs')
import assert from 'assert'
import rmrf from 'rimraf'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { EventStore } from "./utils/stores"
import { SimpleAccessController } from "./utils/access"
import { Level } from "level"
import { Ed25519PublicKey } from "@dao-xyz/peerbit-crypto"


// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
} = require('@dao-xyz/orbit-db-test-utils')

const keysPath = './orbitdb/identity/identitykeys'
const dbPath = './orbitdb/tests/change-identity'

export const createStore = (path = './keystore'): Level => {
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path, { recursive: true })
  }
  return new Level(path, { valueEncoding: 'view' })
}

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Set identities (${API})`, function () {
    jest.setTimeout(config.timeout)

    let ipfsd: Controller, ipfs: IPFS, orbitdb: OrbitDB, keystore: Keystore, options
    let signKey1: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api

      if (fs && fs.mkdirSync) fs.mkdirSync(keysPath, { recursive: true })
      const identityStore = await createStore(keysPath)

      keystore = new Keystore(identityStore)
      signKey1 = await keystore.getKey(new Uint8Array([0])) as KeyWithMeta<Ed25519Keypair>;;
      signKey2 = await keystore.getKey(new Uint8Array([1])) as KeyWithMeta<Ed25519Keypair>;;
      orbitdb = await OrbitDB.createInstance(ipfs, { directory: dbPath })
    })

    afterAll(async () => {
      await keystore.close()
      if (orbitdb)
        await orbitdb.stop()

      if (ipfsd)
        await stopIpfs(ipfsd)
    })

    beforeEach(async () => {
      options = Object.assign({}, options, {})
    })

    it('sets identity', async () => {
      const db = await orbitdb.open(new EventStore<string>({
        name: 'abc',
        accessController: new SimpleAccessController()
      }), options)
      expect(db.publicKey).toEqual(orbitdb.identity)
      db.setPublicKey(new Ed25519PublicKey({
        publicKey: signKey1.publicKey
      }))
      expect(db.publicKey).toEqual(signKey1.publicKey)
      await db.close()
    })
  })
})
