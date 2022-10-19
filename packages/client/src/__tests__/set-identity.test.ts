import { OrbitDB } from "../orbit-db"

import fs from 'fs'
import rmrf from 'rimraf'
import { Keystore, KeyWithMeta } from '@dao-xyz/peerbit-keystore'
import { EventStore } from "./utils/stores"

import { Level } from "level"
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto"
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";

// Include test utilities
import {
  nodeConfig as config,
  startIpfs,
  stopIpfs,
  testAPIs,
} from '@dao-xyz/peerbit-test-utils'

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

    let ipfsd: Controller, ipfs: IPFS, orbitdb: OrbitDB, keystore: Keystore, options: any
    let signKey1: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api

      if (fs && fs.mkdirSync) fs.mkdirSync(keysPath, { recursive: true })
      const identityStore = await createStore(keysPath)

      keystore = new Keystore(identityStore)
      signKey1 = await keystore.createEd25519Key() as KeyWithMeta<Ed25519Keypair>;;
      signKey2 = await keystore.createEd25519Key() as KeyWithMeta<Ed25519Keypair>;;
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

      }), options)
      expect(db.store.identity.publicKey.equals(orbitdb.identity.publicKey))
      db.store.setIdentity({
        publicKey: signKey1.keypair.publicKey,
        privateKey: signKey1.keypair.privateKey,
        sign: (data) => signKey1.keypair.sign(data)
      })
      expect(db.store.identity.publicKey.equals(signKey1.keypair.publicKey))
      await db.store.close()
    })
  })
})
