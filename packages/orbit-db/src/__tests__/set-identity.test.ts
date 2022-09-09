import { Identities } from "@dao-xyz/orbit-db-identity-provider"
import { OrbitDB } from "../orbit-db"

const fs = require('fs')
const assert = require('assert')
const rmrf = require('rimraf')
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { EventStore } from "./utils/stores"
import { SimpleAccessController } from "./utils/access"
import { Level } from "level"


// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
} = require('orbit-db-test-utils')

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

    let ipfsd, ipfs, orbitdb: OrbitDB, keystore, options
    let identity1, identity2

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api

      if (fs && fs.mkdirSync) fs.mkdirSync(keysPath, { recursive: true })
      const identityStore = await createStore(keysPath)

      keystore = new Keystore(identityStore)
      identity1 = await Identities.createIdentity({ id: new Uint8Array([0]), keystore })
      identity2 = await Identities.createIdentity({ id: new Uint8Array([1]), keystore })
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
      assert.equal(db.identity, orbitdb.identity)
      db.setIdentity(identity1)
      assert.equal(db.identity, identity1)
      await db.close()
    })
  })
})
