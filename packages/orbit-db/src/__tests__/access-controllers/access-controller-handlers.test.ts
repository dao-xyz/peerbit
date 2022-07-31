const assert = require('assert')
const rmrf = require('rimraf')
const Web3 = require('web3')
const OrbitDB = require('../../src/OrbitDB.js')
import { Identities as IdentityProvider } from '@dao-xyz/orbit-db-identity-provider'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
const AccessControllers = require('@dao-xyz/orbit-db-access-controllers')
const ganache = require('ganache-cli')
const { abi, bytecode } = require('./Access')

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')
const API = 'js-ipfs'

const dbPath1 = './orbitdb/tests/orbitdb-access-controller/1'
const dbPath2 = './orbitdb/tests/orbitdb-access-controller/2'

describe(`orbit-db - Access Controller Handlers`, function () {
  jest.setTimeout(config.timeout)

  let web3, contract, ipfsd1, ipfsd2, ipfs1, ipfs2, id1, id2
  let orbitdb1, orbitdb2

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
      AccessControllers: AccessControllers,
      directory: dbPath1,
      identity: id1
    })

    orbitdb2 = await OrbitDB.createInstance(ipfs2, {
      AccessControllers: AccessControllers,
      directory: dbPath2,
      identity: id2
    })
  })

  afterAll(async () => {
    if (orbitdb1) { await orbitdb1.stop() }

    if (orbitdb2) { await orbitdb2.stop() }

    if (ipfsd1) { await stopIpfs(ipfsd1) }

    if (ipfsd2) { await stopIpfs(ipfsd2) }
  })

  describe('isSupported', function () {
    it('supports default access controllers', () => {
      assert.strictEqual(AccessControllers.isSupported('ipfs'), true)
      assert.strictEqual(AccessControllers.isSupported('orbitdb'), true)
    })
  })
})
