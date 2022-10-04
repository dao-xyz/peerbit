
import assert from 'assert'
import rmrf from 'rimraf'
import path from 'path'
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto'
import { Keystore, KeyWithMeta<Ed25519Keypair> } from '@dao-xyz/orbit-db-keystore'
import { OrbitDB } from '../orbit-db'
/* const Identities = require('@dao-xyz/orbit-db-identity-provider') */
// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
} = require('@dao-xyz/orbit-db-test-utils')

const {
  CustomTestKeystore,
  databases,
} = require('./utils')

/* Identities.addIdentityProvider(CustomTestKeystore().identityProvider)
 */
const dbPath = './orbitdb/tests/customKeystore'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Use a Custom Keystore (${API})`, function () {
    jest.setTimeout(20000)

    let ipfsd: Controller, ipfs: IPFS, orbitdb1

    beforeAll(async () => {
      rmrf.sync(dbPath)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api

      const signKey: KeyWithMeta<Ed25519Keypair> = await CustomTestKeystore().create().createKey(new Uint8Array([0]), KeyWithMeta<Ed25519Keypair>);

      //const identity = await Identities.createIdentity({ type: 'custom', keystore: CustomTestKeystore().create() })
      orbitdb1 = await OrbitDB.createInstance(ipfs, {
        directory: path.join(dbPath, '1'),
        publicKey: new Ed25519PublicKey({
          publicKey: signKey.publicKey
        }),
        sign: (data) => Keystore.sign(data, signKey)
      })
    })

    afterAll(async () => {
      await orbitdb1.stop()
      await stopIpfs(ipfsd)
    })

    describe('allows orbit to use a custom keystore with different store types', function () {
      databases.forEach(async (database) => {
        it(database.type + ' allows custom keystore', async () => {
          const db1 = await database.create(orbitdb1, 'custom-keystore')
          await database.tryInsert(db1)

          assert.deepEqual(database.getTestValue(db1), database.expectedValue)

          await db1.close()
        })
      })
    })

    describe('allows a custom keystore to be used with different store and write permissions', function () {
      databases.forEach(async (database) => {
        it(database.type + ' allows custom keystore', async () => {
          const options = {
            accessController: {
              // Set write access for both clients
              write: [orbitdb1.identity.id]
            }
          }

          const db1 = await database.create(orbitdb1, 'custom-keystore', options)
          await database.tryInsert(db1)

          assert.deepEqual(database.getTestValue(db1), database.expectedValue)

          await db1.close()
        })
      })
    })
  })
})
