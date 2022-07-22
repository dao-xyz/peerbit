const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { assertPayload } from './utils/assert'
const Keystore = require('orbit-db-keystore')

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Cut (' + IPFS + ')', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      testIdentity = await Identities.createIdentity({ id: 'userA', keystore, signingKeystore })
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)

      await keystore?.close()
      await signingKeystore?.close()
    })


    test('cut back to max oplog length', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A', recycle: { maxOplogLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      assert.strictEqual(log.length, 1);
      assertPayload(log.values[0].data.payload, 'hello3');
    })

    test('cut back to cut length', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A', recycle: { maxOplogLength: 3, cutOplogToLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      assert.strictEqual(log.length, 3);
      await log.append('hello4')
      assert.strictEqual(log.length, 1); // We exceed 'maxOplogLength' and cut back to 'cutOplogToLength'
      assertPayload(log.values[0].data.payload, 'hello4');
    })
  })
})
