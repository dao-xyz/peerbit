const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { assertPayload } from './utils/assert'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Cut', function () {
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

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);

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


    it('cut back to max oplog length', async () => {
      const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A', recycle: { maxOplogLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      expect(log.length).toEqual(1);
      assertPayload(log.values[0].payload.value, 'hello3');
    })

    it('cut back to cut length', async () => {
      const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A', recycle: { maxOplogLength: 3, cutOplogToLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      expect(log.length).toEqual(3);
      await log.append('hello4')
      expect(log.length).toEqual(1); // We exceed 'maxOplogLength' and cut back to 'cutOplogToLength'
      assertPayload(log.values[0].payload.value, 'hello4');
    })
  })
})
