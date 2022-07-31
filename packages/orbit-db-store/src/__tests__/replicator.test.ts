import assert from 'assert'
import { Log } from '@dao-xyz/ipfs-log'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore } from "@dao-xyz/orbit-db-keystore"
import { Identities } from '@dao-xyz/orbit-db-identity-provider'

import {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} from 'orbit-db-test-utils'
import { Replicator } from '../replicator'
import { DefaultOptions, Store } from '../store'
const storage = require('orbit-db-storage-adapter')(require('memdown'))

// Tests timeout
const timeout = 30000

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`Replicator, ${IPFS}`, function () {

    jest.setTimeout(timeout);

    let log, ipfsd, ipfs, replicator, store, keystore, signingKeystore, cacheStore

    const { identityKeysPath } = config

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)

      ipfsd = await startIpfs(IPFS, config.daemon1)
      ipfs = ipfsd.api
      const id = await ipfsd.api.id()

      const testIdentity = await Identities.createIdentity({ id, keystore })
      log = new Log(ipfs, testIdentity)
      cacheStore = await storage.createStore('cache')
      const cache = new Cache(cacheStore)
      const options = Object.assign({}, DefaultOptions, { cache })
      store = new Store(ipfs, testIdentity, log.id, options)
      replicator = new Replicator(store, 123)
    })

    afterAll(async () => {
      await replicator?.stop()
      ipfsd && await stopIpfs(ipfsd)
      await keystore?.close()
    })

    it('default options', async () => {
      assert.deepStrictEqual(replicator._logs, [])
    })

    describe('concurrency = 123', function () {
      let log2

      jest.setTimeout(timeout)

      const logLength = 100

      beforeAll(async () => {
        const testIdentity = await Identities.createIdentity({ id: new Uint8Array([1]), keystore, signingKeystore })
        log2 = new Log(ipfs, testIdentity, { logId: log.id })

        console.log(`writing ${logLength} entries to the log`)
        for (let i = 0; i < logLength; i++) {
          await log2.append(`entry${i}`, 123)
        }
        expect(log2.values.length).toEqual(logLength)
      })

      it('replicates all entries in the log', (done) => {
        let replicated = 0
        assert.strictEqual(log.id, log2.id)

        assert.strictEqual(replicator._logs.length, 0)
        assert.strictEqual(replicator.tasksQueued, 0)

        replicator.onReplicationProgress = () => replicated++
        replicator.onReplicationComplete = async (replicatedLogs) => {
          assert.strictEqual(replicator.tasksRunning, 0)
          assert.strictEqual(replicator.tasksQueued, 0)
          assert.strictEqual(replicator.unfinished.length, 0)
          for (const replicatedLog of replicatedLogs) {
            await log.join(replicatedLog)
          }
          assert.strictEqual(log.values.length, logLength)
          assert.deepStrictEqual(log.values, log2.values)
          done();
        }

        replicator.load(log2.heads)
      })
    })
  })
})
