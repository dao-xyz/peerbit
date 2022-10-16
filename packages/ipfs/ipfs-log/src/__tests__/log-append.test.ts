import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/peerbit-keystore'
import { jest } from '@jest/globals';

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { Controller } from 'ipfsd-ctl'
import { IPFS } from 'ipfs-core-types'
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Append', function () {
    jest.setTimeout(config.timeout)

    const { signingKeyFixtures, signingKeysPath } = config
    let keystore: Keystore

    beforeAll(async () => {

      rmrf.sync(signingKeysPath(__filenameBase))

      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

      keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

      signKey = await keystore.getKey(new Uint8Array([0])) as KeyWithMeta<Ed25519Keypair>;;
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api

    })

    afterAll(async () => {
      await stopIpfs(ipfsd)

      rmrf.sync(signingKeysPath(__filenameBase))

      await keystore?.close()

    })

    describe('append one', () => {
      let log: Log<string>

      beforeEach(async () => {
        log = new Log(ipfs, {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log.append('hello1')
      })

      it('added the correct amount of items', () => {
        expect(log.length).toEqual(1)
      })

      it('added the correct values', async () => {
        log.values.forEach((entry) => {
          expect(entry.payload.getValue()).toEqual('hello1')
        })
      })

      it('added the correct amount of next pointers', async () => {
        log.values.forEach((entry) => {
          expect(entry.next.length).toEqual(0)
        })
      })

      it('has the correct heads', async () => {
        log.heads.forEach((head) => {
          expect(head.hash).toEqual(log.values[0].hash)
        })
      })

      it('updated the clocks correctly', async () => {
        log.values.forEach((entry) => {
          expect(entry.clock.id).toEqual(signKey.keypair.publicKey.bytes);
          expect(entry.clock.time).toEqual(0n)
        })
      })
    })

    describe('append 100 items to a log', () => {
      const amount = 100
      const nextPointerAmount = 64

      let log: Log<string>

      beforeAll(async () => {
        // Do sign function really need to returnr publcikey
        log = new Log(ipfs, { ...signKey.keypair, sign: (data) => signKey.keypair.sign(data) }, { logId: 'A' })
        let prev = undefined;
        for (let i = 0; i < amount; i++) {
          prev = await log.append('hello' + i, { pin: false, nexts: prev ? [prev] : undefined })//,  refs: log.getPow2Refs(nextPointerAmount) })
          // Make sure the log has the right heads after each append
          const values = log.values
          expect(log.heads.length).toEqual(1)
          expect(log.heads[0].hash).toEqual(values[values.length - 1].hash)
        }
      })

      it('added the correct amount of items', () => {
        expect(log.length).toEqual(amount)
      })

      it('added the correct values', async () => {
        log.values.forEach((entry, index) => {
          expect(entry.payload.getValue()).toEqual('hello' + index)
        })
      })

      it('updated the clocks correctly', async () => {
        log.values.forEach((entry, index) => {
          expect(entry.clock.time).toEqual(BigInt(index))
          expect(entry.clock.id).toEqual(signKey.keypair.publicKey.bytes);
        })
      })

      /*    it('added the correct amount of refs pointers', async () => {
           log.values.forEach((entry, index) => {
             expect(entry.refs.length).toEqual(index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
           })
         }) */
    })
  })
})
