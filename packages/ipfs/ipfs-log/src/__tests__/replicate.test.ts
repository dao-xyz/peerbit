import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  waitForPeers
} from '@dao-xyz/orbit-db-test-utils'

import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { Session } from '@dao-xyz/orbit-db-test-utils'
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

Object.keys(testAPIs).forEach((IPFS) => {
  describe('ipfs-log - Replication', function () {
    jest.setTimeout(config.timeout * 6)

    let session: Session, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      // Start two connected IPFS instances
      session = await Session.connected(2)

      keystore = new Keystore(await createStore(identityKeysPath))
      signingKeystore = new Keystore(await createStore(signingKeysPath))

      // Create an identity for each peers
      // @ts-ignore
      signKey = await keystore.getKey(new Uint8Array([0]));
      // @ts-ignore
      signKey2 = await keystore.getKey(new Uint8Array([1]));
    })

    afterAll(async () => {
      await session.stop();
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)

      await keystore?.close()
      await signingKeystore?.close()
    })

    describe('replicates logs deterministically', function () {
      const amount = 128 + 1
      const channel = 'XXX'
      const logId = 'A'

      let log1: Log<string>, log2: Log<string>, input1: Log<string>, input2: Log<string>
      const buffer1: string[] = []
      const buffer2: string[] = []
      let processing = 0

      const handleMessage = async (message: any) => {
        if (session.peers[0].id.equals(message.from)) {
          return
        }
        const hash = Buffer.from(message.data).toString()
        buffer1.push(hash)
        processing++
        process.stdout.write('\r')
        process.stdout.write(`> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`)
        const log = await Log.fromMultihash<string>(session.peers[0].ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, hash, {})
        await log1.join(log)
        processing--
      }

      const handleMessage2 = async (message: any) => {
        if (session.peers[1].id.equals(message.from)) {
          return
        }
        const hash = Buffer.from(message.data).toString()
        buffer2.push(hash)
        processing++
        process.stdout.write('\r')
        process.stdout.write(`> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`)
        const log = await Log.fromMultihash<string>(session.peers[1].ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, hash, {})
        await log2.join(log)
        processing--
      }

      beforeEach(async () => {
        log1 = new Log(session.peers[0].ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId })
        log2 = new Log(session.peers[1].ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, { logId })
        input1 = new Log(session.peers[0].ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId })
        input2 = new Log(session.peers[1].ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, { logId })
        await session.peers[0].ipfs.pubsub.subscribe(channel, handleMessage)
        await session.peers[1].ipfs.pubsub.subscribe(channel, handleMessage2)
      })

      afterEach(async () => {
        await session.peers[0].ipfs.pubsub.unsubscribe(channel, handleMessage)
        await session.peers[1].ipfs.pubsub.unsubscribe(channel, handleMessage2)
      })

      it('replicates logs', async () => {
        await waitForPeers(session.peers[0].ipfs, [session.peers[1].id.toString()], channel)

        for (let i = 1; i <= amount; i++) {
          await input1.append('A' + i)
          await input2.append('B' + i)
          const hash1 = await input1.toMultihash()
          const hash2 = await input2.toMultihash()
          await session.peers[0].ipfs.pubsub.publish(channel, Buffer.from(hash1))
          await session.peers[1].ipfs.pubsub.publish(channel, Buffer.from(hash2))
        }

        console.log('\nAll messages sent')

        const whileProcessingMessages = (timeoutMs: number) => {
          return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('timeout')), timeoutMs)
            const timer = setInterval(() => {
              if (buffer1.length + buffer2.length === amount * 2 &&
                processing === 0) {
                console.log('\nAll messages received')
                clearInterval(timer)
                clearTimeout(timeout)
                resolve(undefined)
              }
            }, 200)
          })
        }

        console.log('Waiting for all to process')
        await whileProcessingMessages(config.timeout * 2)

        const result = new Log<string>(session.peers[0].ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId })
        await result.join(log1)
        await result.join(log2)

        expect(buffer1.length).toEqual(amount)
        expect(buffer2.length).toEqual(amount)
        expect(result.length).toEqual(amount * 2)
        expect(log1.length).toEqual(amount)
        expect(log2.length).toEqual(amount)
        expect(result.values[0].payload.value).toEqual('A1')
        expect(result.values[1].payload.value).toEqual('B1')
        expect(result.values[2].payload.value).toEqual('A2')
        expect(result.values[3].payload.value).toEqual('B2')
        expect(result.values[99].payload.value).toEqual('B50')
        expect(result.values[100].payload.value).toEqual('A51')
        expect(result.values[198].payload.value).toEqual('A100')
        expect(result.values[199].payload.value).toEqual('B100')
      })
    })
  })
})
