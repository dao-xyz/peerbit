import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/peerbit-keystore'

// Test utils
import {
  nodeConfig as config,
  waitForPeers
} from '@dao-xyz/peerbit-test-utils'

import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { Session } from '@dao-xyz/peerbit-test-utils'
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';
import { Entry } from '../entry.js'
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

describe('ipfs-log - Replication', function () {
  jest.setTimeout(config.timeout * 6)

  let session: Session, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>

  const { signingKeyFixtures, signingKeysPath } = config

  let keystore: Keystore

  beforeAll(async () => {

    rmrf.sync(signingKeysPath(__filenameBase))

    await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

    // Start two connected IPFS instances
    session = await Session.connected(2)

    keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

    // Create an identity for each peers
    // @ts-ignore
    signKey = await keystore.getKey(new Uint8Array([0]));
    // @ts-ignore
    signKey2 = await keystore.getKey(new Uint8Array([1]));

    // sort keys so that the output becomes deterministic
    if (signKey.keypair.publicKey.publicKey > signKey2.keypair.publicKey.publicKey) {
      signKey = [signKey2, signKey2 = signKey][0];
    }
  })

  afterAll(async () => {
    await session.stop();

    rmrf.sync(signingKeysPath(__filenameBase))

    await keystore?.close()

  })

  describe('replicates logs deterministically', function () {
    const amount = 10 + 1
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
        ...signKey.keypair,
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
        ...signKey2.keypair,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, hash, {})
      await log2.join(log)
      processing--
    }

    beforeEach(async () => {
      log1 = new Log(session.peers[0].ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId })
      log2 = new Log(session.peers[1].ipfs, {
        ...signKey2.keypair,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId })
      input1 = new Log(session.peers[0].ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId })
      input2 = new Log(session.peers[1].ipfs, {
        ...signKey2.keypair,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId })
      await session.peers[0].ipfs.pubsub.subscribe(channel, handleMessage)
      await session.peers[1].ipfs.pubsub.subscribe(channel, handleMessage2)
    })

    afterEach(async () => {
      await session.peers[0].ipfs.pubsub.unsubscribe(channel, handleMessage)
      await session.peers[1].ipfs.pubsub.unsubscribe(channel, handleMessage2)
    })
    // TODO why is this test doing a lot of unchaught rejections? (Reproduce in VSCODE tick `Uncaught exceptions`)
    it('replicates logs', async () => {

      await waitForPeers(session.peers[0].ipfs, [session.peers[1].id.toString()], channel)
      let prev1: Entry<any> = undefined as any;
      let prev2: Entry<any> = undefined as any;
      for (let i = 1; i <= amount; i++) {
        prev1 = await input1.append('A' + i, { nexts: prev1 ? [prev1] : undefined })
        prev2 = await input2.append('B' + i, { nexts: prev2 ? [prev2] : undefined })
        const hash1 = await input1.toMultihash()
        const hash2 = await input2.toMultihash()
        await session.peers[0].ipfs.pubsub.publish(channel, Buffer.from(hash1))
        await session.peers[1].ipfs.pubsub.publish(channel, Buffer.from(hash2))
      }

      console.log('\nAll messages sent')

      const whileProcessingMessages = (timeoutMs: number) => {
        return new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('timeout')), timeoutMs)
          const timer = setInterval(() => {
            if (buffer1.length + buffer2.length === amount * 2 &&
              processing === 0) {
              console.log('\nAll messages received')
              clearInterval(timer)
              clearTimeout(timeout)
              resolve()
            }
          }, 200)
        })
      }

      console.log('Waiting for all to process')
      await whileProcessingMessages(config.timeout * 2)

      const result = new Log<string>(session.peers[0].ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId })
      await result.join(log1)
      await result.join(log2)

      expect(buffer1.length).toEqual(amount)
      expect(buffer2.length).toEqual(amount)
      expect(result.length).toEqual(amount * 2)
      expect(log1.length).toEqual(amount)
      expect(log2.length).toEqual(amount)
      expect([0, 1, 2, 3, 9, 10].map(i => result.values[i].payload.getValue())).toEqual(['A1', 'B1', 'A2', 'B2', 'B5', 'A6'])
    })
  })
})

