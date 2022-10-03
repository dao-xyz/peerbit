import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs,
  getIpfsPeerId,
  waitForPeers,
  connectPeers
} = require('@dao-xyz/orbit-db-test-utils')

Object.keys(testAPIs).forEach((IPFS) => {
  describe('ipfs-log - Replication', function () {
    jest.setTimeout(config.timeout * 6)

    let ipfsd1, ipfsd2, ipfs1, ipfs2, id1: string, id2: string, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      // Start two IPFS instances
      ipfsd1 = await startIpfs(IPFS, config.daemon1)
      ipfsd2 = await startIpfs(IPFS, config.daemon2)
      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api

      await connectPeers(ipfs1, ipfs2)

      // Get the peer IDs
      id1 = await getIpfsPeerId(ipfs1)
      id2 = await getIpfsPeerId(ipfs2)

      keystore = new Keystore(await createStore(identityKeysPath)))
    signingKeystore = new Keystore(await createStore(signingKeysPath)))

  // Create an identity for each peers
  signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
  signKey2 = await keystore.getKeyByPath(new Uint8Array([1]), SignKeyWithMeta);
})

afterAll(async () => {
  await stopIpfs(ipfsd1)
  await stopIpfs(ipfsd2)
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

  const handleMessage = async (message) => {
    if (id1 === message.from) {
      return
    }
    const hash = Buffer.from(message.data).toString()
    buffer1.push(hash)
    processing++
    process.stdout.write('\r')
    process.stdout.write(`> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`)
    const log = await Log.fromMultihash<string>(ipfs1, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, {})
    await log1.join(log)
    processing--
  }

  const handleMessage2 = async (message) => {
    if (id2 === message.from) {
      return
    }
    const hash = Buffer.from(message.data).toString()
    buffer2.push(hash)
    processing++
    process.stdout.write('\r')
    process.stdout.write(`> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`)
    const log = await Log.fromMultihash<string>(ipfs2, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, {})
    await log2.join(log)
    processing--
  }

  beforeEach(async () => {
    log1 = new Log(ipfs1, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId })
    log2 = new Log(ipfs2, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId })
    input1 = new Log(ipfs1, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId })
    input2 = new Log(ipfs2, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId })
    await ipfs1.pubsub.subscribe(channel, handleMessage)
    await ipfs2.pubsub.subscribe(channel, handleMessage2)
  })

  afterEach(async () => {
    await ipfs1.pubsub.unsubscribe(channel, handleMessage)
    await ipfs2.pubsub.unsubscribe(channel, handleMessage2)
  })

  it('replicates logs', async () => {
    await waitForPeers(ipfs1, [id2], channel)

    for (let i = 1; i <= amount; i++) {
      await input1.append('A' + i)
      await input2.append('B' + i)
      const hash1 = await input1.toMultihash()
      const hash2 = await input2.toMultihash()
      await ipfs1.pubsub.publish(channel, Buffer.from(hash1))
      await ipfs2.pubsub.publish(channel, Buffer.from(hash2))
    }

    console.log('\nAll messages sent')

    const whileProcessingMessages = (timeoutMs) => {
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

    const result = new Log<string>(ipfs1, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId })
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
