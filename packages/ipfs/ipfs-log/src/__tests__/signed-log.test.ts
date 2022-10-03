import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { CanAppendAccessController } from '../default-access-controller.js'
import { Log } from '../log.js'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto'
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto"

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Signed Log', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(await createStore(identityKeysPath)))
    signingKeystore = new Keystore(await createStore(signingKeysPath)))


  signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
  signKey2 = await keystore.getKeyByPath(new Uint8Array([1]), SignKeyWithMeta);
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



it('has the correct identity', () => {
  const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  expect(log._publicKey).toMatchSnapshot('publicKeyFromLog');
})

it('has the correct public key', () => {
  const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  expect(log._publicKey).toEqual(signKey.publicKey)
})

it('has the correct pkSignature', () => {
  const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  expect(log._publicKey).toEqual(signKey.publicKey)
})

it('has the correct signature', () => {
  const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  expect(log._publicKey).toEqual(signKey.publicKey)
})

it('entries contain an identity', async () => {
  const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  await log.append('one')
  assert.notStrictEqual(await log.values[0].signature, null)
  assert.deepStrictEqual(await log.values[0].publicKey, signKey.publicKey)
})

it('doesn\'t sign entries when identity is not defined', async () => {
  let err
  try {
    const log = new Log(ipfs, undefined, undefined) // eslint-disable-line no-unused-vars
  } catch (e) {
    err = e
  }
  expect(err.message).toEqual('Identity is required')
})

it('doesn\'t join logs with different IDs ', async () => {
  const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'B' })

  let err
  try {
    await log1.append('one')
    await log2.append('two')
    await log2.append('three')
    await log1.join(log2)
  } catch (e) {
    err = e.toString()
    throw e
  }

  expect(err).toEqual(undefined)
  expect(log1._id).toEqual('A')
  expect(log1.values.length).toEqual(1)
  expect(log1.values[0].payload.value).toEqual('one')
})



it('throws an error if log is signed but trying to merge an entry that doesn\'t have a signature', async () => {
  const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A' })

  let err
  try {
    await log1.append('one')
    await log2.append('two')
    delete (log2.values[0]._signature)
    await log1.join(log2)
  } catch (e) {
    err = e.toString()
  }
  expect(err).toEqual('Error: Unsupported')
})

it('throws an error if log is signed but the signature doesn\'t verify', async () => {
  const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A' })
  let err

  try {
    await log1.append('one');
    await log2.append('two');
    let entry: Entry<string> = log2.values[0]
    entry._signature = await log1.values[0]._signature;
    await log1.join(log2)
  } catch (e) {
    err = e.toString()
  }

  const entry = log2.values[0]
  expect(err).toEqual(`Error: Could not validate signature "${await entry.signature}" for entry "${entry.hash}" and key "${(await entry.publicKey)}"`)
  expect(log1.values.length).toEqual(1)
  expect(log1.values[0].payload.value).toEqual('one')
})

it('throws an error if entry doesn\'t have append access', async () => {
  const denyAccess = { canAppend: (_, __) => Promise.resolve(false) } as CanAppendAccessController<string>
  const log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
  const log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A', access: denyAccess })

  let err
  try {
    await log1.append('one')
    await log2.append('two')
    await log1.join(log2)
  } catch (e) {
    err = e.toString()
  }

  expect(err).toEqual(`Error: Could not append Entry<T>, key "${signKey2.publicKey}" is not allowed to write to the log`)
})

it('throws an error upon join if entry doesn\'t have append access', async () => {
  const testACL = {
    canAppend: async (_entry, publicKey: MaybeEncrypted<Ed25519PublicKey>, _) => Buffer.compare(publicKey.decrypted.getValue(Ed25519PublicKey).publicKey.getBuffer(), signKey.publicKey.getBuffer()) === 0
  } as CanAppendAccessController<string>;
  const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A', access: testACL })
  const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A' })

  let err
  try {
    await log1.append('one')
    await log2.append('two')
    await log1.join(log2)
  } catch (e) {
    err = e.toString()
  }

  expect(err).toEqual(`Error: Could not append Entry<T>, key "${signKey2.publicKey}" is not allowed to write to the log`)
})
})
})
