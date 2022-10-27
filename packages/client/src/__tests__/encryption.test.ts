
import assert from 'assert'
import rmrf from 'rimraf'
import { Entry } from '@dao-xyz/ipfs-log'
import { Peerbit } from '../peer'
import { EventStore, Operation } from './utils/stores/event-store'
import { IStoreOptions } from '@dao-xyz/peerbit-store';
import { Ed25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { AccessError } from "@dao-xyz/peerbit-crypto"

import { jest } from '@jest/globals';
import { KeyWithMeta } from '@dao-xyz/peerbit-keystore'
import { waitFor } from '@dao-xyz/peerbit-time'

// Include test utilities
import {
  nodeConfig as config,
  testAPIs,
  waitForPeers,
  Session,
} from '@dao-xyz/peerbit-test-utils'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network'

const orbitdbPath1 = './orbitdb/tests/encryption/1'
const orbitdbPath2 = './orbitdb/tests/encryption/2'
const orbitdbPath3 = './orbitdb/tests/encryption/3'

const dbPath1 = './orbitdb/tests/encryption/1/db1'
const dbPath2 = './orbitdb/tests/encryption/2/db2'
const dbPath3 = './orbitdb/tests/encryption/3/db3'


const addHello = async (db: EventStore<string>, receiver: X25519PublicKey) => {
  await db.add('hello', { reciever: { clock: receiver, payload: receiver, signature: receiver } })

}
const checkHello = async (db: EventStore<string>) => {
  const entries: Entry<Operation<string>>[] = db.iterator({ limit: -1 }).collect()
  expect(entries.length).toEqual(1)
  await entries[0].getPayload();
  expect(entries[0].payload.getValue().value).toEqual('hello')
}

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - encryption`, function () {
    jest.setTimeout(config.timeout * 2)

    let session: Session;
    let orbitdb1: Peerbit, orbitdb2: Peerbit, orbitdb3: Peerbit, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>
    let recieverKey: KeyWithMeta<Ed25519Keypair>
    let options: IStoreOptions<any>
    let replicationTopic: string;


    beforeAll(async () => {
      session = await Session.connected(3);


    })

    afterAll(async () => {
      await session.stop();
    })

    beforeEach(async () => {
      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(orbitdbPath3)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)

      orbitdb1 = await Peerbit.create(session.peers[0].ipfs, {
        directory: orbitdbPath1, waitForKeysTimout: 1000
      },)

      const network = await orbitdb1.openNetwork(new TrustedNetwork({ id: 'network-tests', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })
      await orbitdb1.joinNetwork(network);

      // Trusted client 2
      orbitdb2 = await Peerbit.create(session.peers[1].ipfs, { directory: orbitdbPath2, waitForKeysTimout: 1000 })
      await network.add(orbitdb2.id)
      await network.add(orbitdb2.identity.publicKey)
      replicationTopic = network.address.toString();
      await orbitdb2.openNetwork(network.address)
      await waitFor(() => orbitdb2.getNetwork(replicationTopic)?.trustGraph.index.size === 3);
      await orbitdb2.joinNetwork(network)

      // Untrusted client 3
      orbitdb3 = await Peerbit.create(session.peers[2].ipfs, { directory: orbitdbPath3, waitForKeysTimout: 1000 })

      recieverKey = await orbitdb2.keystore.createEd25519Key();

      options = Object.assign({}, options, { directory: dbPath1 })
      db1 = await orbitdb1.open(new EventStore<string>({}), {
        replicationTopic,
        ...options
      })
    })

    afterEach(async () => {

      options = {}

      if (db1)
        await db1.drop()

      if (db2)
        await db2.drop()

      if (db3)
        await db3.drop()

      if (orbitdb1)
        await orbitdb1.disconnect()

      if (orbitdb2)
        await orbitdb2.disconnect()

      if (orbitdb3)
        await orbitdb3.disconnect()
    })

    it('replicates database of 1 entry known keys', async () => {

      options = Object.assign({}, options, { directory: dbPath2 })
      let done = false;


      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), {
        replicationTopic,
        ...options, onReplicationComplete: async (_store) => {
          await checkHello(db1);
          done = true;
        }
      })
      await orbitdb2.keystore.saveKey(recieverKey);
      expect(await orbitdb2.keystore.getKey(recieverKey.keypair.publicKey)).toBeDefined()
      await addHello(db1, recieverKey.keypair.publicKey);
      await waitFor(() => done);
    })


    it('replicates database of 1 entry unknown keys', async () => {
      // TODO this test is flaky when running all tests at once

      options = Object.assign({}, options, { directory: dbPath2 })

      const unknownKey = await orbitdb1.keystore.createEd25519Key({ id: 'unknown', group: replicationTopic });

      // We expect during opening that keys are exchange
      let done = false;
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), {
        replicationTopic,
        ...options, onReplicationComplete: async (_store) => {
          await checkHello(db1);
          done = true;
        }
      })

      expect(await orbitdb1.keystore.hasKey(unknownKey.keypair.publicKey));
      const xKey = await X25519PublicKey.from(unknownKey.keypair.publicKey);
      const getXKEy = await orbitdb1.keystore.getKey(xKey, replicationTopic);
      expect(getXKEy).toBeDefined();
      expect(!(await orbitdb2.keystore.hasKey(unknownKey.keypair.publicKey)));

      // ... so that append with reciever key, it the reciever will be able to decrypt
      await addHello(db1, unknownKey.keypair.publicKey);
      await waitFor(() => done);
    })



    it('can retrieve secret keys if trusted', async () => {

      await waitForPeers(session.peers[2].ipfs, [orbitdb1.id], replicationTopic)

      const db1Key = await orbitdb1.keystore.createEd25519Key({ id: 'unknown', group: replicationTopic });

      // Open store from orbitdb3 so that both client 1 and 2 is listening to the replication topic
      options = Object.assign({}, options, { directory: dbPath2 })
      await orbitdb2.open(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), {
        replicationTopic,
        ...options
      })
      const reciever = await orbitdb2.getEncryptionKey(replicationTopic) as KeyWithMeta<Ed25519Keypair>;
      expect(reciever).toBeDefined();
      expect(db1Key.keypair.publicKey.equals(reciever.keypair.publicKey));
    })

    it('can relay with end to end encryption with public id and clock (E2EE-weak)', async () => {

      await waitForPeers(session.peers[2].ipfs, [orbitdb1.id], replicationTopic)

      options = Object.assign({}, options, { directory: dbPath2 })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), { replicationTopic, ...options })

      const client3Key = await orbitdb3.keystore.createEd25519Key({ id: 'unknown' });

      await db2.add('hello', { reciever: { clock: undefined, payload: client3Key.keypair.publicKey, signature: client3Key.keypair.publicKey } })

      // Wait for db1 (the relay) to get entry
      await waitFor(() => db1.store.oplog.values.length === 1)
      const entriesRelay: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
      expect(entriesRelay.length).toEqual(1)
      try {
        await entriesRelay[0].getPayload(); // should fail, since relay can not see the message
        assert(false);
      } catch (error) {
        expect(error).toBeInstanceOf(AccessError);
      }


      const sender: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
      expect(sender.length).toEqual(1)
      await sender[0].getPayload();
      expect(sender[0].payload.getValue().value).toEqual('hello')


      // Now close db2 and open db3 and make sure message are available
      await db2.drop();
      options = Object.assign({}, options, { directory: dbPath3 })
      db3 = await orbitdb3.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb3._ipfs, db1.address), {
        replicationTopic,
        ...options, onReplicationComplete: async (store) => {
          const entriesRelay: Entry<Operation<string>>[] = db3.iterator({ limit: -1 }).collect()
          expect(entriesRelay.length).toEqual(1)
          await entriesRelay[0].getPayload(); // should pass since orbitdb3 got encryption key
        }
      })
    })
  })
})
