
import rmrf from 'rimraf'
import { waitFor } from '@dao-xyz/peerbit-time'
import { jest } from '@jest/globals';
import { Peerbit } from '../peer'

import { EventStore } from './utils/stores/event-store'
// @ts-ignore
import { v4 as uuid } from 'uuid';

// Include test utilities
import {
  nodeConfig as config,
  testAPIs,
  Session,
} from '@dao-xyz/peerbit-test-utils'

const orbitdbPath1 = './orbitdb/tests/replication-topic/1'
const orbitdbPath2 = './orbitdb/tests/replication-topic/2'
const dbPath1 = './orbitdb/tests/replication-topic/1/db1'
const dbPath2 = './orbitdb/tests/replication-topic/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication topic (${API})`, function () {
    jest.setTimeout(config.timeout * 2)

    let session: Session;
    let orbitdb1: Peerbit, orbitdb2: Peerbit, eventStore: EventStore<string>

    let timer: any

    beforeAll(async () => {
      session = await Session.connected(2);

    })

    afterAll(async () => {
      await session.stop();
    })

    beforeEach(async () => {
      clearInterval(timer)

      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)

      orbitdb1 = await Peerbit.create(session.peers[0].ipfs, { directory: orbitdbPath1 })
      orbitdb2 = await Peerbit.create(session.peers[1].ipfs, { directory: orbitdbPath2 })


    })

    afterEach(async () => {
      clearInterval(timer)
      if (eventStore) {
        await eventStore.drop();
      }
      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()
    })

    it('replicates database of 1 entry', async () => {
      const replicationTopic = uuid();
      orbitdb2.subscribeToReplicationTopic(replicationTopic);

      eventStore = new EventStore<string>({});
      eventStore = await orbitdb1.open(eventStore, { replicationTopic })
      eventStore.add("hello");
      await waitFor(() => (orbitdb2.programs[replicationTopic]?.[eventStore.address!.toString()]?.program as EventStore<string>)?.store?.oplog.values.length === 1)
    })
  })
})
