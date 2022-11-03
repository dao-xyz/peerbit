
import rmrf from 'rimraf'
import { delay, waitFor } from '@dao-xyz/peerbit-time'
import { variant, field, Constructor } from '@dao-xyz/borsh'
import { Peerbit } from '../peer'

import { EventStore } from './utils/stores/event-store'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// @ts-ignore 
import { v4 as uuid } from 'uuid';

import { Documents, PutOperation, Operation, DocumentIndex } from '@dao-xyz/peerbit-document';

// Include test utilities
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    connectPeers,
    waitForPeers,
} from '@dao-xyz/peerbit-test-utils'
import { CanOpenSubPrograms, Program } from '@dao-xyz/peerbit-program'
import { AnySearch } from '@dao-xyz/peerbit-anysearch'
import { DQuery } from '@dao-xyz/peerbit-query'
import { CanAppend, Entry, Payload } from '@dao-xyz/ipfs-log'

const orbitdbPath1 = './orbitdb/tests/write-only/1'
const orbitdbPath2 = './orbitdb/tests/write-only/2'
const dbPath1 = './orbitdb/tests/write-only/1/db1'
const dbPath2 = './orbitdb/tests/write-only/2/db2'


describe(`orbit-db - Write-only`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS
    let orbitdb1: Peerbit, orbitdb2: Peerbit, db1: EventStore<string>, db2: EventStore<string>
    let replicationTopic: string;
    let timer: any

    beforeAll(async () => {
        ipfsd1 = await startIpfs('js-ipfs', config.daemon1)
        ipfsd2 = await startIpfs('js-ipfs', config.daemon2)
        ipfs1 = ipfsd1.api
        ipfs2 = ipfsd2.api
        replicationTopic = uuid();
        // Connect the peers manually to speed up test times
        const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
        await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
        console.log("Peers connected")
    })

    afterAll(async () => {
        if (ipfsd1)
            await stopIpfs(ipfsd1)

        if (ipfsd2)
            await stopIpfs(ipfsd2)
    })

    beforeEach(async () => {
        clearInterval(timer)

        rmrf.sync(orbitdbPath1)
        rmrf.sync(orbitdbPath2)
        rmrf.sync(dbPath1)
        rmrf.sync(dbPath2)

        orbitdb1 = await Peerbit.create(ipfs1, {
            directory: orbitdbPath1,/*  canAccessKeys: async (requester, _keyToAccess) => {
                return requester.equals(orbitdb2.identity.publicKey); // allow orbitdb1 to share keys with orbitdb2
            },  */waitForKeysTimout: 1000
        })
        orbitdb2 = await Peerbit.create(ipfs2, { directory: orbitdbPath2, limitSigning: true }) // limitSigning = dont sign exchange heads request
        db1 = await orbitdb1.open(new EventStore<string>({
            id: 'abc',

        }), { replicationTopic, directory: dbPath1 })
    })

    afterEach(async () => {
        clearInterval(timer)

        if (db1)
            await db1.store.drop()

        if (db2)
            await db2.store.drop()

        if (orbitdb1)
            await orbitdb1.stop()

        if (orbitdb2)
            await orbitdb2.stop()
    })

    it('write 1 entry replicate false', async () => {

        await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)
        db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), { replicationTopic, directory: dbPath2, replicate: false })

        await db1.add('hello');
        /*   await waitFor(() => db2._oplog.clock.time > 0); */
        await db2.add('world');

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(db1.store.oplog.values.map(x => x.payload.getValue().value)).toContainAllValues(['hello', 'world'])
        expect(db2.store.oplog.values.length).toEqual(1);

    })


    it('encrypted clock sync write 1 entry replicate false', async () => {

        await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)
        const encryptionKey = await orbitdb1.keystore.createEd25519Key({ id: 'encryption key', group: replicationTopic });
        db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), { replicationTopic, directory: dbPath2, replicate: false })

        await db1.add('hello', {
            reciever: {
                clock: encryptionKey.keypair.publicKey,
                payload: encryptionKey.keypair.publicKey,
                signature: encryptionKey.keypair.publicKey
            }
        });

        /*   await waitFor(() => db2._oplog.clock.time > 0); */

        // Now the db2 will request sync clocks even though it does not replicate any content
        await db2.add('world');

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(db1.store.oplog.values.map(x => x.payload.getValue().value)).toContainAllValues(['hello', 'world'])
        expect(db2.store.oplog.values.length).toEqual(1);
    })

    it('will open store on exchange heads message', async () => {

        const replicationTopic = 'x';
        const store = new EventStore<string>({ id: 'replication-tests' });
        await orbitdb2.subscribeToReplicationTopic(replicationTopic);
        await orbitdb1.open(store, { replicationTopic, replicate: false });

        const hello = await store.add('hello', { nexts: [] });
        const world = await store.add('world', { nexts: [hello] });

        expect(store.store.oplog.heads).toHaveLength(1);

        await waitFor(() => Object.values(orbitdb2.programs[replicationTopic]).length > 0, { timeout: 20 * 1000, delayInterval: 50 });

        const replicatedProgramAndStores = Object.values(orbitdb2.programs[replicationTopic])[0];
        const replicatedStore = replicatedProgramAndStores.program.stores[0]
        await waitFor(() => replicatedStore.oplog.values.length == 2);
        expect(replicatedStore).toBeDefined();
        expect(replicatedStore.oplog.heads).toHaveLength(1);
        expect(replicatedStore.oplog.heads[0].hash).toEqual(world.hash);

    })

    it('will open store on exchange heads message when trusted', async () => {

        const replicationTopic = 'x';

        let cb: { entry: Entry<any> }[] = [];

        @variant([0, 239])
        class ProgramWithSubprogram extends Program implements CanOpenSubPrograms {

            @field({ type: Documents })
            eventStore: Documents<EventStore<string>>

            constructor(eventStore: Documents<EventStore<string>>) {
                super()
                this.eventStore = eventStore;
            }

            async canAppend(entry: Entry<any>): Promise<boolean> {

                cb.push({ entry }); // this is what we are testing, are we going here when opening a subprogram?
                return true;
            }

            setup(): Promise<void> {
                return this.eventStore.setup({ type: EventStore, canAppend: this.canAppend.bind(this) });
            }

            async canOpen(program: Program, fromEntry: Entry<any>): Promise<boolean> {
                return program.constructor === EventStore && this.canAppend(fromEntry)
            }

        }
        const store = new ProgramWithSubprogram(new Documents<EventStore<string>>({ id: 'replication-tests', index: new DocumentIndex({ indexBy: 'id', search: new AnySearch({ query: new DQuery({}) }) }) }));
        await orbitdb2.subscribeToReplicationTopic(replicationTopic);
        const openedStore = await orbitdb1.open(store, { replicationTopic, replicate: false });

        const eventStore = await store.eventStore.put(new EventStore({ id: 'store 1' }));
        const _eventStore2 = await store.eventStore.put(new EventStore({ id: 'store 2' }));
        expect(store.eventStore.store.oplog.heads).toHaveLength(2); // two independent documents

        await waitFor(() => Object.values(orbitdb2.programs[replicationTopic]).length > 0, { timeout: 20 * 1000, delayInterval: 50 });


        const eventStoreString = ((await eventStore.payload.getValue()) as PutOperation<any>).value as EventStore<string>;
        await orbitdb1.open(eventStoreString, { replicationTopic, replicate: false });
        cb = [];
        await eventStoreString.add("hello") // This will exchange an head that will make client 1 open the store 
        await waitFor(() => cb.length === 1); // one for checking 'can open store' 
        expect((await cb[0].entry.getPublicKey()).equals(orbitdb1.identity.publicKey))


    })
})