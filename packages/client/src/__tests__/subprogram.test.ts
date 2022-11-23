import rmrf from "rimraf";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { variant, field, Constructor } from "@dao-xyz/borsh";
import { Peerbit } from "../peer";

import { EventStore } from "./utils/stores/event-store";
import { jest } from "@jest/globals";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// @ts-ignore
import { v4 as uuid } from "uuid";

import {
    Documents,
    PutOperation,
    Operation,
    DocumentIndex,
} from "@dao-xyz/peerbit-document";

// Include test utilities
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    connectPeers,
    waitForPeers,
} from "@dao-xyz/peerbit-test-utils";
import { CanOpenSubPrograms, Program } from "@dao-xyz/peerbit-program";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { Entry } from "@dao-xyz/ipfs-log";

const orbitdbPath1 = "./orbitdb/tests/write-only/1";
const orbitdbPath2 = "./orbitdb/tests/write-only/2";
const dbPath1 = "./orbitdb/tests/write-only/1/db1";
const dbPath2 = "./orbitdb/tests/write-only/2/db2";

describe(`Write-only`, function () {
    jest.setTimeout(config.timeout * 2);

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;
    let replicationTopic: string;
    let timer: any;

    beforeAll(async () => {
        ipfsd1 = await startIpfs("js-ipfs", config.daemon1);
        ipfsd2 = await startIpfs("js-ipfs", config.daemon2);
        ipfs1 = ipfsd1.api;
        ipfs2 = ipfsd2.api;
        replicationTopic = uuid();
        // Connect the peers manually to speed up test times
        const isLocalhostAddress = (addr: string) =>
            addr.toString().includes("127.0.0.1");
        await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress });
    });

    afterAll(async () => {
        if (ipfsd1) await stopIpfs(ipfsd1);

        if (ipfsd2) await stopIpfs(ipfsd2);
    });

    beforeEach(async () => {
        clearInterval(timer);

        rmrf.sync(orbitdbPath1);
        rmrf.sync(orbitdbPath2);
        rmrf.sync(dbPath1);
        rmrf.sync(dbPath2);

        orbitdb1 = await Peerbit.create(ipfs1, {
            directory: orbitdbPath1,
            /*  canAccessKeys: async (requester, _keyToAccess) => {
                return requester.equals(orbitdb2.identity.publicKey); // allow orbitdb1 to share keys with orbitdb2
            },  */ waitForKeysTimout: 1000,
        });
        orbitdb2 = await Peerbit.create(ipfs2, {
            directory: orbitdbPath2,
            limitSigning: true,
        }); // limitSigning = dont sign exchange heads request
        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: "abc",
            }),
            { replicationTopic, directory: dbPath1 }
        );
    });

    afterEach(async () => {
        clearInterval(timer);

        if (db1) await db1.store.drop();

        if (db2) await db2.store.drop();

        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();
    });

    @variant("program_with_subprogram")
    class ProgramWithSubprogram extends Program implements CanOpenSubPrograms {
        @field({ type: Documents })
        eventStore: Documents<EventStore<string>>;

        accessRequests: { entry: Entry<any> }[] = [];

        constructor(eventStore: Documents<EventStore<string>>) {
            super();
            this.eventStore = eventStore;
        }

        async canAppend(entry: Entry<any>): Promise<boolean> {
            this.accessRequests.push({ entry }); // this is what we are testing, are we going here when opening a subprogram?
            return true;
        }

        setup(): Promise<void> {
            return this.eventStore.setup({
                type: EventStore,
                canAppend: this.canAppend.bind(this),
            });
        }

        async canOpen(
            program: Program,
            fromEntry: Entry<any>
        ): Promise<boolean> {
            return (
                program.constructor === EventStore && this.canAppend(fromEntry)
            );
        }
    }

    it("can open store on exchange heads message when trusted", async () => {
        const replicationTopic = "x";

        const store = new ProgramWithSubprogram(
            new Documents<EventStore<string>>({
                index: new DocumentIndex({
                    indexBy: "id",
                    query: new RPC(),
                }),
            })
        );
        await orbitdb2.subscribeToReplicationTopic(replicationTopic);

        await orbitdb1.open(store, {
            replicationTopic,
            replicate: false,
        });

        const eventStore = await store.eventStore.put(
            new EventStore({ id: "store 1" })
        );
        const _eventStore2 = await store.eventStore.put(
            new EventStore({ id: "store 2" })
        );
        expect(store.eventStore.store.oplog.heads).toHaveLength(2); // two independent documents

        await waitFor(
            () => orbitdb2.programs.get(replicationTopic)?.size || 0 > 0,
            { timeout: 20 * 1000, delayInterval: 50 }
        );

        const eventStoreString = (
            (await eventStore.payload.getValue()) as PutOperation<any>
        ).value as EventStore<string>;
        await orbitdb1.open(eventStoreString, {
            replicationTopic,
            replicate: false,
        });

        const programFromReplicator = [
            ...orbitdb2.programs.get(replicationTopic)?.values()!,
        ][0].program as ProgramWithSubprogram;
        programFromReplicator.accessRequests = [];
        await eventStoreString.add("hello"); // This will exchange an head that will make client 1 open the store
        await waitFor(() => programFromReplicator.accessRequests.length === 1); // one for checking 'can open store'
        expect(
            (
                await programFromReplicator.accessRequests[0].entry.getPublicKeys()
            )[0].equals(orbitdb1.identity.publicKey)
        );
    });
});
