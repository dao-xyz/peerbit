import rmrf from "rimraf";
import { waitFor } from "@dao-xyz/peerbit-time";
import { variant, field } from "@dao-xyz/borsh";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import {
    Documents,
    PutOperation,
    DocumentIndex,
} from "@dao-xyz/peerbit-document";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { CanOpenSubPrograms, Program } from "@dao-xyz/peerbit-program";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { Entry } from "@dao-xyz/ipfs-log";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

const orbitdbPath1 = "./orbitdb/tests/subprogram/1";
const orbitdbPath2 = "./orbitdb/tests/subprogram/2";
const dbPath1 = "./orbitdb/tests/subprogram/1/db1";
const dbPath2 = "./orbitdb/tests/subprogram/2/db2";

describe(`Subprogram`, function () {
    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;
    let topic: string;
    let timer: any;

    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        clearInterval(timer);
        rmrf.sync(orbitdbPath1);
        rmrf.sync(orbitdbPath2);
        rmrf.sync(dbPath1);
        rmrf.sync(dbPath2);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            directory: orbitdbPath1,
            /*  canAccessKeys: async (requester, _keyToAccess) => {
                return requester.equals(orbitdb2.identity.publicKey); // allow orbitdb1 to share keys with orbitdb2
            },  */ waitForKeysTimout: 1000,
        });
        orbitdb2 = await Peerbit.create(session.peers[1], {
            directory: orbitdbPath2,
            limitSigning: true,
        }); // limitSigning = dont sign exchange heads request
        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: "abc",
            }),
            { topic: topic, directory: dbPath1 }
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
        const topic = "x";

        const store = new ProgramWithSubprogram(
            new Documents<EventStore<string>>({
                index: new DocumentIndex({
                    indexBy: "id",
                    query: new RPC(),
                }),
            })
        );
        await orbitdb2.subscribeToTopic(topic, true);

        await orbitdb1.open(store, {
            topic: topic,
            replicate: false,
        });

        const eventStore = await store.eventStore.put(
            new EventStore({ id: "store 1" })
        );
        const _eventStore2 = await store.eventStore.put(
            new EventStore({ id: "store 2" })
        );
        expect(store.eventStore.store.oplog.heads).toHaveLength(2); // two independent documents

        await waitFor(() => orbitdb2.programs.get(topic)?.size || 0 > 0, {
            timeout: 20 * 1000,
            delayInterval: 50,
        });

        const eventStoreString = (
            (await eventStore.payload.getValue()) as PutOperation<any>
        ).value as EventStore<string>;
        await orbitdb1.open(eventStoreString, {
            topic: topic,
            replicate: false,
        });

        const programFromReplicator = [
            ...orbitdb2.programs.get(topic)?.values()!,
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
