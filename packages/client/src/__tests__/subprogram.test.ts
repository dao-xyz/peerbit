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
import { Entry } from "@dao-xyz/peerbit-log";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Subprogram`, () => {
    let session: LSession;
    let client1: Peerbit,
        client2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;
    let topic: string;
    let timer: any;

    beforeAll(async () => {
        session = await LSession.connected(2);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        clearInterval(timer);

        client1 = await Peerbit.create(session.peers[0], {
            /*  canAccessKeys: async (requester, _keyToAccess) => {
                return requester.equals(client2.identity.publicKey); // allow client1 to share keys with client2
            },  */ waitForKeysTimout: 1000,
        });
        client2 = await Peerbit.create(session.peers[1], {
            limitSigning: true,
        }); // limitSigning = dont sign exchange heads request
        db1 = await client1.open(
            new EventStore<string>({
                id: "abc",
            })
        );
    });

    afterEach(async () => {
        clearInterval(timer);

        if (db1) await db1.store.drop();

        if (db2) await db2.store.drop();

        if (client1) await client1.stop();

        if (client2) await client2.stop();
    });

    @variant("program_with_subprogram")
    class ProgramWithSubprogram extends Program implements CanOpenSubPrograms {
        @field({ type: Documents })
        eventStore: Documents<EventStore<string>>;

        accessRequests: { entry: Entry<any> }[];

        constructor(eventStore: Documents<EventStore<string>>) {
            super();
            this.eventStore = eventStore;
        }

        async canAppend(entry: Entry<any>): Promise<boolean> {
            this.accessRequests.push({ entry }); // this is wat we are testing, are we going here when opening a subprogram?
            return true;
        }

        setup(): Promise<void> {
            this.accessRequests = [];
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
        await client2.subscribeToTopic();

        await client1.open(store, {
            replicate: false,
        });

        const { entry: eventStore } = await store.eventStore.put(
            new EventStore({ id: "store 1" })
        );
        const _eventStore2 = await store.eventStore.put(
            new EventStore({ id: "store 2" })
        );
        expect(store.eventStore.store.oplog.heads).toHaveLength(2); // two independent documents

        await waitFor(() => client2.programs.size || 0 > 0, {
            timeout: 20 * 1000,
            delayInterval: 50,
        });

        const eventStoreString = (
            (await eventStore.payload.getValue()) as PutOperation<any>
        ).value as EventStore<string>;
        await client1.open(eventStoreString, {
            replicate: false,
        });

        const programFromReplicator = [
            ...client2.programs.values()!,
        ][0].program as ProgramWithSubprogram;
        programFromReplicator.accessRequests = [];
        await eventStoreString.add("hello"); // This will exchange an head that will make client 1 open the store
        await waitFor(() => programFromReplicator.accessRequests.length === 1); // one for checking 'can open store'
        expect(
            (
                await programFromReplicator.accessRequests[0].entry.getPublicKeys()
            )[0].equals(client1.identity.publicKey)
        );
    });
});
