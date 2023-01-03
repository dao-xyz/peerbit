import { waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer";

import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Replication topic`, function () {
    let session: LSession;
    let client1: Peerbit, client2: Peerbit, eventStore: EventStore<string>;

    let timer: any;

    beforeAll(async () => {
        session = await LSession.connected(2);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        clearInterval(timer);
        const topic = uuid();

        client1 = await Peerbit.create(session.peers[0], { topic });
        client2 = await Peerbit.create(session.peers[1], { topic });
    });

    afterEach(async () => {
        clearInterval(timer);
        if (eventStore) {
            await eventStore.drop();
        }
        if (client1) await client1.stop();

        if (client2) await client2.stop();
    });

    // TODO rm
    it("will open program if subscribing to replication topic", async () => {
        client2.subscribeToTopic();

        eventStore = new EventStore<string>({});
        eventStore = await client1.open(eventStore);
        eventStore.add("hello");
        await waitFor(
            () =>
                (
                    client2.programs.get(eventStore.address!.toString())
                        ?.program as EventStore<string>
                )?.store?.oplog.values.length === 1
        );
    });
});
