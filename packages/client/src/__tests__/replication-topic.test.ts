import rmrf from "rimraf";
import { waitFor } from "@dao-xyz/peerbit-time";
import { jest } from "@jest/globals";
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
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        clearInterval(timer);

        client1 = await Peerbit.create(session.peers[0], {});
        client2 = await Peerbit.create(session.peers[1], {});
    });

    afterEach(async () => {
        clearInterval(timer);
        if (eventStore) {
            await eventStore.drop();
        }
        if (client1) await client1.stop();

        if (client2) await client2.stop();
    });

    it("replicates database of 1 entry", async () => {
        const topic = uuid();
        client2.subscribeToTopic(topic, true);

        eventStore = new EventStore<string>({});
        eventStore = await client1.open(eventStore, {
            topic: topic,
        });
        eventStore.add("hello");
        await waitFor(
            () =>
                (
                    client2.programs
                        .get(topic)
                        ?.get(eventStore.address!.toString())
                        ?.program as EventStore<string>
                )?.store?.oplog.values.length === 1
        );
    });
});
