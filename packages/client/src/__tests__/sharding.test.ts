import { Peerbit } from "../peer";

import { EventStore } from "./utils/stores/event-store";

import rmrf from "rimraf";
import { jest } from "@jest/globals";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { delay, waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./utils/stores/test-store";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`sharding`, function () {
    jest.retryTimes(3); // TODO this test is FLAKY

    let session: LSession;
    let client1: Peerbit,
        client2: Peerbit,
        client3: Peerbit,
        db1: PermissionedEventStore,
        db2: PermissionedEventStore,
        db3: PermissionedEventStore;

    beforeEach(async () => {
        session = await LSession.connected(3, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        client1 = await Peerbit.create(session.peers[0], {});
        client2 = await Peerbit.create(session.peers[1], {});
        client3 = await Peerbit.create(session.peers[2], {});

        const network = new TrustedNetwork({
            id: "network-tests",
            rootTrust: client1.identity.publicKey,
        });
        db1 = await client1.open<PermissionedEventStore>(
            new PermissionedEventStore({ network })
        );

        await client1.join(db1);

        // trust client 3
        await network.add(client2.id);
        await network.add(client2.identity.publicKey);
        db2 = await client2.open<PermissionedEventStore>(db1.address!);
        await network.add(client3.id);
        await network.add(client3.identity.publicKey);
        db3 = await client3.open<PermissionedEventStore>(db1.address!);
    });

    afterEach(async () => {
        if (client1) {
            await client1.stop();
        }

        if (client2) {
            await client2.stop();
        }
        if (client3) {
            await client3.stop();
        }
        await session.stop();
    });

    it("can distribute evenly among peers", async () => {
        // TODO this test is flaky, because it sometimes timeouts because distribution of data among peers is random for small entry counts
        const entryCount = 60;

        // expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
        const promises: Promise<any>[] = [];
        for (let i = 0; i < entryCount; i++) {
            promises.push(db1.store.add(i.toString(), { nexts: [] }));
        }

        await Promise.all(promises);
        await waitFor(() => db1.store.store.oplog.values.length === entryCount);

        // this could failed, if we are unlucky probability wise
        await waitFor(
            () =>
                db2.store.store.oplog.values.length > entryCount * 0.5 &&
                db2.store.store.oplog.values.length < entryCount * 0.85
        );
        await waitFor(
            () =>
                db3.store.store.oplog.values.length > entryCount * 0.5 &&
                db3.store.store.oplog.values.length < entryCount * 0.85
        );

        const checkConverged = async (db: EventStore<any>) => {
            const a = db.store.oplog.values.length;
            await delay(5000); // arb delay
            return a === db.store.oplog.values.length;
        };

        await waitForAsync(() => checkConverged(db2.store), {
            timeout: 20000,
            delayInterval: 5000,
        });
        await waitForAsync(() => checkConverged(db3.store), {
            timeout: 20000,
            delayInterval: 5000,
        });
    });
});
