import { waitFor } from "@dao-xyz/peerbit-time";
import { getObserverTopic, getReplicationTopic, Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";

// Include test utilities
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

/**
 * Tests that are relavent for browser environments
 */

describe(`browser`, function () {
    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;

    afterAll(async () => {});

    beforeEach(async () => {});

    afterEach(async () => {
        if (db1) await db1.store.drop();

        if (db2) await db2.store.drop();

        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();

        await session.stop();
    });

    it("can replicate entries", async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            browser: true,
        });
        orbitdb2 = await Peerbit.create(session.peers[1], {
            browser: true,
        });

        let topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: uuid(),
            }),
            { topic: topic, replicate: true }
        );

        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, replicate: true }
        );

        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getReplicationTopic(topic)
        );
        expect(orbitdb1._directConnections.size).toEqual(0); // since browser
        expect(orbitdb2._directConnections.size).toEqual(0); // since browser

        await db1.add("hello");
        await db2.add("world");

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        expect(db2.store.oplog.values.length).toEqual(2);
    });
    it("can replicate entries through relay", async () => {
        session = await LSession.disconnected(3);
        let topic = uuid();

        // peer 3 is relay, and dont connect 1 with 2 directly
        session.peers[2].pubsub.subscribe(DEFAULT_BLOCK_TRANSPORT_TOPIC);
        session.peers[2].pubsub.subscribe(topic);
        session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
        session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

        await waitForPeers(
            session.peers[0],
            session.peers[2],
            DEFAULT_BLOCK_TRANSPORT_TOPIC
        );
        await waitForPeers(session.peers[0], session.peers[2], topic);
        await waitForPeers(
            session.peers[1],
            session.peers[2],
            DEFAULT_BLOCK_TRANSPORT_TOPIC
        );
        await waitForPeers(session.peers[1], session.peers[2], topic);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            browser: true,
        });
        orbitdb2 = await Peerbit.create(session.peers[1], {
            browser: true,
        });

        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: uuid(),
            }),
            { topic: topic, replicate: true }
        );

        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, replicate: true }
        );

        await waitForPeers(
            session.peers[2],
            [orbitdb1.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[2],
            [orbitdb2.id],
            getObserverTopic(topic)
        );

        expect(orbitdb1._directConnections.size).toEqual(0); // since browser
        expect(orbitdb2._directConnections.size).toEqual(0); // since browser

        await db1.add("hello");
        await db2.add("world");

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        expect(db2.store.oplog.values.length).toEqual(2);
    });

    it("will share entries as replicator on peer connect", async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            browser: true,
        });
        orbitdb2 = await Peerbit.create(session.peers[1], {
            browser: true,
        });

        let topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: uuid(),
            }),
            { topic: topic, replicate: true }
        );

        await db1.add("hello");
        await db1.add("world");

        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, replicate: true }
        );

        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getReplicationTopic(topic)
        );
        expect(orbitdb1._directConnections.size).toEqual(0); // since browser
        expect(orbitdb2._directConnections.size).toEqual(0); // since browser

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        await waitFor(() => db2.store.oplog.values.length === 2);
    });

    it("will share entries as observer on peer connect", async () => {
        let topic = uuid();
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            browser: true,
        });
        orbitdb2 = await Peerbit.create(session.peers[1], {
            browser: true,
        });

        db1 = await orbitdb1.open(
            new EventStore<string>({
                id: uuid(),
            }),
            { topic: topic, replicate: false }
        );

        await db1.add("hello");
        await db1.add("world");

        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, replicate: true }
        );

        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getReplicationTopic(topic)
        );
        expect(
            orbitdb1._topicSubscriptions.has(getReplicationTopic(topic))
        ).toEqual(false);
        expect(orbitdb1._directConnections.size).toEqual(0); // since browser
        expect(orbitdb2._directConnections.size).toEqual(0); // since browser

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        await waitFor(() => db2.store.oplog.values.length === 2);
    });
});
