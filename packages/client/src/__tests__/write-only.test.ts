import rmrf from "rimraf";
import { waitFor } from "@dao-xyz/peerbit-time";
import { getReplicationTopic, Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

const orbitdbPath1 = "./orbitdb/tests/write-only/1";
const orbitdbPath2 = "./orbitdb/tests/write-only/2";
const dbPath1 = "./orbitdb/tests/write-only/1/db1";
const dbPath2 = "./orbitdb/tests/write-only/2/db2";

describe(`Write-only`, function () {
    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;
    let topic: string;
    let timer: any;

    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        topic = uuid();
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
            waitForKeysTimout: 1000,
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

    it("write 1 entry replicate false", async () => {
        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );
        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, directory: dbPath2, replicate: false }
        );

        await db1.add("hello");
        /*   await waitFor(() => db2._oplog.clock.time > 0); */
        await db2.add("world");

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        expect(db2.store.oplog.values.length).toEqual(1);
    });

    it("encrypted clock sync write 1 entry replicate false", async () => {
        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );
        const encryptionKey = await orbitdb1.keystore.createEd25519Key({
            id: "encryption key",
            group: topic,
        });
        db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            { topic: topic, directory: dbPath2, replicate: false }
        );

        await db1.add("hello", {
            reciever: {
                next: encryptionKey.keypair.publicKey,
                metadata: encryptionKey.keypair.publicKey,
                payload: encryptionKey.keypair.publicKey,
                signatures: encryptionKey.keypair.publicKey,
            },
        });

        /*   await waitFor(() => db2._oplog.clock.time > 0); */

        // Now the db2 will request sync clocks even though it does not replicate any content
        await db2.add("world");

        await waitFor(() => db1.store.oplog.values.length === 2);
        expect(
            db1.store.oplog.values.map((x) => x.payload.getValue().value)
        ).toContainAllValues(["hello", "world"]);
        expect(db2.store.oplog.values.length).toEqual(1);
    });

    it("will open store on exchange heads message", async () => {
        const topic = "x";
        const store = new EventStore<string>({ id: "replication-tests" });
        await orbitdb2.subscribeToTopic(topic, true);
        await orbitdb1.open(store, {
            topic: topic,
            replicate: false,
        });

        const hello = await store.add("hello", { nexts: [] });
        const world = await store.add("world", { nexts: [hello] });

        expect(store.store.oplog.heads).toHaveLength(1);

        await waitFor(() => orbitdb2.programs.get(topic)?.size || 0 > 0, {
            timeout: 20 * 1000,
            delayInterval: 50,
        });

        const replicatedProgramAndStores = orbitdb2.programs
            .get(topic)
            ?.values()
            .next().value;
        const replicatedStore = replicatedProgramAndStores.program.stores[0];
        await waitFor(() => replicatedStore.oplog.values.length == 2);
        expect(replicatedStore).toBeDefined();
        expect(replicatedStore.oplog.heads).toHaveLength(1);
        expect(replicatedStore.oplog.heads[0].hash).toEqual(world.hash);
    });
});
