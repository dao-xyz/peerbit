import rmrf from "rimraf";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { SimpleStoreContract } from "./utils/access";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`shared`, function () {
    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: SimpleStoreContract,
        db2: SimpleStoreContract;

    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        orbitdb1 = await Peerbit.create(session.peers[0], {});
        orbitdb2 = await Peerbit.create(session.peers[1], {});
    });

    afterEach(async () => {
        if (db1) await db1.store.drop();

        if (db2) await db2.store.drop();

        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();
    });

    it("open same store twice will share instance", async () => {
        const topic = "topic";
        db1 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore({ id: "some db" }),
            }),
            { topic: topic }
        );
        const sameDb = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore({ id: "some db" }),
            }),
            { topic: topic }
        );
        expect(db1 === sameDb);
    });

    it("can share nested stores", async () => {
        const topic = "topic";
        db1 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore<string>({
                    id: "event store",
                }),
            }),
            { topic: topic }
        );
        db2 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore<string>({
                    id: "event store",
                }),
            }),
            { topic: topic }
        );
        expect(db1 !== db2);
        expect(db1.store === db2.store);
    });

    // TODO add tests and define behaviour for cross topic programs
    // TODO add tests for shared subprogams
    // TODO add tests for subprograms that is also open as root program
});
