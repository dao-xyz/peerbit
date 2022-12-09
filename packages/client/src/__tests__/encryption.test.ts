import { Entry } from "@dao-xyz/peerbit-log";
import { getObserverTopic, getReplicationTopic, Peerbit } from "../peer";
import { Operation } from "./utils/stores/event-store";
import { Ed25519Keypair, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";
import { KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { delay, waitFor } from "@dao-xyz/peerbit-time";

// Include test utilities
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { PermissionedEventStore } from "./utils/stores/test-store";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

const addHello = async (
    db: PermissionedEventStore,
    receiver: X25519PublicKey
) => {
    await db.store.add("hello", {
        reciever: {
            metadata: receiver,
            next: receiver,
            payload: receiver,
            signatures: receiver,
        },
    });
};
const checkHello = async (db: PermissionedEventStore) => {
    await waitFor(
        () => db.store.iterator({ limit: -1 }).collect().length === 1
    );

    const entries: Entry<Operation<string>>[] = db.store
        .iterator({ limit: -1 })
        .collect();

    expect(entries.length).toEqual(1);
    await entries[0].getPayload();
    expect(entries[0].payload.getValue().value).toEqual("hello");
};

describe(`encryption`, function () {
    jest.retryTimes(1); // TODO Side effects may cause failures (or something else? Like missing await somewhere which makes this test fail if multiple tests are running and slowing down the system)

    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        orbitdb3: Peerbit,
        db1: PermissionedEventStore,
        db2: PermissionedEventStore,
        db3: PermissionedEventStore;
    let recieverKey: KeyWithMeta<Ed25519Keypair>;
    let topic: string;

    beforeAll(async () => {});
    beforeEach(async () => {
        session = await LSession.connected(3, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            waitForKeysTimout: 10000,
        });
        const program = await orbitdb1.open(
            new PermissionedEventStore({
                network: new TrustedNetwork({
                    id: "network-tests",
                    rootTrust: orbitdb1.identity.publicKey,
                }),
            })
        );
        await orbitdb1.join(program);

        // Trusted client 2
        orbitdb2 = await Peerbit.create(session.peers[1], {
            waitForKeysTimout: 10000,
        });
        await program.network.add(orbitdb2.id);
        await program.network.add(orbitdb2.identity.publicKey);
        topic = program.address!.toString();

        // Untrusted client 3
        orbitdb3 = await Peerbit.create(session.peers[2], {
            waitForKeysTimout: 10000,
        });

        recieverKey = await orbitdb2.keystore.createEd25519Key();

        db1 = program;
    });

    afterEach(async () => {
        if (db1) await db1.drop();

        if (db2) await db2.drop();

        if (db3) await db3.drop();

        if (orbitdb1) {
            await orbitdb1.disconnect();
        }
        if (orbitdb2) {
            await orbitdb2.disconnect();
        }
        if (orbitdb3) {
            await orbitdb3.disconnect();
        }
        await session.stop();
    });

    afterAll(async () => {});

    it("replicates database of 1 entry known keys", async () => {
        let done = false;

        db2 = await orbitdb2.open<PermissionedEventStore>(db1.address, {
            topic: topic,
            onReplicationComplete: async (_store) => {
                await checkHello(db1);
                done = true;
            },
        });
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getReplicationTopic(topic)
        );
        await orbitdb2.keystore.saveKey(recieverKey);
        expect(
            await orbitdb2.keystore.getKey(recieverKey.keypair.publicKey)
        ).toBeDefined();

        await addHello(db1, recieverKey.keypair.publicKey);
        await waitFor(() => done);
    });

    it("replicates database of 1 entry unknown keys", async () => {
        // TODO this test is flaky when running all tests at once

        const unknownKey = await orbitdb1.keystore.createEd25519Key({
            id: "unknown",
            group: topic,
        });

        // We expect during opening that keys are exchange
        let done = false;
        db2 = await orbitdb2.open<PermissionedEventStore>(db1.address, {
            topic: topic,
            onReplicationComplete: async (store) => {
                if (store === db2.store.store) {
                    await checkHello(db1);
                    done = true;
                }
            },
        });
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getReplicationTopic(topic)
        );

        await waitFor(() => db2.network?.trustGraph.index.size >= 3);
        await orbitdb2.join(db2);

        expect(await orbitdb1.keystore.hasKey(unknownKey.keypair.publicKey));
        const xKey = await X25519PublicKey.from(unknownKey.keypair.publicKey);
        const getXKEy = await orbitdb1.keystore.getKey(xKey, topic);
        expect(getXKEy).toBeDefined();
        expect(!(await orbitdb2.keystore.hasKey(unknownKey.keypair.publicKey)));

        // ... so that append with reciever key, it the reciever will be able to decrypt
        await delay(5000);
        const t = 12;
        await addHello(db1, unknownKey.keypair.publicKey);
        await waitFor(() => done);
    });

    it("can retrieve secret keys if trusted", async () => {
        await waitForPeers(
            session.peers[2],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );

        const db1Key = await orbitdb1.keystore.createEd25519Key({
            id: "unknown",
            group: db1.address.toString(),
        });

        // Open store from orbitdb2 so that both client 1 and 2 is listening to the replication topic
        db2 = await orbitdb2.open<PermissionedEventStore>(db1.address!, {
            topic: topic,
        });
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getObserverTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            session.peers[0],
            getReplicationTopic(topic)
        );
        await waitFor(() => db2.network?.trustGraph.index.size >= 3);
        await orbitdb2.join(db2);
        await waitFor(() => db1.network?.trustGraph.index.size >= 4);

        const reciever = (await orbitdb2.getEncryptionKey(
            topic,
            db2.address.toString()
        )) as KeyWithMeta<Ed25519Keypair>;
        expect(reciever).toBeDefined();
        expect(db1Key.keypair.publicKey.equals(reciever.keypair.publicKey));
    });

    /* it("can relay with end to end encryption with public id and clock (E2EE-weak)", async () => {
        await waitForPeers(
            session.peers[2],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );

        db2 = await orbitdb2.open<PermissionedEventStore>(db1.address!, {
            topic: topic,
        });
        await waitForPeers(session.peers[1], session.peers[0], getObserverTopic(topic));
        await waitForPeers(session.peers[1], session.peers[0], getReplicationTopic(topic));
        await waitFor(() => db2.network?.trustGraph.index.size >= 3);
        await orbitdb2.join(db2);

        const client3Key = await orbitdb3.keystore.createEd25519Key({
            id: "unknown",
            group: db1.address.toString(),
        });

        await db2.store.add("hello", {
            reciever: {
                metadata: undefined,
                next: undefined,
                signatures: undefined,
                payload: client3Key.keypair.publicKey,
            },
        });

        // Wait for db1 (the relay) to get entry
        await waitFor(() => db1.store.store.oplog.values.length === 1);
        const entriesRelay: Entry<Operation<string>>[] = db1.store
            .iterator({ limit: -1 })
            .collect();
        expect(entriesRelay.length).toEqual(1);
        try {
            await entriesRelay[0].getPayload(); // should fail, since relay can not see the message
            assert(false);
        } catch (error) {
            expect(error).toBeInstanceOf(AccessError);
        }

        const sender: Entry<Operation<string>>[] = db2.store
            .iterator({ limit: -1 })
            .collect();
        expect(sender.length).toEqual(1);
        await sender[0].getPayload();
        expect(sender[0].payload.getValue().value).toEqual("hello");

        // Now close db2 and open db3 and make sure message are available
        await db2.drop();
        db3 = await orbitdb3.open<PermissionedEventStore>(db1.address, {
            topic: topic,
            onReplicationComplete: async (store) => {
                const entriesRelay: Entry<Operation<string>>[] = db3.store
                    .iterator({ limit: -1 })
                    .collect();
                expect(entriesRelay.length).toEqual(1);
                await entriesRelay[0].getPayload(); // should pass since orbitdb3 got encryption key
            },
        });
    }); */
});
