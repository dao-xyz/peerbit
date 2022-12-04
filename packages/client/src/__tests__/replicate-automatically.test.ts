import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { KeyBlocks } from "./utils/stores/key-value-store";
import assert from "assert";
import mapSeries from "p-each-series";
import { v4 as uuid } from "uuid";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { waitFor } from "@dao-xyz/peerbit-time";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Automatic Replication`, function () {
    /*  let ipfsd1: Controller, ipfsd2: Controller, ipfsd3: Controller, ipfsd4: Controller, ipfs1: IPFS, ipfs2: IPFS, ipfs3: IPFS, ipfs4: IPFS */
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        orbitdb3: Peerbit,
        orbitdb4: Peerbit;
    let session: LSession;
    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        orbitdb1 = await Peerbit.create(session.peers[0], {});
        orbitdb2 = await Peerbit.create(session.peers[1], {});
    });

    afterAll(async () => {
        if (orbitdb1) {
            await orbitdb1.stop();
        }
        if (orbitdb2) {
            await orbitdb2.stop();
        }
        if (orbitdb3) {
            await orbitdb3.stop();
        }
        if (orbitdb4) {
            await orbitdb4.stop();
        }

        await session.stop();
    });

    it("starts replicating the database when peers connect", async () => {
        const entryCount = 33;
        const entryArr: number[] = [];

        const topic = uuid();

        const db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replicate-automatically-tests" }),
            { topic: topic }
        );

        const db3 = await orbitdb1.open(
            new KeyBlocks<string>({
                id: "replicate-automatically-tests-kv",
            }),
            {
                topic: topic,
                onReplicationComplete: (_) => {
                    fail();
                },
            }
        );

        // Create the entries in the first database
        for (let i = 0; i < entryCount; i++) {
            entryArr.push(i);
        }

        await mapSeries(entryArr, (i) => db1.add("hello" + i));

        // Open the second database
        let done = false;
        const db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            {
                topic: topic,
                onReplicationComplete: (_) => {
                    // Listen for the 'replicated' events and check that all the entries
                    // were replicated to the second database
                    expect(
                        db2.iterator({ limit: -1 }).collect().length
                    ).toEqual(entryCount);
                    const result1 = db1.iterator({ limit: -1 }).collect();
                    const result2 = db2.iterator({ limit: -1 }).collect();
                    expect(result1.length).toEqual(result2.length);
                    for (let i = 0; i < result1.length; i++) {
                        assert(result1[i].equals(result2[i]));
                    }
                    done = true;
                },
            }
        );

        const _db4 = await orbitdb2.open<KeyBlocks<string>>(
            await KeyBlocks.load<KeyBlocks<string>>(
                orbitdb2._store,
                db3.address!
            ),
            {
                topic: topic,
                onReplicationComplete: (_) => {
                    fail();
                },
            }
        );

        await waitFor(() => done);
    });

    it("starts replicating the database when peers connect in write mode", async () => {
        const entryCount = 1;
        const entryArr: number[] = [];
        const topic = uuid();
        const db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replicate-automatically-tests" }),
            { topic: topic, replicate: false }
        );

        // Create the entries in the first database
        for (let i = 0; i < entryCount; i++) {
            entryArr.push(i);
        }

        await mapSeries(entryArr, (i) => db1.add("hello" + i));

        // Open the second database
        let done = false;
        const db2 = await orbitdb2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                orbitdb2._store,
                db1.address!
            ),
            {
                topic: topic,
                onReplicationComplete: (_) => {
                    // Listen for the 'replicated' events and check that all the entries
                    // were replicated to the second database
                    expect(
                        db2.iterator({ limit: -1 }).collect().length
                    ).toEqual(entryCount);
                    const result1 = db1.iterator({ limit: -1 }).collect();
                    const result2 = db2.iterator({ limit: -1 }).collect();
                    expect(result1.length).toEqual(result2.length);
                    for (let i = 0; i < result1.length; i++) {
                        expect(result1[i].equals(result2[i])).toBeTrue();
                    }
                    done = true;
                },
            }
        );

        await waitFor(() => done);
    });
});
