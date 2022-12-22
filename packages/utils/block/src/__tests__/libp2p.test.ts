import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";

describe(`pubsub`, function () {
    let session: LSession, store: Blocks, store2: Blocks;

    beforeAll(async () => {
        session = await LSession.connected(2);
    });

    afterEach(async () => {
        await store.close();
        await store2.close();
    });
    afterAll(async () => {
        await session.stop();
    });

    it("large", async () => {
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();

        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );

        expect((store._store as LibP2PBlockStore)._gossipCache).toBeUndefined();
        expect((store._store as LibP2PBlockStore)._gossip).toBeFalse();

        await store2.open();

        const cids: string[] = [];

        const rnds: Uint8Array[] = [];
        let len = 3900000;
        const t1 = +new Date();
        for (let i = 0; i < 100; i++) {
            rnds.push(crypto.randomBytes(len));
        }

        for (let i = 0; i < 100; i++) {
            cids.push(await store.put(rnds[i], "raw", { pin: true }));
        }

        for (const [i, cid] of cids.entries()) {
            const readData = await store2.get<Uint8Array>(stringifyCid(cid));
            expect(readData).toHaveLength(len);
        }
        const t2 = +new Date();
        console.log("Large", t2 - t1);
    });

    it("small", async () => {
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();

        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );

        expect((store._store as LibP2PBlockStore)._gossipCache).toBeUndefined();
        expect((store._store as LibP2PBlockStore)._gossip).toBeFalse();

        await store2.open();

        const cids: string[] = [];

        const rnds: Uint8Array[] = [];
        let len = 100;
        const t1 = +new Date();
        for (let i = 0; i < 5000; i++) {
            rnds.push(crypto.randomBytes(len));
        }

        for (let i = 0; i < 5000; i++) {
            cids.push(await store.put(rnds[i], "raw", { pin: true }));
        }

        for (const [i, cid] of cids.entries()) {
            const readData = await store2.get<Uint8Array>(stringifyCid(cid));
            expect(readData).toHaveLength(len);
        }
        const t2 = +new Date();
        console.log("Small", t2 - t1);
    });

    it("rw", async () => {
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();

        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );

        expect((store._store as LibP2PBlockStore)._gossipCache).toBeUndefined();
        expect((store._store as LibP2PBlockStore)._gossip).toBeFalse();

        await store2.open();

        const data = new Uint8Array([5, 4, 3]);
        const cid = await store.put(data, "raw", { pin: true });
        expect(stringifyCid(cid)).toEqual(
            "zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr"
        );
        await waitForPeers(
            session.peers[1],
            [session.peers[0].peerId],
            (store._store as LibP2PBlockStore)._transportTopic
        );
        const readData = await store2.get<Uint8Array>(stringifyCid(cid));
        expect(readData).toEqual(data);
    });
    it("timeout", async () => {
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();
        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await store2.open();

        await waitForPeers(
            session.peers[1],
            [session.peers[0].peerId],
            (store._store as LibP2PBlockStore)._transportTopic
        );

        const t1 = +new Date();
        const readData = await store.get<Uint8Array>(
            "zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr",
            { timeout: 3000 }
        );
        const t2 = +new Date();
        expect(readData).toBeUndefined();
        expect(t2 - t1 < 3100);
    });

    it("gossip", async () => {
        store = new Blocks(
            new LibP2PBlockStore(
                session.peers[0],
                new MemoryLevelBlockStore(),
                { gossip: { cache: {} } }
            )
        );
        await store.open();

        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], undefined, {
                gossip: { cache: {} },
            })
        );
        await store2.open();

        const data = new Uint8Array([1, 2, 3]);
        await waitForPeers(
            session.peers[1],
            [session.peers[0].peerId],
            (store._store as LibP2PBlockStore)._transportTopic
        );

        const cid = await store.put(data, "raw", { pin: true });
        expect(stringifyCid(cid)).toEqual(
            "zb3wdq9czZ6jYX1DMYx3b5AhawVNWcBawniwy4TVDpqXkCHgV"
        );

        await delay(5000);

        await waitFor(
            () => (store2._store as LibP2PBlockStore)._gossipCache!.size === 1
        );

        (store2._store as LibP2PBlockStore)._readFromPubSub = () =>
            Promise.resolve(undefined); // make sure we only read from gossipCache

        const readData = await store2.get<Uint8Array>(stringifyCid(cid));
        expect(readData).toEqual(data);
    });
});
