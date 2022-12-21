import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";
import { waitFor } from "@dao-xyz/peerbit-time";

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

        const data = new Uint8Array([1, 2, 3]);
        const cid = await store.put(data, "raw", { pin: true });
        expect(stringifyCid(cid)).toEqual(
            "zb3wdq9czZ6jYX1DMYx3b5AhawVNWcBawniwy4TVDpqXkCHgV"
        );
        await waitForPeers(
            session.peers[1],
            [session.peers[0].peerId],
            (store._store as LibP2PBlockStore)._transportTopic
        );
        const readData = await store2.get<Uint8Array>(stringifyCid(cid));
        expect(readData).toEqual(data);
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

        await waitFor(
            () => (store2._store as LibP2PBlockStore)._gossipCache!.size === 1
        );

        (store2._store as LibP2PBlockStore)._readFromPubSub = () =>
            Promise.resolve(undefined); // make sure we only read from gossipCache

        const readData = await store2.get<Uint8Array>(stringifyCid(cid));
        expect(readData).toEqual(data);
    });
});
