// Test utils
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";

describe(`pubsub`, function () {
    let session: LSession, store: Blocks, store2: Blocks;

    beforeAll(async () => {
        session = await LSession.connected(2);
    });
    beforeEach(async () => {
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();

        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await store2.open();
    });
    afterEach(async () => {
        await store.close();
        await store2.close();
    });
    afterAll(async () => {
        await session.stop();
    });

    it("rw", async () => {
        const data = new Uint8Array([1, 2, 3]);
        const cid = await store.put(data, "raw", { pin: true });
        expect(stringifyCid(cid)).toEqual(
            "zb3wdq9czZ6jYX1DMYx3b5AhawVNWcBawniwy4TVDpqXkCHgV"
        );
        session.peers[0].pubsub.getSubscribers("raw");
        await waitForPeers(
            session.peers[1],
            [session.peers[0].peerId],
            (store._store as LibP2PBlockStore)._transportTopic
        );
        const readData = await store2.get<Uint8Array>(stringifyCid(cid));
        expect(readData).toEqual(data);
    });
});
