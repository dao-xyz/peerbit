import { MemoryLevelBlockStore } from "../level";
import { createBlock, getBlockValue, stringifyCid } from "../block.js";
import { delay } from "@dao-xyz/peerbit-time";

describe(`level`, function () {
    let store: MemoryLevelBlockStore;

    afterEach(async () => {
        await store.close();
    });

    it("rw", async () => {
        store = new MemoryLevelBlockStore();
        await store.open();
        const data = new Uint8Array([1, 2, 3]);
        const cid = await store.put(await createBlock(data, "raw"));
        expect(stringifyCid(cid)).toEqual(
            "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
        );

        const readData = await store.get<Uint8Array>(stringifyCid(cid));
        expect(await getBlockValue(readData!)).toEqual(data);
    });

    it("batch interval", async () => {
        store = new MemoryLevelBlockStore({ batch: { interval: 1000 } });
        await store.open();

        let cids: string[] = [];
        const t1 = +new Date();
        for (let i = 0; i < 100; i++) {
            cids.push(
                await store.put(await createBlock(new Uint8Array([i]), "raw"))
            );
        }
        expect(store._tempStore.size).toEqual(100);
        expect(store._txQueue).toHaveLength(100);
        for (let i = 0; i < 100; i++) {
            expect(await store.get<Uint8Array>(cids[i])).toBeDefined();
        }
        const t2 = +new Date();
        expect(t2 - t1).toBeLessThan(1000);
        await delay(1001);
        expect(store._tempStore.size).toEqual(0);
        expect(store._txQueue).toHaveLength(0);

        for (let i = 0; i < 100; i++) {
            expect(await store.get<Uint8Array>(cids[i])).toBeDefined();
        }
    });

    it("idle then write", async () => {
        store = new MemoryLevelBlockStore({ batch: { interval: 50 } });
        await store.open();
        await store.idle();
        const cid = await store.put(
            await createBlock(new Uint8Array([3, 2, 1]), "raw")
        );
        await delay(100);
        expect(await store.get<Uint8Array>(cid)).toBeDefined();
    });
});
