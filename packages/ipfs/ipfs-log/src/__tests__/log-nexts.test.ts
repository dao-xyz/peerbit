import rmrf from "rimraf";
import fs from "fs-extra";
import { jest } from "@jest/globals";

import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Log } from "../log.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";

let signKey: KeyWithMeta<Ed25519Keypair>;

describe("Log - Nexts", function () {
    let keystore: Keystore, store: Blocks;

    beforeAll(async () => {
        rmrf.sync(testKeyStorePath(__filenameBase));

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );
        signKey = (await keystore.getKey(
            new Uint8Array([0])
        )) as KeyWithMeta<Ed25519Keypair>;
        store = new Blocks(new MemoryLevelBlockStore());
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });
    describe("Custom next", () => {
        /*   it('will filter refs from next', async () => {
              const log1 = new Log(store, {
 ...signKey.keypair,
sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
}, { logId: 'A' })
              const e0 = await log1.append("0", { refs: [] })
              await log1.append("1a", { refs: [e0.hash], nexts: [e0.hash] })
              expect(log1.values[0].next?.length).toEqual(0)
              expect(log1.values[0].refs?.length).toEqual(0)
              expect(log1.values[1].next?.length).toEqual(1)
              expect(log1.values[1].refs?.length).toEqual(0)
              expect(log1.heads.length).toEqual(1)
              await log1.append("1b", { refs: [e0.hash], nexts: [e0.hash] })
              expect(log1.values[0].next?.length).toEqual(0)
              expect(log1.values[0].refs?.length).toEqual(0)
              expect(log1.values[1].next?.length).toEqual(1)
              expect(log1.values[1].refs?.length).toEqual(0)
              expect(log1.values[2].next?.length).toEqual(1)
              expect(log1.values[2].next[0]).toEqual(e0.hash)
              expect(log1.values[2].refs?.length).toEqual(0)
              expect(log1.heads.length).toEqual(2) // 1a and 1b since nextsResolver is not including 1a (e0 has two branchs)
          }) */

        /*   it('can get nexts by references', async () => {
     
              const log1 = new Log(store, {
 ...signKey.keypair,
sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
}, { logId: 'A' })
              const e0 = await log1.append("0", { refs: [] })
              const e1 = await log1.append("1", { refs: [e0.hash], nexts: log1.getHeadsFromHashes([e0.hash]) })
              const e2a = await log1.append("2a", { refs: [e0.hash], nexts: log1.getHeadsFromHashes([e0.hash]) })
              const e2b = await log1.append("2b", { refs: [e0.hash], nexts: [e1.hash] })
     
              const fork = await log1.append("x", { refs: [], nexts: [] }) // independent entry, not related to the other entries that are chained
     
              expect(log1.values).toStrictEqual([e0, e1, e2a, e2b, fork]);
              expect(e0.next?.length).toEqual(0)
              expect(e0.refs?.length).toEqual(0)
              expect(e1.next).toContainAllValues([e0.hash])
              expect(e1.refs?.length).toEqual(0)
              expect(e2a.next).toContainAllValues([e1.hash])
              expect(e2a.refs).toContainAllValues([e0.hash])
              expect(e2b.next).toContainAllValues([e1.hash])
              expect(e2b.refs).toContainAllValues([e0.hash])
              expect(fork.refs).toBeEmpty();
              expect(fork.next).toBeEmpty();
              expect(log1.heads.map(h => h.hash)).toContainValues([e2a.hash, e2a.hash, fork.hash])
              expect([...log1._nextsIndexToHead[e0.hash]]).toEqual([e1.hash])
     
              // we will get next where there should be two applicable heads
              const nexts = log1.getHeadsFromHashes([e0.hash]);
              const e3 = await log1.append("3", { refs: [e0.hash], nexts: nexts })
              expect(log1.values[5].next).toContainAllValues([e2a.hash, e2b.hash])
              expect(log1.values[5].refs).toContainAllValues([e0.hash])
              expect(log1.heads.map(h => h.hash)).toContainValues([e3.hash, fork.hash])
          })
    */
        it("can fork explicitly", async () => {
            const log1 = new Log(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId: "A" }
            );
            const e0 = await log1.append("0", { nexts: [] });
            const e1 = await log1.append("1", { nexts: [e0] });

            const e2a = await log1.append("2a", {
                nexts: log1.getHeadsFromHashes([e0.hash]),
            });
            expect(log1.values[0].next?.length).toEqual(0);
            expect(log1.values[1].next).toEqual([e0.hash]);
            expect(log1.values[2].next).toEqual([e1.hash]);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
            ]);
            expect([...log1._nextsIndexToHead[e0.hash]]).toEqual([e1.hash]);

            // fork at root
            const e2ForkAtRoot = await log1.append("2b", { nexts: [] });
            expect(log1.values[3]).toEqual(e2ForkAtRoot); // Due to clock  // If we only use logical clok then it should be index 1 since clock is reset as this is a root "fork"
            expect(log1.values[2]).toEqual(e2a);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
                e2ForkAtRoot.hash,
            ]);

            // fork at 0
            const e2ForkAt0 = await log1.append("2c", { nexts: [e0] });
            expect(log1.values[4].next).toEqual([e0.hash]);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
                e2ForkAtRoot.hash,
                e2ForkAt0.hash,
            ]);

            // fork at 1
            const e2ForkAt1 = await log1.append("2d", { nexts: [e1] });
            expect(log1.values[5].next).toEqual([e1.hash]);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
                e2ForkAtRoot.hash,
                e2ForkAt0.hash,
                e2ForkAt1.hash,
            ]);
        });
    });
});
