import rmrf from "rimraf";
import fs from "fs-extra";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
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
import { createStore } from "./utils.js";

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
            const { entry: e0 } = await log1.append("0", { nexts: [] });
            const { entry: e1 } = await log1.append("1", { nexts: [e0] });

            const { entry: e2a } = await log1.append("2a", {
                nexts: log1.heads,
            });
            expect(log1.values[0].next?.length).toEqual(0);
            expect(log1.values[1].next).toEqual([e0.hash]);
            expect(log1.values[2].next).toEqual([e1.hash]);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
            ]);
            /*    expect([...log1._nextsIndexToHead[e0.hash]]).toEqual([e1.hash]); */

            // fork at root
            const { entry: e2ForkAtRoot } = await log1.append("2b", {
                nexts: [],
            });
            expect(log1.values[3]).toEqual(e2ForkAtRoot); // Due to clock  // If we only use logical clok then it should be index 1 since clock is reset as this is a root "fork"
            expect(log1.values[2]).toEqual(e2a);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
                e2ForkAtRoot.hash,
            ]);

            // fork at 0
            const { entry: e2ForkAt0 } = await log1.append("2c", {
                nexts: [e0],
            });
            expect(log1.values[4].next).toEqual([e0.hash]);
            expect(log1.heads.map((h) => h.hash)).toContainAllValues([
                e2a.hash,
                e2ForkAtRoot.hash,
                e2ForkAt0.hash,
            ]);

            // fork at 1
            const { entry: e2ForkAt1 } = await log1.append("2d", {
                nexts: [e1],
            });
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
