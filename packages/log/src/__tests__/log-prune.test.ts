import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";

import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>;

describe("Append prune", function () {
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

        //@ts-ignore
        signKey = await keystore.getKey(new Uint8Array([0]));

        store = new Blocks(new MemoryLevelBlockStore());
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });

    it("cut back to max oplog length", async () => {
        const log = new Log<string>(
            store,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            { logId: "A", prune: { from: 1, to: 1 } }
        );
        await log.append("hello1");
        await log.append("hello2");
        await log.append("hello3");
        expect(log.length).toEqual(1);
        expect(log.values[0].payload.getValue()).toEqual("hello3");
    });

    it("cut back to cut length", async () => {
        const log = new Log<string>(
            store,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            { logId: "A", prune: { from: 3, to: 1 } } // when length > 3 cut back to 1
        );
        const a1 = await log.append("hello1");
        const a2 = await log.append("hello2");
        const a3 = await log.append("hello3");
        expect(await log._storage.get(a1.hash)).toBeDefined();
        expect(await log._storage.get(a2.hash)).toBeDefined();
        expect(await log._storage.get(a3.hash)).toBeDefined();
        expect(log.length).toEqual(3);
        const a4 = await log.append("hello4");
        expect(log.length).toEqual(1);
        expect(await log._storage.get(a1.hash)).toBeUndefined();
        expect(await log._storage.get(a2.hash)).toBeUndefined();
        expect(await log._storage.get(a3.hash)).toBeUndefined();
        expect(await log._storage.get(a4.hash)).toBeDefined();
        expect(log.values[0].payload.getValue()).toEqual("hello4");
    });

    it("cut back to bytelength", async () => {
        const log = new Log<string>(
            store,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            { logId: "A", prune: { bytelength: 15 } } // bytelength is 15 so for every new helloX we hav eto delete the previous helloY
        );
        const a1 = await log.append("hello1");
        expect(await log._storage.get(a1.hash)).toBeDefined();
        expect(log.values.map((x) => x.payload.getValue())).toEqual(["hello1"]);
        const a2 = await log.append("hello2");
        expect(await log._storage.get(a2.hash)).toBeDefined();
        expect(log.values.map((x) => x.payload.getValue())).toEqual(["hello2"]);
        const a3 = await log.append("hello3");
        expect(await log._storage.get(a3.hash)).toBeDefined();
        expect(log.values.map((x) => x.payload.getValue())).toEqual(["hello3"]);
        const a4 = await log.append("hello4");
        expect(log.values.map((x) => x.payload.getValue())).toEqual(["hello4"]);
        expect(await log._storage.get(a1.hash)).toBeUndefined();
        expect(await log._storage.get(a2.hash)).toBeUndefined();
        expect(await log._storage.get(a3.hash)).toBeUndefined();
        expect(await log._storage.get(a4.hash)).toBeDefined();
    });
});
