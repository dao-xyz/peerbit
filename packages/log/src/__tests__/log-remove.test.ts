import rmrf from "rimraf";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import fs from "fs-extra";
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

describe("Log remove", function () {
    let keystore: Keystore;
    let store: Blocks;

    beforeAll(async () => {
        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );

        signKey = (await await keystore.getKey(
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
    describe("remove", () => {
        it("removes by next", async () => {
            const log = new Log<string>(store, {
                ...signKey.keypair,
                sign: (data) => signKey.keypair.sign(data),
            });
            expect(log.values instanceof Array).toEqual(true);
            expect(log.length).toEqual(0);
            await log.append("hello1");
            await log.append("hello2");
            const h3 = await log.append("hello3");
            expect(log.values instanceof Array).toEqual(true);
            expect(log.length).toEqual(3);
            expect(log.values[0].payload.getValue()).toEqual("hello1");
            expect(log.values[1].payload.getValue()).toEqual("hello2");
            expect(log.values[2].payload.getValue()).toEqual("hello3");
            log.removeAll([h3]);
            expect(log.length).toEqual(0);
            expect(log.values.length).toEqual(0);
        });
    });
});
