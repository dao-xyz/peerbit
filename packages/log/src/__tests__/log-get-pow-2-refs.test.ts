import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LogCreator } from "./utils/log-creator.js";
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

let signKey: KeyWithMeta<Ed25519Keypair>,
    signKey2: KeyWithMeta<Ed25519Keypair>,
    signKey3: KeyWithMeta<Ed25519Keypair>;

describe("Log - GetPow2Refs", function () {
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
        signKey = await keystore.getKey(new Uint8Array([3]));
        store = new Blocks(new MemoryLevelBlockStore());
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });

    describe("Basic iterator functionality", () => {
        let log1: Log<string>;

        beforeEach(async () => {
            log1 = new Log(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId: "X" }
            );

            for (let i = 0; i <= 100; i++) {
                await log1.append("entry" + i);
            }
        });
        it("get refs one", async () => {
            const heads = log1.heads;
            expect(heads).toHaveLength(1);
            const refs = log1.getPow2Refs(1);
            expect(refs).toHaveLength(1);
            for (const head of heads) {
                expect(refs.find((x) => x.hash === head.hash)).toBeDefined();
            }
        });

        it("get refs 8", async () => {
            const heads = log1.heads;
            const refs = log1.getPow2Refs(8);
            expect(refs).toHaveLength(3); // 2**3 = 8
            for (const head of heads) {
                expect(refs.find((x) => x.hash === head.hash));
            }
            let i = 0;
            for (const entry of refs) {
                expect(entry.payload.getValue()).toEqual(
                    "entry" + (100 + 1 - 2 ** i++)
                );
            }
        });
    });
});
