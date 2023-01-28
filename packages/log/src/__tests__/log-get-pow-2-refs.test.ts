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
import {
    BlockStore,
    MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>,
    signKey2: KeyWithMeta<Ed25519Keypair>,
    signKey3: KeyWithMeta<Ed25519Keypair>;

describe("Log - GetPow2Refs", function () {
    let keystore: Keystore, store: BlockStore;

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
        store = new MemoryLevelBlockStore();
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });

    describe("Single log", () => {
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
            const refs = log1.getReferenceSamples(heads[0], {
                pointerCount: 1,
            });
            expect(refs).toHaveLength(1);
            for (const head of heads) {
                expect(refs.find((x) => x.hash === head.hash)).toBeDefined();
            }
        });

        it("get refs 4", async () => {
            const heads = log1.heads;
            const refs = log1.getReferenceSamples(heads[0], {
                pointerCount: 4,
            });
            expect(refs).toHaveLength(2); // 2**2 = 4
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

        it("get refs 8", async () => {
            const heads = log1.heads;
            const refs = log1.getReferenceSamples(heads[0], {
                pointerCount: 8,
            });
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

        it("get refs with memory limit", async () => {
            const heads = log1.heads;
            expect(heads).toHaveLength(1);
            const refs = log1.getReferenceSamples(heads[0], {
                pointerCount: Number.MAX_SAFE_INTEGER,
                memoryLimit: 100,
            });
            const sum = refs
                .map((r) => r._payload.byteLength)
                .reduce((sum, current) => {
                    sum = sum || 0;
                    sum += current;
                    return sum;
                });
            expect(sum).toBeLessThan(100);
            expect(sum).toBeGreaterThan(80);
        });
    });

    describe("multiple heads", () => {
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

            for (let i = 0; i <= 10; i++) {
                await log1.append("entry" + i, { nexts: [] });
            }
        });

        it("no refs if no nexts", async () => {
            const heads = log1.heads;
            const refs = log1.getReferenceSamples(heads[0], {
                pointerCount: 8,
            });
            expect(refs).toHaveLength(1); // because heads[0] has no nexts (all commits are roots)
            expect(heads[0].hash).toEqual(refs[0].hash);
        });
    });
});
