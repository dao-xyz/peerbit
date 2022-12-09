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

describe("Log - Iterator", function () {
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
        //@ts-ignore
        signKey2 = await keystore.getKey(new Uint8Array([2]));
        //@ts-ignore
        signKey3 = await keystore.getKey(new Uint8Array([1]));
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

        it("returns a Symbol.iterator object", async () => {
            const it = log1.iterator({
                lte: log1.values[10].hash,
                amount: 0,
            });

            expect(typeof it[Symbol.iterator]).toEqual("function");
            assert.deepStrictEqual(it.next(), {
                value: undefined,
                done: true,
            });
        });

        it("returns length with lte and amount", async () => {
            const amount = 10;

            const it = log1.iterator({
                lte: log1.values[10].hash,
                amount: amount,
            });
            const length = [...it].length;
            expect(length).toEqual(10);
        });

        it("returns entries with lte and amount and payload", async () => {
            const amount = 10;

            const it = log1.iterator({
                lte: log1.values[67].hash,
                amount: amount,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (67 - i++));
            }
            expect(i).toEqual(amount);
        });

        it("returns correct length with gt and amount", async () => {
            const amount = 5;
            const it = log1.iterator({
                gt: log1.values[67].hash,
                amount: amount,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (72 - i++));
            }
            expect(i).toEqual(amount);
        });

        it("returns entries with gte and amount and payload", async () => {
            const amount = 12;

            const it = log1.iterator({
                gt: log1.values[79 - amount].hash,
                amount: amount,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (79 - i++));
            }
            expect(i).toEqual(amount);
        });

        /* eslint-disable camelcase */
        it("iterates with lt and gt", async () => {
            const it = log1.iterator({
                gt: log1.values[10].hash,
                lt: log1.values[20].hash,
            });
            const hashes = [...it].map((e) => e.hash);

            // neither hash should appear in the array
            assert.strictEqual(hashes.indexOf(log1.values[10].hash), -1);
            assert.strictEqual(hashes.indexOf(log1.values[20].hash), -1);
            expect(hashes.length).toEqual(9);
        });

        it("iterates with lt and gte", async () => {
            const it = log1.iterator({
                gte: log1.values[10].hash,
                lt: log1.values[20].hash,
            });
            const hashes = [...it].map((e) => e.hash);

            // only the gte hash should appear in the array
            assert.strictEqual(hashes.indexOf(log1.values[10].hash), 9);
            assert.strictEqual(hashes.indexOf(log1.values[20].hash), -1);
            expect(hashes.length).toEqual(10);
        });

        it("iterates with lte and gt", async () => {
            const it = log1.iterator({
                gt: log1.values[10].hash,
                lte: log1.values[20].hash,
            });
            const hashes = [...it].map((e) => e.hash);

            // only the lte hash should appear in the array
            assert.strictEqual(hashes.indexOf(log1.values[10].hash), -1);
            assert.strictEqual(hashes.indexOf(log1.values[20].hash), 0);
            expect(hashes.length).toEqual(10);
        });

        it("iterates with lte and gte", async () => {
            const it = log1.iterator({
                gte: log1.values[10].hash,
                lte: log1.values[20].hash,
            });
            const hashes = [...it].map((e) => e.hash);

            // neither hash should appear in the array
            assert.strictEqual(hashes.indexOf(log1.values[10].hash), 10);
            assert.strictEqual(hashes.indexOf(log1.values[20].hash), 0);
            expect(hashes.length).toEqual(11);
        });

        it("returns length with gt and default amount", async () => {
            const it = log1.iterator({
                gt: log1.values[67].hash,
            });

            expect([...it].length).toEqual(33);
        });

        it("returns entries with gt and default amount", async () => {
            const it = log1.iterator({
                gt: log1.values[0].hash,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (100 - i++));
            }
        });

        it("returns length with gte and default amount", async () => {
            const it = log1.iterator({
                gte: log1.values[80].hash,
            });

            expect([...it].length).toEqual(21);
        });

        it("returns entries with gte and default amount", async () => {
            const it = log1.iterator({
                gte: log1.values[10].hash,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (100 - i++));
            }
        });

        it("returns length with lt and default amount value", async () => {
            const it = log1.iterator({
                lt: log1.values[67].hash,
            });

            expect([...it].length).toEqual(67);
        });

        it("returns entries with lt and default amount value", async () => {
            const it = log1.iterator({
                lt: log1.values[67].hash,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (66 - i++));
            }
        });

        it("returns length with lte and default amount value", async () => {
            const it = log1.iterator({
                lte: log1.values[68].hash,
            });

            expect([...it].length).toEqual(69);
        });

        it("returns entries with lte and default amount value", async () => {
            const it = log1.iterator({
                lte: log1.values[67].hash,
            });

            let i = 0;
            for (const entry of it) {
                expect(entry.payload.getValue()).toEqual("entry" + (67 - i++));
            }
        });
    });

    describe("Iteration over forked/joined logs", () => {
        let fixture: {
                log: Log<string>;
                expectedData: string[];
                json: any;
            },
            identities;

        beforeAll(async () => {
            identities = [signKey3, signKey2, signKey3, signKey];
            fixture = await LogCreator.createLogWithSixteenEntries(
                store,
                identities
            );
        });

        it("returns the full length from all heads", async () => {
            const it = fixture.log.iterator({
                lte: fixture.log.heads,
            });

            expect([...it].length).toEqual(16);
        });

        it("returns partial entries from all heads", async () => {
            const it = fixture.log.iterator({
                lte: fixture.log.heads,
                amount: 6,
            });

            assert.deepStrictEqual(
                [...it].map((e) => e.payload.getValue()),
                [
                    "entryA10",
                    "entryA9",
                    "entryA8",
                    "entryA7",
                    "entryC0",
                    "entryA6",
                ]
            );
        });

        it("returns partial logs from single heads #1", async () => {
            const it = fixture.log.iterator({
                lte: [fixture.log.heads[0]],
            });

            expect([...it].length).toEqual(10);
        });

        it("returns partial logs from single heads #2", async () => {
            const it = fixture.log.iterator({
                lte: [fixture.log.heads[1]],
            });

            expect([...it].length).toEqual(11);
        });
    });
});
