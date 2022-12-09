import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Entry } from "../entry.js";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { arraysCompare } from "@dao-xyz/peerbit-borsh-utils";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import {
    LibP2PBlockStore,
    MemoryLevelBlockStore,
    Blocks,
    DEFAULT_BLOCK_TRANSPORT_TOPIC,
} from "@dao-xyz/peerbit-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>,
    signKey2: KeyWithMeta<Ed25519Keypair>,
    signKey3: KeyWithMeta<Ed25519Keypair>,
    signKey4: KeyWithMeta<Ed25519Keypair>;

const last = (arr: any[]) => {
    return arr[arr.length - 1];
};

describe("Log - Join", function () {
    let keystore: Keystore;
    let session: LSession;
    let store: Blocks;
    let store2: Blocks;

    beforeAll(async () => {
        rmrf.sync(testKeyStorePath(__filenameBase));

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );

        // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
        const keys: KeyWithMeta<Ed25519Keypair>[] = [];
        for (let i = 0; i < 4; i++) {
            keys.push(
                (await keystore.getKey(
                    new Uint8Array([i])
                )) as KeyWithMeta<Ed25519Keypair>
            );
        }
        keys.sort((a, b) => {
            return arraysCompare(
                a.keypair.publicKey.publicKey,
                b.keypair.publicKey.publicKey
            );
        });
        signKey = keys[0];
        signKey2 = keys[1];
        signKey3 = keys[2];
        signKey4 = keys[3];
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();
        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await store2.open();
    });

    afterAll(async () => {
        await store.close();
        await store2.close();
        await session.stop();
        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });

    describe("join", () => {
        let log1: Log<string>,
            log2: Log<string>,
            log3: Log<string>,
            log4: Log<string>;

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
            log2 = new Log(
                store,
                {
                    ...signKey2.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey2.keypair.sign(data),
                },
                { logId: "X" }
            );
            log3 = new Log(
                store2,
                {
                    ...signKey3.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey3.keypair.sign(data),
                },
                { logId: "X" }
            );
            log4 = new Log(
                store2,
                {
                    ...signKey4.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey4.keypair.sign(data),
                },
                { logId: "X" }
            );
        });

        it("joins logs", async () => {
            const items1: Entry<string>[] = [];
            const items2: Entry<string>[] = [];
            const items3: Entry<string>[] = [];
            const amount = 100;

            for (let i = 1; i <= amount; i++) {
                const prev1 = last(items1);
                const prev2 = last(items2);
                const prev3 = last(items3);
                const n1 = await Entry.create({
                    store,
                    identity: {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    gidSeed: "X" + i,
                    data: "entryA" + i,
                    next: prev1 ? [prev1] : undefined,
                });
                const n2 = await Entry.create({
                    store,
                    identity: {
                        ...signKey2.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey2.keypair.sign(data),
                    },
                    data: "entryB" + i,
                    next: prev2 ? [prev2, n1] : [n1],
                });
                const n3 = await Entry.create({
                    store: store2,
                    identity: {
                        ...signKey3.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey3.keypair.sign(data),
                    },
                    data: "entryC" + i,
                    next: prev3 ? [prev3, n1, n2] : [n1, n2],
                });
                items1.push(n1);
                items2.push(n2);
                items3.push(n3);
            }

            // Here we're creating a log from entries signed by A and B
            // but we accept entries from C too
            const logA = await Log.fromEntry(
                store,
                {
                    ...signKey3.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey3.keypair.sign(data),
                },
                last(items2),
                { length: -1 }
            );

            // Here we're creating a log from entries signed by peer A, B and C
            // "logA" accepts entries from peer C so we can join logs A and B
            const logB = await Log.fromEntry(
                store2,
                {
                    ...signKey3.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey3.keypair.sign(data),
                },
                last(items3),
                { length: -1 }
            );
            expect(logA.length).toEqual(items2.length + items1.length);
            expect(logB.length).toEqual(
                items3.length + items2.length + items1.length
            );

            await logA.join(logB);

            expect(logA.length).toEqual(
                items3.length + items2.length + items1.length
            );
            // The last Entry<T>, 'entryC100', should be the only head
            // (it points to entryB100, entryB100 and entryC99)
            expect(logA.heads.length).toEqual(1);
        });

        it("joins only unique items", async () => {
            await log1.append("helloA1");
            await log2.append("helloB1");
            await log1.append("helloA2");
            await log2.append("helloB2");
            await log1.join(log2);
            await log1.join(log2);

            const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

            expect(log1.length).toEqual(4);
            expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                expectedData
            );

            const item = last(log1.values);
            expect(item.next.length).toEqual(1);
        });

        it("joins logs two ways", async () => {
            const a1 = await log1.append("helloA1");
            const b1 = await log2.append("helloB1");
            const a2 = await log1.append("helloA2");
            const b2 = await log2.append("helloB2");
            await log1.join(log2);
            await log2.join(log1);

            const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

            expect(log1.heads).toContainValues([a2, b2]);
            expect(log2.heads).toContainValues([a2, b2]);
            expect(a2.next).toContainValues([a1.hash]);
            expect(b2.next).toContainValues([b1.hash]);

            expect(log1.values.map((e) => e.hash)).toEqual(
                log2.values.map((e) => e.hash)
            );
            expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                expectedData
            );
            expect(log2.values.map((e) => e.payload.getValue())).toEqual(
                expectedData
            );
        });

        it("joins logs twice", async () => {
            await log1.append("helloA1");
            await log2.append("helloB1");
            await log2.join(log1);

            await log1.append("helloA2");
            await log2.append("helloB2");
            await log2.join(log1);

            const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

            expect(log2.length).toEqual(4);
            assert.deepStrictEqual(
                log2.values.map((e) => e.payload.getValue()),
                expectedData
            );
        });

        it("joins 2 logs two ways", async () => {
            await log1.append("helloA1");
            await log2.append("helloB1");
            await log2.join(log1);
            await log1.join(log2);
            await log1.append("helloA2");
            await log2.append("helloB2");
            await log2.join(log1);

            const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

            expect(log2.length).toEqual(4);
            assert.deepStrictEqual(
                log2.values.map((e) => e.payload.getValue()),
                expectedData
            );
        });

        it("joins 2 logs two ways and has the right heads at every step", async () => {
            await log1.append("helloA1");
            expect(log1.heads.length).toEqual(1);
            expect(log1.heads[0].payload.getValue()).toEqual("helloA1");

            await log2.append("helloB1");
            expect(log2.heads.length).toEqual(1);
            expect(log2.heads[0].payload.getValue()).toEqual("helloB1");

            await log2.join(log1);
            expect(log2.heads.length).toEqual(2);
            expect(log2.heads[0].payload.getValue()).toEqual("helloB1");
            expect(log2.heads[1].payload.getValue()).toEqual("helloA1");

            await log1.join(log2);
            expect(log1.heads.length).toEqual(2);
            expect(log1.heads[0].payload.getValue()).toEqual("helloB1");
            expect(log1.heads[1].payload.getValue()).toEqual("helloA1");

            await log1.append("helloA2");
            expect(log1.heads.length).toEqual(1);
            expect(log1.heads[0].payload.getValue()).toEqual("helloA2");

            await log2.append("helloB2");
            expect(log2.heads.length).toEqual(1);
            expect(log2.heads[0].payload.getValue()).toEqual("helloB2");

            await log2.join(log1);
            expect(log2.heads.length).toEqual(2);
            expect(log2.heads[0].payload.getValue()).toEqual("helloB2");
            expect(log2.heads[1].payload.getValue()).toEqual("helloA2");
        });

        it("joins 4 logs to one", async () => {
            // order determined by identity's publicKey
            await log1.append("helloA1");
            await log2.append("helloB1");
            await log3.append("helloC1");
            await log4.append("helloD1");
            await log1.append("helloA2");
            await log2.append("helloB2");
            await log3.append("helloC2");
            await log4.append("helloD2");
            await log1.join(log2);
            await log1.join(log3);
            await log1.join(log4);

            const expectedData = [
                "helloA1",
                "helloB1",
                "helloC1",
                "helloD1",
                "helloA2",
                "helloB2",
                "helloC2",
                "helloD2",
            ];

            expect(log1.length).toEqual(8);
            expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                expectedData
            );
        });

        it("joins 4 logs to one is commutative", async () => {
            await log1.append("helloA1");
            await log1.append("helloA2");
            await log2.append("helloB1");
            await log2.append("helloB2");
            await log3.append("helloC1");
            await log3.append("helloC2");
            await log4.append("helloD1");
            await log4.append("helloD2");
            await log1.join(log2);
            await log1.join(log3);
            await log1.join(log4);
            await log2.join(log1);
            await log2.join(log3);
            await log2.join(log4);

            expect(log1.length).toEqual(8);
            assert.deepStrictEqual(
                log1.values.map((e) => e.payload.getValue()),
                log2.values.map((e) => e.payload.getValue())
            );
        });

        it("joins logs and updates clocks", async () => {
            const a1 = await log1.append("helloA1");
            const b1 = await log2.append("helloB1");
            await log2.join(log1);
            const a2 = await log1.append("helloA2");
            const b2 = await log2.append("helloB2");

            expect(a2.metadata.clock.id).toEqual(
                signKey.keypair.publicKey.bytes
            );
            expect(b2.metadata.clock.id).toEqual(
                signKey2.keypair.publicKey.bytes
            );
            expect(
                a2.metadata.clock.timestamp.compare(a1.metadata.clock.timestamp)
            ).toBeGreaterThan(0);
            expect(
                b2.metadata.clock.timestamp.compare(b1.metadata.clock.timestamp)
            ).toBeGreaterThan(0);

            await log3.join(log1);

            await log3.append("helloC1");
            const c2 = await log3.append("helloC2");
            await log1.join(log3);
            await log1.join(log2);
            await log4.append("helloD1");
            const d2 = await log4.append("helloD2");
            await log4.join(log2);
            await log4.join(log1);
            await log4.join(log3);
            const d3 = await log4.append("helloD3");
            expect(d3.gid).toEqual(c2.gid); // because c2 is the longest
            await log4.append("helloD4");
            await log1.join(log4);
            await log4.join(log1);
            const d5 = await log4.append("helloD5");
            expect(d5.gid).toEqual(c2.gid); // because c2 previously

            const a5 = await log1.append("helloA5");
            expect(a5.gid).toEqual(c2.gid); // because log1 joined with lgo4 and log4 was c2 (and len log4 > log1)

            await log4.join(log1);
            const d6 = await log4.append("helloD6");
            expect(d5.gid).toEqual(a5.gid);
            expect(d6.gid).toEqual(a5.gid);

            const expectedData = [
                {
                    payload: "helloA1",
                    gid: a1.gid,
                },
                {
                    payload: "helloB1",
                    gid: b1.gid,
                },

                {
                    payload: "helloA2",
                    gid: a2.gid,
                },
                {
                    payload: "helloB2",
                    gid: b2.gid,
                },
                {
                    payload: "helloC1",
                    gid: a1.gid,
                },
                {
                    payload: "helloC2",
                    gid: c2.gid,
                },
                {
                    payload: "helloD1",
                    gid: d2.gid,
                },
                {
                    payload: "helloD2",
                    gid: d2.gid,
                },
                {
                    payload: "helloD3",
                    gid: d3.gid,
                },
                {
                    payload: "helloD4",
                    gid: d3.gid,
                },
                {
                    payload: "helloD5",
                    gid: d5.gid,
                },
                {
                    payload: "helloA5",
                    gid: a5.gid,
                },
                {
                    payload: "helloD6",
                    gid: d6.gid,
                },
            ];

            const transformed = log4.values.map((e) => {
                return {
                    payload: e.payload.getValue(),
                    gid: e.gid,
                };
            });

            expect(log4.length).toEqual(13);
            expect(transformed).toEqual(expectedData);
        });

        it("joins logs from 4 logs", async () => {
            const a1 = await log1.append("helloA1");
            await log1.join(log2);
            const b1 = await log2.append("helloB1");
            await log2.join(log1);
            const a2 = await log1.append("helloA2");
            const b2 = await log2.append("helloB2");

            await log1.join(log3);
            // Sometimes failes because of clock ids are random TODO Fix
            expect(log1.heads[log1.heads.length - 1].gid).toEqual(a1.gid);
            expect(a2.metadata.clock.id).toEqual(
                signKey.keypair.publicKey.bytes
            );
            expect(
                a2.metadata.clock.timestamp.compare(a1.metadata.clock.timestamp)
            ).toBeGreaterThan(0);

            await log3.join(log1);
            expect(log3.heads[log3.heads.length - 1].gid).toEqual(a1.gid); // because longest

            await log3.append("helloC1");
            await log3.append("helloC2");
            await log1.join(log3);
            await log1.join(log2);
            await log4.append("helloD1");
            await log4.append("helloD2");
            await log4.join(log2);
            await log4.join(log1);
            await log4.join(log3);
            await log4.append("helloD3");
            const d4 = await log4.append("helloD4");

            expect(d4.metadata.clock.id).toEqual(
                signKey4.keypair.publicKey.bytes
            );

            const expectedData = [
                "helloA1",
                "helloB1",
                "helloA2",
                "helloB2",
                "helloC1",
                "helloC2",
                "helloD1",
                "helloD2",
                "helloD3",
                "helloD4",
            ];

            expect(log4.length).toEqual(10);
            assert.deepStrictEqual(
                log4.values.map((e) => e.payload.getValue()),
                expectedData
            );
        });

        describe("gid shadow callback", () => {
            it("it emits callback when gid is shadowed, triangle shape", async () => {
                /*  
                Either A or B shaded
                ┌─┐┌─┐  
                │a││b│  
                └┬┘└┬┘  
                ┌▽──▽──┐
                │a or b│
                └──────┘
                */

                const a1 = await log1.append("helloA1", { nexts: [] });
                const b1 = await log1.append("helloB1", { nexts: [] });
                let callbackValue: string[] = undefined as any;
                const ab1 = await log1.append("helloAB1", {
                    nexts: [a1, b1],
                    onGidsShadowed: (gids) => (callbackValue = gids),
                });
                expect(callbackValue).toHaveLength(1);
                expect(callbackValue[0]).toEqual(
                    ab1.gid === a1.gid ? b1.gid : a1.gid
                ); // if ab1 has gid a then b will be shadowed
            });

            it("it emits callback when gid is shadowed, N shape", async () => {
                /*  
                    No shadows
                    ┌──┐┌───┐ 
                    │a0││b1 │ 
                    └┬─┘└┬─┬┘ 
                    ┌▽─┐ │┌▽─┐
                    │a1│ ││b2│
                    └┬─┘ │└──┘
                    ┌▽───▽┐   
                    │a2   │   
                    └─────┘   
                */

                const a0 = await log1.append("helloA0", { nexts: [] });
                const a1 = await log1.append("helloA1", { nexts: [a0] });
                const b1 = await log1.append("helloB1", { nexts: [] });
                const b2 = await log1.append("helloB2", { nexts: [b1] });

                let callbackValue: any;
                // make sure gid is choosen from 1 bs

                const a2 = await log1.append("helloA2", {
                    nexts: [a1, b1],
                    onGidsShadowed: (gids) => (callbackValue = gids),
                });
                expect(callbackValue).toBeUndefined();
            });
        });
        describe("takes length as an argument", () => {
            beforeEach(async () => {
                await log1.append("helloA1");
                await log2.append("helloB1");
                await log1.append("helloA2");
                await log2.append("helloB2");
            });

            it("joins only specified amount of entries - one entry", async () => {
                await log1.join(log2, { size: 1 });

                const expectedData = ["helloB2"];
                const lastEntry = last(log1.values);

                expect(log1.length).toEqual(1);
                assert.deepStrictEqual(
                    log1.values.map((e) => e.payload.getValue()),
                    expectedData
                );
                expect(lastEntry.next.length).toEqual(1);
            });

            it("joins only specified amount of entries - two entries", async () => {
                await log1.join(log2, { size: 2 });

                const expectedData = ["helloA2", "helloB2"];
                const lastEntry = last(log1.values);

                expect(log1.length).toEqual(2);
                expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                    expectedData
                );
                expect(lastEntry.next.length).toEqual(1);
            });

            it("joins only specified amount of entries - three entries", async () => {
                await log1.join(log2, { size: 3 });

                const expectedData = ["helloB1", "helloA2", "helloB2"];
                const lastEntry = last(log1.values);

                expect(log1.length).toEqual(3);
                expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                    expectedData
                );
                expect(lastEntry.next.length).toEqual(1);
            });

            it("joins only specified amount of entries - (all) four entries", async () => {
                await log1.join(log2, { size: 4 });

                const expectedData = [
                    "helloA1",
                    "helloB1",
                    "helloA2",
                    "helloB2",
                ];
                const lastEntry = last(log1.values);

                expect(log1.length).toEqual(4);
                expect(log1.values.map((e) => e.payload.getValue())).toEqual(
                    expectedData
                );
                expect(lastEntry.next.length).toEqual(1);
            });
        });
    });
});
