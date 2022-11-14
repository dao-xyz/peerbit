import assert from "assert";
import rmrf from "rimraf";
import { CID } from "multiformats/cid";
import { base58btc } from "multiformats/bases/base58";
import { Entry, Payload } from "../entry.js";
import { LamportClock as Clock } from "../lamport-clock.js";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import fs from "fs-extra";
import io from "@dao-xyz/peerbit-io-utils";

// For tiebreaker testing
import { LastWriteWins } from "../log-sorting.js";
import { DecryptedThing, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { serialize } from "@dao-xyz/borsh";
import { jest } from "@jest/globals";

const FirstWriteWins = (a: any, b: any) => LastWriteWins(a, b) * -1;

// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs,
} from "@dao-xyz/peerbit-test-utils";

import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { arraysCompare } from "@dao-xyz/peerbit-borsh-utils";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let ipfsd: Controller,
    ipfs: IPFS,
    signKey: KeyWithMeta<Ed25519Keypair>,
    signKey2: KeyWithMeta<Ed25519Keypair>,
    signKey3: KeyWithMeta<Ed25519Keypair>;

Object.keys(testAPIs).forEach((IPFS) => {
    describe("Log", function () {
        jest.setTimeout(config.timeout);

        const { signingKeyFixtures, signingKeysPath } = config;

        let keystore: Keystore;

        beforeAll(async () => {
            await fs.copy(
                signingKeyFixtures(__dirname),
                signingKeysPath(__filenameBase)
            );

            keystore = new Keystore(
                await createStore(signingKeysPath(__filenameBase))
            );
            const signKeys: KeyWithMeta<Ed25519Keypair>[] = [];
            for (let i = 0; i < 3; i++) {
                signKeys.push(
                    (await keystore.getKey(
                        new Uint8Array([i])
                    )) as KeyWithMeta<Ed25519Keypair>
                );
            }
            signKeys.sort((a, b) =>
                arraysCompare(
                    a.keypair.publicKey.publicKey,
                    b.keypair.publicKey.publicKey
                )
            );
            // @ts-ignore
            signKey = signKeys[0];
            // @ts-ignore
            signKey2 = signKeys[1];
            // @ts-ignore
            signKey3 = signKeys[2];
            ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig);
            ipfs = ipfsd.api;
        });

        afterAll(async () => {
            await stopIpfs(ipfsd);
            rmrf.sync(signingKeysPath(__filenameBase));

            await keystore?.close();
        });

        describe("constructor", () => {
            it("creates an empty log with default params", () => {
                const log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    undefined
                );
                assert.notStrictEqual(log._entryIndex, null);
                assert.notStrictEqual(log._headsIndex, null);
                assert.notStrictEqual(log._id, null);
                assert.notStrictEqual(log._id, null);
                assert.notStrictEqual(log.values, null);
                assert.notStrictEqual(log.heads, null);
                assert.notStrictEqual(log.tails, null);
                // assert.notStrictEqual(log.tailCids, null)
                assert.deepStrictEqual(log.values, []);
                assert.deepStrictEqual(log.heads, []);
                assert.deepStrictEqual(log.tails, []);
            });

            it("sets an id", async () => {
                const log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "ABC" }
                );
                expect(log._id).toEqual("ABC");
            });

            it("generates id string if id is not passed as an argument", () => {
                const log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    undefined
                );
                assert.strictEqual(typeof log._id === "string", true);
            });

            it("sets items if given as params", async () => {
                const one = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryA",
                    next: [],
                    clock: new Clock(new Uint8Array([0]), 0),
                });
                const two = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryB",
                    next: [],
                    clock: new Clock(new Uint8Array([1]), 0),
                });
                const three = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryC",
                    next: [],
                    clock: new Clock(new Uint8Array([2]), 0),
                });
                const log = new Log<string>(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "A", entries: [one, two, three] }
                );
                expect(log.length).toEqual(3);
                expect(log.values[0].payload.getValue()).toEqual("entryA");
                expect(log.values[1].payload.getValue()).toEqual("entryB");
                expect(log.values[2].payload.getValue()).toEqual("entryC");
            });

            it("sets heads if given as params", async () => {
                const one = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryA",
                    next: [],
                });
                const two = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryB",
                    next: [],
                });
                const three = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryC",
                    next: [],
                });
                const log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "B", entries: [one, two, three], heads: [three] }
                );
                expect(log.heads.length).toEqual(1);
                expect(log.heads[0].hash).toEqual(three.hash);
            });

            it("finds heads if heads not given as params", async () => {
                const one = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryA",
                    next: [],
                });
                const two = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryB",
                    next: [],
                });
                const three = await Entry.create({
                    ipfs,
                    identity: {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    },
                    gidSeed: "A",
                    data: "entryC",
                    next: [],
                });
                const log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "A", entries: [one, two, three] }
                );
                expect(log.heads.length).toEqual(3);
                expect(log.heads[2].hash).toEqual(one.hash);
                expect(log.heads[1].hash).toEqual(two.hash);
                expect(log.heads[0].hash).toEqual(three.hash);
            });

            it("throws an error if entries is not an array", () => {
                let err;
                try {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A", entries: {} as any }
                    ); // eslint-disable-line no-unused-vars
                } catch (e: any) {
                    err = e;
                }
                assert.notStrictEqual(err, undefined);
                expect(err.message).toEqual(
                    "'entries' argument must be an array of Entry instances"
                );
            });

            it("throws an error if heads is not an array", () => {
                let err;
                try {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A", entries: [], heads: {} }
                    ); // eslint-disable-line no-unused-vars
                } catch (e: any) {
                    err = e;
                }
                assert.notStrictEqual(err, undefined);
                expect(err.message).toEqual(
                    "'heads' argument must be an array"
                );
            });
        });

        describe("toString", () => {
            let log: Log<string>;
            const expectedData =
                '"five"\n└─"four"\n  └─"three"\n    └─"two"\n      └─"one"';

            beforeEach(async () => {
                log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "A" }
                );
                await log.append("one", { gidSeed: "a" });
                await log.append("two", { gidSeed: "a" });
                await log.append("three", { gidSeed: "a" });
                await log.append("four", { gidSeed: "a" });
                await log.append("five", { gidSeed: "a" });
            });

            it("returns a nicely formatted string", () => {
                expect(
                    log.toString((p) => Buffer.from(p.data).toString())
                ).toEqual(expectedData);
            });
        });

        describe("get", () => {
            let log: Log<any>;

            beforeEach(async () => {
                log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "AAA" }
                );
                await log.append("one", { gidSeed: "a" });
            });

            it("returns an Entry", () => {
                const entry = log.get(log.values[0].hash)!;
                expect(entry.hash).toMatchSnapshot();
            });

            it("returns undefined when Entry is not in the log", () => {
                const entry = log.get("QmFoo");
                assert.deepStrictEqual(entry, undefined);
            });
        });

        describe("setIdentity", () => {
            let log: Log<string>;

            beforeEach(async () => {
                log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "AAA" }
                );
                await log.append("one", { gidSeed: "a" });
            });

            it("changes identity", async () => {
                assert.deepStrictEqual(
                    log.values[0].clock.id,
                    signKey.keypair.publicKey.bytes
                );
                expect(log.values[0].clock.time).toEqual(0n);
                log.setIdentity({
                    ...signKey2.keypair,
                    sign: signKey2.keypair.sign,
                });
                await log.append("two", { gidSeed: "a" });
                assert.deepStrictEqual(
                    log.values[1].clock.id,
                    signKey2.keypair.publicKey.bytes
                );
                expect(log.values[1].clock.time).toEqual(1n);
                log.setIdentity({
                    ...signKey3.keypair,
                    sign: signKey3.keypair.sign,
                });
                await log.append("three", { gidSeed: "a" });
                assert.deepStrictEqual(
                    log.values[2].clock.id,
                    signKey3.keypair.publicKey.bytes
                );
                expect(log.values[2].clock.time).toEqual(2n);
            });
        });

        describe("has", () => {
            let log: Log<string>;

            beforeAll(async () => {
                const clock = new Clock(signKey.keypair.publicKey.bytes, 1);
            });

            beforeEach(async () => {
                log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId: "AAA" }
                );
                await log.append("one", { gidSeed: "a" });
            });

            it("returns true if it has an Entry", () => {
                assert(log.has(log.values[0].hash));
            });

            it("returns true if it has an Entry, hash lookup", () => {
                assert(log.has(log.values[0].hash));
            });

            it("returns false if it doesn't have the Entry", () => {
                assert.strictEqual(log.has("zdFoo"), false);
            });
        });

        describe("serialize", () => {
            let log: Log<string>,
                logId: string = "AAA";

            beforeEach(async () => {
                log = new Log(
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    { logId }
                );
                await log.append("one", { gidSeed: "a" });
                await log.append("two", { gidSeed: "a" });
                await log.append("three", { gidSeed: "a" });
            });

            describe("toJSON", () => {
                it("returns the log in JSON format", () => {
                    expect(JSON.stringify(log.toJSON())).toEqual(
                        JSON.stringify({
                            id: logId,
                            heads: [log.values[2].hash],
                        })
                    );
                });
            });

            describe("toSnapshot", () => {
                it("returns the log snapshot", () => {
                    const expectedData = {
                        id: logId,
                        heads: [log.values[2].hash],
                        values: log.values.map((x) => x.hash),
                    };
                    const snapshot = log.toSnapshot();
                    expect(snapshot.id).toEqual(expectedData.id);
                    expect(snapshot.heads.length).toEqual(
                        expectedData.heads.length
                    );
                    expect(snapshot.heads[0].hash).toEqual(
                        expectedData.heads[0]
                    );
                    expect(snapshot.values.length).toEqual(
                        expectedData.values.length
                    );
                    expect(snapshot.values[0].hash).toEqual(
                        expectedData.values[0]
                    );
                    expect(snapshot.values[1].hash).toEqual(
                        expectedData.values[1]
                    );
                    expect(snapshot.values[2].hash).toEqual(
                        expectedData.values[2]
                    );
                });
            });

            describe("toBuffer", () => {
                it("returns the log as a Buffer", () => {
                    assert.deepStrictEqual(
                        log.toBuffer(),
                        Buffer.from(
                            JSON.stringify({
                                id: logId,
                                heads: [log.values[2].hash],
                            })
                        )
                    );
                });
            });

            describe("toMultihash - cbor", () => {
                it("returns the log as ipfs CID", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const hash = await log.toMultihash();
                    expect(hash).toMatchSnapshot();
                });

                it("log serialized to ipfs contains the correct data", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const hash = await log.toMultihash();
                    expect(hash).toMatchSnapshot();
                    const result = (await io.read(ipfs, hash)) as Log<any>;
                    const heads = result.heads.map((head) => head.toString()); // base58btc
                    expect(heads).toMatchSnapshot();
                });

                it("throws an error if log items is empty", async () => {
                    const emptyLog = new Log(ipfs, {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    });
                    let err;
                    try {
                        await emptyLog.toMultihash();
                    } catch (e: any) {
                        err = e;
                    }
                    assert.notStrictEqual(err, null);
                    expect(err.message).toEqual("Can't serialize an empty log");
                });
            });

            describe("toMultihash - pb", () => {
                it("returns the log as ipfs multihash", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const multihash = await log.toMultihash({
                        format: "dag-pb",
                    });
                    expect(multihash).toMatchSnapshot();
                });

                it("log serialized to ipfs contains the correct data", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const multihash = await log.toMultihash({
                        format: "dag-pb",
                    });
                    expect(multihash).toMatchSnapshot();
                    const result = await ipfs.object.get(CID.parse(multihash));
                    const res = JSON.parse(
                        Buffer.from(result.Data as Uint8Array).toString()
                    );
                    expect(res.heads).toMatchSnapshot();
                });

                it("throws an error if log items is empty", async () => {
                    const emptyLog = new Log(ipfs, {
                        ...signKey.keypair,
                        sign: (data) => signKey.keypair.sign(data),
                    });
                    let err;
                    try {
                        await emptyLog.toMultihash();
                    } catch (e: any) {
                        err = e;
                    }
                    assert.notStrictEqual(err, null);
                    expect(err.message).toEqual("Can't serialize an empty log");
                });
            });

            describe("fromMultihash", () => {
                it("creates a log from ipfs CID - one entry", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "X" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const hash = await log.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: -1 }
                    );
                    expect(JSON.stringify(res.toJSON())).toMatchSnapshot();
                    expect(res.length).toEqual(1);
                    expect(res.values[0].payload.getValue()).toEqual("one");
                    expect(res.values[0].clock.id).toEqual(
                        signKey.keypair.publicKey.bytes
                    );
                    expect(res.values[0].clock.time).toEqual(0n);
                });

                it("creates a log from ipfs CID - three entries", async () => {
                    const hash = await log.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: -1 }
                    );
                    expect(res.length).toEqual(3);
                    expect(res.values[0].payload.getValue()).toEqual("one");
                    expect(res.values[0].clock.time).toEqual(0n);
                    expect(res.values[1].payload.getValue()).toEqual("two");
                    expect(res.values[1].clock.time).toEqual(1n);
                    expect(res.values[2].payload.getValue()).toEqual("three");
                    expect(res.values[2].clock.time).toEqual(2n);
                });

                it("creates a log from ipfs multihash (backwards compat)", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "X" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const multihash = await log.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        multihash,
                        { length: -1 }
                    );
                    expect(JSON.stringify(res.toJSON())).toMatchSnapshot();
                    expect(res.length).toEqual(1);
                    expect(res.values[0].payload.getValue()).toEqual("one");
                    expect(res.values[0].clock.id).toEqual(
                        signKey.keypair.publicKey.bytes
                    );
                    expect(res.values[0].clock.time).toEqual(0n);
                });

                it("has the right sequence number after creation and appending", async () => {
                    const hash = await log.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: -1 }
                    );
                    expect(res.length).toEqual(3);
                    await res.append("four");
                    expect(res.length).toEqual(4);
                    expect(res.values[3].payload.getValue()).toEqual("four");
                    expect(res.values[3].clock.time).toEqual(3n);
                });

                it("creates a log from ipfs CID that has three heads", async () => {
                    const log1 = new Log<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    const log2 = new Log<string>(
                        ipfs,
                        {
                            ...signKey2.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey2.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    const log3 = new Log<string>(
                        ipfs,
                        {
                            ...signKey3.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey3.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log1.append("one"); // order is determined by the identity's publicKey
                    await log2.append("two");
                    await log3.append("three");
                    await log1.join(log2);
                    await log1.join(log3);
                    const hash = await log1.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: -1 }
                    );
                    expect(res.length).toEqual(3);
                    expect(res.heads.length).toEqual(3);
                    expect(
                        res.heads.map((x) => x.payload.getValue())
                    ).toContainAllValues(["one", "two", "three"]);
                });

                it("creates a log from ipfs CID that has three heads w/ custom tiebreaker", async () => {
                    const log1 = new Log<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    const log2 = new Log<string>(
                        ipfs,
                        {
                            ...signKey2.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey2.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    const log3 = new Log<string>(
                        ipfs,
                        {
                            ...signKey3.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey3.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    await log1.append("one"); // order is determined by the identity's publicKey
                    await log2.append("two");
                    await log3.append("three");
                    await log1.join(log2);
                    await log1.join(log3);
                    const hash = await log1.toMultihash();
                    const res = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { sortFn: FirstWriteWins }
                    );
                    expect(res.length).toEqual(3);
                    expect(res.heads.length).toEqual(3);
                    expect(res.heads[0].payload.getValue()).toEqual("one"); // order is determined by the identity's publicKey
                    expect(res.heads[1].payload.getValue()).toEqual("two");
                    expect(res.heads[2].payload.getValue()).toEqual("three");
                });

                it("creates a log from ipfs CID up to a size limit", async () => {
                    const amount = 100;
                    const size = amount / 2;
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    for (let i = 0; i < amount; i++) {
                        await log.append(i.toString());
                    }
                    const hash = await log.toMultihash();
                    const res = await Log.fromMultihash(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: size }
                    );
                    expect(res.length).toEqual(size);
                });

                it("creates a log from ipfs CID up without size limit", async () => {
                    const amount = 100;
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    for (let i = 0; i < amount; i++) {
                        await log.append(i.toString());
                    }
                    const hash = await log.toMultihash();
                    const res = await Log.fromMultihash(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        { length: -1 }
                    );
                    expect(res.length).toEqual(amount);
                });

                it("throws an error if data from hash is not valid JSON", async () => {
                    const value = "hello";
                    const cid = CID.parse(
                        await io.write(ipfs, "dag-pb", value)
                    );
                    let err;
                    try {
                        const hash = cid.toString(base58btc);
                        await Log.fromMultihash(
                            ipfs,
                            {
                                ...signKey.keypair,
                                sign: async (data: Uint8Array) =>
                                    await signKey.keypair.sign(data),
                            },
                            hash,
                            undefined as any
                        );
                    } catch (e: any) {
                        err = e;
                    }
                    expect(err.message).toEqual(
                        "Unexpected token h in JSON at position 0"
                    );
                });

                it("throws an error when data from CID is not instance of Log", async () => {
                    const hash = await ipfs.dag.put({});
                    let err;
                    try {
                        await Log.fromMultihash(
                            ipfs,
                            {
                                ...signKey.keypair,
                                sign: async (data: Uint8Array) =>
                                    await signKey.keypair.sign(data),
                            },
                            hash.toString(),
                            undefined as any
                        );
                    } catch (e: any) {
                        err = e;
                    }
                    expect(err.message).toEqual(
                        "Given argument is not an instance of Log"
                    );
                });

                it("onProgress callback is fired for each entry", async () => {
                    const amount = 100;
                    const log = new Log<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "A" }
                    );
                    for (let i = 0; i < amount; i++) {
                        await log.append(i.toString());
                    }

                    const items = log.values;
                    let i = 0;
                    const loadProgressCallback = (entry: Entry<string>) => {
                        assert.notStrictEqual(entry, null);
                        expect(entry.hash).toEqual(
                            items[items.length - i - 1].hash
                        );
                        expect(entry.payload.getValue()).toEqual(
                            items[items.length - i - 1].payload.getValue()
                        );
                        i++;
                    };

                    const hash = await log.toMultihash();
                    const result = await Log.fromMultihash<string>(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        hash,
                        {
                            length: -1,
                            exclude: [],
                            onProgressCallback: loadProgressCallback,
                        }
                    );

                    // Make sure the onProgress callback was called for each entry
                    expect(i).toEqual(amount);
                    // Make sure the log entries are correct ones
                    expect(result.values[0].clock.time).toEqual(0n);
                    expect(result.values[0].payload.getValue()).toEqual("0");
                    expect(result.values[result.length - 1].clock.time).toEqual(
                        99n
                    );
                    expect(
                        result.values[result.length - 1].payload.getValue()
                    ).toEqual("99");
                });
            });

            describe("fromEntryHash", () => {
                /*      afterEach(() => {
               if (Log.fromEntryHash["restore"]) {
                 Log.fromEntryHash["restore"]()
               }
             })
      */
                it("calls fromEntryHash", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "X" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const res = await Log.fromEntryHash(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        log.values[0].hash,
                        { logId: log._id, length: -1 }
                    );
                    expect(JSON.stringify(res.toJSON())).toMatchSnapshot();
                });
            });

            describe("fromMultihash", () => {
                /*   afterEach(() => {
            if (Log.fromMultihash["restore"]) {
              Log.fromMultihash["restore"]()
            }
          }) */

                it("calls fromMultihash", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "X" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const multihash = await log.toMultihash();
                    const res = await Log.fromMultihash(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        multihash,
                        { length: -1 }
                    );
                    expect(JSON.stringify(res.toJSON())).toMatchSnapshot();
                });

                it("calls fromMultihash with custom tiebreaker", async () => {
                    const log = new Log(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        { logId: "X" }
                    );
                    await log.append("one", { gidSeed: "a" });
                    const multihash = await log.toMultihash();
                    const res = await Log.fromMultihash(
                        ipfs,
                        {
                            ...signKey.keypair,
                            sign: async (data: Uint8Array) =>
                                await signKey.keypair.sign(data),
                        },
                        multihash,
                        { length: -1, sortFn: FirstWriteWins }
                    );
                    expect(JSON.stringify(res.toJSON())).toMatchSnapshot();
                });
            });
        });

        describe("values", () => {
            it("returns all entries in the log", async () => {
                const log = new Log<string>(ipfs, {
                    ...signKey.keypair,
                    sign: (data) => signKey.keypair.sign(data),
                });
                expect(log.values instanceof Array).toEqual(true);
                expect(log.length).toEqual(0);
                await log.append("hello1");
                await log.append("hello2");
                await log.append("hello3");
                expect(log.values instanceof Array).toEqual(true);
                expect(log.length).toEqual(3);
                expect(log.values[0].payload.getValue()).toEqual("hello1");
                expect(log.values[1].payload.getValue()).toEqual("hello2");
                expect(log.values[2].payload.getValue()).toEqual("hello3");
            });
        });
    });
});
