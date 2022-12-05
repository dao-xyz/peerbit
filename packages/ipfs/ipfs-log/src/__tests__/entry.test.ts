import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Entry, Payload } from "../entry.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { deserialize, serialize } from "@dao-xyz/borsh";
import {
    Ed25519Keypair,
    Ed25519PublicKey,
    X25519Keypair,
    X25519PublicKey,
    toBase64,
} from "@dao-xyz/peerbit-crypto";
import sodium from "libsodium-wrappers";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { Identity } from "../identity.js";
import { LamportClock, Timestamp } from "../clock.js";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

const identityFromSignKey = (key: KeyWithMeta<Ed25519Keypair>): Identity => {
    if (!key) {
        throw new Error("Key not defined");
    }
    return {
        ...key.keypair,
        sign: async (data: Uint8Array) => await key.keypair.sign(data),
    };
};
const API = "js-ipfs";

describe("Entry", function () {
    let store: Blocks;

    let keystore: Keystore, signKey: KeyWithMeta<Ed25519Keypair>;

    beforeAll(async () => {
        await sodium.ready;

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );
        await keystore.waitForOpen();

        signKey = (await keystore.getKey(
            new Uint8Array([0])
        )) as KeyWithMeta<Ed25519Keypair>;

        store = new Blocks(new MemoryLevelBlockStore());
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        rmrf.sync(testKeyStorePath(__filenameBase));
        await keystore?.close();
    });
    describe("endocing", () => {
        it("can serialize and deserialialize", async () => {
            const entry = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello",
            });
            deserialize(serialize(entry), Entry);
        });
    });

    describe("create", () => {
        it("creates a an empty entry", async () => {
            const clock = new LamportClock({
                id: new Uint8Array([1, 2, 3]),
                timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
            });

            const entry = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello",
                clock,
            });
            expect(entry.hash).toMatchSnapshot();
            expect(entry.gid).toEqual(
                await toBase64(await sodium.crypto_generichash(32, "A"))
            );
            expect(entry.metadata.clock.equals(clock)).toBeTrue();

            expect(entry.payload.getValue()).toEqual("hello");
            expect(entry.next.length).toEqual(0);
        });

        it("creates a entry with payload", async () => {
            const payload = "hello world";
            const clock = new LamportClock({
                id: new Uint8Array([1, 2, 3]),
                timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
            });
            const entry = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload,
                next: [],
                clock,
            });
            expect(entry.hash).toMatchSnapshot();
            expect(entry.payload.getValue()).toEqual(payload);
            expect(entry.gid).toEqual(
                await toBase64(await sodium.crypto_generichash(32, "A"))
            );
            expect(entry.metadata.clock.equals(clock)).toBeTrue();
            expect(entry.next.length).toEqual(0);
        });

        it("creates a encrypted entry with payload", async () => {
            const payload = "hello world";
            const senderKey = await keystore.createX25519Key({
                id: "sender",
                overwrite: true,
            });
            const receiverKey = await keystore.createX25519Key({
                id: "reciever",
                overwrite: true,
            });
            const entry = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload,
                next: [],
                encryption: {
                    reciever: {
                        metadata: undefined,
                        signatures: undefined,
                        payload: receiverKey.keypair.publicKey,
                        next: undefined,
                    },
                    options: {
                        getEncryptionKeypair: () => senderKey.keypair,
                        getAnyKeypair: async (
                            publicKeys: X25519PublicKey[]
                        ) => {
                            for (let i = 0; i < publicKeys.length; i++) {
                                if (
                                    publicKeys[i].equals(
                                        (senderKey.keypair as X25519Keypair)
                                            .publicKey
                                    )
                                ) {
                                    return {
                                        index: i,
                                        keypair: senderKey.keypair as
                                            | Ed25519Keypair
                                            | X25519Keypair,
                                    };
                                }
                                if (
                                    publicKeys[i].equals(
                                        (receiverKey.keypair as X25519Keypair)
                                            .publicKey
                                    )
                                ) {
                                    return {
                                        index: i,
                                        keypair: receiverKey.keypair as
                                            | Ed25519Keypair
                                            | X25519Keypair,
                                    };
                                }
                            }
                        },
                    },
                },
            });
            assert(entry.payload instanceof Payload);
            expect(entry.payload.getValue()).toEqual(payload);

            // We can not have a hash check because nonce of encryption will always change
            expect(entry.gid).toEqual(
                await toBase64(await sodium.crypto_generichash(32, "A"))
            );
            assert.deepStrictEqual(
                entry.metadata.clock.id,
                new Ed25519PublicKey({
                    publicKey: signKey.keypair.publicKey.publicKey,
                }).bytes
            );
            expect(entry.metadata.clock.timestamp.logical).toEqual(0);
            expect(entry.next.length).toEqual(0);
        });

        it("creates a entry with payload and next", async () => {
            const payload1 = "hello world";
            const payload2 = "hello again";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                next: [],
                clock: new LamportClock({
                    id: new Uint8Array([0]),
                    timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
                }),
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [entry1],
                clock: new LamportClock({
                    id: new Uint8Array([0]),
                    timestamp: new Timestamp({ wallTime: 1n, logical: 0 }),
                }),
            });
            expect(entry2.payload.getValue()).toEqual(payload2);
            expect(entry2.next.length).toEqual(1);
            expect(entry2.maxChainLength).toEqual(2n); // because 1 next
            expect(entry2.hash).toMatchSnapshot();
        });

        it("`next` parameter can be an array of strings", async () => {
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello2",
                next: [entry1],
            });
            assert.strictEqual(typeof entry2.next[0] === "string", true);
        });

        it("`next` parameter can be an array of Entry instances", async () => {
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello2",
                next: [entry1],
            });
            assert.strictEqual(typeof entry2.next[0] === "string", true);
        });

        it("can calculate join gid from `next` max chain length", async () => {
            const entry0A = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });

            const entry1A = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [entry0A],
            });

            const entry1B = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "B",
                clock: entry1A.metadata.clock,
                data: "hello1",
                next: [],
            });

            expect(entry1A.gid > entry1B.gid); // so that gid is not choosen because A has smaller gid
            expect(entry1A.metadata.clock.timestamp.logical).toEqual(
                entry1B.metadata.clock.timestamp.logical
            );

            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "Should not be used",
                data: "hello2",
                next: [entry1A, entry1B],
            });
            expect(entry2.gid).toEqual(entry1A.gid); // because A has alonger chain
        });

        it("can calculate join gid from `next` max clock", async () => {
            const entry1A = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "B",
                data: "hello1",
                next: [],
            });

            const entry1B = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                clock: entry1A.metadata.clock.advance(),
                data: "hello1",
                next: [],
            });

            expect(entry1B.gid > entry1A.gid); // so that gid is not choosen because B has smaller gid
            expect(entry1A.maxChainLength).toEqual(entry1B.maxChainLength);
            expect(
                entry1B.metadata.clock.timestamp.compare(
                    entry1A.metadata.clock.timestamp
                )
            ).toBeGreaterThan(0);

            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "Should not be used",
                data: "hello2",
                next: [entry1A, entry1B],
            });
            expect(entry2.gid).toEqual(entry1B.gid); // because A has alonger chain
        });

        it("can calculate join gid from `next` gid comparison", async () => {
            const entry1A = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });

            const entry1B = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "B",
                clock: entry1A.metadata.clock,
                data: "hello1",
                next: [],
            });

            expect(entry1B.gid < entry1A.gid).toBeTrue(); // so that B is choosen because of gid
            expect(entry1A.maxChainLength).toEqual(entry1B.maxChainLength);
            expect(entry1A.metadata.clock.timestamp.logical).toEqual(
                entry1B.metadata.clock.timestamp.logical
            );

            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "Should not be used",
                data: "hello2",
                next: [entry1A, entry1B],
            });
            expect(entry2.gid).toEqual(entry1B.gid); // because gid B < gid A
        });

        it("can calculate reuse gid from `next`", async () => {
            const entry1A = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });

            const entry1B = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gid: entry1A.gid,
                data: "hello1",
                next: [],
            });

            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "Should not be used",
                data: "hello2",
                next: [entry1A, entry1B],
            });
            expect(entry2.gid).toEqual(entry1A.gid);
            expect(entry1A.gid).toEqual(entry1B.gid);
        });

        it("will use next for gid instaed of gidSeed", async () => {
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello1",
                next: [],
            });

            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "Should not be used",
                data: "hello2",
                next: [entry1],
            });
            expect(entry2.gid).toEqual(entry1.gid);
        });

        it("throws an error if data is not defined", async () => {
            let err: any;
            try {
                await Entry.create({
                    store,
                    identity: identityFromSignKey(signKey),
                    gidSeed: "A",
                    data: null,
                    next: [],
                });
            } catch (e: any) {
                err = e;
            }
            expect(err.message).toEqual("Entry requires data");
        });

        it("throws an error if next is not an array", async () => {
            let err: any;
            try {
                await Entry.create({
                    store,
                    identity: identityFromSignKey(signKey),
                    gidSeed: "A",
                    data: "hello",
                    next: {} as any,
                });
            } catch (e: any) {
                err = e;
            }
            expect(err.message).toEqual("'next' argument is not an array");
        });
    });

    describe("toMultihash", () => {
        it("returns an ipfs multihash", async () => {
            const entry = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: "hello",
                next: [],
                clock: new LamportClock({
                    id: new Uint8Array([1, 2, 3]),
                    timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
                }),
            });
            const hash = entry.hash;
            entry.hash = undefined as any;
            const multihash = await Entry.toMultihash(store, entry);
            expect(multihash).toEqual(hash);
            expect(multihash).toMatchSnapshot();
        });

        /*  TODO what is the point of this test?
    
    it('throws an error if the object being passed is invalid', async () => {
      let err
      try {
        const entry = await Entry.create({ store, identity: identityFromSignKey(signKey), gidSeed:   'A', data: 'hello', next: [] })
        delete ((entry.metadata as MetadataSecure)._metadata as DecryptedThing<Metadata>)
        await Entry.toMultihash(store, entry)
      } catch (e: any) {
        err = e
      }
      expect(err.message).toEqual('Invalid object format, cannot generate entry hash')
    }) */
    });

    describe("fromMultihash", () => {
        it("creates a entry from ipfs hash", async () => {
            const payload1 = "hello world";
            const payload2 = "hello again";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                next: [],
                clock: new LamportClock({
                    id: new Uint8Array([1, 2, 3]),
                    timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
                }),
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [entry1],
                clock: new LamportClock({
                    id: new Uint8Array([1, 2, 3]),
                    timestamp: new Timestamp({ wallTime: 3n, logical: 3 }),
                }),
            });
            const final = await Entry.fromMultihash<string>(store, entry2.hash);
            final.init(entry2);
            assert(final.equals(entry2));
            expect(final.gid).toEqual(
                await toBase64(await sodium.crypto_generichash(32, "A"))
            );
            expect(final.payload.getValue()).toEqual(payload2);
            expect(final.next.length).toEqual(1);
            expect(final.next[0]).toEqual(entry1.hash);
            expect(final.hash).toMatchSnapshot();
        });
    });

    describe("isParent", () => {
        it("returns true if entry has a child", async () => {
            const payload1 = "hello world";
            const payload2 = "hello again";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [entry1],
            });
            expect(Entry.isDirectParent(entry1, entry2)).toEqual(true);
        });

        it("returns false if entry does not have a child", async () => {
            const payload1 = "hello world";
            const payload2 = "hello again";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [],
            });
            const entry3 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [entry2],
            });
            expect(Entry.isDirectParent(entry1, entry2)).toEqual(false);
            expect(Entry.isDirectParent(entry1, entry1)).toEqual(false);
            expect(Entry.isDirectParent(entry2, entry3)).toEqual(true);
        });
    });

    describe("compare", () => {
        it("returns true if entries are the same", async () => {
            const payload1 = "hello world";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                clock: new LamportClock({
                    id: new Uint8Array([1]),
                    timestamp: new Timestamp({ wallTime: 3n, logical: 2 }),
                }),
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                clock: new LamportClock({
                    id: new Uint8Array([1]),
                    timestamp: new Timestamp({ wallTime: 3n, logical: 2 }),
                }),
                next: [],
            });
            expect(Entry.isEqual(entry1, entry2)).toEqual(true);
        });

        it("returns true if entries are not the same", async () => {
            const payload1 = "hello world1";
            const payload2 = "hello world2";
            const entry1 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload1,
                next: [],
            });
            const entry2 = await Entry.create({
                store,
                identity: identityFromSignKey(signKey),
                gidSeed: "A",
                data: payload2,
                next: [],
            });
            expect(Entry.isEqual(entry1, entry2)).toEqual(false);
        });
    });
});
