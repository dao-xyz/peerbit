import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
    Ed25519Keypair,
    PublicKeyEncryptionResolver,
    SignatureWithKey,
    X25519Keypair,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";

import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>;

const last = <T>(arr: T[]): T => {
    return arr[arr.length - 1];
};

describe("Log - Encryption", function () {
    let keystore: Keystore,
        senderKey: KeyWithMeta<X25519Keypair>,
        recieverKey: KeyWithMeta<X25519Keypair>,
        store: Blocks;

    beforeAll(async () => {
        rmrf.sync(testKeyStorePath(__filenameBase));

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );

        senderKey = await keystore.createKey(await X25519Keypair.create(), {
            id: "sender",
            overwrite: true,
        });
        recieverKey = await keystore.createKey(await X25519Keypair.create(), {
            id: "reciever",
            overwrite: true,
        });

        // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
        signKey = (await keystore.getKey(
            new Uint8Array([0])
        )) as KeyWithMeta<Ed25519Keypair>;
        signKey2 = (await keystore.getKey(
            new Uint8Array([1])
        )) as KeyWithMeta<Ed25519Keypair>;

        store = new Blocks(new MemoryLevelBlockStore());
        await store.open();
    });

    afterAll(async () => {
        await store.close();

        rmrf.sync(testKeyStorePath(__filenameBase));

        await keystore?.close();
    });

    describe("join", () => {
        let log1: Log<string>, log2: Log<string>;

        beforeEach(async () => {
            const logOptions = {
                gid: "X",
                encryption: {
                    getEncryptionKeypair: () => senderKey.keypair,
                    getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
                        for (let i = 0; i < publicKeys.length; i++) {
                            if (
                                publicKeys[i].equals(
                                    senderKey.keypair.publicKey
                                )
                            ) {
                                return {
                                    index: i,
                                    keypair: senderKey.keypair,
                                };
                            }
                            if (
                                publicKeys[i].equals(
                                    recieverKey.keypair.publicKey
                                )
                            ) {
                                return {
                                    index: i,
                                    keypair: recieverKey.keypair,
                                };
                            }
                        }
                    },
                } as PublicKeyEncryptionResolver,
            };
            log1 = new Log(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                logOptions
            );
            log2 = new Log(
                store,
                {
                    ...signKey2.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey2.keypair.sign(data),
                },
                logOptions
            );
        });

        it("can encrypt signatures with particular reciever", async () => {
            // dummy signer
            const extraSigner = await Ed25519Keypair.create();
            const extraSigner2 = await Ed25519Keypair.create();

            await log2.append("helloA1", {
                reciever: {
                    metadata: undefined,
                    signatures: {
                        [await log2._identity.publicKey.hashcode()]:
                            recieverKey.keypair.publicKey, // reciever 1
                        [await extraSigner.publicKey.hashcode()]: [
                            recieverKey.keypair.publicKey,
                            (await X25519Keypair.create()).publicKey,
                        ], // reciever 1 again and 1 unknown reciever
                        [await extraSigner2.publicKey.hashcode()]: (
                            await X25519Keypair.create()
                        ).publicKey, // unknown reciever
                    },
                    payload: recieverKey.keypair.publicKey,
                    next: recieverKey.keypair.publicKey,
                },
                signers: [
                    async (data) =>
                        new SignatureWithKey({
                            publicKey: log2._identity.publicKey,
                            signature: await log2._identity.sign(data),
                        }),
                    async (data) => {
                        return new SignatureWithKey({
                            publicKey: extraSigner.publicKey,
                            signature: await extraSigner.sign(data),
                        });
                    },
                    async (data) => {
                        return new SignatureWithKey({
                            publicKey: extraSigner2.publicKey,
                            signature: await extraSigner2.sign(data),
                        });
                    },
                ],
            });

            // Remove decrypted caches of the log2 values
            log2.values.forEach((value) => {
                value._metadata.clear();
                value._payload.clear();
                value._signatures!.signatures.forEach((signature) =>
                    signature.clear()
                );
                value._next.clear();
            });

            await log1.join(log2);
            expect(log1.length).toEqual(1);
            const item = last(log1.values);
            expect(item.next.length).toEqual(0);
            expect(
                (await item.getSignatures()).map((x) => x.publicKey.hashcode())
            ).toContainAllValues([
                extraSigner.publicKey.hashcode(),
                log2._identity.publicKey.hashcode(),
            ]);
        });

        it("joins encrypted identities only with knowledge of id and clock", async () => {
            await log1.append("helloA1", {
                reciever: {
                    metadata: undefined,
                    signatures: recieverKey.keypair.publicKey,
                    payload: recieverKey.keypair.publicKey,
                    next: recieverKey.keypair.publicKey,
                },
            });
            await log1.append("helloA2", {
                reciever: {
                    metadata: undefined,
                    signatures: recieverKey.keypair.publicKey,
                    payload: recieverKey.keypair.publicKey,
                    next: recieverKey.keypair.publicKey,
                },
            });
            await log2.append("helloB1", {
                reciever: {
                    metadata: undefined,
                    signatures: recieverKey.keypair.publicKey,
                    payload: recieverKey.keypair.publicKey,
                    next: recieverKey.keypair.publicKey,
                },
            });
            await log2.append("helloB2", {
                reciever: {
                    metadata: undefined,
                    signatures: recieverKey.keypair.publicKey,
                    payload: recieverKey.keypair.publicKey,
                    next: recieverKey.keypair.publicKey,
                },
            });

            // Remove decrypted caches of the log2 values
            log2.values.forEach((value) => {
                value._metadata.clear();
                value._payload.clear();
                value._signatures!.signatures.forEach((signature) =>
                    signature.clear()
                );
                value._next.clear();
            });

            await log1.join(log2);
            expect(log1.length).toEqual(4);
            const item = last(log1.values);
            expect(item.next.length).toEqual(1);
        });
    });
});
