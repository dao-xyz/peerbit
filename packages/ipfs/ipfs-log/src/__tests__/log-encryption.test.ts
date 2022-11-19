import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
    Ed25519Keypair,
    PublicKeyEncryptionResolver,
    X25519Keypair,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs,
} from "@dao-xyz/peerbit-test-utils";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { jest } from "@jest/globals";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let ipfsd: Controller,
    ipfs: IPFS,
    signKey: KeyWithMeta<Ed25519Keypair>,
    signKey2: KeyWithMeta<Ed25519Keypair>,
    signKey3: KeyWithMeta<Ed25519Keypair>,
    signKey4: KeyWithMeta<Ed25519Keypair>;

const last = (arr: any[]) => {
    return arr[arr.length - 1];
};

Object.keys(testAPIs).forEach((IPFS) => {
    describe("Log - Encryption", function () {
        jest.setTimeout(config.timeout);

        const { signingKeyFixtures, signingKeysPath } = config;

        let keystore: Keystore,
            senderKey: KeyWithMeta<X25519Keypair>,
            recieverKey: KeyWithMeta<X25519Keypair>;

        beforeAll(async () => {
            rmrf.sync(signingKeysPath(__filenameBase));

            await fs.copy(
                signingKeyFixtures(__dirname),
                signingKeysPath(__filenameBase)
            );

            keystore = new Keystore(
                await createStore(signingKeysPath(__filenameBase))
            );

            senderKey = await keystore.createKey(await X25519Keypair.create(), {
                id: "sender",
                overwrite: true,
            });
            recieverKey = await keystore.createKey(
                await X25519Keypair.create(),
                {
                    id: "reciever",
                    overwrite: true,
                }
            );

            // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
            signKey = (await keystore.getKey(
                new Uint8Array([0])
            )) as KeyWithMeta<Ed25519Keypair>;
            signKey2 = (await keystore.getKey(
                new Uint8Array([1])
            )) as KeyWithMeta<Ed25519Keypair>;
            ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig);
            ipfs = ipfsd.api;
        });

        afterAll(async () => {
            await stopIpfs(ipfsd);

            rmrf.sync(signingKeysPath(__filenameBase));

            await keystore?.close();
        });

        describe("join", () => {
            let log1: Log<string>, log2: Log<string>;

            beforeEach(async () => {
                const logOptions = {
                    gid: "X",
                    encryption: {
                        getEncryptionKeypair: () =>
                            Promise.resolve(senderKey.keypair),
                        getAnyKeypair: async (
                            publicKeys: X25519PublicKey[]
                        ) => {
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
                    ipfs,
                    {
                        ...signKey.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey.keypair.sign(data),
                    },
                    logOptions
                );
                log2 = new Log(
                    ipfs,
                    {
                        ...signKey2.keypair,
                        sign: async (data: Uint8Array) =>
                            await signKey2.keypair.sign(data),
                    },
                    logOptions
                );
            });

            it("join encrypted identities only with knowledge of id and clock", async () => {
                await log1.append("helloA1", {
                    reciever: {
                        coordinate: undefined,
                        signature: recieverKey.keypair.publicKey,
                        payload: recieverKey.keypair.publicKey,
                        next: recieverKey.keypair.publicKey,
                    },
                });
                await log1.append("helloA2", {
                    reciever: {
                        coordinate: undefined,
                        signature: recieverKey.keypair.publicKey,
                        payload: recieverKey.keypair.publicKey,
                        next: recieverKey.keypair.publicKey,
                    },
                });
                await log2.append("helloB1", {
                    reciever: {
                        coordinate: undefined,
                        signature: recieverKey.keypair.publicKey,
                        payload: recieverKey.keypair.publicKey,
                        next: recieverKey.keypair.publicKey,
                    },
                });
                await log2.append("helloB2", {
                    reciever: {
                        coordinate: undefined,
                        signature: recieverKey.keypair.publicKey,
                        payload: recieverKey.keypair.publicKey,
                        next: recieverKey.keypair.publicKey,
                    },
                });

                // Remove decrypted caches of the log2 values
                log2.values.forEach((value) => {
                    value._coordinate.clear();
                    value._payload.clear();
                    value._signature!.clear();
                    value._next.clear();
                });

                await log1.join(log2);
                expect(log1.length).toEqual(4);
                const item = last(log1.values);
                expect(item.next.length).toEqual(1);
            });
        });
    });
});
