import assert from "assert";
import {
    Store,
    DefaultOptions,
    HeadsCache,
    IInitializationOptions,
} from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
    PublicKeyEncryptionResolver,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { SimpleIndex } from "./utils.js";
import { v4 as uuid } from "uuid";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { fileURLToPath } from "url";
import path from "path";
import { jest } from "@jest/globals";
const __filename = fileURLToPath(import.meta.url);
import { AbstractLevel } from "abstract-level";

// Test utils
import {
    nodeConfig as config,
    createStore,
    Session,
} from "@dao-xyz/peerbit-test-utils";

import { Entry } from "@dao-xyz/ipfs-log";
import { delay, waitFor, waitForAsync } from "@dao-xyz/peerbit-time";

describe(`addOperation`, function () {
    let session: Session,
        signKey: KeyWithMeta<Ed25519Keypair>,
        keystore: Keystore,
        identityStore: AbstractLevel<any, string>,
        store: Store<any>,
        cacheStore: AbstractLevel<any, string>,
        senderKey: KeyWithMeta<Ed25519Keypair>,
        recieverKey: KeyWithMeta<Ed25519Keypair>,
        encryption: PublicKeyEncryptionResolver;
    let index: SimpleIndex<string>;

    jest.setTimeout(config.timeout);

    beforeAll(async () => {
        session = await Session.connected(1);
    });

    beforeEach(async () => {
        identityStore = await createStore(
            path.join(__filename, "identity" + uuid())
        );
        cacheStore = await createStore(path.join(__filename, "cache" + uuid()));
        keystore = new Keystore(identityStore);
        signKey = await keystore.createEd25519Key();
        index = new SimpleIndex();
        senderKey = await keystore.createEd25519Key();
        recieverKey = await keystore.createEd25519Key();
        encryption = {
            getEncryptionKeypair: () => Promise.resolve(senderKey.keypair),
            getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
                for (let i = 0; i < publicKeys.length; i++) {
                    if (
                        publicKeys[i].equals(
                            await X25519PublicKey.from(
                                senderKey.keypair.publicKey
                            )
                        )
                    ) {
                        return {
                            index: i,
                            keypair: senderKey.keypair,
                        };
                    }
                    if (
                        publicKeys[i].equals(
                            await X25519PublicKey.from(
                                recieverKey.keypair.publicKey
                            )
                        )
                    ) {
                        return {
                            index: i,
                            keypair: recieverKey.keypair,
                        };
                    }
                }
            },
        };
    });

    afterEach(async () => {
        await store?.close();
        await identityStore?.close();
        await cacheStore?.close();
    });

    afterAll(async () => {
        await session?.stop();
    });

    it("encrypted entry is appended known key", async () => {
        const data = { data: 12345 };

        let done = false;
        const onWrite = async (store: Store<any>, entry: Entry<any>) => {
            try {
                const heads = store.oplog.heads;
                expect(heads.length).toEqual(1);
                assert.deepStrictEqual(entry.payload.getValue(), data);
                /*   expect(store.replicationStatus.progress).toEqual(1n);
                  expect(store.replicationStatus.max).toEqual(1n); */
                assert.deepStrictEqual(index._index, heads);
                await delay(5000); // seems because write is async?
                await waitForAsync(
                    async () =>
                        (await store._cache.getBinary(
                            store.localHeadsPath,
                            HeadsCache
                        )) !== undefined
                );
                const localHeads = await store._cache.getBinary(
                    store.localHeadsPath,
                    HeadsCache
                );
                if (!localHeads) {
                    fail();
                }
                localHeads.heads[0].init({
                    encryption: store.oplog._encryption,
                    encoding: store.oplog._encoding,
                });
                await localHeads.heads[0].getPayload();
                assert.deepStrictEqual(
                    localHeads.heads[0].payload.getValue(),
                    data
                );
                assert(localHeads.heads[0].equals(heads[0]));
                expect(heads.length).toEqual(1);
                expect(localHeads.heads.length).toEqual(1);
                done = true;
            } catch (error) {
                throw error;
            }
        };

        const cache = new Cache(cacheStore);
        const options: IInitializationOptions<any> = {
            ...DefaultOptions,
            resolveCache: () => Promise.resolve(cache),
            onUpdate: index.updateIndex.bind(index),
            encryption,
            onWrite,
        };
        store = new Store({ storeIndex: 0 });
        await store.init(
            session.peers[0].ipfs,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            options
        );

        await store._addOperation(data, {
            reciever: {
                coordinate: undefined,
                next: recieverKey.keypair.publicKey,
                payload: recieverKey.keypair.publicKey,
                signatures: recieverKey.keypair.publicKey,
            },
        });

        await waitFor(() => done);
    });

    it("encrypted entry is append unkown key", async () => {
        const data = { data: 12345 };
        let done = false;

        const onWrite = async (store: Store<any>, entry: Entry<any>) => {
            const heads = store.oplog.heads;
            expect(heads.length).toEqual(1);
            assert.deepStrictEqual(entry.payload.getValue(), data);
            /* expect(store.replicationStatus.progress).toEqual(1n);
            expect(store.replicationStatus.max).toEqual(1n); */
            assert.deepStrictEqual(index._index, heads);
            await waitForAsync(
                async () =>
                    (await store._cache.getBinary(
                        store.localHeadsPath,
                        HeadsCache
                    )) !== undefined
            );
            const localHeads = await store._cache.getBinary(
                store.localHeadsPath,
                HeadsCache
            );

            if (!localHeads) {
                fail();
            }

            localHeads.heads[0].init({
                encryption: store.oplog._encryption,
                encoding: store.oplog._encoding,
            });
            try {
                await localHeads.heads[0].getPayload();
                assert(false);
            } catch (error) {
                if (error instanceof AccessError === false) {
                    console.error(error);
                }
                expect(error).toBeInstanceOf(AccessError);
            }
            assert(localHeads.heads[0].equals(heads[0]));
            expect(heads.length).toEqual(1);
            expect(localHeads.heads.length).toEqual(1);
            done = true;
        };

        const cache = new Cache(cacheStore);
        const options: IInitializationOptions<any> = {
            ...DefaultOptions,
            resolveCache: () => Promise.resolve(cache),
            onUpdate: index.updateIndex.bind(index),
            encryption,
            onWrite,
        };
        store = new Store({ storeIndex: 0 });
        await store.init(
            session.peers[0].ipfs,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            options
        );

        const reciever = await keystore.createEd25519Key();
        await store._addOperation(data, {
            reciever: {
                coordinate: undefined,
                next: reciever.keypair.publicKey,
                payload: reciever.keypair.publicKey,
                signatures: reciever.keypair.publicKey,
            },
        });

        await waitFor(() => done);
    });
});
