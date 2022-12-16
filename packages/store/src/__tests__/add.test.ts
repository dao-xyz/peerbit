import assert, { rejects } from "assert";
import { Store, DefaultOptions, HeadsCache } from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "@dao-xyz/peerbit-log";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { fileURLToPath } from "url";
import path from "path";
import { AbstractLevel } from "abstract-level";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
describe(`addOperation`, function () {
    let blockStore: Blocks,
        signKey: KeyWithMeta<Ed25519Keypair>,
        identityStore: AbstractLevel<any, string, Uint8Array>,
        store: Store<any>,
        cacheStore: AbstractLevel<any, string, Uint8Array>;
    let index: SimpleIndex<string>;

    const ipfsConfig = Object.assign(
        {},
        {
            repo: "repo-add" + __filenameBase + new Date().getTime(),
        }
    );

    beforeAll(async () => {
        identityStore = await createStore(path.join(__filename, "identity"));

        const keystore = new Keystore(identityStore);

        signKey = await keystore.createEd25519Key();

        blockStore = new Blocks(new MemoryLevelBlockStore());
        await blockStore.open();

        cacheStore = await createStore(path.join(__filename, "cache"));
    });

    afterAll(async () => {
        await store?.close();
        await blockStore?.close();
        await identityStore?.close();
        await cacheStore?.close();
    });

    beforeEach(async () => {
        await cacheStore.clear();
    });

    it("adds an operation and triggers the write event", async () => {
        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
        const onWrite = async (store: Store<any>, entry: Entry<any>) => {
            const heads = await store.oplog.heads;
            expect(heads.length).toEqual(1);
            assert.deepStrictEqual(entry.payload.getValue(), data);
            assert.deepStrictEqual(index._index, heads);
            store.getCachedHeads().then((localHeads) => {
                if (!localHeads) {
                    fail();
                }
                assert.deepStrictEqual(localHeads[0], entry.hash);
                expect(localHeads[0]).toEqual(heads[0].hash);
                expect(heads.length).toEqual(1);
                expect(localHeads.length).toEqual(1);
                done = true;
            });
        };

        store = new Store({ storeIndex: 0 });
        await store.init(
            blockStore,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index.updateIndex.bind(index),
                onWrite: onWrite,
            }
        );

        const data = { data: 12345 };

        await store.addOperation(data).then((entry) => {
            expect(entry.entry).toBeInstanceOf(Entry);
        });

        await waitFor(() => done);
    });

    it("adds multiple operations and triggers multiple write events", async () => {
        const writes = 3;
        let eventsFired = 0;

        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
        const onWrite = async (store: Store<any>, entry: Entry<any>) => {
            eventsFired++;
            if (eventsFired === writes) {
                const heads = store.oplog.heads;
                expect(heads.length).toEqual(1);
                expect(index._index.length).toEqual(writes);
                store.getCachedHeads().then((localHeads) => {
                    if (!localHeads) {
                        fail();
                    }
                    expect(localHeads).toHaveLength(1);
                    expect(localHeads[0]).toEqual(index._index[2].hash);
                    done = true;
                });
            }
        };

        store = new Store({ storeIndex: 1 });
        await store.init(
            blockStore,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index.updateIndex.bind(index),
                onWrite: onWrite,
            }
        );

        for (let i = 0; i < writes; i++) {
            await store.addOperation({ step: i });
        }

        await waitFor(() => done);
    });

    it("can add as unique heads", async () => {
        const writes = 3;
        let eventsFired = 0;

        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
        const allAddedEntries: string[] = [];
        const onWrite = async (store: Store<any>, entry: Entry<any>) => {
            eventsFired++;
            if (eventsFired === writes) {
                const heads = store.oplog.heads;
                expect(heads.length).toEqual(3);
                expect(index._index.length).toEqual(writes);
                store.getCachedHeads().then((localHeads) => {
                    if (!localHeads) {
                        fail();
                    }
                    expect(localHeads).toContainAllValues(allAddedEntries);
                    done = true;
                });
            }
        };

        store = new Store({ storeIndex: 1 });
        await store.init(
            blockStore,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index.updateIndex.bind(index),
                onWrite: onWrite,
            }
        );

        for (let i = 0; i < writes; i++) {
            allAddedEntries.push(
                (await store.addOperation({ step: i }, { nexts: [] })).entry
                    .hash
            );
        }

        await waitFor(() => done);
    });
});
