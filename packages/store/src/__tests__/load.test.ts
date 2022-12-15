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
describe(`load`, function () {
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

    it("closes and loads", async () => {
        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
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
                onWrite: () => {
                    done = true;
                },
            }
        );

        const data = { data: 12345 };
        await store._addOperation(data).then((entry) => {
            expect(entry.entry).toBeInstanceOf(Entry);
        });

        await waitFor(() => done);

        await store.close();
        await store.load();
        expect(store.oplog.values.length).toEqual(1);
    });

    it("loads when missing cache", async () => {
        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
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
                onWrite: () => {
                    done = true;
                },
            }
        );

        const data = { data: 12345 };
        await store._addOperation(data).then((entry) => {
            expect(entry.entry).toBeInstanceOf(Entry);
        });

        await waitFor(() => done);

        await store.close();
        await store._cache.del(store.headsPath);
        await store.load();
        expect(store.oplog.values.length).toEqual(0);
    });

    it("loads when corrupt cache", async () => {
        index = new SimpleIndex();
        const cache = new Cache(cacheStore);
        let done = false;
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
                onWrite: () => {
                    done = true;
                },
            }
        );

        const data = { data: 12345 };
        await store._addOperation(data).then((entry) => {
            expect(entry.entry).toBeInstanceOf(Entry);
        });

        await waitFor(() => done);

        await store.close();
        const headsPath = (await store._cache.get<string>(store.headsPath))!;
        await store._cache.set(headsPath, new Uint8Array([255]));
        await expect(() => store.load()).rejects.toThrowError();
    });
});
