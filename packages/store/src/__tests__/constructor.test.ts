import { Store, DefaultOptions, CachedValue } from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";

// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { AbstractLevel } from "abstract-level";
import { fileURLToPath } from "url";
import path from "path";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

describe(`Constructor`, function () {
    let blockStore: Blocks,
        signKey: KeyWithMeta<Ed25519Keypair>,
        identityStore: AbstractLevel<any, string, Uint8Array>,
        store: Store<any>,
        cacheStore: AbstractLevel<any, string, Uint8Array>;

    const ipfsConfig = Object.assign(
        {},
        {
            repo: "repo-entry" + __filenameBase + new Date().getTime(),
        }
    );

    beforeAll(async () => {
        identityStore = await createStore(__filenameBase + "/identity");
        const keystore = new Keystore(identityStore);

        cacheStore = await createStore(__filenameBase + "/cache");
        const cache = new Cache<CachedValue>(cacheStore);

        signKey = await keystore.createEd25519Key();

        blockStore = new Blocks(new MemoryLevelBlockStore());
        await blockStore.open();

        const options = Object.assign({}, DefaultOptions, {
            resolveCache: () => Promise.resolve(cache),
        });
        store = new Store({ storeIndex: 0 });
        await store.init(
            blockStore,
            {
                ...signKey.keypair,

                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            options
        );
    });

    afterAll(async () => {
        await store?.close();
        await blockStore?.close();
        await identityStore?.close();
        await cacheStore?.close();
    });

    it("creates a new Store instance", async () => {
        expect(typeof store._options).toEqual("object");
        expect(typeof store._store).toEqual("object");
        expect(typeof store._cache).toEqual("object");
        expect(typeof store._oplog).toEqual("object");
    });

    it("properly defines a cache", async () => {
        expect(typeof store._cache).toEqual("object");
    });
    it("can clone", async () => {
        const clone = store.clone();
        expect(clone).not.toEqual(store);
    });
});
