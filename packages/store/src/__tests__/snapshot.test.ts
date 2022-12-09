import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Store, DefaultOptions } from "../store.js";
import { Entry } from "@dao-xyz/peerbit-log";
import { SimpleIndex } from "./utils.js";
import { createStore } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { AbstractLevel } from "abstract-level";
import { fileURLToPath } from "url";
import path from "path";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

describe(`Snapshots`, function () {
    let blockStore: Blocks,
        signKey: KeyWithMeta<Ed25519Keypair>,
        identityStore: AbstractLevel<any, string, Uint8Array>,
        store: Store<any>,
        cacheStore: AbstractLevel<any, string, Uint8Array>;
    let index: SimpleIndex<string>;
    const ipfsConfig = Object.assign(
        {},
        {
            repo: "repo-entry" + __filenameBase + new Date().getTime(),
        }
    );

    beforeAll(async () => {
        identityStore = await createStore(path.join(__filename, "identity"));
        cacheStore = await createStore(path.join(__filename, "cache"));

        const keystore = new Keystore(identityStore);

        signKey = await keystore.createEd25519Key();
    });

    beforeEach(async () => {
        blockStore = new Blocks(new MemoryLevelBlockStore());
        await blockStore.open();

        const cache = new Cache(cacheStore);
        index = new SimpleIndex();
        const options = Object.assign({}, DefaultOptions, {
            resolveCache: () => Promise.resolve(cache),
            onUpdate: index.updateIndex.bind(index),
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

    it("Saves a local snapshot", async () => {
        const writes = 10;

        for (let i = 0; i < writes; i++) {
            await store._addOperation({ step: i });
        }
        const snapshot = await store.saveSnapshot();
        /*  expect(snapshot[0].path.length).toEqual(46);
         expect(snapshot[0].cid.toString().length).toEqual(46);
         expect(snapshot[0].path).toEqual(snapshot[0].cid.toString());
         expect(snapshot[0].size > writes * 200).toEqual(true); */
        expect(snapshot).toBeDefined();
    });

    it("Successfully loads a saved snapshot", async () => {
        const writes = 10;

        for (let i = 0; i < writes; i++) {
            await store._addOperation({ step: i });
        }
        await store.saveSnapshot();
        index._index = [];
        await store.loadFromSnapshot();
        expect(index._index.length).toEqual(10);

        for (let i = 0; i < writes; i++) {
            expect(
                (index._index[i] as Entry<any>).payload.getValue().step
            ).toEqual(i);
        }
    });

    // TODO test resume unfishid replication
});
