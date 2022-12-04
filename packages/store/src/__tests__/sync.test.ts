import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions, Store } from "../store.js";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";
import { fileURLToPath } from "url";
import path from "path";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";
import { Entry } from "@dao-xyz/ipfs-log";
import {
    LibP2PBlockStore,
    MemoryLevelBlockStore,
    Blocks,
} from "@dao-xyz/peerbit-block";

const __filename = fileURLToPath(import.meta.url);

// Tests timeout
const timeout = 30000;

describe(`Sync`, () => {
    jest.setTimeout(timeout);

    let session: LSession,
        signKey: KeyWithMeta<Ed25519Keypair>,
        store: Store<any>,
        keystore: Keystore,
        cacheStore: AbstractLevel<any, string>,
        blockStore: Blocks,
        blockStore2: Blocks;

    let index: SimpleIndex<string>;

    beforeAll(async () => {
        cacheStore = await createStore(path.join(__filename, "cache"));
        keystore = new Keystore(
            await createStore(path.join(__filename, "identity"))
        );

        session = await LSession.connected(2);
        signKey = await keystore.createEd25519Key();
    });

    beforeEach(async () => {
        const cache = new Cache(cacheStore);
        index = new SimpleIndex();
        store = new Store({ storeIndex: 0 });
        blockStore = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await blockStore.open();
        await store.init(
            blockStore,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                replicationConcurrency: 123,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index.updateIndex.bind(index),
            }
        );
    });

    afterEach(async () => {
        await blockStore?.close();
        await blockStore2?.close();
    });
    afterAll(async () => {
        await session.stop();
        await keystore?.close();
    });

    it("syncs normally", async () => {
        const cache = new Cache(cacheStore);
        const index2 = new SimpleIndex();
        const store2 = new Store({ storeIndex: 1 });
        blockStore2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await blockStore2.open();

        await store2.init(
            blockStore2,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                replicationConcurrency: 123,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index2.updateIndex.bind(index2),
            }
        );

        const entryCount = 10;
        for (let i = 0; i < entryCount; i++) {
            await store._addOperation("i: " + i);
        }

        expect(store.oplog.heads.length).toEqual(1);
        expect(store.oplog.values.length).toEqual(entryCount);
        await store2.sync(store.oplog.heads);
        await waitFor(() => store2.oplog.values.length == entryCount);
        expect(store2.oplog.heads).toHaveLength(1);
    });

    it("syncs with references", async () => {
        const cache = new Cache(cacheStore);
        const index2 = new SimpleIndex();
        const store2 = new Store({ storeIndex: 1 });

        const progressCallbackEntries: Entry<any>[] = [];

        blockStore2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await blockStore2.open();

        await store2.init(
            blockStore2,
            {
                ...signKey.keypair,
                sign: async (data: Uint8Array) =>
                    await signKey.keypair.sign(data),
            },
            {
                ...DefaultOptions,
                replicationConcurrency: 123,
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index2.updateIndex.bind(index2),
                onReplicationProgress: (store, entry) => {
                    progressCallbackEntries.push(entry);
                },
            }
        );

        const entryCount = 10;
        for (let i = 0; i < entryCount; i++) {
            await store._addOperation(i);
        }

        expect(store.oplog.heads.length).toEqual(1);
        expect(store.oplog.values.length).toEqual(entryCount);
        await store2.sync([
            {
                entry: store.oplog.heads[0],
                references: [store.oplog.values[3], store.oplog.values[6]],
            },
        ]);
        await waitFor(() => store2.oplog.values.length == entryCount);
        expect(progressCallbackEntries).toHaveLength(10);
        expect(progressCallbackEntries[0].payload.getValue()).toEqual(9); // because head
        expect(progressCallbackEntries[1].payload.getValue()).toEqual(3); // because first reference
        expect(progressCallbackEntries[2].payload.getValue()).toEqual(6); // because second reference

        // the order of the rest is kind of random because we do Log.fromEntry/fromEntryHash which loads concurrently so we dont know what entries arrive firs
        expect(store2.oplog.heads).toHaveLength(1);
    });
});
