import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions, Store } from "../store.js";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";
import { Entry } from "@dao-xyz/peerbit-log";

// Tests timeout
const timeout = 30000;

describe(`Sync`, () => {
    jest.setTimeout(timeout);

    let session: LSession,
        signKey: KeyWithMeta<Ed25519Keypair>,
        store: Store<any>,
        keystore: Keystore,
        cacheStore: AbstractLevel<any, string, Uint8Array>;

    let index: SimpleIndex<string>;

    beforeAll(async () => {
        cacheStore = await createStore();
        keystore = new Keystore(await createStore());
        session = await LSession.connected(2);
        signKey = await keystore.createEd25519Key();
    });

    beforeEach(async () => {
        const cache = new Cache(cacheStore);
        store = new Store({ storeIndex: 0 });
        index = new SimpleIndex(store);
        await store.init(
            session.peers[0].directblock,
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
                cacheId: "id",
            }
        );
    });

    afterAll(async () => {
        await session.stop();
        await keystore?.close();
    });

    it("syncs normally", async () => {
        const cache = new Cache(cacheStore);
        const index2 = new SimpleIndex(store);
        const store2 = new Store({ storeIndex: 1 });

        await store2.init(
            session.peers[1].directblock,
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
                cacheId: "id",
            }
        );

        const entryCount = 10;
        for (let i = 0; i < entryCount; i++) {
            await store.addOperation("i: " + i);
        }

        expect(store.oplog.heads.length).toEqual(1);
        expect(store.oplog.values.length).toEqual(entryCount);
        await store2.sync(store.oplog.heads);
        await waitFor(() => store2.oplog.values.length == entryCount);
        expect(store2.oplog.heads).toHaveLength(1);
    });

    it("syncs with references", async () => {
        const cache = new Cache(cacheStore);
        const index2 = new SimpleIndex(store);
        const store2 = new Store({ storeIndex: 1 });

        const fetchCallBackEntries: Entry<any>[] = [];
        await store2.init(
            session.peers[1].directblock,
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
                cacheId: "id",
                onReplicationFetch: (store, entry) => {
                    fetchCallBackEntries.push(entry);
                },
            }
        );

        const entryCount = 10;
        for (let i = 0; i < entryCount; i++) {
            await store.addOperation(i);
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
        expect(fetchCallBackEntries).toHaveLength(10);
        expect(fetchCallBackEntries[0].payload.getValue()).toEqual(9); // because head
        expect(fetchCallBackEntries[1].payload.getValue()).toEqual(3); // because first reference
        expect(fetchCallBackEntries[2].payload.getValue()).toEqual(6); // because second reference

        // the order of the rest is kind of random because we do Log.fromEntry/fromEntryHash which loads concurrently so we dont know what entries arrive firs
        expect(store2.oplog.heads).toHaveLength(1);
    });
});
