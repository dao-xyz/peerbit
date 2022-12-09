import { Log } from "@dao-xyz/peerbit-log";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";

import { createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions, Store } from "../store.js";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";

import { fileURLToPath } from "url";
import path from "path";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";
const __filename = fileURLToPath(import.meta.url);

// Tests timeout
const timeout = 30000;

describe(`Replicator`, () => {
    jest.setTimeout(timeout);

    let signKey: KeyWithMeta<Ed25519Keypair>,
        store: Store<any>,
        keystore: Keystore,
        blockStore: Blocks,
        cacheStore;
    let index: SimpleIndex<string>;

    beforeAll(async () => {
        keystore = new Keystore(
            await createStore(path.join(__filename, "identity"))
        );

        blockStore = new Blocks(new MemoryLevelBlockStore());
        await blockStore.open();

        signKey = await keystore.createEd25519Key();
        cacheStore = await createStore(path.join(__filename, "cache"));
        const cache = new Cache(cacheStore);
        index = new SimpleIndex();

        const options = Object.assign({}, DefaultOptions, {
            replicationConcurrency: 123,
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
        await store._replicator?.stop();
        await keystore?.close();
        await blockStore?.close();
    });

    it("default options", async () => {
        expect(store._replicator._logs).toBeEmpty();
    });

    describe("replication progress", function () {
        let log2: Log<string>;

        jest.setTimeout(timeout);

        const logLength = 100;

        beforeAll(async () => {
            log2 = new Log(
                blockStore,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId: store._oplog._id }
            );
            console.log(`writing ${logLength} entries to the log`);
            let prev: any = undefined;
            for (let i = 0; i < logLength; i++) {
                prev = await log2.append(`entry${i}`, {
                    nexts: prev ? [prev] : undefined,
                });
            }
            expect(log2.values.length).toEqual(logLength);
        });

        it("replicates all entries in the log", (done) => {
            let replicated = 0;
            expect(store._oplog._id).toEqual(log2._id);
            expect(store._replicator._logs.length).toEqual(0);
            expect(store._replicator.tasksQueued).toEqual(0);
            store._replicator.onReplicationProgress = () => replicated++;
            store._replicator.onReplicationComplete = async (
                replicatedLogs
            ) => {
                expect(store._replicator.tasksRunning).toEqual(0);
                expect(store._replicator.tasksQueued).toEqual(0);
                expect(store._replicator.unfinished.length).toEqual(0);
                for (const replicatedLog of replicatedLogs) {
                    await store._oplog.join(replicatedLog);
                }
                expect(store._oplog.values.length).toEqual(logLength);
                expect(store._oplog.values.length).toEqual(log2.values.length);
                for (let i = 0; i < store._oplog.values.length; i++) {
                    expect(
                        store._oplog.values[i].equals(log2.values[i])
                    ).toBeTrue();
                }
                done();
            };

            store._replicator.load(log2.heads);
        });
    });
});
