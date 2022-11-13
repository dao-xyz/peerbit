import assert from "assert";

import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Store, DefaultOptions } from "../store.js";
import { Entry, JSON_ENCODING } from "@dao-xyz/ipfs-log";
import { SimpleIndex } from "./utils.js";
import { jest } from "@jest/globals";

// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs,
    createStore,
} from "@dao-xyz/peerbit-test-utils";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { AbstractLevel } from "abstract-level";
import { fileURLToPath } from "url";
import path from "path";
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

Object.keys(testAPIs).forEach((IPFS) => {
    describe(`Snapshots ${IPFS}`, function () {
        let ipfsd: Controller,
            ipfs: IPFS,
            signKey: KeyWithMeta<Ed25519Keypair>,
            identityStore: AbstractLevel<any, string>,
            store: Store<any>,
            cacheStore: AbstractLevel<any, string>;
        let index: SimpleIndex<string>;
        jest.setTimeout(config.timeout);

        const ipfsConfig = Object.assign({}, config, {
            repo: "repo-entry" + __filenameBase + new Date().getTime(),
        });

        beforeAll(async () => {
            identityStore = await createStore(
                path.join(__filename, "identity")
            );
            cacheStore = await createStore(path.join(__filename, "cache"));

            const keystore = new Keystore(identityStore);

            signKey = await keystore.createEd25519Key();
            ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1);
            ipfs = ipfsd.api;
        });

        beforeEach(async () => {
            const cache = new Cache(cacheStore);
            index = new SimpleIndex();
            const options = Object.assign({}, DefaultOptions, {
                resolveCache: () => Promise.resolve(cache),
                onUpdate: index.updateIndex.bind(index),
            });
            store = new Store({ storeIndex: 0 });
            await store.init(
                ipfs,
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
            ipfsd && (await stopIpfs(ipfsd));
            await identityStore?.close();
            await cacheStore?.close();
        });

        it("Saves a local snapshot", async () => {
            const writes = 10;

            for (let i = 0; i < writes; i++) {
                await store._addOperation({ step: i });
            }
            const snapshot = await store.saveSnapshot();
            expect(snapshot[0].path.length).toEqual(46);
            expect(snapshot[0].cid.toString().length).toEqual(46);
            expect(snapshot[0].path).toEqual(snapshot[0].cid.toString());
            assert.strictEqual(snapshot[0].size > writes * 200, true);
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
                assert.strictEqual(
                    (index._index[i] as Entry<any>).payload.getValue().step,
                    i
                );
            }
        });

        // TODO test resume unfishid replication
    });
});
