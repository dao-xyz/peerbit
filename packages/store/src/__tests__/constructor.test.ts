import { Store, DefaultOptions, CachedValue } from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";

// Test utils
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    createStore,
} from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { Controller } from "ipfsd-ctl";
import { AbstractLevel } from "abstract-level";
import { jest } from "@jest/globals";
import { fileURLToPath } from "url";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

describe(`Constructor`, function () {
    let ipfs: Controller,
        signKey: KeyWithMeta<Ed25519Keypair>,
        identityStore: AbstractLevel<any, string>,
        store: Store<any>,
        cacheStore: AbstractLevel<any, string>;

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config, {
        repo: "repo-entry" + __filenameBase + new Date().getTime(),
    });

    beforeAll(async () => {
        identityStore = await createStore(__filenameBase + "/identity");
        const keystore = new Keystore(identityStore);

        cacheStore = await createStore(__filenameBase + "/cache");
        const cache = new Cache<CachedValue>(cacheStore);

        signKey = await keystore.createEd25519Key();
        ipfs = await startIpfs("js-ipfs", ipfsConfig.daemon1);
        const options = Object.assign({}, DefaultOptions, {
            resolveCache: () => Promise.resolve(cache),
        });
        store = new Store({ storeIndex: 0 });
        await store.init(
            ipfs.api,
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
        ipfs && (await stopIpfs(ipfs));
        await identityStore?.close();
        await cacheStore?.close();
    });

    it("creates a new Store instance", async () => {
        expect(typeof store._options).toEqual("object");
        expect(typeof store._ipfs).toEqual("object");
        expect(typeof store._cache).toEqual("object");
        expect(typeof store._oplog).toEqual("object");
        expect(typeof store._stats).toEqual("object");
        expect(typeof store._loader).toEqual("object");
    });

    it("properly defines a cache", async () => {
        expect(typeof store._cache).toEqual("object");
    });
    it("can clone", async () => {
        const clone = store.clone();
        expect(clone).not.toEqual(store);
    });
});
