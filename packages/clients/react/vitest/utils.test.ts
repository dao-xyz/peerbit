import {
    getAllKeyPairs,
    getFreeKeypair,
    releaseKey,
} from "../src/utils.js";
import nodelocalstorage from "node-localstorage";
import { FastMutex } from "../src/lockstorage.js";
import { delay } from "@peerbit/time";
import { default as sodium } from "libsodium-wrappers";
import { v4 as uuid } from "uuid";
import { expect } from "chai";
import { beforeAll, afterAll, describe, it } from "vitest";

describe("getKeypair", () => {
    beforeAll(async () => {
        await sodium.ready;

        var LocalStorage = nodelocalstorage.LocalStorage;
        var localStorage = new LocalStorage("./tmp/getKeypair");
        globalThis.localStorage = localStorage;
    });

    afterAll(() => {
        globalThis.localStorage.clear();
    });

    it("can aquire multiple keypairs", async () => {
        let timeout = 1000;
        let mutex = new FastMutex({ localStorage, timeout });
        let lock = true;
        const lockCondition = () => lock;
        let id = uuid();
        const { key: keypair, path: path1 } = await getFreeKeypair(
            id,
            mutex,
            lockCondition
        );
        const { key: keypair2, path: path2 } = await getFreeKeypair(id, mutex);
        expect(keypair!.equals(keypair2!)).to.be.false;
        expect(path1).not.to.eq(path2);
        lock = false;
        await delay(timeout);
        const { path: path3, key: keypair3 } = await getFreeKeypair(id, mutex);
        expect(path3).to.eq(path1);
        expect(keypair3.equals(keypair)).to.be.true;

        const allKeypair = await getAllKeyPairs(id);
        expect(allKeypair.map((x) => x.publicKey.hashcode())).to.deep.eq([
            keypair3.publicKey.hashcode(),
            keypair2.publicKey.hashcode(),
        ]);
    });

    it("can release if same id", async () => {
        let timeout = 1000;
        let mutex = new FastMutex({ localStorage, timeout });
        let lock = true;
        const lockCondition = () => lock;
        let id = uuid();
        const { key: keypair, path: path1 } = await getFreeKeypair(
            id,
            mutex,
            lockCondition,
            { releaseLockIfSameId: true }
        );
        const { key: keypair2, path: path2 } = await getFreeKeypair(
            id,
            mutex,
            undefined,
            { releaseLockIfSameId: true }
        );
        expect(keypair!.equals(keypair2!)).to.be.true;
        expect(path1).to.eq(path2);
        const allKeypair = await getAllKeyPairs(id);
        expect(allKeypair).to.have.length(1);
    });

    it("releases manually", async () => {
        let timeout = 1000;
        let mutex = new FastMutex({ localStorage, timeout });
        const id = uuid();

        const { key: keypair, path: path1 } = await getFreeKeypair(id, mutex);

        const { key: keypair2, path: path2 } = await getFreeKeypair(id, mutex);

        expect(path1).not.to.eq(path2);
        releaseKey(path1, mutex);
        expect(mutex.getLockedInfo(path1)).to.be.undefined;
        const { key: keypair3, path: path3 } = await getFreeKeypair(id, mutex);

        expect(path1).to.eq(path3); // we can now acquire key at path1 again, since we released it
    });
});
