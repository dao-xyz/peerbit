/* eslint-env mocha */
import { strict as assert } from "assert";

it("todo", () => {
    // if we have dependencies for test below installed, we are going to ruin the whole dev setup since we are going to get dependency conflicts with libp2p and gossipsub (we are using new version without ipfs)
});
// Test utils
/*
import {
    
    startIpfs,
    stopIpfs,
} from "@dao-xyz/peerbit-test-utils";
 import { IPFS } from "ipfs-core-types";
import { Controller } from "ipfsd-ctl";
import { jest } from "@jest/globals";
import { BlockStore, GetOptions, PutOptions } from "../store";
import { Block, decode } from "multiformats/block";
import { cidifyString, codecCodes, stringifyCid } from "../block";
import { Blocks } from "..";
import { sha256 as hasher } from "multiformats/hashes/sha2";

const mhtype = "sha2-256";

class IPFSBLockStore implements BlockStore {
    _ipfs: IPFS;
    constructor(ipfs: IPFS) {
        this._ipfs = ipfs;
    }
    async get<T>(
        cid: string,
        options?: GetOptions | undefined
    ): Promise<Block<T> | undefined> {
        const cidObject = cidifyString(cid);
        const bytes = await this._ipfs.block.get(cidObject as any, options);
        const codec = codecCodes[cidObject.code];
        const block = await decode({ bytes, codec, hasher });
        return block as Block<T>;
    }
    async put(
        block: Block<any>,
        options?: PutOptions | undefined
    ): Promise<string> {
        const cid = await this._ipfs.block.put(block.bytes, {
            format: options?.format,
            version: block.cid.version,
            mhtype,
            pin: options?.pin,
            timeout: options?.timeout,
        });
        return stringifyCid(cid);
    }
    async rm(cid: string): Promise<void> {
        const cidObject = cidifyString(cid);
        try {
            await this._ipfs.pin.rm(cidObject as any);
        } catch (error) {
            // not pinned // TODO add bettor error handling
        }
        for await (const result of this._ipfs.block.rm(cidObject as any)) {
            if (result.error) {
                throw new Error(
                    `Failed to remove block ${result.cid} due to ${result.error.message}`
                );
            }
        }
    }
    async open(): Promise<void> {
        // return this._ipfs.start(); Dont do anything, let someone else open ipfs for now
    }
    async close(): Promise<void> {
        //  return this._ipfs.stop(); Dont do anything, let someone else close ipfs  for now
    }
}

describe(`encoding`, function () {
    jest.setTimeout(10000);

    let ipfs: IPFS, ipfsd: Controller, store: Blocks;

    beforeAll(async () => {
        ipfsd = await startIpfs("js-ipfs", config);
        ipfs = ipfsd.api;
        store = new Blocks(new IPFSBLockStore(ipfs));
    });

    afterAll(async () => {
        await stopIpfs(ipfsd);
    });
    describe("dag-cbor", () => {
        let cid1, cid2;
        const data: any = { test: "object" };

        it("writes", async () => {
            cid1 = await store.put(data, "dag-cbor", { pin: true, hasher });
            expect(cid1).toEqual(
                "zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4"
            );

            let obj = await store.get(cid1);
            assert.deepStrictEqual(obj, data);

            data[cid1] = cid1;
            cid2 = await store.put(data, "dag-cbor", {
                links: [cid1],
                hasher,
            });
            expect(cid2).toEqual(
                "zdpuAqeyAtvp1ACxnWZLPW9qMEN5rJCD9N3vjUbMs4AAodTdz"
            );

            obj = await store.get(cid2, { links: [cid1] });
            data[cid1] = cid1;
            assert.deepStrictEqual(obj, data);
        });
    });

    describe("raw", () => {
        let cid;
        const data = new Uint8Array([1, 2, 3]);

        it("writes", async () => {
            cid = await store.put(data, "raw", { pin: true, hasher });
            expect(cid).toEqual(
                "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
            );
            const obj = await store.get(cid);
            expect(obj).toEqual(data);
        });
    });

    describe("rm", () => {
        it("pinned", async () => {
            const data: any = { test: "pinned" };
            const cid1 = await store.put(data, "dag-cbor", {
                pin: true,
                hasher,
            });
            let obj = await store.get(cid1);
            assert.deepStrictEqual(obj, data);

            await store.rm(cid1);
            try {
                await store.get(cid1, { timeout: 3000 });
                fail();
            } catch (error) { }
        });

        it("unpinned", async () => {
            const data: any = { test: "unpinned" };
            const cid1 = await store.put(data, "dag-cbor", {
                pin: false,
                hasher,
            });
            let obj = await store.get(cid1);
            assert.deepStrictEqual(obj, data);

            await store.rm(cid1);
            try {
                await store.get(cid1, { timeout: 3000 });
                fail();
            } catch (error) { }
        });
    });
});
 */
