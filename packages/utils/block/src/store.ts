import { cidifyString } from "./block";
import * as dagCbor from "@ipld/dag-cbor";
import * as Block from "multiformats/block";
import type { MultihashHasher } from "multiformats/hashes/hasher";

const unsupportedCodecError = () => new Error("unsupported codec");

export type GetOptions = { timeout?: number; hasher?: MultihashHasher<number> };
export type PutOptions = {
    pin?: boolean;
    format: string;
    timeout?: number;
    hasher?: MultihashHasher<number>;
};

export interface BlockStore {
    get<T>(
        cid: string,
        options?: GetOptions
    ): Promise<Block.Block<T, any, any, any> | undefined>;
    put(
        block: Block.Block<any, any, any, any>,
        options?: PutOptions
    ): Promise<string>;
    rm(cid: string): Promise<void>;
    open(): Promise<void>;
    close(): Promise<void>;
}

export class FallbackBlockStore implements BlockStore {
    _stores: BlockStore[];
    constructor(stores: BlockStore[]) {
        this._stores = stores;
    }

    async get<T>(
        cid: string,
        options?: GetOptions
    ): Promise<Block.Block<T, any, any, any> | undefined> {
        const promises = this._stores.map((s) => s.get<T>(cid, options));
        const result = await Promise.any(promises);
        if (!result) {
            const results = await Promise.all(promises);
            for (const result of results) {
                if (result) {
                    return result;
                }
            }
        }
        return result;
    }

    async put(value: any, options?: PutOptions | undefined): Promise<string> {
        const cids = await Promise.all(
            this._stores.map((x) => x.put(value, options))
        );
        for (let i = 1; i < cids.length; i++) {
            if (cids[i] === cids[0]) {
                throw new Error("Expecting CIDs to be equal");
            }
        }
        return cids[0];
    }
    async rm(cid: string): Promise<void> {
        await Promise.all(this._stores.map((x) => x.rm(cid)));
    }

    async open(): Promise<void> {
        await Promise.all(this._stores.map((s) => s.open()));
    }

    async close(): Promise<void> {
        await Promise.all(this._stores.map((s) => s.close()));
    }
}

export const prepareBlockWrite = (value: any, codec: any, links?: string[]) => {
    if (!codec) throw unsupportedCodecError();
    if (codec.code === dagCbor.code) {
        links = links || [];
        links.forEach((prop) => {
            if (value[prop]) {
                value[prop] = Array.isArray(value[prop])
                    ? value[prop].map(cidifyString)
                    : cidifyString(value[prop]);
            }
        });
    }
    return value;
};

/* async function write(
    saveBlock: (block: Block.Block<any>) => Promise<void> | void,
    format: string,
    value: any,
    options: PutOptions = {}
): Promise<CID> {

    const codec = codecMap[format];
    value = prepareBlockWrite(value, codec, options?.links);
    const block = await Block.encode({ value, codec, hasher });
    await saveBlock(block);
    return block.cid;
}
 */
