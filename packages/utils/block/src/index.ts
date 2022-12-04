import { BlockStore, prepareBlockWrite } from "./store.js";
export * from "./store.js";
import { codecMap, defaultHasher, stringifyCid } from "./block.js";
import * as Block from "multiformats/block";
import * as dagCbor from "@ipld/dag-cbor";
import * as raw from "multiformats/codecs/raw";
import type { MultihashHasher } from "multiformats/hashes/hasher";
export { cidifyString, stringifyCid } from "./block.js";
export const getBlockValue = async <T>(
    block: Block.Block<T, any, any, any>,
    links?: string[]
): Promise<T> => {
    if (block.cid.code === dagCbor.code) {
        const value = block.value as any;
        links = links || [];
        links.forEach((prop) => {
            if (value[prop]) {
                value[prop] = Array.isArray(value[prop])
                    ? value[prop].map(stringifyCid)
                    : stringifyCid(value[prop]);
            }
        });
        return value;
    }
    if (block.cid.code === raw.code) {
        return block.value as T;
    }
    throw new Error("Unsupported");
};

export class Blocks {
    _store: BlockStore;

    constructor(store: BlockStore) {
        this._store = store;
    }

    async get<T>(
        cid: string,
        options?: { links?: string[]; timeout?: number }
    ): Promise<T | undefined> {
        const block = await this._store.get(cid, options);
        if (!block) {
            return;
        }
        return getBlockValue(block, options?.links) as T;
    }

    async block(
        value: any,
        format: string,
        options?: {
            hasher?: MultihashHasher<number>;
            links?: string[];
            pin?: boolean;
        }
    ): Promise<Block.Block<any, any, any, any>> {
        const codec = codecMap[format];
        value = prepareBlockWrite(value, codec, options?.links);
        const block = await Block.encode({
            value,
            codec,
            hasher: options?.hasher || defaultHasher,
        });
        return block as Block.Block<any, any, any, any>;
    }
    async put(
        value: any,
        format: string,
        options?: {
            hasher?: MultihashHasher<number>;
            links?: string[];
            timeout?: number;
            pin?: boolean;
        }
    ): Promise<string> {
        const block = await this.block(value, format, options);
        await this._store.put(block, {
            ...options,
            format,
            hasher: options?.hasher || defaultHasher,
        });
        return stringifyCid(block.cid);
    }

    async rm(cid: string): Promise<void> {
        await this._store.rm(cid);
    }

    async open(): Promise<void> {
        return this._store.open();
    }

    async close(): Promise<void> {
        return this._store.close();
    }
}

export * from "./store.js";
export * from "./level.js";
export * from "./libp2p.js";
