import { cidifyString } from "./block.js";
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

export type StoreStatus = "open" | "opening" | "closed" | "closing";
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
    idle(): Promise<void>;
    get status(): StoreStatus;
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
