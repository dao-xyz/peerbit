import { cidifyString } from "./block.js";
import * as dagCbor from "@ipld/dag-cbor";
import * as Block from "multiformats/block";
import type { MultihashHasher } from "multiformats/hashes/hasher";

export type GetOptions = { timeout?: number; hasher?: MultihashHasher<number> };
export type PutOptions = {
    timeout?: number;
};

export type StoreStatus = "open" | "opening" | "closed" | "closing";
export interface BlockStore {
    get<T>(
        cid: string,
        options?: GetOptions
    ): Promise<Block.Block<T, any, any, any> | undefined>;
    put<T>(
        value: Block.Block<T, any, any, any>,
        optsions?: PutOptions
    ): Promise<string>;
    rm(cid: string): Promise<void>;
    open(): Promise<void>;
    close(): Promise<void>;
    get status(): StoreStatus;
}

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
