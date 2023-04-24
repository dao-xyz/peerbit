import * as Block from "multiformats/block";
import type { MultihashHasher } from "multiformats/hashes/hasher";

export type GetOptions = {
	timeout?: number;
	replicate?: boolean;
	hasher?: MultihashHasher<number>;
};
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
		options?: PutOptions
	): Promise<string>;
	has(cid: string): Promise<boolean> | boolean;
	rm(cid: string): Promise<void>;
	open(): Promise<this>;
	close(): Promise<void>;
	get status(): StoreStatus;
}
