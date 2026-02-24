import { type WaitForPeer } from "@peerbit/stream-interface";
import { type Block } from "multiformats/block";

export type GetOptions = {
	remote?:
		| {
				signal?: AbortSignal;
				timeout?: number;
				replicate?: boolean;
				from?: string[];
		  }
		| boolean;
};
export type PutOptions = {
	timeout?: number;
};

type MaybePromise<T> = Promise<T> | T;

export interface Blocks extends WaitForPeer {
	put(
		data: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): MaybePromise<string>;
	has(cid: string): MaybePromise<boolean>;
	get(cid: string, options?: GetOptions): MaybePromise<Uint8Array | undefined>;
	/**
	 * Best-effort provider hints for `get(..., { remote: true })` without explicit `remote.from`.
	 *
	 * Implementations should treat hints as advisory and keep them bounded (LRU/TTL).
	 */
	hintProviders?(cid: string, providers: string[]): void;
	rm(cid: string): MaybePromise<void>;
	iterator(): AsyncGenerator<[string, Uint8Array], void, void>;
	size(): MaybePromise<number>;
	persisted(): MaybePromise<boolean>;
}

export {
	cidifyString,
	stringifyCid,
	createBlock,
	getBlockValue,
	calculateRawCid,
	checkDecodeBlock,
	codecCodes,
	defaultHasher,
	codecMap,
} from "./block.js";
