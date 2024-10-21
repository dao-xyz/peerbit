import { type WaitForPeer } from "@peerbit/stream-interface";

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
	put(bytes: Uint8Array): MaybePromise<string>;
	has(cid: string): MaybePromise<boolean>;
	get(cid: string, options?: GetOptions): MaybePromise<Uint8Array | undefined>;
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
