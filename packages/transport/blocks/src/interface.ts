import type { MaybePromise } from "@peerbit/any-store-interface";
import type { Blocks as IBlockStore, GetOptions } from "@peerbit/blocks-interface";
import type { Block } from "multiformats/block";

export type StoreStatus = MaybePromise<
	"open" | "opening" | "closed" | "closing"
>;
export interface BlockStore extends IBlockStore {
	putMany(
		blocks: Array<Uint8Array | { block: Block<any, any, any, any>; cid: string }>,
	): MaybePromise<string[]>;
	putKnown(cid: string, bytes: Uint8Array): MaybePromise<string>;
	putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): MaybePromise<string[]>;
	getMany(
		cids: string[],
		options?: GetOptions,
	): MaybePromise<Array<Uint8Array | undefined>>;
	hasMany(cids: string[]): MaybePromise<boolean[]>;
	rmMany(cids: string[]): MaybePromise<number | void>;
	getNativeLogBlockStoreHandle?(): unknown;
	start(): Promise<void>;
	stop(): Promise<void>;
	status(): StoreStatus;
}
