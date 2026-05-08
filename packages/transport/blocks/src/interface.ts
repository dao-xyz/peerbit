import type { MaybePromise } from "@peerbit/any-store-interface";
import type { Blocks as IBlockStore, GetOptions } from "@peerbit/blocks-interface";
import type { Block } from "multiformats/block";

export type StoreStatus = MaybePromise<
	"open" | "opening" | "closed" | "closing"
>;
export interface BlockStore extends IBlockStore {
	putMany(
		blocks: Array<Uint8Array | { block: Block<any, any, any, any>; cid: string }>,
	): Promise<string[]>;
	getMany(
		cids: string[],
		options?: GetOptions,
	): Promise<Array<Uint8Array | undefined>>;
	rmMany(cids: string[]): Promise<number | void>;
	start(): Promise<void>;
	stop(): Promise<void>;
	status(): StoreStatus;
}
