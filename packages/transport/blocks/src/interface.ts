import { MaybePromise } from "@peerbit/any-store";
import { Blocks as IBlockStore } from "@peerbit/blocks-interface";

export type StoreStatus = MaybePromise<
	"open" | "opening" | "closed" | "closing"
>;
export interface BlockStore extends IBlockStore {
	start(): Promise<void>;
	stop(): Promise<void>;
	status(): StoreStatus;
}
