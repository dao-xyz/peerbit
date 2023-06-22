import { Blocks as IBlockStore } from "@peerbit/blocks-interface";

export type StoreStatus = "open" | "opening" | "closed" | "closing";
export interface BlockStore extends IBlockStore {
	start(): Promise<void>;
	stop(): Promise<void>;
	get status(): StoreStatus;
}
