import { OPFSStore } from "@peerbit/any-store-opfs/opfs-store";
import { MemoryStore } from "./memory.js";

export const createStore = (directory?: string) => {
	return directory ? new OPFSStore(directory) : new MemoryStore();
};

/* export const estimate = (directory: string): Promise<{ quota?: number, usage?: number }> => {
	return navigator.storage.estimate().then(x => { return { quota: x.quota, usage: x.usage } })
} */
