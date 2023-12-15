import { MemoryStore } from "./memory.js";
import { OPFSStore } from "./opfs.js";

export const createStore = (directory?: string) => {
	return directory ? new OPFSStore([directory]) : new MemoryStore();
};

/* export const estimate = (directory: string): Promise<{ quota?: number, usage?: number }> => {
	return navigator.storage.estimate().then(x => { return { quota: x.quota, usage: x.usage } })
} */
