import { MemoryStore } from "./memory.js";
import { OPFSStore } from "./opfs.js";

export const createStore = (directory?: string) => {
	return directory ? new OPFSStore([directory]) : new MemoryStore();
};
