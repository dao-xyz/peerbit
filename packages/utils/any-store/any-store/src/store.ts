import { Level } from "level";
import { LevelStore } from "./level.js";
import { MemoryStore } from "./memory.js";

/* import os from 'os'
import { check } from 'diskusage' */

export const createStore = (directory?: string) => {
	return directory
		? new LevelStore(new Level(directory, { valueEncoding: "view" }) as any) // TODO types? hooks does not seem to be updated
		: new MemoryStore();
};

/* export const estimate = (directory: string): Promise<{ quota?: number, usage?: number }> => {
	return check(directory).then(x => { return { quota: x.total, usage: x.total - x.free } })
} */
