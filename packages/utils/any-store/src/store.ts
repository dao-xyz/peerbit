import { LevelStore } from "./level.js";
import { Level } from "level";
import { MemoryStore } from "./memory.js";

export const createStore = (directory?: string) => {
	return directory
		? new LevelStore(new Level(directory, { valueEncoding: "view" }))
		: new MemoryStore();
};
