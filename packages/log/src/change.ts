import type { Entry, ShallowOrFullEntry } from "./entry.js";

export type Change<T> = {
	added: { head: boolean; entry: Entry<T> }[];
	removed: ShallowOrFullEntry<T>[];
};
