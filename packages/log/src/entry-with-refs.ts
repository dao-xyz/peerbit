import type { Entry } from "./entry.js";

export interface EntryWithRefs<T> {
	entry: Entry<T>;
	references: Entry<T>[];
}
