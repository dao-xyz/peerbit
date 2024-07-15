import { Entry, type ShallowOrFullEntry } from "./entry.js";
export type Change<T> = { added: Entry<T>[]; removed: ShallowOrFullEntry<T>[] };
