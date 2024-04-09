import { Entry } from "./entry.js";
export type Change<T> = { added: Entry<T>[]; removed: Entry<T>[] };
