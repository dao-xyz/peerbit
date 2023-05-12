import { Entry } from "./entry";
export type Change<T> = { added: Entry<T>[]; removed: Entry<T>[] };
