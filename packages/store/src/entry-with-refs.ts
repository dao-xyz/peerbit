import { Entry } from "@dao-xyz/ipfs-log";

export interface EntryWithRefs<T> {
    entry: Entry<T>;
    references: Entry<T>[];
}
