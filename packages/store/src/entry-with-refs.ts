import { Entry } from "@dao-xyz/peerbit-log";

export interface EntryWithRefs<T> {
	entry: Entry<T>;
	references: Entry<T>[];
}
