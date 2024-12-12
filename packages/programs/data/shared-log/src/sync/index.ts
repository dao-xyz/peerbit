import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry } from "@peerbit/log";
import type { Log } from "@peerbit/log";
import type { RequestContext } from "@peerbit/rpc";
import type { RPC } from "@peerbit/rpc";
import type { EntryWithRefs } from "../exchange-heads.js";
import type { TransportMessage } from "../message.js";
import type { EntryReplicated } from "../ranges.js";
import { type ReplicationRangeIndexable } from "../ranges.js";

export type SynchronizerComponents<R extends "u32" | "u64"> = {
	rpc: RPC<TransportMessage, TransportMessage>;
	rangeIndex: Index<ReplicationRangeIndexable<R>, any>;
	entryIndex: Index<EntryReplicated<R>, any>;
	log: Log<any>;
	coordinateToHash: Cache<string>;
};
export type SynchronizerConstructor<R extends "u32" | "u64"> = new (
	properties: SynchronizerComponents<R>,
) => Syncronizer<R>;

export type SyncableKey = string | bigint; // hash or coordinate

export interface Syncronizer<R extends "u32" | "u64"> {
	onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<R>>;
		targets: string[];
	}): Promise<void> | void;

	onMessage(
		message: TransportMessage,
		context: RequestContext,
	): Promise<boolean> | boolean;

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void;

	onEntryAdded(entry: Entry<any>): void;
	onEntryRemoved(hash: string): void;
	onPeerDisconnected(key: PublicSignKey): void;

	open(): Promise<void> | void;
	close(): Promise<void> | void;

	get pending(): number;

	get syncInFlight(): Map<string, Map<SyncableKey, { timestamp: number }>>;
}
