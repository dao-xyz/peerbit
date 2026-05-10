import type { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import type { Index } from "@peerbit/indexer-interface";
import type { Entry, Log } from "@peerbit/log";
import type { RPC, RequestContext } from "@peerbit/rpc";
import type { EntryWithRefs } from "../exchange-heads.js";
import type { Numbers } from "../integers.js";
import type { TransportMessage } from "../message.js";
import type { EntryReplicated, ReplicationRangeIndexable } from "../ranges.js";
import type { SyncProfileFn } from "./profile.js";

export type { SyncProfileEvent, SyncProfileFn } from "./profile.js";

export type SyncPriorityFn<R extends "u32" | "u64"> = (
	entry: EntryReplicated<R>,
) => number;

export type SyncOptions<R extends "u32" | "u64"> = {
	/**
	 * Higher numbers are synced first.
	 * The callback should be fast and side-effect free.
	 */
	priority?: SyncPriorityFn<R>;

	/**
	 * When using rateless IBLT sync, optionally pre-sync up to this many
	 * high-priority entries using the simple synchronizer.
	 */
	maxSimpleEntries?: number;

	/**
	 * Maximum number of hash strings in one simple sync message.
	 */
	maxSimpleHashesPerMessage?: number;

	/**
	 * Maximum number of coordinates in one simple sync coordinate message.
	 */
	maxSimpleCoordinatesPerMessage?: number;

	/**
	 * Maximum number of hashes tracked per convergent repair session target.
	 * Large sessions still dispatch all entries, but only this many are tracked
	 * for deterministic completion metadata.
	 */
	maxConvergentTrackedHashes?: number;

	/**
	 * Maximum number of candidate entries buffered per target before the
	 * background repair sweep dispatches a maybe-sync batch.
	 * Larger values reduce orchestration overhead but increase per-target memory.
	 */
	repairSweepTargetBufferSize?: number;

	/**
	 * Optional profiling callback. It is only invoked when provided, and should
	 * avoid blocking because it runs inside sync hot paths.
	 */
	profile?: SyncProfileFn;
};

export type HashSymbolResolver = (
	symbols: bigint[],
) =>
	| ReadonlyMap<bigint, Iterable<string>>
	| undefined
	| Promise<ReadonlyMap<bigint, Iterable<string>> | undefined>;

export type HashSymbolRangeResolver = (range: {
	start1: bigint | number;
	end1: bigint | number;
	start2: bigint | number;
	end2: bigint | number;
}) =>
	| Iterable<bigint | number>
	| undefined
	| Promise<Iterable<bigint | number> | undefined>;

export type SynchronizerComponents<R extends "u32" | "u64"> = {
	rpc: RPC<TransportMessage, TransportMessage>;
	rangeIndex: Index<ReplicationRangeIndexable<R>, any>;
	entryIndex: Index<EntryReplicated<R>, any>;
	log: Log<any>;
	coordinateToHash: Cache<string>;
	numbers: Numbers<R>;
	resolveHashesForSymbols?: HashSymbolResolver;
	resolveHashNumbersInRange?: HashSymbolRangeResolver;
	sync?: SyncOptions<R>;
};
export type SynchronizerConstructor<R extends "u32" | "u64"> = new (
	properties: SynchronizerComponents<R>,
) => Syncronizer<R>;

export type SyncableKey = string | bigint; // hash or coordinate

export type RepairSessionMode = "best-effort" | "convergent";

export type RepairSessionResult = {
	target: string;
	requested: number;
	resolved: number;
	unresolved: string[];
	attempts: number;
	durationMs: number;
	completed: boolean;
	requestedTotal?: number;
	truncated?: boolean;
};

export type RepairSession = {
	id: string;
	done: Promise<RepairSessionResult[]>;
	cancel: () => void;
};

export interface Syncronizer<R extends "u32" | "u64"> {
	startRepairSession(properties: {
		entries: Map<string, EntryReplicated<R>>;
		targets: string[];
		mode?: RepairSessionMode;
		timeoutMs?: number;
		retryIntervalsMs?: number[];
	}): RepairSession;

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
	onPeerDisconnected(key: PublicSignKey | string): void;

	open(): Promise<void> | void;
	close(): Promise<void> | void;

	get pending(): number;

	get syncInFlight(): Map<string, Map<SyncableKey, { timestamp: number }>>;
}
