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

export type SyncEntryCoordinates<R extends "u32" | "u64"> = Pick<
	EntryReplicated<R>,
	"assignedToRangeBoundary" | "hash" | "hashNumber"
>;

export type SyncOptions<R extends "u32" | "u64"> = {
	/**
	 * Orders entries inside a sync batch; higher numbers are selected first.
	 * This does not change the transport message priority/lane.
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
	 * Experimental sync path for peers known to support raw exchange-head
	 * responses. When enabled, simple sync requests advertise raw-head support
	 * and capable responders can avoid full Entry materialization before sending.
	 */
	rawExchangeHeads?: boolean;

	/**
	 * Experimental receive-side raw-head parsing mode for native backbone. When
	 * enabled, signature verification happens while raw entries are decoded
	 * instead of during the later native commit step. When unset, shared-log may
	 * enable this automatically for retained native receive paths where planning
	 * already indicates that the local peer is replicating.
	 */
	rawExchangeHeadsVerifySignaturesDuringPrepare?: boolean;

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
	 * Native wire receive fusion (requires nativeBackbone). Shared-log
	 * registers its RPC topic with this session so raw exchange-head payloads
	 * recognized by the native wire decoder stay in wasm memory: the TS borsh
	 * decode of the entries and the JS-to-wasm copy of their block bytes are
	 * skipped. Messages without a stash entry fall back to the regular path.
	 */
	nativeWireSync?: SharedLogNativeWireSync;

	/**
	 * Optional profiling callback. It is only invoked when provided, and should
	 * avoid blocking because it runs inside sync hot paths.
	 */
	profile?: SyncProfileFn;
};

/**
 * The per-node native wire-sync session surface shared-log consumes
 * (implemented by `NativeBackboneWireSyncSession` in `@peerbit/native-backbone`;
 * the same object also implements the `nativeWire` option of
 * `@peerbit/stream`'s DirectStream).
 */
export type SharedLogNativeWireSync = {
	/** Raw wasm session handle consumed by `prepareStashedRawReceive*`. */
	handle: unknown;
	registerTopic(topic: string): void;
	unregisterTopic(topic: string): boolean;
	stashedMeta(id: Uint8Array):
		| {
				hashes: string[];
				gidRefrences: string[][];
				byteLengths: Uint32Array;
				reserved: Uint8Array;
				payloadLength: number;
		  }
		| undefined;
	stashedBlocks(
		id: Uint8Array,
		indexes?: Uint32Array,
	): Uint8Array[] | undefined;
	release(id: Uint8Array): boolean;
};

/**
 * Fused raw exchange-heads sender provided by the shared log when the native
 * backbone can serialize the sync payload in wasm (block bytes never
 * materialize in JS). Resolves to the number of messages sent, or `undefined`
 * when the fused path is unavailable for these hashes — the caller then falls
 * back to the TS message path.
 */
export type RawExchangeHeadsSender = (
	hashes: string[],
	to: string[],
	options?: { priority?: number },
) => Promise<number | undefined>;

export type HashSymbolInput = readonly bigint[] | BigUint64Array;

export type HashSymbolResolver = (
	symbols: HashSymbolInput,
) =>
	| ReadonlyMap<bigint, Iterable<string>>
	| undefined
	| Promise<ReadonlyMap<bigint, Iterable<string>> | undefined>;

export type HashSymbolHashListResolver = (
	symbols: HashSymbolInput,
) =>
	| Iterable<string>
	| undefined
	| Promise<Iterable<string> | undefined>;

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
	resolveHashListForSymbols?: HashSymbolHashListResolver;
	resolveHashNumbersInRange?: HashSymbolRangeResolver;
	sync?: SyncOptions<R>;
	isEntryRecentlyKnownByPeer?: (
		hash: string,
		peer: string,
		maxAgeMs: number,
	) => boolean;
	sendRawExchangeHeads?: RawExchangeHeadsSender;
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
		entries: Map<string, SyncEntryCoordinates<R>>;
		targets: string[];
		mode?: RepairSessionMode;
		timeoutMs?: number;
		retryIntervalsMs?: number[];
	}): RepairSession;

	onMaybeMissingEntries(properties: {
		entries: Map<string, SyncEntryCoordinates<R>>;
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

	onReceivedEntryHashes?(properties: {
		hashes: string[];
		from: PublicSignKey;
	}): Promise<void> | void;

	onEntryAddedHashes?(hashes: string[]): void;
	onEntryAddedHash?(hash: string): void;
	onEntryAdded(entry: Entry<any>): void;
	onEntryRemovedHashes?(hashes: string[]): void;
	onEntryRemoved(hash: string): void;
	onPeerDisconnected(key: PublicSignKey | string): void;

	open(): Promise<void> | void;
	close(): Promise<void> | void;

	get pending(): number;

	get syncInFlight(): Map<string, Map<SyncableKey, { timestamp: number }>>;
}
