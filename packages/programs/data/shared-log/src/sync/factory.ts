import type { Cache } from "@peerbit/cache";
import type { Index } from "@peerbit/indexer-interface";
import type { Log } from "@peerbit/log";
import type { NativePeerbitBackbone } from "@peerbit/native-backbone";
import type { RPC } from "@peerbit/rpc";
import type { SharedLogNativeState } from "@peerbit/shared-log-rust";
import type { Numbers } from "../integers.js";
import type { TransportMessage } from "../message.js";
import type { EntryReplicated, ReplicationRangeIndexable } from "../ranges.js";
import type {
	RawExchangeHeadsSender,
	SyncOptions,
	SynchronizerConstructor,
	Syncronizer,
} from "./index.js";
import { RatelessIBLTSynchronizer } from "./rateless-iblt.js";

export type CreateSyncronizerProps<R extends "u32" | "u64"> = {
	// pass-through refs (snapshotted at open() time; stable post-open)
	numbers: Numbers<R>;
	entryIndex: Index<EntryReplicated<R>, any>;
	rangeIndex: Index<ReplicationRangeIndexable<R>, any>;
	log: Log<any>;
	rpc: RPC<TransportMessage, TransportMessage>;
	coordinateToHash: Cache<string>;
	// late-bound callbacks (native state fields are re-assigned across
	// open/close and failure recovery; must be re-read per call)
	getNativeState: () =>
		| NativePeerbitBackbone
		| SharedLogNativeState
		| undefined;
	isEntryRecentlyKnownByPeer: (
		hash: string,
		peer: string,
		maxAgeMs: number,
	) => boolean;
	peerSupportsRawExchangeHeads: (peer: string) => boolean;
	sendRawExchangeHeads: RawExchangeHeadsSender;
	warn: (message: string) => void;
	// construction-time scalars
	resolution: R;
	sync?: SyncOptions<R>;
	syncronizer?: SynchronizerConstructor<R>;
};

export function createSyncronizer<R extends "u32" | "u64">(
	props: CreateSyncronizerProps<R>,
): Syncronizer<R> {
	const resolveHashesForSymbols = (
		symbols: readonly bigint[] | BigUint64Array,
	) => {
		const nativeState = props.getNativeState();
		if (!nativeState) {
			return undefined;
		}
		if (
			typeof BigUint64Array !== "undefined" &&
			typeof nativeState.getEntryHashesForHashNumbersU64 === "function"
		) {
			return nativeState.getEntryHashesForHashNumbersU64(
				symbols instanceof BigUint64Array
					? symbols
					: BigUint64Array.from(symbols),
			);
		}
		return nativeState.getEntryHashesForHashNumbers(symbols);
	};
	const resolveHashListForSymbols = (
		symbols: readonly bigint[] | BigUint64Array,
	) => {
		const nativeState = props.getNativeState();
		if (
			!nativeState ||
			typeof BigUint64Array === "undefined" ||
			typeof nativeState.getEntryHashListForHashNumbersU64 !== "function"
		) {
			return undefined;
		}
		return nativeState.getEntryHashListForHashNumbersU64(
			symbols instanceof BigUint64Array
				? symbols
				: BigUint64Array.from(symbols),
		);
	};
	const resolveHashNumbersInRange = (range: {
		start1: bigint | number;
		end1: bigint | number;
		start2: bigint | number;
		end2: bigint | number;
		limit: number;
	}) => {
		const nativeState = props.getNativeState();
		return (
			nativeState?.getEntryHashNumbersInRangeU64?.(range) ??
			nativeState?.getEntryHashNumbersInRange(range)
		);
	};

	if (props.syncronizer) {
		return new props.syncronizer({
			numbers: props.numbers,
			entryIndex: props.entryIndex,
			log: props.log,
			rangeIndex: props.rangeIndex,
			rpc: props.rpc,
			coordinateToHash: props.coordinateToHash,
			resolveHashesForSymbols,
			resolveHashListForSymbols,
			resolveHashNumbersInRange,
			sync: props.sync,
			isEntryRecentlyKnownByPeer: props.isEntryRecentlyKnownByPeer,
			peerSupportsRawExchangeHeads: props.peerSupportsRawExchangeHeads,
			sendRawExchangeHeads: props.sendRawExchangeHeads,
		});
	}

	// Default synchronizer. SimpleSyncronizer stays a first-class explicit
	// choice via the `syncronizer` option above; only the retired
	// compatibility-coupled defaulting arm that once selected it is gone (B12).
	if (props.resolution === "u32") {
		props.warn(
			"u32 resolution is not recommended for RatelessIBLTSynchronizer",
		);
	}

	return new RatelessIBLTSynchronizer<R>({
		numbers: props.numbers,
		entryIndex: props.entryIndex,
		log: props.log,
		rangeIndex: props.rangeIndex,
		rpc: props.rpc,
		coordinateToHash: props.coordinateToHash,
		resolveHashesForSymbols,
		resolveHashListForSymbols,
		resolveHashNumbersInRange,
		sync: props.sync,
		isEntryRecentlyKnownByPeer: props.isEntryRecentlyKnownByPeer,
		peerSupportsRawExchangeHeads: props.peerSupportsRawExchangeHeads,
		sendRawExchangeHeads: props.sendRawExchangeHeads,
	}) as Syncronizer<R>;
}
