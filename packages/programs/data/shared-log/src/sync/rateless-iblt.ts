import {
	type BinaryReader,
	type BinaryWriter,
	field,
	variant,
} from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { type PublicSignKey, randomBytes, toBase64 } from "@peerbit/crypto";
import {
	And,
	type Index,
	IntegerCompare,
	Or,
	type Query,
} from "@peerbit/indexer-interface";
import type { Entry } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import {
	DecoderWrapper,
	EncoderWrapper,
	ready as ribltReady,
} from "@peerbit/riblt";
import type { RequestContext } from "@peerbit/rpc";
import { SilentDelivery } from "@peerbit/stream-interface";
import { type EntryWithRefs } from "../exchange-heads.js";
import { TransportMessage } from "../message.js";
import { type EntryReplicated } from "../ranges.js";
import type {
	HashSymbolRangeResolver,
	RepairSession,
	RepairSessionMode,
	RepairSessionResult,
	SyncEntryCoordinates,
	SyncableKey,
	SynchronizerComponents,
	Syncronizer,
} from "./index.js";
import {
	type SyncProfileFn,
	emitSyncProfileDuration,
	emitSyncProfileEvent,
	syncProfileStart,
} from "./profile.js";
import {
	ResponseMaybeSync,
	ResponseMaybeSyncCapabilities,
	SYNC_MESSAGE_PRIORITY,
	SimpleSyncronizer,
} from "./simple.js";

export const logger = loggerFn("peerbit:shared-log:rateless");

type NumberOrBigint = number | bigint;

const coerceBigInt = (value: NumberOrBigint): bigint =>
	typeof value === "bigint" ? value : BigInt(value);

export interface SSymbol {
	count: bigint;
	hash: bigint;
	symbol: bigint;
}

class SymbolSerialized implements SSymbol {
	@field({ type: "u64" })
	count: bigint;

	@field({ type: "u64" })
	hash: bigint;

	@field({ type: "u64" })
	symbol: bigint;

	constructor(props: { count: bigint; hash: bigint; symbol: bigint }) {
		this.count = props.count;
		this.hash = props.hash;
		this.symbol = props.symbol;
	}
}

const CODED_SYMBOL_WORDS = 3;
const CODED_SYMBOL_WORD_BYTES = 8;
const CODED_SYMBOL_BYTES = CODED_SYMBOL_WORDS * CODED_SYMBOL_WORD_BYTES;
const MAX_CODED_SYMBOL_BATCH_SIZE = 1_024;
const BIG_UINT64_ARRAY_IS_LITTLE_ENDIAN =
	typeof BigUint64Array !== "undefined" &&
	new Uint8Array(new BigUint64Array([1n]).buffer)[0] === 1;

type CodedSymbol = SSymbol | SymbolSerialized;
type CodedSymbolInput =
	| CodedSymbolBatch
	| readonly CodedSymbol[]
	| BigUint64Array;

const assertValidFlatCodedSymbols = (flat: BigUint64Array) => {
	if (flat.length % CODED_SYMBOL_WORDS !== 0) {
		throw new Error("Invalid RIBLT coded symbol batch");
	}
};

export class CodedSymbolBatch implements Iterable<CodedSymbol> {
	private flat?: BigUint64Array;
	private readonly symbols?: readonly CodedSymbol[];

	private constructor(properties: {
		flat?: BigUint64Array;
		symbols?: readonly CodedSymbol[];
	}) {
		this.flat = properties.flat;
		this.symbols = properties.symbols;
	}

	static from(symbols: CodedSymbolInput): CodedSymbolBatch {
		if (symbols instanceof CodedSymbolBatch) {
			return symbols;
		}

		if (
			typeof BigUint64Array !== "undefined" &&
			symbols instanceof BigUint64Array
		) {
			return CodedSymbolBatch.fromFlat(symbols);
		}

		return CodedSymbolBatch.fromSymbols(symbols as readonly CodedSymbol[]);
	}

	static fromFlat(flat: BigUint64Array): CodedSymbolBatch {
		assertValidFlatCodedSymbols(flat);
		return new CodedSymbolBatch({ flat });
	}

	static fromSymbols(symbols: readonly CodedSymbol[]): CodedSymbolBatch {
		return new CodedSymbolBatch({ symbols });
	}

	get length(): number {
		return this.flat
			? this.flat.length / CODED_SYMBOL_WORDS
			: (this.symbols?.length ?? 0);
	}

	toFlat(): BigUint64Array {
		if (this.flat) {
			return this.flat;
		}

		const symbols = this.symbols ?? [];
		const flat = new BigUint64Array(symbols.length * CODED_SYMBOL_WORDS);
		for (let i = 0; i < symbols.length; i++) {
			const offset = i * CODED_SYMBOL_WORDS;
			const symbol = symbols[i];
			flat[offset] = coerceBigInt(symbol.count);
			flat[offset + 1] = coerceBigInt(symbol.hash);
			flat[offset + 2] = coerceBigInt(symbol.symbol);
		}
		this.flat = flat;
		return flat;
	}

	toSymbols(): SymbolSerialized[] {
		const symbols: SymbolSerialized[] = [];
		for (const symbol of this) {
			symbols.push(
				symbol instanceof SymbolSerialized
					? symbol
					: new SymbolSerialized({
							count: symbol.count,
							hash: symbol.hash,
							symbol: symbol.symbol,
						}),
			);
		}
		return symbols;
	}

	*[Symbol.iterator](): IterableIterator<CodedSymbol> {
		if (this.symbols) {
			yield* this.symbols;
			return;
		}

		const flat = this.flat;
		if (!flat) {
			return;
		}

		for (let i = 0; i < flat.length; i += CODED_SYMBOL_WORDS) {
			yield {
				count: flat[i],
				hash: flat[i + 1],
				symbol: flat[i + 2],
			};
		}
	}
}

const codedSymbolBatchField = {
	serialize: (symbols: CodedSymbolInput, writer: BinaryWriter) => {
		const batch = CodedSymbolBatch.from(symbols);
		const length = batch.length;
		writer.u32(length);
		if (length === 0) {
			return;
		}

		if (typeof BigUint64Array !== "undefined") {
			const flat = batch.toFlat();
			if (BIG_UINT64_ARRAY_IS_LITTLE_ENDIAN) {
				writer.set(
					new Uint8Array(flat.buffer, flat.byteOffset, flat.byteLength),
				);
				return;
			}

			for (let i = 0; i < flat.length; i++) {
				writer.u64(flat[i]);
			}
			return;
		}

		for (const symbol of batch) {
			writer.u64(symbol.count);
			writer.u64(symbol.hash);
			writer.u64(symbol.symbol);
		}
	},
	deserialize: (reader: BinaryReader): CodedSymbolBatch => {
		const length = reader.u32();
		if (length > MAX_CODED_SYMBOL_BATCH_SIZE) {
			throw new Error("RIBLT coded symbol batch exceeds the receiver limit");
		}
		const wordLength = length * CODED_SYMBOL_WORDS;
		const byteLength = length * CODED_SYMBOL_BYTES;

		if (
			typeof BigUint64Array !== "undefined" &&
			BIG_UINT64_ARRAY_IS_LITTLE_ENDIAN
		) {
			if (reader._offset + byteLength > reader._buf.length) {
				throw new Error("Invalid RIBLT coded symbol batch length");
			}
			const bytes = reader.buffer(byteLength);
			const flat = new BigUint64Array(wordLength);
			new Uint8Array(flat.buffer).set(bytes);
			return CodedSymbolBatch.fromFlat(flat);
		}

		const symbols: SymbolSerialized[] = [];
		for (let i = 0; i < length; i++) {
			symbols.push(
				new SymbolSerialized({
					count: reader.u64(),
					hash: reader.u64(),
					symbol: reader.u64(),
				}),
			);
		}
		return CodedSymbolBatch.fromSymbols(symbols);
	},
};

type RibltSymbolAdder = {
	add_symbol: (symbol: bigint) => void;
	add_symbols?: (symbols: BigUint64Array) => void;
};

type BatchEncoderWrapper = EncoderWrapper & {
	produce_next_coded_symbols?: (count: number) => BigUint64Array;
};

type StartSyncEncoderWrapper = EncoderWrapper & {
	add_symbols_sorted_and_find_range?: (
		symbols: BigUint64Array,
		maxValue: bigint,
	) => BigUint64Array;
	add_symbols_sorted_find_range_and_produce?: (
		symbols: BigUint64Array,
		maxValue: bigint,
		count: number,
	) => BigUint64Array;
};

type BatchDecoderWrapper = DecoderWrapper & {
	add_symbols?: (symbols: BigUint64Array) => void;
	add_coded_symbols_and_try_decode?: (symbols: BigUint64Array) => boolean;
	get_remote_symbol_values?: () => BigUint64Array;
};

const addSymbolsToRiblt = (
	target: RibltSymbolAdder,
	symbols: Iterable<NumberOrBigint> | BigUint64Array,
) => {
	if (
		typeof BigUint64Array !== "undefined" &&
		typeof target.add_symbols === "function"
	) {
		target.add_symbols(
			symbols instanceof BigUint64Array
				? symbols
				: BigUint64Array.from(symbols, coerceBigInt),
		);
		return;
	}

	for (const symbol of symbols) {
		target.add_symbol(coerceBigInt(symbol));
	}
};

const produceNextCodedSymbols = (
	encoder: EncoderWrapper,
	count: number,
): CodedSymbolBatch => {
	const produceBatch = (encoder as BatchEncoderWrapper)
		.produce_next_coded_symbols;
	if (typeof BigUint64Array !== "undefined" && produceBatch) {
		return CodedSymbolBatch.fromFlat(produceBatch.call(encoder, count));
	}

	const symbols: SymbolSerialized[] = [];
	for (let i = 0; i < count; i++) {
		symbols.push(new SymbolSerialized(encoder.produce_next_coded_symbol()));
	}
	return CodedSymbolBatch.fromSymbols(symbols);
};

const flatFromCodedSymbols = (symbols: CodedSymbolInput): BigUint64Array => {
	if (
		typeof BigUint64Array !== "undefined" &&
		symbols instanceof BigUint64Array
	) {
		assertValidFlatCodedSymbols(symbols);
		return symbols;
	}
	return CodedSymbolBatch.from(symbols).toFlat();
};

const getRemoteSymbolValues = (decoder: DecoderWrapper): bigint[] => {
	const getBatch = (decoder as BatchDecoderWrapper).get_remote_symbol_values;
	if (typeof BigUint64Array !== "undefined" && getBatch) {
		return Array.from(getBatch.call(decoder));
	}

	const symbols: bigint[] = [];
	for (const missingSymbol of decoder.get_remote_symbols()) {
		symbols.push(coerceBigInt(missingSymbol));
	}
	return symbols;
};

const prepareStartSyncEncoder = (
	coordinates: readonly bigint[],
	maxValue: NumberOrBigint,
	initialSymbolCount: number,
): {
	encoder: EncoderWrapper;
	start: bigint;
	end: bigint;
	initialSymbols?: CodedSymbolBatch;
} => {
	const encoder = new EncoderWrapper();
	let complete = false;
	try {
		const prepareAndProduceNative = (encoder as StartSyncEncoderWrapper)
			.add_symbols_sorted_find_range_and_produce;
		if (typeof BigUint64Array !== "undefined" && prepareAndProduceNative) {
			const prepared = prepareAndProduceNative.call(
				encoder,
				BigUint64Array.from(coordinates),
				coerceBigInt(maxValue),
				initialSymbolCount,
			);
			if (
				prepared.length < 2 ||
				(prepared.length - 2) % CODED_SYMBOL_WORDS !== 0
			) {
				throw new Error("Invalid RIBLT prepared encoder result");
			}
			complete = true;
			return {
				encoder,
				start: prepared[0],
				end: prepared[1],
				initialSymbols: CodedSymbolBatch.fromFlat(prepared.subarray(2)),
			};
		}

		const prepareNative = (encoder as StartSyncEncoderWrapper)
			.add_symbols_sorted_and_find_range;
		if (typeof BigUint64Array !== "undefined" && prepareNative) {
			const range = prepareNative.call(
				encoder,
				BigUint64Array.from(coordinates),
				coerceBigInt(maxValue),
			);
			if (range.length !== 2) {
				throw new Error("Invalid RIBLT range result");
			}
			complete = true;
			return { encoder, start: range[0], end: range[1] };
		}

		let sortedEntries: bigint[] | BigUint64Array;
		if (typeof BigUint64Array !== "undefined") {
			const typed = new BigUint64Array(coordinates.length);
			for (let i = 0; i < coordinates.length; i++) {
				typed[i] = coordinates[i];
			}
			typed.sort();
			sortedEntries = typed;
		} else {
			sortedEntries = [...coordinates].sort((a, b) => {
				if (a > b) {
					return 1;
				} else if (a < b) {
					return -1;
				} else {
					return 0;
				}
			});
		}

		// assume sorted, and find the largest gap
		let largestGap = 0n;
		let largestGapIndex = 0;
		for (let i = 0; i < sortedEntries.length; i++) {
			const current = sortedEntries[i];
			const next = sortedEntries[(i + 1) % sortedEntries.length];
			const gap =
				next >= current
					? next - current
					: coerceBigInt(maxValue) - current + next;
			if (gap > largestGap) {
				largestGap = gap;
				largestGapIndex = i;
			}
		}

		const smallestRangeStartIndex =
			(largestGapIndex + 1) % sortedEntries.length;
		const smallestRangeEndIndex = largestGapIndex; /// === (smallRangeStartIndex + 1) % sortedEntries.length
		let smallestRangeStart = sortedEntries[smallestRangeStartIndex];
		let smallestRangeEnd = sortedEntries[smallestRangeEndIndex];
		let start: bigint, end: bigint;
		if (smallestRangeEnd === smallestRangeStart) {
			start = smallestRangeEnd;
			end = smallestRangeEnd + 1n;
			if (end > maxValue) {
				end = 0n;
			}
		} else {
			start = smallestRangeStart;
			end = smallestRangeEnd;
		}

		addSymbolsToRiblt(encoder, sortedEntries);
		complete = true;
		return { encoder, start, end };
	} finally {
		if (!complete) {
			encoder.free();
		}
	}
};

const getSyncIdString = (message: { syncId: Uint8Array }) => {
	return toBase64(message.syncId);
};

const DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS = 30_000;
const DEFAULT_CONVERGENT_RETRY_INTERVALS_MS = [0, 1_000, 3_000, 7_000];
const DEFAULT_MAX_CONVERGENT_TRACKED_HASHES = 4_096;
const MIN_MORE_SYMBOLS_BATCH_SIZE = 64;
const MAX_MORE_SYMBOLS_BATCH_SIZE = MAX_CODED_SYMBOL_BATCH_SIZE;
const MAX_INCOMING_RATELESS_PROCESSES = 32;
const MAX_INCOMING_RATELESS_PROCESSES_PER_SENDER = 4;
const MAX_INCOMING_RATELESS_QUEUED_BATCHES = 8;
const MAX_INCOMING_RATELESS_SEQUENCE_GAP = BigInt(
	MAX_INCOMING_RATELESS_QUEUED_BATCHES,
);
const MIN_RATELESS_SYMBOL_BUDGET = 4_096;
const MAX_RATELESS_SYMBOL_BUDGET = 262_144;
const RATELESS_SYMBOL_BUDGET_MULTIPLIER = 4;
const INCOMING_RATELESS_IDLE_TIMEOUT_MS = 20_000;
const OUTGOING_RATELESS_IDLE_TIMEOUT_MS = 10_000;
const RATELESS_PROCESS_ABSOLUTE_TIMEOUT_MS = 120_000;
const INCOMING_RATELESS_FALLBACK_GRACE_MS = 5_000;
const MAX_RATELESS_RESPONSE_HASHES_INSPECTED = 10_000;
export const MAX_ACTIVE_RATELESS_RESPONSES_PER_PEER = 4;
export const MAX_ACTIVE_RATELESS_RESPONSES_GLOBAL = 32;

const getIncomingRatelessSymbolBudget = (initialSymbols: number): number =>
	Math.min(
		MAX_RATELESS_SYMBOL_BUDGET,
		Math.max(
			MIN_RATELESS_SYMBOL_BUDGET,
			initialSymbols * initialSymbols * RATELESS_SYMBOL_BUDGET_MULTIPLIER,
		),
	);

const getOutgoingRatelessSymbolBudget = (
	entries: number,
	initialSymbols: number,
): number =>
	Math.min(
		MAX_RATELESS_SYMBOL_BUDGET,
		Math.max(
			MIN_RATELESS_SYMBOL_BUDGET,
			initialSymbols,
			entries * RATELESS_SYMBOL_BUDGET_MULTIPLIER,
		),
	);

type IncomingRatelessProcessResult = boolean | undefined | "fallback-to-simple";

@variant([3, 0])
export class StartSync extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	@field({ type: "u64" })
	start: bigint;

	@field({ type: "u64" })
	end: bigint;

	@field({ type: codedSymbolBatchField })
	symbols: CodedSymbolBatch;

	constructor(props: {
		from: NumberOrBigint;
		to: NumberOrBigint;
		symbols: CodedSymbolInput;
	}) {
		super();
		this.syncId = randomBytes(32);
		this.start = coerceBigInt(props.from);
		this.end = coerceBigInt(props.to);
		this.symbols = CodedSymbolBatch.from(props.symbols);
	}
}

@variant([3, 1])
export class MoreSymbols extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	@field({ type: "u64" })
	seqNo: bigint;

	@field({ type: codedSymbolBatchField })
	symbols: CodedSymbolBatch;

	constructor(props: {
		syncId: Uint8Array;
		lastSeqNo: bigint;
		symbols: CodedSymbolInput;
	}) {
		super();
		this.syncId = props.syncId;
		this.seqNo = props.lastSeqNo + 1n;
		this.symbols = CodedSymbolBatch.from(props.symbols);
	}
}

@variant([3, 2])
export class RequestMoreSymbols extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	@field({ type: "u64" })
	lastSeqNo: bigint;

	constructor(props: { syncId: Uint8Array; lastSeqNo: bigint }) {
		super();
		this.syncId = props.syncId;
		this.lastSeqNo = props.lastSeqNo;
	}
}

@variant([3, 3])
export class RequestAll extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	constructor(props: { syncId: Uint8Array }) {
		super();
		this.syncId = props.syncId;
	}
}

const matchEntriesByHashNumberInRangeQuery = (range: {
	start1: number | bigint;
	end1: number | bigint;
	start2: number | bigint;
	end2: number | bigint;
}): Query => {
	const c1 = new And([
		new IntegerCompare({
			key: "hashNumber",
			compare: "gte",
			value: range.start1,
		}),
		new IntegerCompare({
			key: "hashNumber",
			compare: "lt",
			value: range.end1,
		}),
	]);

	// if range2 has length 0 or range 2 is equal to range 1 only make one query
	if (
		range.start2 === range.end2 ||
		(range.start1 === range.start2 && range.end1 === range.end2)
	) {
		return c1;
	}

	return new Or([
		c1,
		new And([
			new IntegerCompare({
				key: "hashNumber",
				compare: "gte",
				value: range.start2,
			}),
			new IntegerCompare({
				key: "hashNumber",
				compare: "lt",
				value: range.end2,
			}),
		]),
	]);
};

const buildEncoderOrDecoderFromRange = async <
	T extends "encoder" | "decoder",
	E = T extends "encoder" ? EncoderWrapper : DecoderWrapper,
	D extends "u32" | "u64" = "u64",
>(
	ranges: {
		start1: NumberOrBigint;
		end1: NumberOrBigint;
		start2: NumberOrBigint;
		end2: NumberOrBigint;
	},
	entryIndex: Index<EntryReplicated<D>>,
	type: T,
	profile?: SyncProfileFn,
	resolveHashNumbersInRange?: HashSymbolRangeResolver,
): Promise<E | false> => {
	await ribltReady;
	const encoder =
		type === "encoder" ? new EncoderWrapper() : new DecoderWrapper();
	let transferred = false;
	try {
		const rangeQueryStartedAt = syncProfileStart(profile);
		let hashNumbers: Array<bigint | number> | BigUint64Array | undefined;
		let source = "index";
		const resolved = await resolveHashNumbersInRange?.({
			end1: ranges.end1,
			start1: ranges.start1,
			end2: ranges.end2,
			start2: ranges.start2,
		});
		if (resolved) {
			source = "native";
			hashNumbers =
				typeof BigUint64Array !== "undefined" &&
				resolved instanceof BigUint64Array
					? resolved
					: [...resolved];
		} else {
			const entries = await entryIndex
				.iterate(
					{
						// Range sync for IBLT is done in hashNumber space.
						query: matchEntriesByHashNumberInRangeQuery({
							end1: ranges.end1,
							start1: ranges.start1,
							end2: ranges.end2,
							start2: ranges.start2,
						}),
					},
					{
						shape: {
							hash: true,
							hashNumber: true,
						},
					},
				)
				.all();
			hashNumbers = entries.map((entry) => entry.value.hashNumber);
		}
		if (profile) {
			emitSyncProfileDuration(profile, rangeQueryStartedAt, {
				name: "rateless.rangeQuery",
				entries: hashNumbers.length,
				details: { type, source },
			});
		}

		if (hashNumbers.length === 0) {
			return false;
		}

		const addSymbolsStartedAt = syncProfileStart(profile);
		if (
			typeof BigUint64Array !== "undefined" &&
			typeof (encoder as RibltSymbolAdder).add_symbols === "function"
		) {
			const symbols =
				hashNumbers instanceof BigUint64Array
					? hashNumbers
					: BigUint64Array.from(hashNumbers, coerceBigInt);
			addSymbolsToRiblt(encoder as RibltSymbolAdder, symbols);
		} else {
			for (const hashNumber of hashNumbers) {
				encoder.add_symbol(coerceBigInt(hashNumber));
			}
		}
		if (profile) {
			emitSyncProfileDuration(profile, addSymbolsStartedAt, {
				name: "rateless.rangeAddSymbols",
				entries: hashNumbers.length,
				symbols: hashNumbers.length,
				details: { type },
			});
		}
		transferred = true;
		return encoder as E;
	} finally {
		if (!transferred) {
			encoder.free();
		}
	}
};

type RatelessDispatchLifecycle = {
	ownershipLifecycleController: AbortController;
	callerSignal?: AbortSignal;
	controller: AbortController;
	targets: Map<string, RatelessDispatchTargetLifecycle>;
	onOwnerOrCallerAbort: () => void;
	dispatchFinished: boolean;
	disposed: boolean;
};

type RatelessDispatchTargetLifecycle = {
	lifecycle: RatelessDispatchLifecycle;
	target: string;
	controller: AbortController;
	retainedByProcess: boolean;
	responseLeases: number;
};

type OutgoingRatelessSyncProcess = {
	target: string;
	targetLifecycle: RatelessDispatchTargetLifecycle;
	outgoingHashes: string[];
	authorizedHashes: ReadonlySet<string>;
	consumedResponseHashes: Set<string>;
	encoder: EncoderWrapper;
	timeout?: ReturnType<typeof setTimeout>;
	deadlineTimeout?: ReturnType<typeof setTimeout>;
	refresh: () => void;
	next: (message: {
		lastSeqNo: bigint;
	}) => { symbols: CodedSymbolBatch; exhaustedAfterSend: boolean } | undefined;
	startSimpleFallback: () => Promise<void>;
	simpleFallbackStarted: boolean;
	free: (reason?: unknown) => void;
	processController: AbortController;
	signal: AbortSignal;
	callerSignal?: AbortSignal;
};

type AuthorizedRatelessResponseLease = {
	process: OutgoingRatelessSyncProcess;
	authorized: string[];
	remaining: string[];
	signal: AbortSignal;
	release: (options?: { rollback?: boolean }) => void;
};

type IncomingRatelessSyncProcess = {
	key: string;
	syncId: string;
	sender: string;
	from: PublicSignKey;
	ownershipLifecycleController: AbortController;
	controller: AbortController;
	decoder?: DecoderWrapper;
	completedSynchronizations: Cache<string>;
	timeout?: ReturnType<typeof setTimeout>;
	deadlineTimeout?: ReturnType<typeof setTimeout>;
	refresh: () => void;
	lastSeqNo: bigint;
	processedSymbols: number;
	queuedSymbols: number;
	symbolBudget: number;
	process: (message: {
		seqNo: bigint;
		symbols: CodedSymbolInput;
	}) => Promise<IncomingRatelessProcessResult>;
	requestAll: () => Promise<void>;
	fallbackToSimple: (reason?: unknown) => Promise<void>;
	free: (reason?: unknown) => void;
	complete: () => void;
};

type IncomingRatelessProcessAdmission = {
	key: string;
	sender: string;
};

type RatelessRepairSessionLifecycle = {
	id: string;
	ownershipLifecycleController: AbortController;
	controller: AbortController;
	trackedSession: RepairSession;
	onOwnershipAbort: () => void;
	cancelled: boolean;
	settled: boolean;
	deadlineTimer?: ReturnType<typeof setTimeout>;
};

export class RatelessIBLTSynchronizer<D extends "u32" | "u64">
	implements Syncronizer<D>
{
	simple: SimpleSyncronizer<D>;
	private repairSessionCounter: number;
	private ratelessRepairSessions: Map<string, RatelessRepairSessionLifecycle>;

	startedOrCompletedSynchronizations: Cache<string>;
	private localRangeEncoderCacheVersion = 0;
	private localRangeEncoderCache: Map<
		string,
		{ encoder: EncoderWrapper; version: number; lastUsed: number }
	> = new Map();
	private localRangeEncoderCacheMax = 2;

	ingoingSyncProcesses: Map<string, IncomingRatelessSyncProcess>;
	private incomingRatelessProcessAdmissions: Set<IncomingRatelessProcessAdmission>;

	outgoingSyncProcesses: Map<string, OutgoingRatelessSyncProcess>;
	private outgoingSyncProcessByTarget: Map<string, OutgoingRatelessSyncProcess>;
	private ratelessDispatchLifecycleController: AbortController;
	private ratelessDispatchTargets: Map<
		string,
		Set<RatelessDispatchTargetLifecycle>
	>;
	private activeRatelessResponseCount: number;
	private activeRatelessResponseCountByPeer: Map<string, number>;
	private ratelessClosed: boolean;

	constructor(readonly properties: SynchronizerComponents<D>) {
		this.simple = new SimpleSyncronizer(properties);
		this.repairSessionCounter = 0;
		this.ratelessRepairSessions = new Map();
		this.outgoingSyncProcesses = new Map();
		this.outgoingSyncProcessByTarget = new Map();
		this.ratelessDispatchLifecycleController = new AbortController();
		this.ratelessDispatchTargets = new Map();
		this.activeRatelessResponseCount = 0;
		this.activeRatelessResponseCountByPeer = new Map();
		this.ratelessClosed = false;
		this.ingoingSyncProcesses = new Map();
		this.incomingRatelessProcessAdmissions = new Set();
		this.startedOrCompletedSynchronizations = new Cache({ max: 1e4 });
	}

	private get maxConvergentTrackedHashes() {
		const value = this.properties.sync?.maxConvergentTrackedHashes;
		return value && Number.isFinite(value) && value > 0
			? Math.max(1, Math.floor(value))
			: DEFAULT_MAX_CONVERGENT_TRACKED_HASHES;
	}

	private normalizeRetryIntervals(retryIntervalsMs?: number[]): number[] {
		if (!retryIntervalsMs || retryIntervalsMs.length === 0) {
			return [...DEFAULT_CONVERGENT_RETRY_INTERVALS_MS];
		}

		return [...retryIntervalsMs]
			.map((x) => Math.max(0, Math.floor(x)))
			.filter((x, i, arr) => arr.indexOf(x) === i)
			.sort((a, b) => a - b);
	}

	private getPrioritizedEntries(entries: Map<string, SyncEntryCoordinates<D>>) {
		const priorityFn = this.properties.sync?.priority;
		if (!priorityFn) {
			return [...entries.values()];
		}

		let index = 0;
		const scored: {
			entry: SyncEntryCoordinates<D>;
			index: number;
			priority: number;
		}[] = [];
		for (const entry of entries.values()) {
			const priorityValue = priorityFn(entry as EntryReplicated<D>);
			scored.push({
				entry,
				index,
				priority: Number.isFinite(priorityValue) ? priorityValue : 0,
			});
			index += 1;
		}
		scored.sort((a, b) => b.priority - a.priority || a.index - b.index);
		return scored.map((x) => x.entry);
	}

	private isRatelessRepairSessionActive(
		session: RatelessRepairSessionLifecycle,
	): boolean {
		return (
			!this.ratelessClosed &&
			!session.cancelled &&
			!session.controller.signal.aborted &&
			!session.ownershipLifecycleController.signal.aborted &&
			session.ownershipLifecycleController ===
				this.ratelessDispatchLifecycleController &&
			this.ratelessRepairSessions.get(session.id) === session
		);
	}

	private cancelRatelessRepairSession(
		session: RatelessRepairSessionLifecycle,
		reason: unknown = new Error("rateless repair session cancelled"),
	): void {
		if (session.cancelled) {
			return;
		}
		session.cancelled = true;
		if (session.deadlineTimer !== undefined) {
			clearTimeout(session.deadlineTimer);
			session.deadlineTimer = undefined;
		}
		session.controller.abort(reason);
		session.trackedSession.cancel();
	}

	private disposeRatelessRepairSession(
		session: RatelessRepairSessionLifecycle,
	): void {
		if (session.settled) {
			return;
		}
		session.settled = true;
		if (session.deadlineTimer !== undefined) {
			clearTimeout(session.deadlineTimer);
			session.deadlineTimer = undefined;
		}
		session.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			session.onOwnershipAbort,
		);
		if (this.ratelessRepairSessions.get(session.id) === session) {
			this.ratelessRepairSessions.delete(session.id);
		}
	}

	private cancelRatelessRepairSessions(reason: unknown): void {
		for (const session of [...this.ratelessRepairSessions.values()]) {
			this.cancelRatelessRepairSession(session, reason);
		}
	}

	private waitForRatelessRepairRetry(
		delayMs: number,
		signal: AbortSignal,
	): Promise<boolean> {
		if (delayMs <= 0) {
			return Promise.resolve(!signal.aborted);
		}
		if (signal.aborted) {
			return Promise.resolve(false);
		}
		return new Promise<boolean>((resolve) => {
			let settled = false;
			let timer: ReturnType<typeof setTimeout>;
			const settle = (elapsed: boolean) => {
				if (settled) {
					return;
				}
				settled = true;
				clearTimeout(timer);
				signal.removeEventListener("abort", onAbort);
				resolve(elapsed);
			};
			const onAbort = () => settle(false);
			timer = setTimeout(() => settle(true), delayMs);
			timer.unref?.();
			signal.addEventListener("abort", onAbort, { once: true });
			if (signal.aborted) {
				onAbort();
			}
		});
	}

	private waitForRatelessRepairDispatch(
		dispatch: Promise<void> | void,
		signal: AbortSignal,
	): Promise<boolean> {
		const dispatchPromise = Promise.resolve(dispatch);
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const settle = (completed: boolean) => {
				if (settled) {
					return;
				}
				settled = true;
				signal.removeEventListener("abort", onAbort);
				resolve(completed);
			};
			const onAbort = () => settle(false);
			signal.addEventListener("abort", onAbort, { once: true });
			void dispatchPromise.then(
				() => settle(true),
				() => settle(true),
			);
			if (signal.aborted) {
				onAbort();
			}
		});
	}

	startRepairSession(properties: {
		entries: Map<string, SyncEntryCoordinates<D>>;
		targets: string[];
		mode?: RepairSessionMode;
		timeoutMs?: number;
		retryIntervalsMs?: number[];
	}): RepairSession {
		const mode = properties.mode ?? "best-effort";
		const targets = [...new Set(properties.targets)];
		const timeoutMs = Math.max(
			1,
			Math.floor(properties.timeoutMs ?? DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS),
		);
		const retryIntervalsMs = this.normalizeRetryIntervals(
			properties.retryIntervalsMs,
		);
		const trackedLimit = this.maxConvergentTrackedHashes;
		const requestedHashes = [...properties.entries.keys()];
		const requestedHashesTracked = requestedHashes.slice(0, trackedLimit);
		const truncated = requestedHashesTracked.length < requestedHashes.length;

		if (mode === "convergent") {
			if (properties.entries.size <= trackedLimit) {
				return this.simple.startRepairSession({
					...properties,
					mode: "convergent",
					timeoutMs,
					retryIntervalsMs,
				});
			}

			const id = `rateless-repair-${++this.repairSessionCounter}`;
			const startedAt = Date.now();
			const prioritized = this.getPrioritizedEntries(properties.entries);
			const trackedEntries = new Map<string, SyncEntryCoordinates<D>>();
			for (const entry of prioritized.slice(0, trackedLimit)) {
				trackedEntries.set(entry.hash, entry);
			}

			const trackedSession = this.simple.startRepairSession({
				entries: trackedEntries,
				targets,
				mode: "convergent",
				timeoutMs,
				retryIntervalsMs,
			});
			const ownershipLifecycleController =
				this.ratelessDispatchLifecycleController;
			let session!: RatelessRepairSessionLifecycle;
			const cancel = (
				reason: unknown = new Error("rateless repair session cancelled"),
			) => this.cancelRatelessRepairSession(session, reason);
			session = {
				id,
				ownershipLifecycleController,
				controller: new AbortController(),
				trackedSession,
				onOwnershipAbort: () =>
					cancel(
						ownershipLifecycleController.signal.reason ??
							new Error("rateless repair generation ended"),
					),
				cancelled: false,
				settled: false,
				deadlineTimer: undefined,
			};
			this.ratelessRepairSessions.set(id, session);
			session.deadlineTimer = setTimeout(
				() => cancel(new Error("rateless convergent repair session timed out")),
				Math.max(0, timeoutMs - (Date.now() - startedAt)),
			);
			session.deadlineTimer.unref?.();
			ownershipLifecycleController.signal.addEventListener(
				"abort",
				session.onOwnershipAbort,
				{ once: true },
			);
			if (
				this.ratelessClosed ||
				ownershipLifecycleController.signal.aborted ||
				ownershipLifecycleController !==
					this.ratelessDispatchLifecycleController
			) {
				cancel(
					ownershipLifecycleController.signal.reason ??
						new Error("rateless repair generation is not active"),
				);
			}

			const runDispatchSchedule = async () => {
				let previousDelay = 0;
				for (const delayMs of retryIntervalsMs) {
					if (!this.isRatelessRepairSessionActive(session)) {
						return;
					}
					const elapsed = Date.now() - startedAt;
					if (elapsed >= timeoutMs) {
						return;
					}
					const waitMs = Math.min(
						Math.max(0, delayMs - previousDelay),
						timeoutMs - elapsed,
					);
					previousDelay = delayMs;
					if (
						!(await this.waitForRatelessRepairRetry(
							waitMs,
							session.controller.signal,
						))
					) {
						return;
					}
					if (
						!this.isRatelessRepairSessionActive(session) ||
						Date.now() - startedAt >= timeoutMs
					) {
						return;
					}
					try {
						if (
							!(await this.waitForRatelessRepairDispatch(
								this.onMaybeMissingEntries({
									entries: properties.entries,
									targets,
									signal: session.controller.signal,
								}),
								session.controller.signal,
							))
						) {
							return;
						}
					} catch {
						// Best-effort schedule: tracked session timeout/result decides completion.
					}
				}
			};

			const done = (async (): Promise<RepairSessionResult[]> => {
				try {
					await runDispatchSchedule();
					const trackedResults = await trackedSession.done;
					return trackedResults.map((result) => ({
						...result,
						requestedTotal: requestedHashes.length,
						truncated: true,
					}));
				} finally {
					this.disposeRatelessRepairSession(session);
				}
			})();

			return {
				id,
				done,
				cancel: () => cancel(),
			};
		}

		const id = `rateless-repair-${++this.repairSessionCounter}`;
		const startedAt = Date.now();
		const done = (async (): Promise<RepairSessionResult[]> => {
			await this.onMaybeMissingEntries({
				entries: properties.entries,
				targets,
			});
			const durationMs = Date.now() - startedAt;
			return targets.map((target) => ({
				target,
				requested: requestedHashesTracked.length,
				resolved: 0,
				unresolved: [...requestedHashesTracked],
				attempts: 1,
				durationMs,
				completed: false,
				requestedTotal: requestedHashes.length,
				truncated,
			}));
		})();
		return {
			id,
			done,
			cancel: () => {
				// no-op: best-effort dispatch does not maintain cancelable session state
			},
		};
	}

	private clearLocalRangeEncoderCache() {
		for (const [, cached] of this.localRangeEncoderCache) {
			cached.encoder.free();
		}
		this.localRangeEncoderCache.clear();
	}

	private invalidateLocalRangeEncoderCache() {
		this.localRangeEncoderCacheVersion += 1;
		this.clearLocalRangeEncoderCache();
	}

	private localRangeEncoderCacheKey(ranges: {
		start1: NumberOrBigint;
		end1: NumberOrBigint;
		start2: NumberOrBigint;
		end2: NumberOrBigint;
	}) {
		return `${String(ranges.start1)}:${String(ranges.end1)}:${String(
			ranges.start2,
		)}:${String(ranges.end2)}`;
	}

	private decoderFromCachedEncoder(
		encoder: EncoderWrapper,
		beforeDecoderTransfer?: () => void,
	): DecoderWrapper {
		const clone = encoder.clone();
		let cloneFreed = false;
		let decoder: DecoderWrapper | undefined;
		let decoderTransferred = false;
		const freeClone = () => {
			if (cloneFreed) {
				return;
			}
			cloneFreed = true;
			clone.free();
		};
		try {
			decoder = clone.to_decoder();
			beforeDecoderTransfer?.();
			freeClone();
			decoderTransferred = true;
			return decoder;
		} finally {
			try {
				freeClone();
			} finally {
				if (!decoderTransferred) {
					decoder?.free();
				}
			}
		}
	}

	private async getLocalDecoderForRange(
		ranges: {
			start1: NumberOrBigint;
			end1: NumberOrBigint;
			start2: NumberOrBigint;
			end2: NumberOrBigint;
		},
		options?: {
			ownershipLifecycleController?: AbortController;
			signal?: AbortSignal;
		},
	): Promise<DecoderWrapper | false> {
		const isActive = () =>
			options?.signal?.aborted !== true &&
			(options?.ownershipLifecycleController === undefined ||
				this.isIncomingSyncGenerationActive(
					options.ownershipLifecycleController,
				));
		if (!isActive()) {
			return false;
		}
		const profile = this.properties.sync?.profile;
		const key = this.localRangeEncoderCacheKey(ranges);
		const cached = this.localRangeEncoderCache.get(key);
		if (cached && cached.version === this.localRangeEncoderCacheVersion) {
			const startedAt = syncProfileStart(profile);
			cached.lastUsed = Date.now();
			return this.decoderFromCachedEncoder(cached.encoder, () => {
				if (profile) {
					emitSyncProfileDuration(profile, startedAt, {
						name: "rateless.localDecoder",
						cacheHit: true,
					});
				}
			});
		}

		const startedAt = syncProfileStart(profile);
		const cacheVersion = this.localRangeEncoderCacheVersion;
		const encoder = (await buildEncoderOrDecoderFromRange(
			ranges,
			this.properties.entryIndex,
			"encoder",
			profile,
			this.properties.resolveHashNumbersInRange,
		)) as EncoderWrapper | false;
		if (!encoder) {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "rateless.localDecoder",
					cacheHit: false,
					entries: 0,
				});
			}
			return false;
		}
		if (!isActive()) {
			encoder.free();
			return false;
		}
		if (cacheVersion !== this.localRangeEncoderCacheVersion) {
			try {
				return this.decoderFromCachedEncoder(encoder, () => {
					if (profile) {
						emitSyncProfileDuration(profile, startedAt, {
							name: "rateless.localDecoder",
							cacheHit: false,
							details: { cacheInvalidated: true },
						});
					}
				});
			} finally {
				encoder.free();
			}
		}

		const now = Date.now();
		const existing = this.localRangeEncoderCache.get(key);
		if (existing) {
			existing.encoder.free();
		}
		this.localRangeEncoderCache.set(key, {
			encoder,
			version: this.localRangeEncoderCacheVersion,
			lastUsed: now,
		});

		while (this.localRangeEncoderCache.size > this.localRangeEncoderCacheMax) {
			let oldestKey: string | undefined;
			let oldestUsed = Number.POSITIVE_INFINITY;
			for (const [candidateKey, value] of this.localRangeEncoderCache) {
				if (value.lastUsed < oldestUsed) {
					oldestUsed = value.lastUsed;
					oldestKey = candidateKey;
				}
			}
			if (!oldestKey) {
				break;
			}
			const victim = this.localRangeEncoderCache.get(oldestKey);
			if (victim) {
				victim.encoder.free();
			}
			this.localRangeEncoderCache.delete(oldestKey);
		}

		return this.decoderFromCachedEncoder(encoder, () => {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "rateless.localDecoder",
					cacheHit: false,
				});
			}
		});
	}

	private captureRatelessDispatchLifecycle(
		targets: string[],
		callerSignal?: AbortSignal,
	): RatelessDispatchLifecycle {
		const ownershipLifecycleController =
			this.ratelessDispatchLifecycleController;
		const lifecycle = {
			ownershipLifecycleController,
			callerSignal,
			controller: new AbortController(),
			targets: new Map<string, RatelessDispatchTargetLifecycle>(),
			onOwnerOrCallerAbort: () => {
				const reason =
					callerSignal?.aborted === true
						? callerSignal.reason
						: ownershipLifecycleController.signal.reason;
				this.abortRatelessDispatchLifecycle(lifecycle, reason);
			},
			dispatchFinished: false,
			disposed: false,
		} satisfies RatelessDispatchLifecycle;

		for (const target of [...new Set(targets)]) {
			const targetLifecycle: RatelessDispatchTargetLifecycle = {
				lifecycle,
				target,
				controller: new AbortController(),
				retainedByProcess: false,
				responseLeases: 0,
			};
			lifecycle.targets.set(target, targetLifecycle);
			let activeForTarget = this.ratelessDispatchTargets.get(target);
			if (!activeForTarget) {
				activeForTarget = new Set();
				this.ratelessDispatchTargets.set(target, activeForTarget);
			}
			activeForTarget.add(targetLifecycle);
		}

		ownershipLifecycleController.signal.addEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
			{ once: true },
		);
		if (callerSignal && callerSignal !== ownershipLifecycleController.signal) {
			callerSignal.addEventListener("abort", lifecycle.onOwnerOrCallerAbort, {
				once: true,
			});
		}
		if (
			this.ratelessClosed ||
			ownershipLifecycleController !==
				this.ratelessDispatchLifecycleController ||
			ownershipLifecycleController.signal.aborted ||
			callerSignal?.aborted
		) {
			lifecycle.onOwnerOrCallerAbort();
		}
		return lifecycle;
	}

	private abortRatelessDispatchTarget(
		targetLifecycle: RatelessDispatchTargetLifecycle,
		reason?: unknown,
	): void {
		if (!targetLifecycle.controller.signal.aborted) {
			targetLifecycle.controller.abort(reason);
		}
		this.maybeDisposeRatelessDispatchLifecycle(targetLifecycle.lifecycle);
	}

	private abortRatelessDispatchLifecycle(
		lifecycle: RatelessDispatchLifecycle,
		reason?: unknown,
	): void {
		if (!lifecycle.controller.signal.aborted) {
			lifecycle.controller.abort(reason);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			this.abortRatelessDispatchTarget(targetLifecycle, reason);
		}
		this.maybeDisposeRatelessDispatchLifecycle(lifecycle);
	}

	private finishRatelessDispatchLifecycle(
		lifecycle: RatelessDispatchLifecycle,
	): void {
		lifecycle.dispatchFinished = true;
		this.maybeDisposeRatelessDispatchLifecycle(lifecycle);
	}

	private maybeDisposeRatelessDispatchLifecycle(
		lifecycle: RatelessDispatchLifecycle,
	): void {
		if (
			lifecycle.disposed ||
			!lifecycle.dispatchFinished ||
			[...lifecycle.targets.values()].some(
				(target) => target.retainedByProcess || target.responseLeases > 0,
			)
		) {
			return;
		}
		lifecycle.disposed = true;
		lifecycle.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
		);
		if (
			lifecycle.callerSignal &&
			lifecycle.callerSignal !== lifecycle.ownershipLifecycleController.signal
		) {
			lifecycle.callerSignal.removeEventListener(
				"abort",
				lifecycle.onOwnerOrCallerAbort,
			);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			const activeForTarget = this.ratelessDispatchTargets.get(
				targetLifecycle.target,
			);
			activeForTarget?.delete(targetLifecycle);
			if (activeForTarget?.size === 0) {
				this.ratelessDispatchTargets.delete(targetLifecycle.target);
			}
		}
	}

	private isRatelessDispatchLifecycleActive(
		lifecycle: RatelessDispatchLifecycle,
		target?: string,
	): boolean {
		if (
			this.ratelessClosed ||
			lifecycle.disposed ||
			lifecycle.ownershipLifecycleController !==
				this.ratelessDispatchLifecycleController ||
			lifecycle.ownershipLifecycleController.signal.aborted ||
			lifecycle.callerSignal?.aborted ||
			lifecycle.controller.signal.aborted
		) {
			return false;
		}
		if (target === undefined) {
			return true;
		}
		const targetLifecycle = lifecycle.targets.get(target);
		return (
			targetLifecycle !== undefined &&
			!targetLifecycle.controller.signal.aborted &&
			this.ratelessDispatchTargets.get(target)?.has(targetLifecycle) === true
		);
	}

	private getIncomingSyncProcessKey(sender: string, syncId: string): string {
		return `${sender.length}:${sender}${syncId}`;
	}

	private isIncomingSyncGenerationActive(
		ownershipLifecycleController: AbortController,
	): boolean {
		return (
			!this.ratelessClosed &&
			ownershipLifecycleController ===
				this.ratelessDispatchLifecycleController &&
			!ownershipLifecycleController.signal.aborted
		);
	}

	private isIncomingSyncProcessActive(
		process: IncomingRatelessSyncProcess,
	): boolean {
		return (
			this.isIncomingSyncGenerationActive(
				process.ownershipLifecycleController,
			) &&
			!process.controller.signal.aborted &&
			this.ingoingSyncProcesses.get(process.key) === process
		);
	}

	async onMaybeMissingEntries(properties: {
		entries: Map<string, SyncEntryCoordinates<D>>;
		targets: string[];
		signal?: AbortSignal;
	}): Promise<void> {
		// Capture this rateless open generation synchronously, before any await.
		// In particular, signal-less internal repair work must not resume after
		// close/open and borrow the newly opened Simple generation.
		const lifecycle = this.captureRatelessDispatchLifecycle(
			properties.targets,
			properties.signal,
		);
		try {
			await this.onMaybeMissingEntriesWithLifecycle(properties, lifecycle);
		} finally {
			this.finishRatelessDispatchLifecycle(lifecycle);
		}
	}

	private async onMaybeMissingEntriesWithLifecycle(
		properties: {
			entries: Map<string, SyncEntryCoordinates<D>>;
			targets: string[];
			signal?: AbortSignal;
		},
		lifecycle: RatelessDispatchLifecycle,
	): Promise<void> {
		const signal = lifecycle.controller.signal;
		const profile = this.properties.sync?.profile;
		const startedAt = syncProfileStart(profile);
		let topLevelProfileEmitted = false;
		const emitTopLevelProfile = (
			details: Record<string, string | number | boolean | undefined>,
			measurements: { messages?: number; symbols?: number } = {},
		) => {
			if (!profile || topLevelProfileEmitted) {
				return;
			}
			topLevelProfileEmitted = true;
			emitSyncProfileDuration(profile, startedAt, {
				name: "rateless.onMaybeMissingEntries",
				entries: properties.entries.size,
				targets: properties.targets.length,
				...measurements,
				details,
			});
		};
		const emitCancelledTopLevelProfile = (
			phase: string,
			mode: "simple-small" | "rateless" = "rateless",
		) =>
			emitTopLevelProfile(
				{
					mode,
					phase,
					cancelled: true,
				},
				{ messages: 0, symbols: 0 },
			);
		// NOTE: this method is best-effort dispatch, not a per-hash convergence API.
		// It may require follow-up repair rounds under churn/loss to fully close all gaps.
		// Strategy:
		// - For small sets, prefer the simple synchronizer to reduce complexity and avoid
		//   IBLT overhead on tiny batches.
		// - For large sets, use IBLT, but still allow simple sync for special-case entries
		//   such as those assigned to range boundaries.

		let minSyncIbltSize = 333; // TODO: make configurable
		let maxSyncWithSimpleMethod = 1e3;
		if (signal?.aborted) {
			emitCancelledTopLevelProfile(
				"before-dispatch",
				properties.entries.size <= minSyncIbltSize
					? "simple-small"
					: "rateless",
			);
			return;
		}
		const priorityFn = this.properties.sync?.priority;
		const maxSimpleEntries = this.properties.sync?.maxSimpleEntries;

		// Small batch => use simple synchronizer entirely
		if (properties.entries.size <= minSyncIbltSize) {
			if (profile) {
				emitSyncProfileEvent(profile, {
					name: "rateless.dispatchMode",
					entries: properties.entries.size,
					targets: properties.targets.length,
					details: { mode: "simple-small" },
				});
			}
			try {
				await this.simple.onMaybeMissingEntries({
					entries: properties.entries,
					targets: properties.targets,
					signal,
				});
				if (signal?.aborted) {
					return;
				}
			} finally {
				emitTopLevelProfile({
					mode: "simple-small",
					cancelled: signal?.aborted || undefined,
				});
			}
			return;
		}

		const selectStartedAt = syncProfileStart(profile);
		const naiveHashes: string[] = [];
		const naiveHashSet = new Set<string>();
		let naiveEntriesForPriority:
			| Map<string, SyncEntryCoordinates<D>>
			| undefined;
		const maxAdditionalNaive =
			priorityFn &&
			typeof maxSimpleEntries === "number" &&
			Number.isFinite(maxSimpleEntries) &&
			maxSimpleEntries > 0
				? Math.max(
						0,
						Math.min(Math.floor(maxSimpleEntries), maxSyncWithSimpleMethod),
					)
				: 0;
		const collectPriorityEntries = priorityFn != null && maxAdditionalNaive > 0;
		const nonBoundaryEntries: SyncEntryCoordinates<D>[] = [];
		let allCoordinatesToSyncWithIblt: bigint[] = [];
		const addNaiveEntry = (entry: SyncEntryCoordinates<D>) => {
			if (naiveHashSet.has(entry.hash)) {
				return;
			}
			naiveHashSet.add(entry.hash);
			naiveHashes.push(entry.hash);
			if (priorityFn) {
				naiveEntriesForPriority ??= new Map();
				naiveEntriesForPriority.set(entry.hash, entry);
			}
		};

		for (const entry of properties.entries.values()) {
			const coordinate = coerceBigInt(entry.hashNumber);
			if (entry.assignedToRangeBoundary) {
				addNaiveEntry(entry);
			} else if (collectPriorityEntries) {
				nonBoundaryEntries.push(entry);
			} else {
				allCoordinatesToSyncWithIblt.push(coordinate);
			}
		}

		if (collectPriorityEntries && nonBoundaryEntries.length > 0) {
			let index = 0;
			const scored: {
				entry: SyncEntryCoordinates<D>;
				index: number;
				priority: number;
			}[] = [];
			for (const entry of nonBoundaryEntries) {
				const priorityValue = priorityFn(entry as EntryReplicated<D>);
				scored.push({
					entry,
					index,
					priority: Number.isFinite(priorityValue) ? priorityValue : 0,
				});
				index += 1;
			}
			scored.sort((a, b) => b.priority - a.priority || a.index - b.index);
			const additionalLimit = Math.max(
				0,
				Math.min(
					maxAdditionalNaive,
					maxSyncWithSimpleMethod - naiveHashes.length,
				),
			);
			for (const { entry } of scored.slice(0, additionalLimit)) {
				addNaiveEntry(entry);
			}
			allCoordinatesToSyncWithIblt = [];
			for (const entry of properties.entries.values()) {
				if (!naiveHashSet.has(entry.hash)) {
					allCoordinatesToSyncWithIblt.push(coerceBigInt(entry.hashNumber));
				}
			}
		}

		const useAllCoordinatesForIblt =
			allCoordinatesToSyncWithIblt.length === 0 ||
			naiveHashes.length > maxSyncWithSimpleMethod;
		if (useAllCoordinatesForIblt) {
			// If every entry is a range-boundary special case, or the special-case
			// set itself is too large for the Simple prelude, use one bounded
			// Rateless process for the full set. Sending the oversized Simple
			// prelude first can complete or abort the repair lifecycle before
			// StartSync is ever dispatched.
			allCoordinatesToSyncWithIblt = [];
			for (const entry of properties.entries.values()) {
				allCoordinatesToSyncWithIblt.push(coerceBigInt(entry.hashNumber));
			}
		} else if (naiveHashes.length > 0) {
			// If there are special-case entries, sync them simply in parallel
			if (priorityFn && naiveEntriesForPriority) {
				await this.simple.onMaybeMissingEntries({
					entries: naiveEntriesForPriority,
					targets: properties.targets,
					signal,
				});
			} else {
				await this.simple.onMaybeMissingHashes({
					hashes: naiveHashes,
					targets: properties.targets,
					signal,
				});
			}
			if (signal?.aborted) {
				emitCancelledTopLevelProfile("simple-prelude");
				return;
			}
		}

		const simplePreludeEntries = useAllCoordinatesForIblt
			? 0
			: naiveHashes.length;

		if (profile) {
			emitSyncProfileDuration(profile, selectStartedAt, {
				name: "rateless.selectEntries",
				entries: properties.entries.size,
				symbols: allCoordinatesToSyncWithIblt.length,
				targets: properties.targets.length,
				details: {
					naiveEntries: simplePreludeEntries,
					priority: priorityFn != null,
				},
			});
		}
		if (signal?.aborted) {
			emitCancelledTopLevelProfile("entry-selection");
			return;
		}

		if (allCoordinatesToSyncWithIblt.length === 0) {
			if (profile) {
				emitSyncProfileEvent(profile, {
					name: "rateless.dispatchMode",
					entries: properties.entries.size,
					targets: properties.targets.length,
					details: { mode: "simple-only" },
				});
			}
			emitTopLevelProfile({ mode: "simple-only" });
			return;
		}

		const outgoingHashes =
			priorityFn == null
				? [...properties.entries.keys()]
				: this.getPrioritizedEntries(properties.entries).map(
						(entry) => entry.hash,
					);
		await ribltReady;
		if (signal?.aborted) {
			emitCancelledTopLevelProfile("riblt-ready");
			return;
		}

		// For smaller sets, the original `sqrt(n)` heuristic can occasionally under-provision
		// low-degree symbols early, causing an unnecessary `MoreSymbols` round-trip. Use a
		// small floor to make small-delta syncs more reliable without affecting large-n behavior.
		let initialSymbolCount = Math.round(
			Math.sqrt(allCoordinatesToSyncWithIblt.length),
		); // TODO choose better
		initialSymbolCount = Math.min(
			MAX_MORE_SYMBOLS_BATCH_SIZE,
			Math.max(64, initialSymbolCount),
		);
		if (signal?.aborted) {
			emitCancelledTopLevelProfile("before-encoder-prepare");
			return;
		}
		const authorizedHashes = new Set(outgoingHashes);
		let messages = 0;
		let symbols = 0;
		for (const [target, targetLifecycle] of lifecycle.targets) {
			if (!this.isRatelessDispatchLifecycleActive(lifecycle)) {
				break;
			}
			if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
				continue;
			}
			const activeProcess = this.outgoingSyncProcessByTarget.get(target);
			if (
				activeProcess &&
				!activeProcess.signal.aborted &&
				this.isRatelessDispatchLifecycleActive(
					activeProcess.targetLifecycle.lifecycle,
					target,
				)
			) {
				// A repair sweep can flush adjacent batches for the same target
				// faster than its first StartSync reaches the transport. Replacing
				// that process here aborts the in-flight send; repeated large
				// batches can therefore starve Rateless sync entirely. Keep the
				// unambiguous active process and use bounded Simple sync only for
				// hashes that it does not already authorize.
				const uncoveredHashes = outgoingHashes.filter(
					(hash) => !activeProcess.authorizedHashes.has(hash),
				);
				if (uncoveredHashes.length > 0) {
					await this.simple.onMaybeMissingHashes({
						hashes: uncoveredHashes,
						targets: [target],
						signal,
					});
				}
				continue;
			}
			const startedSymbols = this.startOutgoingRatelessSyncForTarget({
				targetLifecycle,
				coordinates: allCoordinatesToSyncWithIblt,
				outgoingHashes,
				authorizedHashes,
				initialSymbolCount,
				entryCount: properties.entries.size,
			});
			if (startedSymbols !== undefined) {
				messages += 1;
				symbols += startedSymbols;
			}
		}
		if (signal.aborted) {
			emitTopLevelProfile(
				{
					mode: "rateless",
					phase: "start-sync-send",
					cancelled: true,
					ibltEntries: allCoordinatesToSyncWithIblt.length,
					naiveEntries: simplePreludeEntries,
				},
				{ messages, symbols },
			);
			return;
		}
		emitTopLevelProfile(
			{
				mode: "rateless",
				ibltEntries: allCoordinatesToSyncWithIblt.length,
				naiveEntries: simplePreludeEntries,
			},
			{ messages, symbols },
		);
	}

	private startOutgoingRatelessSyncForTarget(properties: {
		targetLifecycle: RatelessDispatchTargetLifecycle;
		coordinates: bigint[];
		outgoingHashes: string[];
		authorizedHashes: ReadonlySet<string>;
		initialSymbolCount: number;
		entryCount: number;
	}): number | undefined {
		const lifecycle = properties.targetLifecycle.lifecycle;
		const target = properties.targetLifecycle.target;
		const profile = this.properties.sync?.profile;
		if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
			return undefined;
		}

		const prepareStartedAt = syncProfileStart(profile);
		const { encoder, start, end, initialSymbols } = prepareStartSyncEncoder(
			properties.coordinates,
			this.properties.numbers.maxValue,
			properties.initialSymbolCount,
		);
		let encoderFreed = false;
		let encoderTransferred = false;
		let obj!: OutgoingRatelessSyncProcess;
		let processConstructed = false;
		const freeEncoder = () => {
			if (encoderFreed) {
				return;
			}
			encoderFreed = true;
			encoder.free();
		};
		try {
			if (profile) {
				emitSyncProfileDuration(profile, prepareStartedAt, {
					name: "rateless.prepareStartSyncEncoder",
					entries: properties.coordinates.length,
					symbols: initialSymbols?.length,
					targets: 1,
					details: {
						initialSymbolCount: properties.initialSymbolCount,
						includesInitialSymbols: initialSymbols != null,
					},
				});
			}
			if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
				freeEncoder();
				return undefined;
			}

			let startSyncSymbols = initialSymbols;
			if (!startSyncSymbols) {
				const produceStartedAt = syncProfileStart(profile);
				startSyncSymbols = produceNextCodedSymbols(
					encoder,
					properties.initialSymbolCount,
				);
				if (profile) {
					emitSyncProfileDuration(profile, produceStartedAt, {
						name: "rateless.produceStartSyncSymbols",
						symbols: startSyncSymbols.length,
						targets: 1,
					});
				}
			}
			if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
				freeEncoder();
				return undefined;
			}

			const startSync = new StartSync({
				from: start,
				to: end,
				symbols: startSyncSymbols,
			});
			const syncId = getSyncIdString(startSync);
			const targetSignal = properties.targetLifecycle.controller.signal;
			const processController = new AbortController();
			const processSignal = processController.signal;
			let cleared = false;
			let lastSeqNo = -1n;
			let symbolsProduced = startSyncSymbols.length;
			let symbolBudgetExhausted = false;
			let simpleFallbackPromise: Promise<void> | undefined;
			const symbolBudget = getOutgoingRatelessSymbolBudget(
				properties.coordinates.length,
				startSyncSymbols.length,
			);
			let onTargetAbort: (() => void) | undefined;
			const clear = (
				reason: unknown = new Error("rateless outgoing process closed"),
			) => {
				if (cleared) {
					return;
				}
				cleared = true;

				// Timeout/replacement owns only the encoder process. Response leases
				// retain the original open/caller/target lifecycle independently, so a
				// response already accepted for delivery can finish after this process
				// expires while close, caller abort, and disconnect still cancel it.
				processController.abort(reason);
				if (onTargetAbort) {
					targetSignal.removeEventListener("abort", onTargetAbort);
				}
				if (obj.timeout) {
					clearTimeout(obj.timeout);
				}
				if (obj.deadlineTimeout) {
					clearTimeout(obj.deadlineTimeout);
				}
				if (this.outgoingSyncProcesses.get(syncId) === obj) {
					this.outgoingSyncProcesses.delete(syncId);
				}
				if (this.outgoingSyncProcessByTarget.get(target) === obj) {
					this.outgoingSyncProcessByTarget.delete(target);
				}
				properties.targetLifecycle.retainedByProcess = false;
				freeEncoder();
				this.maybeDisposeRatelessDispatchLifecycle(lifecycle);
			};
			const startSimpleFallback = () => {
				if (!simpleFallbackPromise) {
					// From this point overlapping ResponseMaybeSync authorization
					// belongs to Simple. Keeping Rateless first would ship the response
					// while leaving the duplicate Simple lease charged until its TTL.
					obj.simpleFallbackStarted = true;
					simpleFallbackPromise = Promise.resolve(
						this.simple.onMaybeMissingHashes({
							hashes: properties.outgoingHashes,
							targets: [target],
							signal: lifecycle.callerSignal,
						}),
					);
				}
				return simpleFallbackPromise;
			};
			const fallbackAndClear = (reason: Error) => {
				void startSimpleFallback().catch((error) => logger.error(error));
				clear(reason);
			};
			const createTimeout = () => {
				const timeout = setTimeout(
					() =>
						fallbackAndClear(new Error("rateless outgoing process timed out")),
					OUTGOING_RATELESS_IDLE_TIMEOUT_MS,
				);
				timeout.unref?.();
				return timeout;
			};
			const createDeadlineTimeout = () => {
				const timeout = setTimeout(
					() =>
						fallbackAndClear(
							new Error("rateless outgoing process deadline exceeded"),
						),
					RATELESS_PROCESS_ABSOLUTE_TIMEOUT_MS,
				);
				timeout.unref?.();
				return timeout;
			};
			onTargetAbort = () => clear(targetSignal.reason);

			// Keep follow-up symbol payloads bounded. Each symbol is serialized as an
			// object with three bigint fields, so very large batches can dominate heap under
			// concurrent churn even though the native RIBLT encoder itself is compact.
			const nextBatch = Math.max(
				MIN_MORE_SYMBOLS_BATCH_SIZE,
				Math.min(
					MAX_MORE_SYMBOLS_BATCH_SIZE,
					Math.ceil(properties.coordinates.length / 4),
				),
			);
			obj = {
				target,
				targetLifecycle: properties.targetLifecycle,
				encoder,
				timeout: undefined,
				deadlineTimeout: undefined,
				refresh: () => {
					if (obj.timeout) {
						clearTimeout(obj.timeout);
					}
					obj.timeout = createTimeout();
				},
				next: (message: { lastSeqNo: bigint }) => {
					if (processSignal.aborted || symbolBudgetExhausted) {
						return undefined;
					}
					if (message.lastSeqNo !== lastSeqNo + 1n) {
						return undefined;
					}
					lastSeqNo = message.lastSeqNo;

					const remainingBudget = symbolBudget - symbolsProduced;
					if (remainingBudget <= 0) {
						symbolBudgetExhausted = true;
						return {
							symbols: CodedSymbolBatch.fromSymbols([]),
							exhaustedAfterSend: true,
						};
					}

					const produceStartedAt = syncProfileStart(profile);
					const symbols = produceNextCodedSymbols(
						encoder,
						Math.min(nextBatch, remainingBudget),
					);
					symbolsProduced += symbols.length;
					const exhaustedAfterSend = symbols.length === 0;
					if (exhaustedAfterSend) {
						symbolBudgetExhausted = true;
					} else {
						obj.refresh();
					}
					if (profile) {
						emitSyncProfileDuration(profile, produceStartedAt, {
							name: "rateless.produceMoreSymbols",
							syncId,
							symbols: symbols.length,
							targets: 1,
						});
					}
					return { symbols, exhaustedAfterSend };
				},
				startSimpleFallback,
				simpleFallbackStarted: false,
				free: clear,
				outgoingHashes: properties.outgoingHashes,
				authorizedHashes: properties.authorizedHashes,
				consumedResponseHashes: new Set(),
				processController,
				signal: processSignal,
				callerSignal: lifecycle.callerSignal,
			};
			processConstructed = true;

			if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
				clear();
				return undefined;
			}
			// Without a wire process id on ResponseMaybeSync, retaining at most one
			// process per target keeps sender/hash authorization unambiguous.
			this.outgoingSyncProcessByTarget
				.get(target)
				?.free(new Error("rateless outgoing process replaced"));
			if (!this.isRatelessDispatchLifecycleActive(lifecycle, target)) {
				clear();
				return undefined;
			}
			properties.targetLifecycle.retainedByProcess = true;
			this.outgoingSyncProcesses.set(syncId, obj);
			this.outgoingSyncProcessByTarget.set(target, obj);
			obj.timeout = createTimeout();
			obj.deadlineTimeout = createDeadlineTimeout();
			targetSignal.addEventListener("abort", onTargetAbort, { once: true });
			encoderTransferred = true;
			if (targetSignal.aborted) {
				clear(targetSignal.reason);
				return undefined;
			}
			if (profile) {
				emitSyncProfileEvent(profile, {
					name: "rateless.dispatchMode",
					entries: properties.entryCount,
					symbols: startSyncSymbols.length,
					targets: 1,
					syncId,
					details: { mode: "rateless" },
				});
			}

			const sendStartedAt = syncProfileStart(profile);
			let sendResult: Promise<unknown>;
			try {
				sendResult = Promise.resolve(
					this.simple.rpc.send(startSync, {
						mode: new SilentDelivery({
							to: [target],
							redundancy: 1,
						}),
						priority: SYNC_MESSAGE_PRIORITY,
						signal: processSignal,
					}),
				);
			} catch (error) {
				sendResult = Promise.reject(error);
			}
			void sendResult.then(
				() => {
					if (profile) {
						emitSyncProfileDuration(profile, sendStartedAt, {
							name: "rateless.sendStartSync",
							messages: 1,
							symbols: startSyncSymbols.length,
							targets: 1,
							syncId,
						});
					}
				},
				() => {
					clear();
					if (profile) {
						emitSyncProfileDuration(profile, sendStartedAt, {
							name: "rateless.sendStartSync",
							messages: 1,
							symbols: startSyncSymbols.length,
							targets: 1,
							syncId,
							details: {
								rejected: true,
								cancelled: processSignal.aborted || undefined,
							},
						});
					}
				},
			);
			if (processSignal.aborted) {
				clear();
				return undefined;
			}
			return startSyncSymbols.length;
		} catch (error) {
			if (processConstructed) {
				obj.free(error);
			}
			throw error;
		} finally {
			if (!encoderTransferred) {
				freeEncoder();
			}
		}
	}

	private consumeAuthorizedRatelessResponse(
		hashes: Iterable<string>,
		from: PublicSignKey,
	): AuthorizedRatelessResponseLease | undefined {
		const target = from.hashcode();
		const process = this.outgoingSyncProcessByTarget.get(target);
		if (
			!process ||
			process.signal.aborted ||
			!this.isRatelessDispatchLifecycleActive(
				process.targetLifecycle.lifecycle,
				target,
			)
		) {
			return undefined;
		}
		if (
			this.activeRatelessResponseCount >=
				MAX_ACTIVE_RATELESS_RESPONSES_GLOBAL ||
			(this.activeRatelessResponseCountByPeer.get(target) ?? 0) >=
				MAX_ACTIVE_RATELESS_RESPONSES_PER_PEER
		) {
			return undefined;
		}
		const authorized: string[] = [];
		const remaining: string[] = [];
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			if (
				!process.authorizedHashes.has(hash) ||
				process.consumedResponseHashes.has(hash)
			) {
				remaining.push(hash);
				continue;
			}
			process.consumedResponseHashes.add(hash);
			authorized.push(hash);
		}
		if (authorized.length === 0) {
			return {
				process,
				authorized,
				remaining,
				signal: process.targetLifecycle.controller.signal,
				release: () => {},
			};
		}

		const targetLifecycle = process.targetLifecycle;
		targetLifecycle.responseLeases += 1;
		this.activeRatelessResponseCount += 1;
		this.activeRatelessResponseCountByPeer.set(
			target,
			(this.activeRatelessResponseCountByPeer.get(target) ?? 0) + 1,
		);
		let released = false;
		return {
			process,
			authorized,
			remaining,
			signal: targetLifecycle.controller.signal,
			release: (options) => {
				if (released) {
					return;
				}
				released = true;
				if (options?.rollback) {
					for (const hash of authorized) {
						process.consumedResponseHashes.delete(hash);
					}
				}
				targetLifecycle.responseLeases -= 1;
				this.activeRatelessResponseCount -= 1;
				const activeForPeer =
					(this.activeRatelessResponseCountByPeer.get(target) ?? 1) - 1;
				if (activeForPeer === 0) {
					this.activeRatelessResponseCountByPeer.delete(target);
				} else {
					this.activeRatelessResponseCountByPeer.set(target, activeForPeer);
				}
				this.maybeDisposeRatelessDispatchLifecycle(targetLifecycle.lifecycle);
			},
		};
	}

	async onMessage(
		message: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const profile = this.properties.sync?.profile;
		if (message instanceof StartSync) {
			const from = context.from;
			const ownershipLifecycleController =
				this.ratelessDispatchLifecycleController;
			if (
				!from ||
				!this.isIncomingSyncGenerationActive(ownershipLifecycleController)
			) {
				return true;
			}
			const sender = from.hashcode();
			const syncId = getSyncIdString(message);
			const key = this.getIncomingSyncProcessKey(sender, syncId);
			const completedSynchronizations = this.startedOrCompletedSynchronizations;
			const admissionSet = this.incomingRatelessProcessAdmissions;
			if (this.ingoingSyncProcesses.has(key)) {
				return true;
			}

			if (completedSynchronizations.has(key)) {
				return true;
			}

			let admissionsForSender = 0;
			for (const admission of admissionSet) {
				if (admission.key === key) {
					return true;
				}
				if (admission.sender === sender) {
					admissionsForSender += 1;
				}
			}
			if (
				admissionSet.size >= MAX_INCOMING_RATELESS_PROCESSES ||
				admissionsForSender >= MAX_INCOMING_RATELESS_PROCESSES_PER_SENDER
			) {
				return true;
			}
			const admission = { key, sender };
			admissionSet.add(admission);

			const controller = new AbortController();
			let freed = false;
			let completed = false;
			let initializationPending = false;
			let fallbackSendPending = false;
			let admissionReleased = false;
			let fallbackGraceTimeout: ReturnType<typeof setTimeout> | undefined;
			let onOwnershipAbort: (() => void) | undefined;
			let obj!: IncomingRatelessSyncProcess;
			const releaseAdmissionIfSettled = () => {
				if (
					admissionReleased ||
					!freed ||
					initializationPending ||
					fallbackSendPending
				) {
					return;
				}
				admissionReleased = true;
				admissionSet.delete(admission);
			};
			const free = (
				reason: unknown = new Error("incoming rateless process closed"),
			) => {
				if (freed) {
					return;
				}
				freed = true;
				// Abort first: no queued process/send continuation may observe the
				// object detached while its native decoder is still usable.
				controller.abort(reason);
				if (onOwnershipAbort) {
					ownershipLifecycleController.signal.removeEventListener(
						"abort",
						onOwnershipAbort,
					);
				}
				if (obj.timeout) {
					clearTimeout(obj.timeout);
					obj.timeout = undefined;
				}
				if (obj.deadlineTimeout) {
					clearTimeout(obj.deadlineTimeout);
					obj.deadlineTimeout = undefined;
				}
				if (fallbackGraceTimeout) {
					clearTimeout(fallbackGraceTimeout);
					fallbackGraceTimeout = undefined;
				}
				if (this.ingoingSyncProcesses.get(key) === obj) {
					this.ingoingSyncProcesses.delete(key);
				}
				const ownedDecoder = obj.decoder;
				obj.decoder = undefined;
				ownedDecoder?.free();
				releaseAdmissionIfSettled();
			};
			const complete = () => {
				if (completed || !this.isIncomingSyncProcessActive(obj)) {
					return;
				}
				completed = true;
				completedSynchronizations.add(key);
				free(new Error("incoming rateless process completed"));
			};
			obj = {
				key,
				syncId,
				sender,
				from,
				ownershipLifecycleController,
				controller,
				completedSynchronizations,
				timeout: undefined,
				deadlineTimeout: undefined,
				refresh: () => {},
				lastSeqNo: -1n,
				processedSymbols: 0,
				queuedSymbols: 0,
				symbolBudget: getIncomingRatelessSymbolBudget(message.symbols.length),
				process: async () => undefined,
				requestAll: async () => {},
				fallbackToSimple: async () => {},
				free,
				complete,
			};
			this.ingoingSyncProcesses.set(key, obj);
			onOwnershipAbort = () => free(ownershipLifecycleController.signal.reason);
			ownershipLifecycleController.signal.addEventListener(
				"abort",
				onOwnershipAbort,
				{ once: true },
			);
			obj.deadlineTimeout = setTimeout(() => {
				void obj.fallbackToSimple(
					new Error("incoming rateless process deadline exceeded"),
				);
			}, RATELESS_PROCESS_ABSOLUTE_TIMEOUT_MS);
			obj.deadlineTimeout.unref?.();
			if (!this.isIncomingSyncProcessActive(obj)) {
				free(ownershipLifecycleController.signal.reason);
				return true;
			}

			let requestAllPromise: Promise<void> | undefined;
			obj.requestAll = () => {
				if (requestAllPromise) {
					return requestAllPromise;
				}
				requestAllPromise = (async () => {
					if (!this.isIncomingSyncProcessActive(obj)) {
						return;
					}
					fallbackSendPending = true;
					const sendStartedAt = syncProfileStart(profile);
					try {
						await this.simple.rpc.send(
							new RequestAll({
								syncId: message.syncId,
							}),
							{
								mode: new SilentDelivery({
									to: [obj.sender],
									redundancy: 1,
								}),
								priority: SYNC_MESSAGE_PRIORITY,
								signal: controller.signal,
							},
						);
						if (this.isIncomingSyncProcessActive(obj) && profile) {
							emitSyncProfileDuration(profile, sendStartedAt, {
								name: "rateless.sendRequestAll",
								messages: 1,
								targets: 1,
								syncId,
							});
						}
					} finally {
						fallbackSendPending = false;
						if (this.isIncomingSyncProcessActive(obj)) {
							complete();
						}
						releaseAdmissionIfSettled();
					}
				})();
				return requestAllPromise;
			};
			let boundedFallbackPromise: Promise<void> | undefined;
			obj.fallbackToSimple = (
				reason: unknown = new Error("incoming rateless fallback timed out"),
			) => {
				if (boundedFallbackPromise) {
					return boundedFallbackPromise;
				}
				const requestAll = obj.requestAll();
				boundedFallbackPromise = new Promise<void>((resolve) => {
					let settled = false;
					const settle = () => {
						if (settled) {
							return;
						}
						settled = true;
						controller.signal.removeEventListener("abort", onAbort);
						if (fallbackGraceTimeout) {
							clearTimeout(fallbackGraceTimeout);
							fallbackGraceTimeout = undefined;
						}
						resolve();
					};
					const onAbort = () => settle();
					controller.signal.addEventListener("abort", onAbort, {
						once: true,
					});
					fallbackGraceTimeout = setTimeout(() => {
						free(reason);
						settle();
					}, INCOMING_RATELESS_FALLBACK_GRACE_MS);
					fallbackGraceTimeout.unref?.();
					void requestAll.then(settle, settle);
					if (controller.signal.aborted) {
						onAbort();
					}
				});
				return boundedFallbackPromise;
			};

			if (message.symbols.length > MAX_MORE_SYMBOLS_BATCH_SIZE) {
				await obj.fallbackToSimple(
					new Error("oversized incoming rateless initial batch"),
				);
				return true;
			}

			const wrapped = message.end < message.start;
			const decoderStartedAt = syncProfileStart(profile);
			let decoder: DecoderWrapper | false;
			initializationPending = true;
			try {
				decoder = await this.getLocalDecoderForRange(
					{
						start1: message.start,
						end1: wrapped ? this.properties.numbers.maxValue : message.end,
						start2: 0n,
						end2: wrapped ? message.end : 0n,
					},
					{
						ownershipLifecycleController,
						signal: controller.signal,
					},
				);
			} catch (error) {
				const processAborted = controller.signal.aborted;
				free(error);
				if (
					processAborted ||
					!this.isIncomingSyncGenerationActive(ownershipLifecycleController)
				) {
					return true;
				}
				throw error;
			} finally {
				initializationPending = false;
				releaseAdmissionIfSettled();
			}
			if (!this.isIncomingSyncProcessActive(obj)) {
				decoder && decoder.free();
				free(controller.signal.reason);
				return true;
			}
			if (decoder) {
				// Transfer the native decoder before invoking diagnostics. A profile
				// sink is caller code and may throw; from this point process cleanup
				// owns the decoder on every exit.
				obj.decoder = decoder;
			}
			try {
				if (profile) {
					emitSyncProfileDuration(profile, decoderStartedAt, {
						name: "rateless.getLocalDecoderForRange",
						syncId,
						details: { wrapped, found: decoder !== false },
					});
				}
			} catch (error) {
				free(error);
				throw error;
			}

			if (!decoder) {
				try {
					await obj.fallbackToSimple(
						new Error("incoming rateless decoder unavailable"),
					);
				} catch (error) {
					if (controller.signal.aborted) {
						return true;
					}
					throw error;
				}
				return true;
			}

			const createTimeout = () => {
				const timeout = setTimeout(() => {
					void obj.fallbackToSimple(
						new Error("incoming rateless process timed out"),
					);
				}, INCOMING_RATELESS_IDLE_TIMEOUT_MS);
				timeout.unref?.();
				return timeout;
			};

			let messageQueue: {
				seqNo: bigint;
				symbols: CodedSymbolInput;
				symbolCount: number;
			}[] = [];
			obj.refresh = () => {
				if (!this.isIncomingSyncProcessActive(obj)) {
					return;
				}
				if (obj.timeout) {
					clearTimeout(obj.timeout);
				}
				obj.timeout = createTimeout();
			};
			obj.process = async (newMessage: {
				seqNo: bigint;
				symbols: CodedSymbolInput;
			}): Promise<IncomingRatelessProcessResult> => {
				if (!this.isIncomingSyncProcessActive(obj)) {
					return undefined;
				}

				const symbolCount = CodedSymbolBatch.from(newMessage.symbols).length;
				if (symbolCount > MAX_MORE_SYMBOLS_BATCH_SIZE) {
					free(new Error("incoming rateless symbol batch exceeds limit"));
					return undefined;
				}
				if (newMessage.seqNo <= obj.lastSeqNo) {
					return undefined;
				}
				if (
					newMessage.seqNo >
					obj.lastSeqNo + MAX_INCOMING_RATELESS_SEQUENCE_GAP
				) {
					return undefined;
				}
				if (messageQueue.some((queued) => queued.seqNo === newMessage.seqNo)) {
					return undefined;
				}
				if (
					newMessage.seqNo !== obj.lastSeqNo + 1n &&
					messageQueue.length >= MAX_INCOMING_RATELESS_QUEUED_BATCHES
				) {
					return undefined;
				}
				if (
					obj.processedSymbols + obj.queuedSymbols + symbolCount >
					obj.symbolBudget
				) {
					return "fallback-to-simple";
				}
				if (newMessage.seqNo > 0n && symbolCount === 0) {
					return "fallback-to-simple";
				}

				messageQueue.push({ ...newMessage, symbolCount });
				obj.queuedSymbols += symbolCount;
				messageQueue.sort((a, b) => Number(a.seqNo - b.seqNo));
				if (messageQueue[0].seqNo !== obj.lastSeqNo + 1n) {
					return;
				}

				const finalizeIfDecoded = (): boolean => {
					if (!this.isIncomingSyncProcessActive(obj)) {
						return true;
					}
					if (!decoder.decoded()) {
						return false;
					}

					const remoteStartedAt = syncProfileStart(profile);
					const allMissingSymbolsInRemote = getRemoteSymbolValues(decoder);
					if (profile) {
						emitSyncProfileDuration(profile, remoteStartedAt, {
							name: "rateless.remoteSymbols",
							entries: allMissingSymbolsInRemote.length,
							symbols: allMissingSymbolsInRemote.length,
							syncId,
						});
					}

					// The IBLT decoder is based on a local snapshot. Entries can arrive via
					// overlapping repair before we issue the follow-up simple request, so
					// re-check local presence to avoid stale duplicate bounce-back.
					void this.simple
						.queueSync(allMissingSymbolsInRemote, obj.from)
						.catch((error) => logger.error(error));
					obj.complete();
					return true;
				};

				while (
					messageQueue.length > 0 &&
					messageQueue[0].seqNo === obj.lastSeqNo + 1n
				) {
					const symbolMessage = messageQueue.shift();
					if (!symbolMessage) {
						break;
					}

					obj.queuedSymbols -= symbolMessage.symbolCount;
					obj.lastSeqNo = symbolMessage.seqNo;
					obj.processedSymbols += symbolMessage.symbolCount;
					// Only an authenticated, previously unseen contiguous sequence advances
					// the idle deadline. Replays and speculative future batches do not.
					obj.refresh();

					const addBatchAndDecode:
						| ((symbols: BigUint64Array) => boolean)
						| undefined = (decoder as BatchDecoderWrapper)
						.add_coded_symbols_and_try_decode;
					if (typeof BigUint64Array !== "undefined" && addBatchAndDecode) {
						const flatStartedAt = syncProfileStart(profile);
						const flatSymbols = flatFromCodedSymbols(symbolMessage.symbols);
						if (profile) {
							emitSyncProfileDuration(profile, flatStartedAt, {
								name: "rateless.symbolBatchToFlat",
								symbols: flatSymbols.length / CODED_SYMBOL_WORDS,
								syncId,
							});
						}

						const decodeStartedAt = syncProfileStart(profile);
						const decoded = addBatchAndDecode.call(decoder, flatSymbols);
						if (profile) {
							emitSyncProfileDuration(profile, decodeStartedAt, {
								name: "rateless.decodeBatch",
								symbols: flatSymbols.length / CODED_SYMBOL_WORDS,
								syncId,
								details: { decoded },
							});
						}
						if (decoded && finalizeIfDecoded()) {
							return true;
						}
						continue;
					}

					const decodeLoopStartedAt = syncProfileStart(profile);
					let symbolsProcessed = 0;
					for (const symbol of CodedSymbolBatch.from(symbolMessage.symbols)) {
						symbolsProcessed += 1;
						const normalizedSymbol =
							symbol instanceof SymbolSerialized
								? symbol
								: new SymbolSerialized({
										count: symbol.count,
										hash: symbol.hash,
										symbol: symbol.symbol,
									});

						decoder.add_coded_symbol(normalizedSymbol);
						try {
							decoder.try_decode();
							if (finalizeIfDecoded()) {
								return true;
							}
						} catch (error: any) {
							if (
								error?.message === "Invalid degree" ||
								error === "Invalid degree"
							) {
								logger.trace(
									"Decoder reported invalid degree; waiting for more symbols",
								);
								continue;
							}
							throw error;
						}
					}
					if (profile) {
						emitSyncProfileDuration(profile, decodeLoopStartedAt, {
							name: "rateless.decodeSymbolLoop",
							symbols: symbolsProcessed,
							syncId,
						});
					}
				}
				return false;
			};
			obj.timeout = createTimeout();
			let initialResult: IncomingRatelessProcessResult;
			try {
				initialResult = await obj.process({
					seqNo: 0n,
					symbols: message.symbols,
				});
			} catch (error) {
				const wasActive = this.isIncomingSyncProcessActive(obj);
				free(error);
				if (!wasActive) {
					return true;
				}
				throw error;
			}
			if (initialResult === true) {
				return true;
			}
			if (initialResult === "fallback-to-simple") {
				await obj.fallbackToSimple(
					new Error("incoming rateless symbol budget exhausted"),
				);
				return true;
			}
			if (!this.isIncomingSyncProcessActive(obj)) {
				return true;
			}

			// not done, request more symbols
			const sendStartedAt = syncProfileStart(profile);
			try {
				await this.simple.rpc.send(
					new RequestMoreSymbols({
						lastSeqNo: obj.lastSeqNo,
						syncId: message.syncId,
					}),
					{
						mode: new SilentDelivery({ to: [obj.sender], redundancy: 1 }),
						priority: SYNC_MESSAGE_PRIORITY,
						signal: controller.signal,
					},
				);
			} catch (error) {
				if (!this.isIncomingSyncProcessActive(obj)) {
					return true;
				}
				free(error);
				throw error;
			}
			if (!this.isIncomingSyncProcessActive(obj)) {
				return true;
			}
			if (profile) {
				emitSyncProfileDuration(profile, sendStartedAt, {
					name: "rateless.sendRequestMoreSymbols",
					messages: 1,
					targets: 1,
					syncId,
				});
			}

			return true;
		} else if (message instanceof MoreSymbols) {
			const from = context.from;
			if (!from) {
				return true;
			}
			const sender = from.hashcode();
			const syncId = getSyncIdString(message);
			const key = this.getIncomingSyncProcessKey(sender, syncId);
			const obj = this.ingoingSyncProcesses.get(key);
			if (
				!obj ||
				obj.sender !== sender ||
				!this.isIncomingSyncProcessActive(obj)
			) {
				return true;
			}
			let outProcess: IncomingRatelessProcessResult;
			try {
				outProcess = await obj.process(message);
			} catch (error) {
				const wasActive = this.isIncomingSyncProcessActive(obj);
				obj.free(error);
				if (!wasActive) {
					return true;
				}
				throw error;
			}

			if (outProcess === true) {
				return true;
			} else if (outProcess === "fallback-to-simple") {
				try {
					await obj.fallbackToSimple(
						new Error("incoming rateless symbol budget exhausted"),
					);
				} catch {
					// The bounded rateless process is already complete or aborted. A
					// later repair round may retry if the fallback request was lost.
				}
				return true;
			} else if (outProcess === undefined) {
				return true; // we don't have enough information, or received information that is redundant
			}
			if (!this.isIncomingSyncProcessActive(obj)) {
				return true;
			}

			// we are not done

			const sendStartedAt = syncProfileStart(profile);
			try {
				await this.simple.rpc.send(
					new RequestMoreSymbols({
						lastSeqNo: obj.lastSeqNo,
						syncId: message.syncId,
					}),
					{
						mode: new SilentDelivery({ to: [obj.sender], redundancy: 1 }),
						priority: SYNC_MESSAGE_PRIORITY,
						signal: obj.controller.signal,
					},
				);
			} catch {
				if (profile) {
					emitSyncProfileDuration(profile, sendStartedAt, {
						name: "rateless.sendRequestMoreSymbols",
						messages: 1,
						targets: 1,
						syncId,
						details: { rejected: true },
					});
				}
				return true;
			}
			if (!this.isIncomingSyncProcessActive(obj)) {
				return true;
			}
			if (profile) {
				emitSyncProfileDuration(profile, sendStartedAt, {
					name: "rateless.sendRequestMoreSymbols",
					messages: 1,
					targets: 1,
					syncId,
				});
			}

			return true;
		} else if (message instanceof RequestMoreSymbols) {
			const syncId = getSyncIdString(message);
			const obj = this.outgoingSyncProcesses.get(syncId);
			if (!obj) {
				return true;
			}
			if (context.from?.hashcode() !== obj.target) {
				return true;
			}
			const signal = obj.signal;
			if (signal.aborted) {
				return true;
			}
			const next = obj.next(message);
			if (!next) {
				return true;
			}
			const { symbols, exhaustedAfterSend } = next;
			if (signal.aborted) {
				return true;
			}
			const sendStartedAt = syncProfileStart(profile);
			try {
				await this.properties.rpc.send(
					new MoreSymbols({
						lastSeqNo: message.lastSeqNo,
						syncId: message.syncId,
						symbols,
					}),
					{
						mode: new SilentDelivery({ to: [obj.target], redundancy: 1 }),
						priority: SYNC_MESSAGE_PRIORITY,
						signal,
					},
				);
			} catch (error) {
				if (signal?.aborted) {
					return true;
				}
				throw error;
			}
			if (profile) {
				emitSyncProfileDuration(profile, sendStartedAt, {
					name: "rateless.sendMoreSymbols",
					messages: 1,
					symbols: symbols.length,
					targets: 1,
					syncId,
				});
			}
			if (exhaustedAfterSend) {
				// Latch exhaustion in next() before the send begins, then retain this
				// bounded process long enough for RequestAll. Starting Simple eagerly
				// also keeps fallback working with older receivers that ignore an empty
				// terminal symbol batch.
				void obj.startSimpleFallback().catch((error) => logger.error(error));
			}
			return true;
		} else if (message instanceof RequestAll) {
			const p = this.outgoingSyncProcesses.get(getSyncIdString(message));
			if (!p) {
				return true;
			}
			if (context.from?.hashcode() !== p.target) {
				return true;
			}
			if (p.signal.aborted) {
				return true;
			}
			// RequestAll ends only this target's rateless attempt. Other target
			// encoders and response authorizations remain independently owned.
			const fallback = p.startSimpleFallback();
			p.free();
			await fallback;
			return true;
		} else if (
			message instanceof ResponseMaybeSync ||
			message instanceof ResponseMaybeSyncCapabilities
		) {
			const from = context.from!;
			// Simple authorizations are exact request leases and take precedence
			// over Rateless' broader process authorization. This also handles a
			// delayed fallback/prelude response after the Rateless process for the
			// same target has been replaced.
			const simpleLeases = this.simple.consumeAuthorizedMaybeSyncResponse(
				message.hashes,
				from,
			);
			const simpleHashes = simpleLeases.flatMap((lease) => lease.hashes);
			const simpleHashSet = new Set(simpleHashes);
			const ratelessCandidateHashes: string[] = [];
			let inspected = 0;
			const iterator = message.hashes[Symbol.iterator]();
			let exhausted = false;
			try {
				while (inspected < MAX_RATELESS_RESPONSE_HASHES_INSPECTED) {
					const next = iterator.next();
					if (next.done) {
						exhausted = true;
						break;
					}
					inspected += 1;
					if (!simpleHashSet.has(next.value)) {
						ratelessCandidateHashes.push(next.value);
					}
				}
			} finally {
				if (!exhausted) {
					iterator.return?.();
				}
			}
			const response =
				ratelessCandidateHashes.length > 0
					? this.consumeAuthorizedRatelessResponse(
							ratelessCandidateHashes,
							from,
						)
					: undefined;
			if (
				simpleLeases.length === 0 &&
				(!response || response.authorized.length === 0)
			) {
				response?.release();
				return true;
			}

			let firstError: unknown;
			let rollbackRatelessAuthorization = false;
			try {
				const simpleMessage =
					message instanceof ResponseMaybeSyncCapabilities
						? new ResponseMaybeSyncCapabilities({
								hashes: simpleHashes,
								capabilities: message.capabilities,
							})
						: new ResponseMaybeSync({ hashes: simpleHashes });
				if (response && response.authorized.length > 0) {
					const responseStartedAt = syncProfileStart(profile);
					let responseShipment = {
						messages: 0,
						fused: false,
						entries: 0,
					};
					try {
						responseShipment =
							await this.simple.shipAuthorizedMaybeSyncResponse({
								hashes: response.authorized,
								from,
								response: message,
								signal: response.signal,
							});
					} catch (error) {
						firstError = error;
						rollbackRatelessAuthorization = true;
					}
					if (profile) {
						try {
							emitSyncProfileDuration(profile, responseStartedAt, {
								name: "simple.exchangeHeads",
								entries: responseShipment.entries,
								messages: responseShipment.messages,
								targets: 1,
								details: {
									source: "ratelessResponseMaybeSync",
									fused: responseShipment.fused,
								},
							});
						} catch (error) {
							firstError ??= error;
						}
					}
				}

				if (simpleLeases.length > 0) {
					try {
						await this.simple.shipAuthorizedMaybeSyncResponseLeases({
							leases: simpleLeases,
							from,
							response: simpleMessage,
						});
					} catch (error) {
						firstError ??= error;
					}
				}
				if (firstError !== undefined) {
					throw firstError;
				}
				return true;
			} finally {
				response?.release({ rollback: rollbackRatelessAuthorization });
				// The Simple helper owns these leases once entered and releases each
				// one in its own finally. Keep this outer ownership release as the
				// final safety net for diagnostics or setup failures that happen
				// before the helper can take responsibility.
				for (const lease of simpleLeases) {
					lease.release();
				}
			}
		}
		return this.simple.onMessage(message, context);
	}

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void {
		return this.simple.onReceivedEntries(properties);
	}

	onReceivedEntryHashes(properties: {
		hashes: string[];
		from: PublicSignKey;
	}): Promise<void> | void {
		return this.simple.onReceivedEntryHashes(properties);
	}

	onEntryAdded(entry: Entry<any>): void {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryAdded(entry);
	}

	onEntryAddedHash(hash: string): void {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryAddedHash(hash);
	}

	onEntryAddedHashes(hashes: string[]): void {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryAddedHashes(hashes);
	}

	onEntryRemoved(hash: string) {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryRemoved(hash);
	}

	onEntryRemovedHashes(hashes: string[]): void {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryRemovedHashes(hashes);
	}

	onPeerDisconnected(key: PublicSignKey | string) {
		const target = typeof key === "string" ? key : key.hashcode();
		for (const process of [...this.ingoingSyncProcesses.values()]) {
			if (process.sender === target) {
				process.free(new Error("incoming rateless peer disconnected"));
			}
		}
		for (const targetLifecycle of [
			...(this.ratelessDispatchTargets.get(target) ?? []),
		]) {
			this.abortRatelessDispatchTarget(
				targetLifecycle,
				new Error("rateless sync target disconnected"),
			);
		}
		return this.simple.onPeerDisconnected(target);
	}

	open(): Promise<void> | void {
		const reason = new Error("rateless sync generation replaced");
		this.cancelRatelessRepairSessions(reason);
		this.ratelessDispatchLifecycleController.abort(reason);
		this.ratelessDispatchLifecycleController = new AbortController();
		this.startedOrCompletedSynchronizations = new Cache({ max: 1e4 });
		// Admission accounting intentionally spans local generations: native range
		// initialization and fallback delivery are not guaranteed to stop merely
		// because their logical process was aborted.
		this.ratelessClosed = false;
		return this.simple.open();
	}

	close(): Promise<void> | void {
		this.ratelessClosed = true;
		// Abort ownership first. Process abort listeners then cancel any in-flight
		// StartSync/MoreSymbols send before they detach or free native state.
		const reason = new Error("rateless synchronizer closed");
		this.cancelRatelessRepairSessions(reason);
		this.ratelessDispatchLifecycleController.abort(reason);
		for (const obj of [...this.ingoingSyncProcesses.values()]) {
			obj.free();
		}
		for (const obj of [...this.outgoingSyncProcesses.values()]) {
			obj.free();
		}
		this.clearLocalRangeEncoderCache();
		return this.simple.close();
	}

	get syncInFlight(): Map<string, Map<SyncableKey, { timestamp: number }>> {
		return this.simple.syncInFlight;
	}

	get pending(): number {
		return this.simple.pending;
	}
}
