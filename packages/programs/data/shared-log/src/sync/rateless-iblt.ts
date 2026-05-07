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
	RepairSession,
	RepairSessionMode,
	RepairSessionResult,
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
import { SimpleSyncronizer } from "./simple.js";

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
const MAX_MORE_SYMBOLS_BATCH_SIZE = 1_024;

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
): Promise<E | false> => {
	await ribltReady;
	const encoder =
		type === "encoder" ? new EncoderWrapper() : new DecoderWrapper();

	const rangeQueryStartedAt = syncProfileStart(profile);
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
	if (profile) {
		emitSyncProfileDuration(profile, rangeQueryStartedAt, {
			name: "rateless.rangeQuery",
			entries: entries.length,
			details: { type },
		});
	}

	if (entries.length === 0) {
		return false;
	}

	const addSymbolsStartedAt = syncProfileStart(profile);
	if (
		typeof BigUint64Array !== "undefined" &&
		typeof (encoder as RibltSymbolAdder).add_symbols === "function"
	) {
		const symbols = new BigUint64Array(entries.length);
		for (let i = 0; i < entries.length; i++) {
			symbols[i] = coerceBigInt(entries[i].value.hashNumber);
		}
		addSymbolsToRiblt(encoder as RibltSymbolAdder, symbols);
	} else {
		for (const entry of entries) {
			encoder.add_symbol(coerceBigInt(entry.value.hashNumber));
		}
	}
	if (profile) {
		emitSyncProfileDuration(profile, addSymbolsStartedAt, {
			name: "rateless.rangeAddSymbols",
			entries: entries.length,
			symbols: entries.length,
			details: { type },
		});
	}
	return encoder as E;
};

export class RatelessIBLTSynchronizer<D extends "u32" | "u64">
	implements Syncronizer<D>
{
	simple: SimpleSyncronizer<D>;
	private repairSessionCounter: number;

	startedOrCompletedSynchronizations: Cache<string>;
	private localRangeEncoderCacheVersion = 0;
	private localRangeEncoderCache: Map<
		string,
		{ encoder: EncoderWrapper; version: number; lastUsed: number }
	> = new Map();
	private localRangeEncoderCacheMax = 2;

	ingoingSyncProcesses: Map<
		string,
		{
			decoder: DecoderWrapper;
			timeout: ReturnType<typeof setTimeout>;
			refresh: () => void;
			process: (message: {
				seqNo: bigint;
				symbols: CodedSymbolInput;
			}) => Promise<boolean | undefined>;
			free: () => void;
		}
	>;

	outgoingSyncProcesses: Map<
		string,
		{
			outgoing: Map<string, EntryReplicated<D>>;
			encoder: EncoderWrapper;
			timeout: ReturnType<typeof setTimeout>;
			refresh: () => void;
			next: (message: { lastSeqNo: bigint }) => CodedSymbolBatch;
			free: () => void;
		}
	>;

	constructor(readonly properties: SynchronizerComponents<D>) {
		this.simple = new SimpleSyncronizer(properties);
		this.repairSessionCounter = 0;
		this.outgoingSyncProcesses = new Map();
		this.ingoingSyncProcesses = new Map();
		this.startedOrCompletedSynchronizations = new Cache({ max: 1e4 });
	}

	private get maxConvergentTrackedHashes() {
		const value = this.properties.sync?.maxConvergentTrackedHashes;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
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

	private getPrioritizedEntries(entries: Map<string, EntryReplicated<D>>) {
		const priorityFn = this.properties.sync?.priority;
		if (!priorityFn) {
			return [...entries.values()];
		}

		let index = 0;
		const scored: {
			entry: EntryReplicated<D>;
			index: number;
			priority: number;
		}[] = [];
		for (const entry of entries.values()) {
			const priorityValue = priorityFn(entry);
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

	startRepairSession(properties: {
		entries: Map<string, EntryReplicated<D>>;
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
			const trackedEntries = new Map<string, EntryReplicated<D>>();
			for (const entry of prioritized.slice(0, trackedLimit)) {
				trackedEntries.set(entry.hash, entry);
			}

			let cancelled = false;
			const trackedSession = this.simple.startRepairSession({
				entries: trackedEntries,
				targets,
				mode: "convergent",
				timeoutMs,
				retryIntervalsMs,
			});

			const runDispatchSchedule = async () => {
				let previousDelay = 0;
				for (const delayMs of retryIntervalsMs) {
					if (cancelled) {
						return;
					}
					const elapsed = Date.now() - startedAt;
					if (elapsed >= timeoutMs) {
						return;
					}
					const waitMs = Math.max(0, delayMs - previousDelay);
					previousDelay = delayMs;
					if (waitMs > 0) {
						await new Promise<void>((resolve) => {
							const timer = setTimeout(resolve, waitMs);
							timer.unref?.();
						});
					}
					if (cancelled) {
						return;
					}
					try {
						await this.onMaybeMissingEntries({
							entries: properties.entries,
							targets,
						});
					} catch {
						// Best-effort schedule: tracked session timeout/result decides completion.
					}
				}
			};

			const done = (async (): Promise<RepairSessionResult[]> => {
				await runDispatchSchedule();
				const trackedResults = await trackedSession.done;
				return trackedResults.map((result) => ({
					...result,
					requestedTotal: requestedHashes.length,
					truncated: true,
				}));
			})();

			return {
				id,
				done,
				cancel: () => {
					cancelled = true;
					trackedSession.cancel();
				},
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

	private decoderFromCachedEncoder(encoder: EncoderWrapper): DecoderWrapper {
		const clone = encoder.clone();
		const decoder = clone.to_decoder();
		clone.free();
		return decoder;
	}

	private async getLocalDecoderForRange(ranges: {
		start1: NumberOrBigint;
		end1: NumberOrBigint;
		start2: NumberOrBigint;
		end2: NumberOrBigint;
	}): Promise<DecoderWrapper | false> {
		const profile = this.properties.sync?.profile;
		const key = this.localRangeEncoderCacheKey(ranges);
		const cached = this.localRangeEncoderCache.get(key);
		if (cached && cached.version === this.localRangeEncoderCacheVersion) {
			const startedAt = syncProfileStart(profile);
			cached.lastUsed = Date.now();
			try {
				return this.decoderFromCachedEncoder(cached.encoder);
			} finally {
				if (profile) {
					emitSyncProfileDuration(profile, startedAt, {
						name: "rateless.localDecoder",
						cacheHit: true,
					});
				}
			}
		}

		const startedAt = syncProfileStart(profile);
		const encoder = (await buildEncoderOrDecoderFromRange(
			ranges,
			this.properties.entryIndex,
			"encoder",
			profile,
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

		try {
			return this.decoderFromCachedEncoder(encoder);
		} finally {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "rateless.localDecoder",
					cacheHit: false,
				});
			}
		}
	}

	async onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<D>>;
		targets: string[];
	}): Promise<void> {
		const profile = this.properties.sync?.profile;
		const startedAt = syncProfileStart(profile);
		// NOTE: this method is best-effort dispatch, not a per-hash convergence API.
		// It may require follow-up repair rounds under churn/loss to fully close all gaps.
		// Strategy:
		// - For small sets, prefer the simple synchronizer to reduce complexity and avoid
		//   IBLT overhead on tiny batches.
		// - For large sets, use IBLT, but still allow simple sync for special-case entries
		//   such as those assigned to range boundaries.

		let entriesToSyncNaively: Map<string, EntryReplicated<D>> = new Map();
		let minSyncIbltSize = 333; // TODO: make configurable
		let maxSyncWithSimpleMethod = 1e3;

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
				});
			} finally {
				if (profile) {
					emitSyncProfileDuration(profile, startedAt, {
						name: "rateless.onMaybeMissingEntries",
						entries: properties.entries.size,
						targets: properties.targets.length,
						details: { mode: "simple-small" },
					});
				}
			}
			return;
		}

		const selectStartedAt = syncProfileStart(profile);
		const nonBoundaryEntries: EntryReplicated<D>[] = [];
		for (const entry of properties.entries.values()) {
			if (entry.assignedToRangeBoundary) {
				entriesToSyncNaively.set(entry.hash, entry);
			} else {
				nonBoundaryEntries.push(entry);
			}
		}

		const priorityFn = this.properties.sync?.priority;
		const maxSimpleEntries = this.properties.sync?.maxSimpleEntries;
		const maxAdditionalNaive =
			priorityFn &&
			typeof maxSimpleEntries === "number" &&
			Number.isFinite(maxSimpleEntries) &&
			maxSimpleEntries > 0
				? Math.max(
						0,
						Math.min(
							Math.floor(maxSimpleEntries),
							maxSyncWithSimpleMethod - entriesToSyncNaively.size,
						),
					)
				: 0;

		if (priorityFn && maxAdditionalNaive > 0 && nonBoundaryEntries.length > 0) {
			let index = 0;
			const scored: {
				entry: EntryReplicated<D>;
				index: number;
				priority: number;
			}[] = [];
			for (const entry of nonBoundaryEntries) {
				const priorityValue = priorityFn(entry);
				scored.push({
					entry,
					index,
					priority: Number.isFinite(priorityValue) ? priorityValue : 0,
				});
				index += 1;
			}
			scored.sort((a, b) => b.priority - a.priority || a.index - b.index);
			for (const { entry } of scored.slice(0, maxAdditionalNaive)) {
				entriesToSyncNaively.set(entry.hash, entry);
			}
		}

		let allCoordinatesToSyncWithIblt: bigint[] = [];
		for (const entry of nonBoundaryEntries) {
			if (entriesToSyncNaively.has(entry.hash)) {
				continue;
			}
			allCoordinatesToSyncWithIblt.push(coerceBigInt(entry.hashNumber));
		}

		if (entriesToSyncNaively.size > 0) {
			// If there are special-case entries, sync them simply in parallel
			await this.simple.onMaybeMissingEntries({
				entries: entriesToSyncNaively,
				targets: properties.targets,
			});
		}

		if (
			allCoordinatesToSyncWithIblt.length === 0 ||
			entriesToSyncNaively.size > maxSyncWithSimpleMethod
		) {
			// Fallback: if nothing left for IBLT (or simple set is too large), include all in IBLT
			allCoordinatesToSyncWithIblt = [];
			for (const entry of properties.entries.values()) {
				allCoordinatesToSyncWithIblt.push(coerceBigInt(entry.hashNumber));
			}
		}

		if (profile) {
			emitSyncProfileDuration(profile, selectStartedAt, {
				name: "rateless.selectEntries",
				entries: properties.entries.size,
				symbols: allCoordinatesToSyncWithIblt.length,
				targets: properties.targets.length,
				details: {
					naiveEntries: entriesToSyncNaively.size,
					priority: priorityFn != null,
				},
			});
		}

		if (allCoordinatesToSyncWithIblt.length === 0) {
			if (profile) {
				emitSyncProfileEvent(profile, {
					name: "rateless.dispatchMode",
					entries: properties.entries.size,
					targets: properties.targets.length,
					details: { mode: "simple-only" },
				});
				emitSyncProfileDuration(profile, startedAt, {
					name: "rateless.onMaybeMissingEntries",
					entries: properties.entries.size,
					targets: properties.targets.length,
					details: { mode: "simple-only" },
				});
			}
			return;
		}

		await ribltReady;

		// For smaller sets, the original `sqrt(n)` heuristic can occasionally under-provision
		// low-degree symbols early, causing an unnecessary `MoreSymbols` round-trip. Use a
		// small floor to make small-delta syncs more reliable without affecting large-n behavior.
		let initialSymbolCount = Math.round(
			Math.sqrt(allCoordinatesToSyncWithIblt.length),
		); // TODO choose better
		initialSymbolCount = Math.max(64, initialSymbolCount);
		const prepareStartedAt = syncProfileStart(profile);
		const { encoder, start, end, initialSymbols } = prepareStartSyncEncoder(
			allCoordinatesToSyncWithIblt,
			this.properties.numbers.maxValue,
			initialSymbolCount,
		);
		if (profile) {
			emitSyncProfileDuration(profile, prepareStartedAt, {
				name: "rateless.prepareStartSyncEncoder",
				entries: allCoordinatesToSyncWithIblt.length,
				symbols: initialSymbols?.length,
				details: {
					initialSymbolCount,
					includesInitialSymbols: initialSymbols != null,
				},
			});
		}

		let startSyncSymbols = initialSymbols;
		if (!startSyncSymbols) {
			const produceStartedAt = syncProfileStart(profile);
			startSyncSymbols = produceNextCodedSymbols(encoder, initialSymbolCount);
			if (profile) {
				emitSyncProfileDuration(profile, produceStartedAt, {
					name: "rateless.produceStartSyncSymbols",
					symbols: startSyncSymbols.length,
				});
			}
		}

		const startSync = new StartSync({
			from: start,
			to: end,
			symbols: startSyncSymbols,
		});
		const syncId = getSyncIdString(startSync);

		const clear = () => {
			encoder.free();
			clearTimeout(this.outgoingSyncProcesses.get(syncId)?.timeout);
			this.outgoingSyncProcesses.delete(syncId);
		};
		const createTimeout = () => {
			return setTimeout(clear, 1e4); // TODO arg
		};

		let lastSeqNo = -1n;
		// Keep follow-up symbol payloads bounded. Each symbol is serialized as an
		// object with three bigint fields, so very large batches can dominate heap under
		// concurrent churn even though the native RIBLT encoder itself is compact.
		const nextBatch = Math.max(
			MIN_MORE_SYMBOLS_BATCH_SIZE,
			Math.min(
				MAX_MORE_SYMBOLS_BATCH_SIZE,
				Math.ceil(allCoordinatesToSyncWithIblt.length / 4),
			),
		);
		const obj = {
			encoder,
			timeout: createTimeout(),
			refresh: () => {
				let prevTimeout = obj.timeout;
				if (prevTimeout) {
					clearTimeout(prevTimeout);
				}
				obj.timeout = createTimeout();
			},
			next: (properties: { lastSeqNo: bigint }): CodedSymbolBatch => {
				if (properties.lastSeqNo <= lastSeqNo) {
					return CodedSymbolBatch.fromSymbols([]);
				}
				lastSeqNo++;
				obj.refresh(); // TODO use timestamp instead and collective pruning/refresh

				const produceStartedAt = syncProfileStart(profile);
				const symbols = produceNextCodedSymbols(encoder, nextBatch);
				if (profile) {
					emitSyncProfileDuration(profile, produceStartedAt, {
						name: "rateless.produceMoreSymbols",
						syncId,
						symbols: symbols.length,
					});
				}
				return symbols;
			},
			free: clear,
			outgoing: properties.entries,
		};

		this.outgoingSyncProcesses.set(syncId, obj);
		if (profile) {
			emitSyncProfileEvent(profile, {
				name: "rateless.dispatchMode",
				entries: properties.entries.size,
				symbols: startSyncSymbols.length,
				targets: properties.targets.length,
				syncId,
				details: { mode: "rateless" },
			});
		}
		const sendStartedAt = syncProfileStart(profile);
		const sendResult = this.simple.rpc.send(startSync, {
			mode: new SilentDelivery({ to: properties.targets, redundancy: 1 }),
			priority: 1,
		});
		if (profile) {
			void Promise.resolve(sendResult).then(
				() =>
					emitSyncProfileDuration(profile, sendStartedAt, {
						name: "rateless.sendStartSync",
						messages: 1,
						symbols: startSyncSymbols.length,
						targets: properties.targets.length,
						syncId,
					}),
				() =>
					emitSyncProfileDuration(profile, sendStartedAt, {
						name: "rateless.sendStartSync",
						messages: 1,
						symbols: startSyncSymbols.length,
						targets: properties.targets.length,
						syncId,
						details: { rejected: true },
					}),
			);
		}
		if (profile) {
			emitSyncProfileDuration(profile, startedAt, {
				name: "rateless.onMaybeMissingEntries",
				entries: properties.entries.size,
				messages: 1,
				symbols: startSyncSymbols.length,
				targets: properties.targets.length,
				details: {
					mode: "rateless",
					ibltEntries: allCoordinatesToSyncWithIblt.length,
					naiveEntries: entriesToSyncNaively.size,
				},
			});
		}
	}

	async onMessage(
		message: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const profile = this.properties.sync?.profile;
		if (message instanceof StartSync) {
			const syncId = getSyncIdString(message);
			if (this.ingoingSyncProcesses.has(syncId)) {
				return true;
			}

			if (this.startedOrCompletedSynchronizations.has(syncId)) {
				return true;
			}

			this.startedOrCompletedSynchronizations.add(syncId);

			const wrapped = message.end < message.start;
			const decoderStartedAt = syncProfileStart(profile);
			const decoder = await this.getLocalDecoderForRange({
				start1: message.start,
				end1: wrapped ? this.properties.numbers.maxValue : message.end,
				start2: 0n,
				end2: wrapped ? message.end : 0n,
			});
			if (profile) {
				emitSyncProfileDuration(profile, decoderStartedAt, {
					name: "rateless.getLocalDecoderForRange",
					syncId,
					details: { wrapped, found: decoder !== false },
				});
			}

			if (!decoder) {
				const sendStartedAt = syncProfileStart(profile);
				await this.simple.rpc.send(
					new RequestAll({
						syncId: message.syncId,
					}),
					{
						mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
						priority: 1,
					},
				);
				if (profile) {
					emitSyncProfileDuration(profile, sendStartedAt, {
						name: "rateless.sendRequestAll",
						messages: 1,
						targets: 1,
						syncId,
					});
				}
				return true;
			}

			const createTimeout = () => {
				return setTimeout(() => {
					decoder.free();
					this.ingoingSyncProcesses.delete(syncId);
				}, 2e4); // TODO arg
			};

			let messageQueue: {
				seqNo: bigint;
				symbols: CodedSymbolInput;
			}[] = [];
			let lastSeqNo = -1n;
			const obj = {
				decoder,
				timeout: createTimeout(),
				refresh: () => {
					let prevTimeout = obj.timeout;
					if (prevTimeout) {
						clearTimeout(prevTimeout);
					}
					obj.timeout = createTimeout();
				},
				process: async (newMessage: {
					seqNo: bigint;
					symbols: CodedSymbolInput;
				}): Promise<boolean | undefined> => {
					obj.refresh(); // TODO use timestamp instead and collective pruning/refresh

					if (newMessage.seqNo <= lastSeqNo) {
						return undefined;
					}

					messageQueue.push(newMessage);
					messageQueue.sort((a, b) => Number(a.seqNo - b.seqNo));
					if (messageQueue[0].seqNo !== lastSeqNo + 1n) {
						return;
					}

					const finalizeIfDecoded = (): boolean => {
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
						this.simple.queueSync(allMissingSymbolsInRemote, context.from!);
						obj.free();
						return true;
					};

					while (
						messageQueue.length > 0 &&
						messageQueue[0].seqNo === lastSeqNo + 1n
					) {
						const symbolMessage = messageQueue.shift();
						if (!symbolMessage) {
							break;
						}

						lastSeqNo = symbolMessage.seqNo;

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
				},
				free: () => {
					decoder.free();
					clearTimeout(this.ingoingSyncProcesses.get(syncId)?.timeout);
					this.ingoingSyncProcesses.delete(syncId);
				},
			};

			this.ingoingSyncProcesses.set(syncId, obj);

			if (await obj.process({ seqNo: 0n, symbols: message.symbols })) {
				return true;
			}

			// not done, request more symbols
			const sendStartedAt = syncProfileStart(profile);
			await this.simple.rpc.send(
				new RequestMoreSymbols({
					lastSeqNo: 0n,
					syncId: message.syncId,
				}),
				{
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					priority: 1,
				},
			);
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
			const syncId = getSyncIdString(message);
			const obj = this.ingoingSyncProcesses.get(syncId);
			if (!obj) {
				return true;
			}
			const outProcess = await obj.process(message);

			if (outProcess === true) {
				return true;
			} else if (outProcess === undefined) {
				return true; // we don't have enough information, or received information that is redundant
			}

			// we are not done

			const sendStartedAt = syncProfileStart(profile);
			const sendResult = this.simple.rpc.send(
				new RequestMoreSymbols({
					lastSeqNo: message.seqNo,
					syncId: message.syncId,
				}),
				{
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					priority: 1,
				},
			);
			if (profile) {
				void Promise.resolve(sendResult).then(
					() =>
						emitSyncProfileDuration(profile, sendStartedAt, {
							name: "rateless.sendRequestMoreSymbols",
							messages: 1,
							targets: 1,
							syncId,
						}),
					() =>
						emitSyncProfileDuration(profile, sendStartedAt, {
							name: "rateless.sendRequestMoreSymbols",
							messages: 1,
							targets: 1,
							syncId,
							details: { rejected: true },
						}),
				);
			}

			return true;
		} else if (message instanceof RequestMoreSymbols) {
			const syncId = getSyncIdString(message);
			const obj = this.outgoingSyncProcesses.get(syncId);
			if (!obj) {
				return true;
			}
			const symbols = obj.next(message);
			const sendStartedAt = syncProfileStart(profile);
			await this.properties.rpc.send(
				new MoreSymbols({
					lastSeqNo: message.lastSeqNo,
					syncId: message.syncId,
					symbols,
				}),
				{
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					priority: 1,
				},
			);
			if (profile) {
				emitSyncProfileDuration(profile, sendStartedAt, {
					name: "rateless.sendMoreSymbols",
					messages: 1,
					symbols: symbols.length,
					targets: 1,
					syncId,
				});
			}
			return true;
		} else if (message instanceof RequestAll) {
			const p = this.outgoingSyncProcesses.get(getSyncIdString(message));
			if (!p) {
				return true;
			}
			await this.simple.onMaybeMissingEntries({
				entries: p.outgoing,
				targets: [context.from!.hashcode()],
			});
			return true;
		}
		return this.simple.onMessage(message, context);
	}

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void {
		return this.simple.onReceivedEntries(properties);
	}

	onEntryAdded(entry: Entry<any>): void {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryAdded(entry);
	}

	onEntryRemoved(hash: string) {
		this.invalidateLocalRangeEncoderCache();
		return this.simple.onEntryRemoved(hash);
	}

	onPeerDisconnected(key: PublicSignKey | string) {
		return this.simple.onPeerDisconnected(key);
	}

	open(): Promise<void> | void {
		return this.simple.open();
	}

	close(): Promise<void> | void {
		for (const [, obj] of this.ingoingSyncProcesses) {
			obj.free();
		}
		for (const [, obj] of this.outgoingSyncProcesses) {
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
