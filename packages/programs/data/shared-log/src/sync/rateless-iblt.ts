import { field, variant, vec } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { type PublicSignKey, randomBytes, toBase64 } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, Log } from "@peerbit/log";
import init, { DecoderWrapper, EncoderWrapper } from "@peerbit/riblt";
import type { RPC, RequestContext } from "@peerbit/rpc";
import { SilentDelivery } from "@peerbit/stream-interface";
import type { SyncableKey, Syncronizer } from ".";
import { type EntryWithRefs } from "../exchange-heads.js";
import { MAX_U64 } from "../integers.js";
import { TransportMessage } from "../message.js";
import {
	type EntryReplicated,
	type ReplicationRangeIndexable,
	matchEntriesInRangeQuery,
} from "../ranges.js";
import { SimpleSyncronizer } from "./simple.js";

const wasmFetch = async (input: any) =>
	(await (await import("node:fs/promises")).readFile(input)) as any; // TODO fix types.
globalThis.fetch = wasmFetch; // wasm-pack build --target web generated load with 'fetch' but node fetch can not load wasm yet, so we need to do this
await init();

class SymbolSerialized implements Symbol {
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

const getSyncIdString = (message: { syncId: Uint8Array }) => {
	return toBase64(message.syncId);
};

@variant([3, 0])
export class StartSync extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	@field({ type: "u64" })
	start: bigint;

	@field({ type: "u64" })
	end: bigint;

	@field({ type: vec(SymbolSerialized) })
	symbols: SymbolSerialized[];

	constructor(props: {
		from: bigint;
		to: bigint;
		symbols: SymbolSerialized[];
	}) {
		super();
		this.syncId = randomBytes(32);
		this.start = props.from;
		this.end = props.to;
		this.symbols = props.symbols;
	}
}

@variant([3, 1])
export class MoreSymbols extends TransportMessage {
	@field({ type: Uint8Array })
	syncId: Uint8Array;

	@field({ type: "u64" })
	seqNo: bigint;

	@field({ type: vec(SymbolSerialized) })
	symbols: SymbolSerialized[];

	constructor(props: {
		syncId: Uint8Array;
		lastSeqNo: bigint;
		symbols: SymbolSerialized[];
	}) {
		super();
		this.syncId = props.syncId;
		this.seqNo = props.lastSeqNo + 1n;
		this.symbols = props.symbols;
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

export interface Symbol {
	count: bigint;
	hash: bigint;
	symbol: bigint;
}

const buildEncoderOrDecoderFromRange = async <
	T extends "encoder" | "decoder",
	E = T extends "encoder" ? EncoderWrapper : DecoderWrapper,
>(
	ranges: {
		start1: bigint;
		end1: bigint;
		start2: bigint;
		end2: bigint;
	},
	entryIndex: Index<EntryReplicated<"u64">>,
	type: T,
): Promise<E | false> => {
	const encoder =
		type === "encoder" ? new EncoderWrapper() : new DecoderWrapper();

	/* const buildDecoderStart = +new Date(); */
	let symbolCount = 0;
	const hashes = new Set<any>();

	const entries = await entryIndex
		.iterate(
			{
				query: matchEntriesInRangeQuery({
					end1: ranges.end1,
					start1: ranges.start1,
					end2: ranges.end2,
					start2: ranges.start2,
				}),
			},
			{
				shape: {
					coordinates: true,
				},
			},
		)
		.all();

	if (entries.length === 0) {
		return false;
	}

	for (const entry of entries) {
		symbolCount++;
		for (const coordinate of entry.value.coordinates) {
			encoder.add_symbol(BigInt(coordinate));
		}
		hashes.add(entry.value);
	}

	/* console.log(
		(type === "encoder" ? "Encoder" : "Decoder") + " build time (s): ",
		(+new Date() - buildDecoderStart) / 1000,
		"Symbols: ",
		symbolCount,
		", Hashes size: ",
		+hashes.size,
		", Range: ",
		ranges,
	); */
	return encoder as E;
};
/* 
class RangeToEncoders {
	encoders: Map<string, EncoderWrapper>;

	constructor(
		readonly me: PublicSignKey,
		readonly rangeIndex: Index<ReplicationRangeIndexable<"u64">>,
		readonly entryIndex: Index<EntryReplicated<"u64">>,
	) {
		this.encoders = new Map();
	}

	async build() {
		// for all ranges in rangeIndex that belong to me
		// fetch all cursors from entryIndex and build encoder with key from rangeId
		for (const range of await this.rangeIndex
			.iterate({ query: { hash: this.me.hashcode() } })
			.all()) {
			const encoder = await buildEncoderOrDecoderFromRange(
				range.value,
				this.entryIndex,
				"encoder",
			);
			this.encoders.set(range.value.toUniqueSegmentId(), encoder);
		}
	}

	createSymbolGenerator(range: ReplicationRangeIndexable<"u64">): {
		next: () => Symbol;
		free: () => void;
	} {
		let encoder = this.encoders.get(range.toUniqueSegmentId());
		if (!encoder) {
			throw new Error("No encoder found for range");
		}
		const cloned = encoder.clone();
		return {
			next: (): Symbol => {
				return cloned.produce_next_coded_symbol();
			},
			free: () => {
				// TODO?
			},
		};
	}
}



const getAllOverlappingRanges = async (properties: {
	range: {
		// To match
		start1: bigint | number;
		end1: bigint | number;
		start2: bigint | number;
		end2: bigint | number;
	};
	publicKey: PublicSignKey;
	rangeIndex: Index<ReplicationRangeIndexable<"u64">, any>;
}): Promise<IndexedResults<ReplicationRangeIndexable<"u64">>> => {
	const ranges = await properties.rangeIndex
		.iterate({
			query: [
				...getCoveringRangeQuery(properties.range),
				new StringMatch({
					key: "hash",
					value: properties.publicKey.hashcode(),
				}),
			],
		})
		.all();
	return ranges;
}; */

/* const getMissingValuesInRemote = async (properties: {
	myEncoder: RangeToEncoders;
	remoteRange: {
		start1: bigint;
		end1: bigint;
		start2: bigint;
		end2: bigint;
	};
}) => {
	const findOverlappingRangesIOwn = await getAllOverlappingRanges({
		range: properties.remoteRange,
		publicKey: properties.myEncoder.me,
		rangeIndex: properties.myEncoder.rangeIndex,
	});

	const decoders: Map<string, DecoderWrapper> = new Map();
	for (const range of findOverlappingRangesIOwn) {
		const segmentId = range.value.toUniqueSegmentId();
		const encoder: EncoderWrapper | undefined =
			properties.myEncoder.encoders.get(segmentId);
		if (encoder) {
			decoders.set(segmentId, encoder.to_decoder());
		}
	}

	return {
		process: (encodedSymbol: any) => {
			let allMissingSymbols: any[] = [];
			for (const [k, decoder] of decoders) {
				decoder.add_coded_symbol(encodedSymbol);
				decoder.try_decode();
				if (decoder.decoded()) {
					for (const missingSymbol of decoder.get_local_symbols()) {
						allMissingSymbols.push(missingSymbol);
					}
					decoders.delete(k);
				}
			}
			return {
				missing: allMissingSymbols,
				done: decoders.size === 0,
			};
		},
	};
};

export { RangeToEncoders, getMissingValuesInRemote }; */

export class RatelessIBLTSynchronizer implements Syncronizer<"u64"> {
	simple: SimpleSyncronizer<"u64">;

	ingoingSyncProcesses: Map<
		string,
		{
			decoder: DecoderWrapper;
			timeout: ReturnType<typeof setTimeout>;
			refresh: () => void;
			process: (symbol: Symbol) => Promise<boolean>;
			free: () => void;
		}
	>;

	outgoingSyncProcesses: Map<
		string,
		{
			outgoing: Map<string, EntryReplicated<"u64">>;
			encoder: EncoderWrapper;
			timeout: ReturnType<typeof setTimeout>;
			refresh: () => void;
			next: () => Symbol;
			free: () => void;
		}
	>;

	constructor(
		readonly properties: {
			rpc: RPC<TransportMessage, TransportMessage>;
			rangeIndex: Index<ReplicationRangeIndexable<"u64">, any>;
			entryIndex: Index<EntryReplicated<"u64">, any>;
			log: Log<any>;
			coordinateToHash: Cache<string>;
		},
	) {
		this.simple = new SimpleSyncronizer(properties);
		this.outgoingSyncProcesses = new Map();
		this.ingoingSyncProcesses = new Map();
	}

	onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<"u64">>;
		targets: string[];
	}): Promise<void> | void {
		// calculate the smallest range that covers all the entries
		// calculate the largest gap and the smallest range  will be the one that starts at the end of it

		// assume sorted, and find the largest gap
		let largestGap = 0n;
		let largestGapIndex = 0;
		const sortedEntries = Array.from(properties.entries.values())
			.map((x) => x.coordinates)
			.flat()
			.sort((a, b) => {
				if (a > b) {
					return 1;
				} else if (a < b) {
					return -1;
				} else {
					return 0;
				}
			});

		for (let i = 0; i < sortedEntries.length - 1; i++) {
			const current = sortedEntries[i];
			const next = sortedEntries[i + 1];
			const gap = next >= current ? next - current : MAX_U64 - current + next;
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
			if (end > MAX_U64) {
				end = 0n;
			}
		} else {
			start = smallestRangeStart;
			end = smallestRangeEnd;
		}

		const startSync = new StartSync({ from: start, to: end, symbols: [] });
		const encoder = new EncoderWrapper();
		for (const entry of sortedEntries) {
			encoder.add_symbol(BigInt(entry));
		}

		let initialSymbols = 10; // TODO arg
		for (let i = 0; i < initialSymbols; i++) {
			startSync.symbols.push(
				new SymbolSerialized(encoder.produce_next_coded_symbol()),
			);
		}

		const createTimeout = () => {
			return setTimeout(() => {
				// 	encoder.free(); TODO?
				this.outgoingSyncProcesses.delete(getSyncIdString(startSync));
			}, 2e4); // TODO arg
		};

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
			next: (): Symbol => {
				obj.refresh(); // TODO use timestamp instead and collective pruning/refresh
				return encoder.produce_next_coded_symbol();
			},
			free: () => {
				// encoder.free(); TODO?
				clearTimeout(
					this.outgoingSyncProcesses.get(getSyncIdString(startSync))?.timeout,
				);
				this.outgoingSyncProcesses.delete(getSyncIdString(startSync));
			},
			outgoing: properties.entries,
		};

		this.outgoingSyncProcesses.set(getSyncIdString(startSync), obj);
		this.simple.rpc.send(startSync, {
			mode: new SilentDelivery({ to: properties.targets, redundancy: 1 }),
			priority: 1,
		});
	}

	async onMessage(
		message: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		if (message instanceof StartSync) {
			const wrapped = message.end < message.start;
			const decoder = await buildEncoderOrDecoderFromRange(
				{
					start1: message.start,
					end1: wrapped ? MAX_U64 : message.end,
					start2: 0n,
					end2: wrapped ? message.end : 0n,
				},
				this.properties.entryIndex,
				"decoder",
			);

			if (!decoder) {
				await this.simple.rpc.send(
					new RequestAll({
						syncId: message.syncId,
					}),
					{
						mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
						priority: 1,
					},
				);
				return true;
			}

			/* console.log(
				"ALREADY HAVE ENTRIES",
				await this.properties.entryIndex.count(),
				"but log ?",
				this.properties.log.length,
			); */

			const syncId = getSyncIdString(message);
			const createTimeout = () => {
				return setTimeout(() => {
					// decoder.free(); TODO?
					this.ingoingSyncProcesses.delete(syncId);
				}, 2e4); // TODO arg
			};

			let count = 0;
			/* let t0 = +new Date(); */
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
				process: async (
					symbol: Symbol | SymbolSerialized,
				): Promise<boolean> => {
					obj.refresh(); // TODO use timestamp instead and collective pruning/refresh
					decoder.add_coded_symbol(symbol);
					decoder.try_decode();
					count++;
					if (decoder.decoded()) {
						let allMissingSymbolsInRemote: bigint[] = [];
						for (const missingSymbol of decoder.get_remote_symbols()) {
							allMissingSymbolsInRemote.push(missingSymbol);
						}

						/* 	let t1 = +new Date();
							console.log("Done decoding after", count, "symbols", "allMissingSymbolsInRemote: ", allMissingSymbolsInRemote.length, "time: ", (t1 - t0) / 1000, "s"); */

						// now we want to resolve the hashes from the symbols
						this.simple.queueSync(allMissingSymbolsInRemote, context.from!, {
							skipCheck: true,
						});
						obj.free();
						return true;
					}
					return false;
				},
				free: () => {
					// decoder.free(); TODO?
					clearTimeout(this.ingoingSyncProcesses.get(syncId)?.timeout);
					this.ingoingSyncProcesses.delete(syncId);
				},
			};

			this.ingoingSyncProcesses.set(syncId, obj);

			for (const symbol of message.symbols) {
				if (await obj.process(symbol)) {
					return true; // DONE
				}
			}

			// not done, request more symbols
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

			return true;
		} else if (message instanceof MoreSymbols) {
			const obj = this.ingoingSyncProcesses.get(getSyncIdString(message));
			if (!obj) {
				return true;
			}

			for (const symbol of message.symbols) {
				if (await obj.process(symbol)) {
					return true; // DONE
				}
			}

			this.simple.rpc.send(
				new RequestMoreSymbols({
					lastSeqNo: message.seqNo,
					syncId: message.syncId,
				}),
				{
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					priority: 1,
				},
			);

			return true;
		} else if (message instanceof RequestMoreSymbols) {
			const obj = this.outgoingSyncProcesses.get(getSyncIdString(message));
			if (!obj) {
				return true;
			}

			const symbols = [];
			let batch = 100; // TODO arg
			for (let i = 0; i < batch; i++) {
				symbols.push(new SymbolSerialized(obj.next()));
			}
			await this.properties.rpc.send(
				new MoreSymbols({
					lastSeqNo: message.lastSeqNo + 1n,
					syncId: message.syncId,
					symbols,
				}),
				{
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					priority: 1,
				},
			);
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
		return this.simple.onEntryAdded(entry);
	}

	onEntryRemoved(hash: string) {
		return this.simple.onEntryRemoved(hash);
	}

	onPeerDisconnected(key: PublicSignKey) {
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
		return this.simple.close();
	}

	get syncInFlight(): Map<string, Map<SyncableKey, { timestamp: number }>> {
		return this.simple.syncInFlight;
	}

	get pending(): number {
		return this.simple.pending;
	}
}
