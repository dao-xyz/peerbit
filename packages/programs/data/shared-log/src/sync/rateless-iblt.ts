import { field, variant, vec } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { type PublicSignKey, randomBytes, toBase64 } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, Log } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { DecoderWrapper, EncoderWrapper } from "@peerbit/riblt";
import type { RPC, RequestContext } from "@peerbit/rpc";
import { SilentDelivery } from "@peerbit/stream-interface";
import type { SyncableKey, Syncronizer } from "./index.js";
import { type EntryWithRefs } from "../exchange-heads.js";
import { type Numbers } from "../integers.js";
import { TransportMessage } from "../message.js";
import {
	type EntryReplicated,
	type ReplicationRangeIndexable,
	matchEntriesInRangeQuery,
} from "../ranges.js";
import { SimpleSyncronizer } from "./simple.js";

export const logger: ReturnType<typeof loggerFn> = loggerFn({
	module: "shared-log",
});

type NumberOrBigint = number | bigint;

const coerceBigInt = (value: NumberOrBigint): bigint =>
	typeof value === "bigint" ? value : BigInt(value);

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
		from: NumberOrBigint;
		to: NumberOrBigint;
		symbols: SymbolSerialized[];
	}) {
		super();
		this.syncId = randomBytes(32);
		this.start = coerceBigInt(props.from);
		this.end = coerceBigInt(props.to);
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

export interface SSymbol {
	count: bigint;
	hash: bigint;
	symbol: bigint;
}

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
): Promise<E | false> => {
	const encoder =
		type === "encoder" ? new EncoderWrapper() : new DecoderWrapper();

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
					hash: true,
					hashNumber: true,
				},
			},
		)
		.all();

	if (entries.length === 0) {
		return false;
	}

	for (const entry of entries) {
		encoder.add_symbol(coerceBigInt(entry.value.hashNumber));
	}
	return encoder as E;
};

export class RatelessIBLTSynchronizer<D extends "u32" | "u64">
	implements Syncronizer<D>
{
	simple: SimpleSyncronizer<D>;

	startedOrCompletedSynchronizations: Cache<string>;
	ingoingSyncProcesses: Map<
		string,
		{
			decoder: DecoderWrapper;
			timeout: ReturnType<typeof setTimeout>;
			refresh: () => void;
			process: (message: {
				seqNo: bigint;
				symbols: SSymbol[];
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
			next: (message: { lastSeqNo: bigint }) => SSymbol[];
			free: () => void;
		}
	>;

	constructor(
		readonly properties: {
			rpc: RPC<TransportMessage, TransportMessage>;
			rangeIndex: Index<ReplicationRangeIndexable<D>, any>;
			entryIndex: Index<EntryReplicated<D>, any>;
			log: Log<any>;
			coordinateToHash: Cache<string>;
			numbers: Numbers<D>;
		},
	) {
		this.simple = new SimpleSyncronizer(properties);
		this.outgoingSyncProcesses = new Map();
		this.ingoingSyncProcesses = new Map();
		this.startedOrCompletedSynchronizations = new Cache({ max: 1e4 });
	}

	async onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<D>>;
		targets: string[];
	}): Promise<void> {
		// Strategy:
		// - For small sets, prefer the simple synchronizer to reduce complexity and avoid
		//   IBLT overhead on tiny batches.
		// - For large sets, use IBLT, but still allow simple sync for special-case entries
		//   such as those assigned to range boundaries.

		let entriesToSyncNaively: Map<string, EntryReplicated<D>> = new Map();
		let allCoordinatesToSyncWithIblt: bigint[] = [];
		let minSyncIbltSize = 333; // TODO: make configurable
		let maxSyncWithSimpleMethod = 1e3;

		// Small batch => use simple synchronizer entirely
		if (properties.entries.size <= minSyncIbltSize) {
			await this.simple.onMaybeMissingEntries({
				entries: properties.entries,
				targets: properties.targets,
			});
			return;
		}

		// Mixed strategy for larger batches
		for (const entry of properties.entries.values()) {
			if (entry.assignedToRangeBoundary) {
				entriesToSyncNaively.set(entry.hash, entry);
			} else {
				allCoordinatesToSyncWithIblt.push(coerceBigInt(entry.hashNumber));
			}
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
			allCoordinatesToSyncWithIblt = Array.from(
				properties.entries.values(),
			).map((x) => coerceBigInt(x.hashNumber));
		}

		if (allCoordinatesToSyncWithIblt.length === 0) {
			return;
		}

		const sortedEntries = allCoordinatesToSyncWithIblt.sort((a, b) => {
			if (a > b) {
				return 1;
			} else if (a < b) {
				return -1;
			} else {
				return 0;
			}
		});

		// assume sorted, and find the largest gap
		let largestGap = 0n;
		let largestGapIndex = 0;
		for (let i = 0; i < sortedEntries.length - 1; i++) {
			const current = sortedEntries[i];
			const next = sortedEntries[i + 1];
			const gap =
				next >= current
					? next - current
					: coerceBigInt(this.properties.numbers.maxValue) - current + next;
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
			if (end > this.properties.numbers.maxValue) {
				end = 0n;
			}
		} else {
			start = smallestRangeStart;
			end = smallestRangeEnd;
		}

		const startSync = new StartSync({ from: start, to: end, symbols: [] });
		const encoder = new EncoderWrapper();
		for (const entry of sortedEntries) {
			encoder.add_symbol(coerceBigInt(entry));
		}

		let initialSymbols = Math.round(
			Math.sqrt(allCoordinatesToSyncWithIblt.length),
		); // TODO choose better
		for (let i = 0; i < initialSymbols; i++) {
			startSync.symbols.push(
				new SymbolSerialized(encoder.produce_next_coded_symbol()),
			);
		}

		const clear = () => {
			encoder.free();
			clearTimeout(
				this.outgoingSyncProcesses.get(getSyncIdString(startSync))?.timeout,
			);
			this.outgoingSyncProcesses.delete(getSyncIdString(startSync));
		};
		const createTimeout = () => {
			return setTimeout(clear, 1e4); // TODO arg
		};

		let lastSeqNo = -1n;
		let nextBatch = 1e4;
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
			next: (properties: { lastSeqNo: bigint }): SSymbol[] => {
				if (properties.lastSeqNo <= lastSeqNo) {
					return [];
				}
				lastSeqNo++;
				obj.refresh(); // TODO use timestamp instead and collective pruning/refresh

				let result: SSymbol[] = [];
				for (let i = 0; i < nextBatch; i++) {
					result.push(encoder.produce_next_coded_symbol());
				}
				return result;
			},
			free: clear,
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
			const syncId = getSyncIdString(message);
			if (this.ingoingSyncProcesses.has(syncId)) {
				return true;
			}

			if (this.startedOrCompletedSynchronizations.has(syncId)) {
				return true;
			}

			this.startedOrCompletedSynchronizations.add(syncId);

			const wrapped = message.end < message.start;
			const decoder = await buildEncoderOrDecoderFromRange(
				{
					start1: message.start,
					end1: wrapped ? this.properties.numbers.maxValue : message.end,
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

			const createTimeout = () => {
				return setTimeout(() => {
					decoder.free();
					this.ingoingSyncProcesses.delete(syncId);
				}, 2e4); // TODO arg
			};

			let messageQueue: {
				seqNo: bigint;
				symbols: (SSymbol | SymbolSerialized)[];
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
					symbols: (SSymbol | SymbolSerialized)[];
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

						const allMissingSymbolsInRemote: bigint[] = [];
						for (const missingSymbol of decoder.get_remote_symbols()) {
							allMissingSymbolsInRemote.push(missingSymbol);
						}

						this.simple.queueSync(allMissingSymbolsInRemote, context.from!, {
							skipCheck: true,
						});
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

						for (const symbol of symbolMessage.symbols) {
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
									logger.debug(
										"Decoder reported invalid degree; waiting for more symbols",
									);
									continue;
								}
								throw error;
							}
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
			const outProcess = await obj.process(message);

			if (outProcess === true) {
				return true;
			} else if (outProcess === undefined) {
				return true; // we don't have enough information, or received information that is redundant
			}

			// we are not done

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
			await this.properties.rpc.send(
				new MoreSymbols({
					lastSeqNo: message.lastSeqNo,
					syncId: message.syncId,
					symbols: obj.next(message).map((x) => new SymbolSerialized(x)),
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
