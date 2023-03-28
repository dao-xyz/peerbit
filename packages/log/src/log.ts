import { EntryIndex } from "./entry-index.js";
import { LogIO } from "./log-io.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import {
	EntryFetchAllOptions,
	EntryFetchOptions,
	strictFetchOptions,
} from "./entry-io.js";
import { isDefined } from "./is-defined.js";
import { findUniques } from "./find-uniques.js";
import {
	EncryptionTemplateMaybeEncrypted,
	Entry,
	Payload,
	CanAppend,
} from "./entry.js";
import {
	HLC,
	LamportClock as Clock,
	LamportClock,
	Timestamp,
} from "./clock.js";
import {
	PublicKeyEncryptionResolver,
	SignatureWithKey,
} from "@dao-xyz/peerbit-crypto";
import { serialize } from "@dao-xyz/borsh";
import { Encoding, JSON_ENCODING } from "./encoding.js";
import { Identity } from "./identity.js";
import { HeadsIndex } from "./heads.js";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";
import { EntryNode, Values } from "./values.js";
import { Trim, TrimOptions } from "./trim.js";
import { logger } from "./logger.js";
import { Cache } from "@dao-xyz/cache";

const { LastWriteWins, NoZeroes } = Sorting;
const randomId = () => new Date().getTime().toString();
const getHash = <T>(e: Entry<T>) => e.hash;

export type Change<T> = { added: Entry<T>[]; removed: Entry<T>[] };
export type Change2<T> = { added: Entry<T>; removed: Entry<T>[] };

export type LogOptions<T> = {
	encryption?: PublicKeyEncryptionResolver;
	encoding?: Encoding<T>;
	logId?: string;
	clock?: LamportClock;
	sortFn?: Sorting.ISortFunction;
	concurrency?: number;
	trim?: TrimOptions;
};

const ENTRY_CACHE_MAX = 1000; // TODO as param

/**
 * @description
 * Log implements a G-Set CRDT and adds ordering.
 *
 * From:
 * "A comprehensive study of Convergent and Commutative Replicated Data Types"
 * https://hal.inria.fr/inria-00555588
 */

export class Log<T> {
	private _sortFn: Sorting.ISortFunction;
	private _storage: BlockStore;
	private _id: string;
	private _hlc: HLC;

	// Identity
	private _identity: Identity;

	// Keeping track of entries
	private _entryIndex: EntryIndex<T>;
	private _headsIndex: HeadsIndex<T>;
	private _values: Values<T>;

	// Index of all next pointers in this log
	private _nextsIndex: Map<string, Set<string>>;
	private _encryption?: PublicKeyEncryptionResolver;
	private _encoding: Encoding<T>;
	private _trim: Trim<T>;
	private _entryCache: Cache<Entry<T>>;

	constructor(
		store: BlockStore,
		identity: Identity,
		options: LogOptions<T> = {}
	) {
		if (!isDefined(store)) {
			throw LogError.BlockStoreNotDefinedError();
		}

		if (!isDefined(identity)) {
			throw new Error("Identity is required");
		}
		//
		const { logId, encoding, concurrency, trim, encryption } = options;
		let { sortFn } = options;

		if (!isDefined(sortFn)) {
			sortFn = LastWriteWins;
		}
		sortFn = sortFn as Sorting.ISortFunction;

		this._sortFn = NoZeroes(sortFn);

		this._storage = store;
		this._id = logId || randomId();
		/*     this._rootGid = rootGid;
		 */

		// Identity
		this._identity = identity;

		// encoder/decoder
		this._encryption = encryption;
		this._encoding = encoding || JSON_ENCODING;

		// Add entries to the internal cache

		// Index of all next pointers in this log

		// Clock
		this._hlc = new HLC();

		this._nextsIndex = new Map();
		this._headsIndex = new HeadsIndex({});
		this._entryCache = new Cache({ max: ENTRY_CACHE_MAX });
		this._entryIndex = new EntryIndex({
			store: this._storage,
			init: (e) => e.init(this),
			cache: this._entryCache,
		});
		this._values = new Values(this._entryIndex, this._sortFn);
		this._trim = new Trim(
			{
				deleteNode: async (node: EntryNode) => {
					// TODO check if we have before delete?
					const entry = await this.get(node.value.hash);
					if (entry) {
						this.values.deleteNode(node);
						await this.entryIndex.delete(node.value.hash);
						this.headsIndex.del(node.value);
						this.nextsIndex.delete(node.value.hash);
						await this.storage.rm(node.value.hash);
					}
					return entry;
				},
				values: () => this.values,
			},
			trim
		);
	}

	get id() {
		return this._id;
	}

	/**
	 * Returns the length of the log.
	 */
	get length() {
		return this._values.length;
	}

	get values(): Values<T> {
		return this._values;
	}

	/**
	 * Checks if a entry is part of the log
	 * @param {string} hash The hash of the entry
	 * @returns {boolean}
	 */

	has(cid: string) {
		return this._entryIndex._index.has(cid);
	}
	/**
	 * Get all entries sorted. Don't use this method anywhere where performance matters
	 */
	toArray(): Promise<Entry<T>[]> {
		// we call init, because the values might be unitialized
		return this._values.toArray().then((arr) => arr.map((x) => x.init(this)));
	}

	/**
	 * Returns the head index
	 */
	get headsIndex(): HeadsIndex<T> {
		return this._headsIndex;
	}

	/**
	 * Don't use this anywhere performance matters
	 */
	async getHeads(): Promise<Entry<T>[]> {
		const heads: Promise<Entry<T> | undefined>[] = new Array(
			this.headsIndex.index.size
		);
		let i = 0;
		for (const hash of this.headsIndex.index) {
			heads[i++] = this._entryIndex.get(hash).then((x) => x?.init(this));
		}
		const resolved = await Promise.all(heads);
		const defined = resolved.filter((x) => !!x);
		if (defined.length !== resolved.length) {
			logger.error("Failed to resolve all heads");
		}
		return defined as Entry<T>[];
	}

	/**
	 * Returns an array of Entry objects that reference entries which
	 * are not in the log currently.
	 * @returns {Array<Entry<T>>}
	 */
	async getTails(): Promise<Entry<T>[]> {
		return Log.findTails(await this.toArray());
	}

	/**
	 * Returns an array of hashes that are referenced by entries which
	 * are not in the log currently.
	 * @returns {Array<string>} Array of hashes
	 */
	async getTailHashes(): Promise<string[]> {
		return Log.findTailHashes(await this.toArray());
	}

	/**
	 * Get local HLC
	 */
	get hlc(): HLC {
		return this._hlc;
	}

	get identity(): Identity {
		return this._identity;
	}

	get storage(): BlockStore {
		return this._storage;
	}

	get nextsIndex(): Map<string, Set<string>> {
		return this._nextsIndex;
	}

	get entryIndex(): EntryIndex<T> {
		return this._entryIndex;
	}

	get encryption() {
		return this._encryption;
	}

	get encoding() {
		return this._encoding;
	}

	get sortFn() {
		return this._sortFn;
	}

	/**
	 * Set the identity for the log
	 * @param {Identity} [identity] The identity to be set
	 */
	setIdentity(identity: Identity) {
		this._identity = identity;
	}

	/**
	 * Find an entry.
	 * @param {string} [hash] The hashes of the entry
	 */
	get(hash: string): Promise<Entry<T> | undefined> {
		return this._entryIndex.get(hash);
	}

	async traverse(
		rootEntries: Entry<T>[],
		amount = -1,
		endHash?: string
	): Promise<{ [key: string]: Entry<T> }> {
		// Sort the given given root entries and use as the starting stack
		let stack: Entry<T>[] = rootEntries.sort(this._sortFn).reverse();

		// Cache for checking if we've processed an entry already
		let traversed: { [key: string]: boolean } = {};
		// End result
		const result: { [key: string]: Entry<T> } = {};
		let count = 0;
		// Named function for getting an entry from the log
		const getEntry = (e: string) => this.get(e);

		// Add an entry to the stack and traversed nodes index
		const addToStack = (entry: Entry<T>) => {
			// If we've already processed the Entry<T>, don't add it to the stack
			if (!entry || traversed[entry.hash]) {
				return;
			}

			// Add the entry in front of the stack and sort
			stack = [entry, ...stack].sort(this._sortFn).reverse();
			// Add to the cache of processed entries
			traversed[entry.hash] = true;
		};

		const addEntry = (rootEntry: Entry<T>) => {
			result[rootEntry.hash] = rootEntry;
			traversed[rootEntry.hash] = true;
			count++;
		};

		// Start traversal
		// Process stack until it's empty (traversed the full log)
		// or when we have the requested amount of entries
		// If requested entry amount is -1, traverse all
		while (stack.length > 0 && (count < amount || amount < 0)) {
			// eslint-disable-line no-unmodified-loop-condition
			// Get the next element from the stack
			const entry = stack.shift();
			if (!entry) {
				throw new Error("Unexpected");
			}
			// Add to the result
			addEntry(entry);
			// If it is the specified end hash, break out of the while loop
			if (endHash && endHash === entry.hash) break;

			// Add entry's next references to the stack
			const entries = (await Promise.all(entry.next.map(getEntry))).filter(
				(x) => !!x
			) as Entry<any>[];
			entries.forEach(addToStack);
		}

		stack = [];
		traversed = {};
		// End result
		return result;
	}

	async getReferenceSamples(
		from: Entry<T>,
		options?: { pointerCount?: number; memoryLimit?: number }
	): Promise<Entry<T>[]> {
		const hashes = new Set<string>();
		const pointerCount = options?.pointerCount || 0;
		const memoryLimit = options?.memoryLimit;
		const maxDistance = Math.min(pointerCount, this._values.length);
		if (maxDistance === 0) {
			return [];
		}
		hashes.add(from.hash);
		let memoryCounter = from._payload.byteLength;
		if (from.next?.length > 0 && pointerCount >= 2) {
			let next = new Set(from.next);
			let prev = 2;
			outer: for (let i = 2; i <= maxDistance - 1; i *= 2) {
				for (let j = prev; j < i; j++) {
					if (next.size === 0) {
						break outer;
					}
					const nextNext = new Set<string>();
					for (const n of next) {
						const nentry = await this.get(n);
						nentry?.next.forEach((n2) => {
							nextNext.add(n2);
						});
					}
					next = nextNext;
				}

				prev = i;
				if (next) {
					for (const n of next) {
						if (!memoryLimit) {
							hashes.add(n);
						} else {
							const entry = await this.get(n);
							if (!entry) {
								break outer;
							}
							memoryCounter += entry._payload.byteLength;
							if (memoryCounter > memoryLimit) {
								break outer;
							}
							hashes.add(n);
						}
						if (hashes.size === pointerCount) {
							break outer;
						}
					}
				}
			}
		}

		const ret: Entry<any>[] = [];
		for (const hash of hashes) {
			const entry = await this.get(hash);
			if (entry) {
				ret.push(entry);
			}
		}
		return ret;
	}

	/**
	 * Append an entry to the log.
	 * @param {Entry} entry Entry to add
	 * @return {Log} New Log containing the appended value
	 */
	async append(
		data: T,
		options: {
			canAppend?: CanAppend<T>;
			gidSeed?: Uint8Array;
			nexts?: Entry<any>[];
			identity?: Identity;
			signers?: ((data: Uint8Array) => Promise<SignatureWithKey>)[];
			reciever?: EncryptionTemplateMaybeEncrypted;
			onGidsShadowed?: (gids: string[]) => void;
			trim?: TrimOptions;
			timestamp?: Timestamp;
		} = {}
	): Promise<{ entry: Entry<T>; removed: Entry<T>[] }> {
		if (options.reciever && !this._encryption) {
			throw new Error(
				"Message is intended to be encrypted but no encryption methods are provided for the log"
			);
		}

		// Update the clock (find the latest clock)
		if (options.nexts) {
			options.nexts.forEach((n) => {
				if (!n.hash)
					throw new Error(
						"Expecting nexts to already be saved. missing hash for one or more entries"
					);
			});
		}

		const hasNext = !!options.nexts;
		const nexts: Entry<any>[] = options.nexts || (await this.getHeads());

		// Calculate max time for log/graph
		const clock = new Clock({
			id: new Uint8Array(serialize(this._identity.publicKey)),
			timestamp: options.timestamp || this._hlc.now(),
		});

		const identity = options.identity || this._identity;

		const entry = await Entry.create<T>({
			store: this._storage,
			identity: identity,
			signers: options.signers,
			data,
			clock,
			encoding: this._encoding,
			next: nexts,
			gidSeed: options.gidSeed,
			encryption: options.reciever
				? {
						options: this._encryption as PublicKeyEncryptionResolver,
						reciever: {
							...options.reciever,
						},
				  }
				: undefined,
			canAppend: options.canAppend,
		});

		if (!isDefined(entry.hash)) {
			throw new Error("Unexpected");
		}

		nexts.forEach((e) => {
			let nextIndexSet = this._nextsIndex.get(e.hash);
			if (!nextIndexSet) {
				nextIndexSet = new Set();
				nextIndexSet.add(entry.hash);
				this._nextsIndex.set(e.hash, nextIndexSet);
			} else {
				nextIndexSet.add(entry.hash);
			}
		});

		const removedGids: Set<string> = new Set();
		if (hasNext) {
			nexts.forEach((next) => {
				const deletion = this._headsIndex.del(next);
				if (deletion.lastWithGid && next.gid !== entry.gid) {
					removedGids.add(next.gid);
				}
			});
		} else {
			// next is all heads, which means we should just overwrite
			for (const key of this.headsIndex.gids.keys()) {
				if (key !== entry.gid) {
					removedGids.add(key);
				}
			}
			this.headsIndex.reset([entry]);
		}

		await this._entryIndex.set(entry);
		this._headsIndex.put(entry);
		await this._values.put(entry);

		// if next contails all gids
		if (options.onGidsShadowed && removedGids.size > 0) {
			options.onGidsShadowed([...removedGids]);
		}

		entry.init({ encoding: this._encoding, encryption: this._encryption });
		const removed = await this.trim(options?.trim);
		return { entry, removed };
	}

	async reset(entries: Entry<T>[], heads?: Entry<T>[]) {
		this._nextsIndex = new Map();
		this._entryIndex = new EntryIndex({
			store: this._storage,
			init: (e) => e.init(this),
			cache: this._entryCache,
		});
		const promises: Promise<any>[] = [];
		const set = new Set<string>();
		const uniqueEntries: Entry<T>[] = [];
		for (const entry of entries) {
			if (!entry.hash) {
				throw new Error("Unexpected");
			}

			if (set.has(entry.hash)) {
				continue;
			}

			set.add(entry.hash);
			uniqueEntries.push(entry);
			promises.push(this._entryIndex.set(entry));
		}

		await Promise.all(promises);

		// Set heads if not passed as an argument
		const foundHeads = heads ? heads : Log.findHeads(uniqueEntries);

		this._headsIndex = new HeadsIndex({ entries: foundHeads });

		this._values = new Values(this._entryIndex, this._sortFn, uniqueEntries);

		for (const e of entries) {
			for (const a of e.next) {
				let nextIndexSet = this._nextsIndex.get(a);
				if (!nextIndexSet) {
					nextIndexSet = new Set();
					nextIndexSet.add(e.hash);
					this._nextsIndex.set(a, nextIndexSet);
				} else {
					nextIndexSet.add(e.hash);
				}
			}
		}
	}

	iterator(options?: {
		from?: "tail" | "head";
		amount?: number;
	}): IterableIterator<string> {
		const from = options?.from || "tail";
		const amount = typeof options?.amount === "number" ? options?.amount : -1;
		let next = from === "tail" ? this._values.tail : this._values.head;
		const nextFn = from === "tail" ? (e) => e.prev : (e) => e.next;
		return (function* () {
			let counter = 0;
			while (next) {
				if (amount >= 0 && counter >= amount) {
					return;
				}

				yield next.value.hash;
				counter++;

				next = nextFn(next);
			}
		})();
	}

	async trim(option: TrimOptions | undefined = this._trim.options) {
		return this._trim.trim(option);
	}

	async join(
		entriesOrLog: (string | Entry<T>)[] | Log<T>,
		options?: {
			canAppend?: (entry: Entry<T>) => Promise<boolean> | boolean;
			onChange?: (change: Change2<T>) => void | Promise<void>;
			verifySignatures?: boolean;
			trim?: TrimOptions;
		}
	): Promise<void> {
		const stack: string[] = [];
		const visited = new Set<string>();
		const nextRefs: Map<string, Entry<T>[]> = new Map();
		const entriesBottomUp: Entry<T>[] = [];
		const resolvedEntries: Map<string, Entry<T>> = new Map();
		const entries = Array.isArray(entriesOrLog)
			? entriesOrLog
			: await entriesOrLog.values.toArray();
		for (const e of entries) {
			let hash: string;
			if (e instanceof Entry) {
				hash = e.hash;
				resolvedEntries.set(e.hash, e);
			} else {
				hash = e;
			}
			if (this.has(hash)) {
				continue;
			}
			stack.push(hash);
		}

		for (const hash of stack) {
			if (visited.has(hash) || this.has(hash)) {
				continue;
			}
			visited.add(hash);

			const entry =
				resolvedEntries.get(hash) ||
				(await Entry.fromMultihash<T>(this._storage, hash, {
					replicate: true,
				}));
			entry.init(this);
			resolvedEntries.set(entry.hash, entry);
			const nexts = await entry.getNext();

			if (nexts) {
				let isRoot = true;
				for (const next of nexts) {
					if (this.has(next)) {
						if (this._headsIndex.has(next)) {
							this._headsIndex.del((await this.get(next))!);
						}
						continue;
					}

					isRoot = false;

					let nextIndexSet = nextRefs.get(next);
					if (!nextIndexSet) {
						nextIndexSet = [];
						nextIndexSet.push(entry);
						nextRefs.set(next, nextIndexSet);
					} else {
						nextIndexSet.push(entry);
					}
					if (!visited.has(next)) {
						stack.push(next);
					}
				}
				if (isRoot) {
					entriesBottomUp.push(entry);
				}
			} else {
				entriesBottomUp.push(entry);
			}
		}

		// Get the difference of the logs
		const nextFromNew = new Set<string>();
		while (entriesBottomUp.length > 0) {
			const e = entriesBottomUp.shift()!;

			if (options?.verifySignatures) {
				if (!(await e.verifySignatures())) {
					throw new Error('Invalid signature entry with hash "' + e.hash + '"');
				}
			}
			if (!isDefined(e.hash)) {
				throw new Error("Unexpected");
			}

			if (!this.has(e.hash)) {
				if (options?.canAppend && !(await options.canAppend(e))) {
					continue;
				}

				// Update the internal entry index
				await this._entryIndex.set(e);
				await this._values.put(e);
				const forward = nextRefs.get(e.hash);
				if (forward) {
					for (const en of forward) {
						entriesBottomUp.push(en);
					}
				} else {
					this.headsIndex.put(e);
				}

				for (const a of e.next) {
					let nextIndexSet = this._nextsIndex.get(a);
					if (!nextIndexSet) {
						nextIndexSet = new Set();
						nextIndexSet.add(e.hash);
						this._nextsIndex.set(a, nextIndexSet);
					} else {
						nextIndexSet.add(a);
					}
					nextFromNew.add(a);
				}

				const clock = await e.getClock();
				this._hlc.update(clock.timestamp);

				const removed = await this.trim(options?.trim);
				options?.onChange &&
					(await options.onChange({ added: e, removed: removed }));
			}
		}
	}

	/// TODO simplify methods below

	async deleteRecursively(from: Entry<any> | Entry<any>[]) {
		const stack = Array.isArray(from) ? [...from] : [from];
		const promises: Promise<void>[] = [];
		while (stack.length > 0) {
			const entry = stack.pop()!;
			this._trim.deleteFromCache(entry);
			await this._values.delete(entry);
			await this._entryIndex.delete(entry.hash);
			this._headsIndex.del(entry);
			this._nextsIndex.delete(entry.hash);
			for (const next of entry.next) {
				const ne = await this.get(next);
				if (ne) {
					stack.push(ne);
				}
			}
			promises.push(entry.delete(this._storage));
		}

		await Promise.all(promises);
	}

	async delete(entry: Entry<any>) {
		this._trim.deleteFromCache(entry);
		await this._values.delete(entry);
		await this._entryIndex.delete(entry.hash);
		this._headsIndex.del(entry);
		this._nextsIndex.delete(entry.hash);
		for (const next of entry.next) {
			const ne = await this.get(next);
			if (ne) {
				const nexts = this._nextsIndex.get(next)!;
				nexts.delete(entry.hash);
				if (nexts.size === 0) {
					this._headsIndex.put(ne);
				}
			}
		}
		return entry.delete(this._storage);
	}

	/**
	 * Get the log in JSON format.
	 * @returns {Object} An object with the id and heads properties
	 */
	async toJSON() {
		return {
			id: this._id,
			heads: (await this.getHeads())
				.sort(this._sortFn) // default sorting
				.reverse() // we want the latest as the first element
				.map(getHash), // return only the head hashes
		};
	}

	/**
	 * Get the log in JSON format as a snapshot.
	 * @returns {Object} An object with the id, heads and value properties
	 */
	toSnapshot() {
		return {
			id: this._id,
			heads: [...this.headsIndex.index.values()],
			values: this.toArray(),
		};
	}

	/**
	 * Returns the log entries as a formatted string.
	 * @returns {string}
	 * @example
	 * two
	 * └─one
	 *   └─three
	 */
	async toString(
		payloadMapper: (payload: Payload<T>) => string = (payload) =>
			(payload.getValue() as any).toString()
	): Promise<string> {
		return (
			await Promise.all(
				(
					await this.toArray()
				)
					.slice()
					.reverse()
					.map(async (e, idx) => {
						const parents: Entry<any>[] = Entry.findDirectChildren(
							e,
							await this.toArray()
						);
						const len = parents.length;
						let padding = new Array(Math.max(len - 1, 0));
						padding = len > 1 ? padding.fill("  ") : padding;
						padding = len > 0 ? padding.concat(["└─"]) : padding;
						/* istanbul ignore next */
						return (
							padding.join("") +
							(payloadMapper ? payloadMapper(e.payload) : e.payload)
						);
					})
			)
		).join("\n");
	}

	/**
	 * Get the log's multihash.
	 * @returns {Promise<string>} Multihash of the Log as Base58 encoded string.
	 */
	toMultihash(options?: { format?: string }) {
		return LogIO.toMultihash(this._storage, this, options);
	}

	static async fromMultihash<T>(
		store: BlockStore,
		identity: Identity,
		hash: string,
		options: { sortFn?: Sorting.ISortFunction } & EntryFetchAllOptions<T>
	) {
		// TODO: need to verify the entries with 'key'
		const { logId, entries, heads } = await LogIO.fromMultihash(store, hash, {
			length: options?.length,
			shouldFetch: options?.shouldFetch,
			shouldQueue: options?.shouldQueue,
			timeout: options?.timeout,
			onFetched: options?.onFetched,
			concurrency: options?.concurrency,
			sortFn: options?.sortFn,
			replicate: options?.replicate,
		});
		const log = new Log<T>(store, identity, {
			encryption: options?.encryption,
			encoding: options?.encoding,
			logId,
			sortFn: options?.sortFn,
		});
		await log.reset(entries, heads);
		return log;
	}

	static async fromEntryHash<T>(
		store: BlockStore,
		identity: Identity,
		hash: string | string[],
		options: {
			encoding?: Encoding<T>;
			encryption?: PublicKeyEncryptionResolver;
			logId?: string;
			length?: number;
			exclude?: any[];
			shouldFetch?: (hash: string) => boolean;
			timeout?: number;
			concurrency?: number;
			sortFn?: any;
			onFetched?: any;
			replicate?: boolean;
		} = { length: -1, exclude: [] }
	): Promise<Log<T>> {
		// TODO: need to verify the entries with 'key'
		const { entries } = await LogIO.fromEntryHash(store, hash, {
			length: options.length,
			encryption: options?.encryption,
			encoding: options.encoding,
			shouldFetch: options.shouldFetch,
			timeout: options.timeout,
			concurrency: options.concurrency,
			onFetched: options.onFetched,
			sortFn: options.sortFn,
			replicate: options.replicate,
		});

		const log = new Log<T>(store, identity, {
			encryption: options?.encryption,
			encoding: options?.encoding,
			logId: options.logId,
			sortFn: options.sortFn,
		});
		await log.reset(entries);
		return log;
	}

	/**
	 * Create a log from a Log Snapshot JSON.
	 * @param {IPFS} ipfs An IPFS instance
	 * @param {Identity} identity The identity instance
	 * @param {Object} json Log snapshot as JSON object
	 * @param {Object} options
	 * @param {AccessController} options.access The access controller instance
	 * @param {number} options.length How many entries to include in the log
	 * @param {function(hash, entry,  parent, depth)} [options.onFetched]
	 * @param {Function} options.sortFn The sort function - by default LastWriteWins
	 * @return {Promise<Log>} New Log
	 */
	static async fromJSON<T>(
		store: BlockStore,
		identity: Identity,
		json: { id: string; heads: string[] },
		options: {
			encoding?: Encoding<T>;
			encryption?: PublicKeyEncryptionResolver;
			length?: number;
			timeout?: number;
			sortFn?: Sorting.ISortFunction;
			onFetched?: (entry: Entry<T>) => void;
			replicate?: boolean;
		} = { encoding: JSON_ENCODING }
	) {
		// TODO: need to verify the entries with 'key'
		const { logId, entries } = await LogIO.fromJSON(store, json, {
			length: options?.length,
			encryption: options?.encryption,
			encoding: options.encoding,
			timeout: options?.timeout,
			onFetched: options?.onFetched,
			replicate: options.replicate,
		});
		const log = new Log<T>(store, identity, {
			encryption: options?.encryption,
			encoding: options?.encoding,
			logId,
			sortFn: options?.sortFn,
		});
		await log.reset(entries);
		return log;
	}

	static async fromEntry<T>(
		store: BlockStore,
		identity: Identity,
		sourceEntries: Entry<T>[] | Entry<T>,
		options: EntryFetchOptions<T> & {
			shouldFetch?: (hash: string) => boolean;
			encryption?: PublicKeyEncryptionResolver;
			sortFn?: Sorting.ISortFunction;
		}
	) {
		// TODO: need to verify the entries with 'key'
		options = strictFetchOptions(options);
		const { entries } = await LogIO.fromEntry(store, sourceEntries, {
			length: options.length,
			encryption: options?.encryption,
			encoding: options.encoding,
			timeout: options.timeout,
			concurrency: options.concurrency,
			shouldFetch: options.shouldFetch,
			onFetched: options.onFetched,
		});

		const log = new Log<T>(store, identity, {
			encryption: options?.encryption,
			encoding: options?.encoding,
			sortFn: options.sortFn,
		});

		await log.reset(entries);
		return log;
	}

	/**
	 * Find heads from a collection of entries.
	 *
	 * Finds entries that are the heads of this collection,
	 * ie. entries that are not referenced by other entries.
	 *
	 * @param {Array<Entry<T>>} entries Entries to search heads from
	 * @returns {Array<Entry<T>>}
	 */
	static findHeads<T>(entries: Entry<T>[]) {
		const indexReducer = (
			res: { [key: string]: string },
			entry: Entry<any>,
			idx: number
		) => {
			const addToResult = (e: string) => (res[e] = entry.hash);
			entry.next.forEach(addToResult);
			return res;
		};

		const items = entries.reduce(indexReducer, {});
		const exists = (e: Entry<T>) => items[e.hash] === undefined;
		return entries.filter(exists);
	}

	// Find entries that point to another entry that is not in the
	// input array
	static findTails<T>(entries: Entry<T>[]): Entry<T>[] {
		// Reverse index { next -> entry }
		const reverseIndex: { [key: string]: Entry<T>[] } = {};
		// Null index containing entries that have no parents (nexts)
		const nullIndex: Entry<T>[] = [];
		// Hashes for all entries for quick lookups
		const hashes: { [key: string]: boolean } = {};
		// Hashes of all next entries
		let nexts: string[] = [];

		const addToIndex = (e: Entry<T>) => {
			if (e.next.length === 0) {
				nullIndex.push(e);
			}
			const addToReverseIndex = (a: any) => {
				/* istanbul ignore else */
				if (!reverseIndex[a]) reverseIndex[a] = [];
				reverseIndex[a].push(e);
			};

			// Add all entries and their parents to the reverse index
			e.next.forEach(addToReverseIndex);
			// Get all next references
			nexts = nexts.concat(e.next);
			// Get the hashes of input entries
			hashes[e.hash] = true;
		};

		// Create our indices
		entries.forEach(addToIndex);

		const addUniques = (
			res: Entry<T>[],
			entries: Entry<T>[],
			_idx: any,
			_arr: any
		) => res.concat(findUniques(entries, "hash"));
		const exists = (e: string) => hashes[e] === undefined;
		const findFromReverseIndex = (e: string) => reverseIndex[e];

		// Drop hashes that are not in the input entries
		const tails = nexts // For every hash in nexts:
			.filter(exists) // Remove undefineds and nulls
			.map(findFromReverseIndex) // Get the Entry from the reverse index
			.reduce(addUniques, []) // Flatten the result and take only uniques
			.concat(nullIndex); // Combine with tails the have no next refs (ie. first-in-their-chain)

		return findUniques(tails, "hash").sort(Entry.compare);
	}

	// Find the hashes to entries that are not in a collection
	// but referenced by other entries
	static findTailHashes(entries: Entry<any>[]) {
		const hashes: { [key: string]: boolean } = {};
		const addToIndex = (e: Entry<any>) => (hashes[e.hash] = true);
		const reduceTailHashes = (
			res: string[],
			entry: Entry<any>,
			idx: number,
			arr: Entry<any>[]
		) => {
			const addToResult = (e: string) => {
				/* istanbul ignore else */
				if (hashes[e] === undefined) {
					res.splice(0, 0, e);
				}
			};
			entry.next.reverse().forEach(addToResult);
			return res;
		};

		entries.forEach(addToIndex);
		return entries.reduce(reduceTailHashes, []);
	}
}
