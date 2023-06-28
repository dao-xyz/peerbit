import {
	SignatureWithKey,
	randomBytes,
	sha256Base64Sync,
	Identity,
	Keychain,
	X25519Keypair,
} from "@peerbit/crypto";
import { Cache } from "@dao-xyz/cache";
import { SimpleLevel } from "@dao-xyz/lazy-level";

import { EntryIndex } from "./entry-index.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import { isDefined } from "./is-defined.js";
import { findUniques } from "./find-uniques.js";
import {
	EncryptionTemplateMaybeEncrypted,
	Entry,
	Payload,
	CanAppend,
	EntryType,
} from "./entry.js";
import {
	HLC,
	LamportClock as Clock,
	LamportClock,
	Timestamp,
} from "./clock.js";

import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { Encoding, NO_ENCODING } from "./encoding.js";
import { CacheUpdateOptions, HeadsIndex } from "./heads.js";
import { EntryNode, Values } from "./values.js";
import { Trim, TrimOptions } from "./trim.js";
import { logger } from "./logger.js";
import { Change } from "./change.js";
import { EntryWithRefs } from "./entry-with-refs.js";
import { Blocks } from "@peerbit/blocks-interface";

const { LastWriteWins, NoZeroes } = Sorting;

export type LogEvents<T> = {
	onChange?: (change: Change<T>) => void;
};

export type MemoryProperties = {
	cache?: SimpleLevel;
};

export type LogProperties<T> = {
	keychain?: Keychain;
	encoding?: Encoding<T>;
	clock?: LamportClock;
	sortFn?: Sorting.ISortFunction;
	trim?: TrimOptions;
	canAppend?: CanAppend<T>;
};

export type LogOptions<T> = LogProperties<T> & LogEvents<T> & MemoryProperties;

const ENTRY_CACHE_MAX = 1000; // TODO as param

export type AppendOptions<T> = {
	type?: EntryType;
	gidSeed?: Uint8Array;
	nexts?: Entry<any>[];
	identity?: Identity;
	signers?: ((
		data: Uint8Array
	) => Promise<SignatureWithKey> | SignatureWithKey)[];
	onGidsShadowed?: (gids: string[]) => void;
	trim?: TrimOptions;
	timestamp?: Timestamp;
	encryption?: {
		keypair: X25519Keypair;
		reciever: EncryptionTemplateMaybeEncrypted;
	};
};

@variant(0)
export class Log<T> {
	@field({ type: fixedArray("u8", 32) })
	private _id: Uint8Array;

	private _sortFn: Sorting.ISortFunction;
	private _storage: Blocks;
	private _hlc: HLC;

	// Identity
	private _identity: Identity;

	// Keeping track of entries
	private _entryIndex: EntryIndex<T>;
	private _headsIndex: HeadsIndex<T>;
	private _values: Values<T>;

	// Index of all next pointers in this log
	private _nextsIndex: Map<string, Set<string>>;
	private _keychain?: Keychain;
	private _encoding: Encoding<T>;
	private _trim: Trim<T>;
	private _entryCache: Cache<Entry<T>>;

	private _canAppend?: CanAppend<T>;
	private _onChange?: (change: Change<T>) => void;
	private _closed = true;
	private _memory?: SimpleLevel;
	private _joining: Map<string, Promise<any>>; // entry hashes that are currently joining into this log

	constructor(properties?: { id?: Uint8Array }) {
		this._id = properties?.id || randomBytes(32);
	}

	async open(store: Blocks, identity: Identity, options: LogOptions<T> = {}) {
		if (!isDefined(store)) {
			throw LogError.BlockStoreNotDefinedError();
		}

		if (!isDefined(identity)) {
			throw new Error("Identity is required");
		}

		if (this.closed === false) {
			throw new Error("Already open");
		}

		const { encoding, trim, keychain, cache } = options;
		let { sortFn } = options;

		if (!isDefined(sortFn)) {
			sortFn = LastWriteWins;
		}
		sortFn = sortFn as Sorting.ISortFunction;

		this._sortFn = NoZeroes(sortFn);
		this._storage = store;
		this._memory = cache;
		if (this._memory && this._memory.status !== "open") {
			await this._memory.open();
		}

		this._encoding = encoding || NO_ENCODING;
		this._joining = new Map();

		// Identity
		this._identity = identity;

		// encoder/decoder
		this._keychain = keychain;

		// Clock
		this._hlc = new HLC();

		this._nextsIndex = new Map();
		const id = this.id;
		if (!id) {
			throw new Error("Id not set");
		}
		this._headsIndex = new HeadsIndex(id);
		await this._headsIndex.init(this);
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
					//f (!!entry)
					const a = this.values.length;
					if (entry) {
						this.values.deleteNode(node);
						await this.entryIndex.delete(node.value.hash);
						await this.headsIndex.del(node.value);
						this.nextsIndex.delete(node.value.hash);
						await this.storage.rm(node.value.hash);
					}
					const b = this.values.length;
					if (a === b) {
						throw new Error(
							"UNexpected: " +
								this.values.length +
								"_-- " +
								this.entryIndex._index.size
						);
					}
					return entry;
				},
				values: () => this.values,
			},
			trim
		);

		this._canAppend = async (entry) => {
			if (options?.canAppend) {
				if (!(await options.canAppend(entry))) {
					return false;
				}
			}
			return true;
		};

		this._onChange = options?.onChange;
		this._closed = false;
	}

	private _idString: string | undefined;

	get idString() {
		if (!this.id) {
			throw new Error("Id not set");
		}
		return this._idString || (this._idString = Log.createIdString(this.id));
	}

	public static createIdString(id: Uint8Array) {
		return sha256Base64Sync(id);
	}

	get id() {
		return this._id;
	}
	set id(id: Uint8Array) {
		if (this.closed === false) {
			throw new Error("Can not change id after open");
		}
		this._idString = undefined;
		this._id = id;
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

	get memory(): SimpleLevel | undefined {
		return this._memory;
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

	get storage(): Blocks {
		return this._storage;
	}

	get nextsIndex(): Map<string, Set<string>> {
		return this._nextsIndex;
	}

	get entryIndex(): EntryIndex<T> {
		return this._entryIndex;
	}

	get keychain() {
		return this._keychain;
	}

	get encoding() {
		return this._encoding;
	}

	get sortFn() {
		return this._sortFn;
	}

	get closed() {
		return this._closed;
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
	get(
		hash: string,
		options?: { timeout?: number }
	): Promise<Entry<T> | undefined> {
		return this._entryIndex.get(hash, options);
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
		options: AppendOptions<T> = {}
	): Promise<{ entry: Entry<T>; removed: Entry<T>[] }> {
		// Update the clock (find the latest clock)
		if (options.nexts) {
			for (const n of options.nexts) {
				if (!n.hash)
					throw new Error(
						"Expecting nexts to already be saved. missing hash for one or more entries"
					);
			}
		}

		await this.load({ reload: false });

		const hasNext = !!options.nexts; // true for [], which means we have explicitly said that nexts are empty
		const nexts: Entry<any>[] = options.nexts || (await this.getHeads());

		// Calculate max time for log/graph
		const clock = new Clock({
			id: this._identity.publicKey.bytes,
			timestamp: options.timestamp || this._hlc.now(),
		});

		const entry = await Entry.create<T>({
			store: this._storage,
			identity: options.identity || this._identity,
			signers: options.signers,
			data,
			clock,
			type: options.type,
			encoding: this._encoding,
			next: nexts,
			gidSeed: options.gidSeed,
			encryption: options.encryption
				? {
						keypair: options.encryption.keypair,
						reciever: {
							...options.encryption.reciever,
						},
				  }
				: undefined,
			canAppend: this._canAppend,
		});

		if (!isDefined(entry.hash)) {
			throw new Error("Unexpected");
		}

		for (const e of nexts) {
			let nextIndexSet = this._nextsIndex.get(e.hash);
			if (!nextIndexSet) {
				nextIndexSet = new Set();
				nextIndexSet.add(entry.hash);
				this._nextsIndex.set(e.hash, nextIndexSet);
			} else {
				nextIndexSet.add(entry.hash);
			}
		}

		const removedGids: Set<string> = new Set();
		if (hasNext) {
			for (const next of nexts) {
				const deletion = await this._headsIndex.del(next);
				if (deletion.lastWithGid && next.gid !== entry.gid) {
					removedGids.add(next.gid);
				}
			}
		} else {
			// next is all heads, which means we should just overwrite
			for (const key of this.headsIndex.gids.keys()) {
				if (key !== entry.gid) {
					removedGids.add(key);
				}
			}
			await this.headsIndex.reset([entry], { cache: { update: false } });
		}

		await this._entryIndex.set(entry, false); // save === false, because its already saved when Entry.create
		await this._headsIndex.put(entry, { cache: { update: false } }); // we will update the cache a few lines later *
		await this._values.put(entry);

		const removed = await this.processEntry(entry);

		// if next contails all gids
		if (options.onGidsShadowed && removedGids.size > 0) {
			options.onGidsShadowed([...removedGids]);
		}

		entry.init({ encoding: this._encoding, keychain: this._keychain });
		//	console.log('put entry', entry.hash, (await this._entryIndex._index.size));

		const trimmed = await this.trim(options?.trim);

		for (const entry of trimmed) {
			removed.push(entry);
		}

		const changes: Change<T> = {
			added: [entry],
			removed: removed,
		};

		await this._headsIndex.updateHeadsCache(changes); // * here
		await this._onChange?.(changes);
		return { entry, removed };
	}

	async reset(entries: Entry<T>[], heads?: (string | Entry<T>)[]) {
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
		const foundHeads = heads
			? ((await Promise.all(
					heads.map((x) => {
						if (x instanceof Entry) return x;
						const resolved = this._entryIndex.get(x);
						if (!resolved) {
							throw new Error("Missing head with cid: " + x);
						}
						return resolved;
					})
			  )) as Entry<T>[])
			: Log.findHeads(uniqueEntries);

		await this._headsIndex.reset(foundHeads);

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

	async remove(
		entry: Entry<T> | Entry<T>[],
		options?: { recursively?: boolean }
	): Promise<Change<T>> {
		await this.load({ reload: false });
		const entries = Array.isArray(entry) ? entry : [entry];

		if (entries.length === 0) {
			return {
				added: [],
				removed: [],
			};
		}

		if (options?.recursively) {
			await this.deleteRecursively(entry);
		} else {
			for (const entry of entries) {
				await this.delete(entry);
			}
		}

		const change: Change<T> = {
			added: [],
			removed: Array.isArray(entry) ? entry : [entry],
		};

		/* 	await Promise.all([
				this._logCache?.queue(change),
				this._onUpdate(change),
			]); */
		await this._onChange?.(change);
		return change;
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

	/**
	 *
	 * @param entries
	 * @returns change
	 */
	/* async sync(
		entries: (EntryWithRefs<T> | Entry<T> | string)[],
		options: {
			canAppend?: CanAppend<T>;
			onChange?: (change: Change<T>) => void | Promise<void>;
			timeout?: number;
		} = {}
	): Promise<void> {


		logger.debug(`Sync request #${entries.length}`);
		const entriesToJoin: (Entry<T> | string)[] = [];
		for (const e of entries) {
			if (e instanceof Entry || typeof e === "string") {
				entriesToJoin.push(e);
			} else {
				for (const ref of e.references) {
					entriesToJoin.push(ref);
				}
				entriesToJoin.push(e.entry);
			}
		}

		await this.join(entriesToJoin, {
			canAppend: (entry) => {
				const canAppend = options?.canAppend || this.canAppend;
				return !canAppend || canAppend(entry);
			},
			onChange: (change) => {
				options?.onChange?.(change);
				return this._onChange?.({
					added: change.added,
					removed: change.removed,
				});
			},
			timeout: options.timeout,
		});
	} */

	async join(
		entriesOrLog: (string | Entry<T> | EntryWithRefs<T>)[] | Log<T>,
		options?: {
			verifySignatures?: boolean;
			trim?: TrimOptions;
			timeout?: number;
		} & CacheUpdateOptions
	): Promise<void> {
		await this.load({ reload: false });
		if (entriesOrLog.length === 0) {
			return;
		}
		/* const joinLength = options?.length ?? Number.MAX_SAFE_INTEGER;  TODO */
		const visited = new Set<string>();
		const nextRefs: Map<string, Entry<T>[]> = new Map();
		const entriesBottomUp: Entry<T>[] = [];
		const stack: string[] = [];
		const resolvedEntries: Map<string, Entry<T>> = new Map();
		const entries = Array.isArray(entriesOrLog)
			? entriesOrLog
			: await entriesOrLog.values.toArray();

		// Build a list of already resolved entries, and filter out already joined entries
		for (const e of entries) {
			// TODO, do this less ugly
			let hash: string;
			if (e instanceof Entry) {
				hash = e.hash;
				resolvedEntries.set(e.hash, e);
				if (this.has(hash)) {
					continue;
				}
				stack.push(hash);
			} else if (typeof e === "string") {
				hash = e;

				if (this.has(hash)) {
					continue;
				}
				stack.push(hash);
			} else {
				hash = e.entry.hash;
				resolvedEntries.set(e.entry.hash, e.entry);
				if (this.has(hash)) {
					continue;
				}
				stack.push(hash);

				for (const e2 of e.references) {
					resolvedEntries.set(e2.hash, e2);
					if (this.has(e2.hash)) {
						continue;
					}
					stack.push(e2.hash);
				}
			}
		}

		// Resolve missing entries
		const removedHeads: Entry<T>[] = [];
		for (const hash of stack) {
			if (visited.has(hash) || this.has(hash)) {
				continue;
			}
			visited.add(hash);

			const entry =
				resolvedEntries.get(hash) ||
				(await Entry.fromMultihash<T>(this._storage, hash, {
					timeout: options?.timeout,
				}));

			entry.init(this);
			resolvedEntries.set(entry.hash, entry);

			let nexts: string[];
			if (
				entry.metadata.type !== EntryType.CUT &&
				(nexts = await entry.getNext())
			) {
				let isRoot = true;
				for (const next of nexts) {
					if (!this.has(next)) {
						isRoot = false;
					} else {
						if (this._headsIndex.has(next)) {
							const toRemove = (await this.get(next, options))!;
							await this._headsIndex.del(toRemove);
							removedHeads.push(toRemove);
						}
					}
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

		while (entriesBottomUp.length > 0) {
			const e = entriesBottomUp.shift()!;
			await this._joining.get(e.hash);
			const p = this.joinEntry(e, nextRefs, entriesBottomUp, options).then(
				() => this._joining.delete(e.hash) // TODO, if head we run into problems with concurrency here!, we add heads at line 929 but resolve here
			);
			this._joining.set(e.hash, p);
			await p;
		}
	}

	private async joinEntry(
		e: Entry<T>,
		nextRefs: Map<string, Entry<T>[]>,
		stack: Entry<T>[],
		options?: {
			verifySignatures?: boolean;
			trim?: TrimOptions;
			length?: number;
		} & CacheUpdateOptions
	): Promise<void> {
		if (this.length > (options?.length ?? Number.MAX_SAFE_INTEGER)) {
			return;
		}

		if (!isDefined(e.hash)) {
			throw new Error("Unexpected");
		}

		if (!this.has(e.hash)) {
			if (options?.verifySignatures) {
				if (!(await e.verifySignatures())) {
					throw new Error('Invalid signature entry with hash "' + e.hash + '"');
				}
			}

			if (this?._canAppend && !(await this?._canAppend(e))) {
				return;
			}

			// Update the internal entry index
			await this._entryIndex.set(e);
			await this._values.put(e);

			if (e.metadata.type !== EntryType.CUT) {
				for (const a of e.next) {
					if (!this.has(a)) {
						await this.join([a]);
					}

					let nextIndexSet = this._nextsIndex.get(a);
					if (!nextIndexSet) {
						nextIndexSet = new Set();
						nextIndexSet.add(e.hash);
						this._nextsIndex.set(a, nextIndexSet);
					} else {
						nextIndexSet.add(a);
					}
				}
			}

			const clock = await e.getClock();
			this._hlc.update(clock.timestamp);

			const removed = await this.processEntry(e);
			const trimmed = await this.trim(options?.trim);

			for (const entry of trimmed) {
				removed.push(entry);
			}

			await this?._onChange?.({ added: [e], removed: removed });
		}

		const forward = nextRefs.get(e.hash);
		if (forward) {
			if (this._headsIndex.has(e.hash)) {
				await this._headsIndex.del(e, options);
			}
			for (const en of forward) {
				stack.push(en);
			}
		} else {
			await this.headsIndex.put(e, options);
		}
	}

	private async processEntry(entry: Entry<T>) {
		if (entry.metadata.type === EntryType.CUT) {
			return this.deleteRecursively(entry, true);
		}
		return [];
	}

	/// TODO simplify methods below
	async deleteRecursively(from: Entry<any> | Entry<any>[], skipFirst = false) {
		const stack = Array.isArray(from) ? [...from] : [from];
		const promises: Promise<void>[] = [];
		let counter = 0;
		const deleted: Entry<T>[] = [];
		while (stack.length > 0) {
			const entry = stack.pop()!;
			if ((counter > 0 || !skipFirst) && this.has(entry.hash)) {
				// TODO test last argument: It is for when multiple heads point to the same entry, hence we might visit it multiple times? or a concurrent delete process is doing it before us.
				this._trim.deleteFromCache(entry);
				await this._values.delete(entry);
				await this._entryIndex.delete(entry.hash);
				await this._headsIndex.del(entry);
				this._nextsIndex.delete(entry.hash);
				deleted.push(entry);
				promises.push(entry.delete(this._storage));
			}

			for (const next of entry.next) {
				const nextFromNext = this._nextsIndex.get(next);
				if (nextFromNext) {
					nextFromNext.delete(entry.hash);
				}

				if (!nextFromNext || nextFromNext.size === 0) {
					const ne = await this.get(next);
					if (ne) {
						stack.push(ne);
					}
				}
			}
			counter++;
		}
		await Promise.all(promises);
		return deleted;
	}

	async delete(entry: Entry<any>) {
		this._trim.deleteFromCache(entry);
		await this._values.delete(entry);
		await this._entryIndex.delete(entry.hash);
		await this._headsIndex.del(entry);
		this._nextsIndex.delete(entry.hash);
		const newHeads: string[] = [];
		for (const next of entry.next) {
			const ne = await this.get(next);
			if (ne) {
				const nexts = this._nextsIndex.get(next)!;
				nexts.delete(entry.hash);
				if (nexts.size === 0) {
					await this._headsIndex.put(ne);
					newHeads.push(ne.hash);
				}
			}
		}
		await this._headsIndex.updateHeadsCache({
			added: newHeads,
			removed: [entry.hash],
		});
		return entry.delete(this._storage);
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
			(payload.getValue(this.encoding) as any).toString()
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
	async idle() {
		await this._headsIndex.headsCache?.idle();
	}

	async close() {
		// Don't return early here if closed = true, because "load" might create processes that needs to be closed
		this._closed = true; // closed = true before doing below, else we might try to open the headsIndex cache because it is closed as we assume log is still open
		await this._entryCache?.clear();
		await this._headsIndex?.close();
		await this._memory?.close();
	}

	async drop() {
		// Don't return early here if closed = true, because "load" might create processes that needs to be closed
		this._closed = true; // closed = true before doing below, else we might try to open the headsIndex cache because it is closed as we assume log is still open
		await this._headsIndex?.drop();
		await this._entryCache?.clear();
		await this._memory?.clear();
		await this._memory?.close();
	}
	async load(
		opts: ({ fetchEntryTimeout?: number } & (
			| {
					/* amount?: number  TODO */
			  }
			| { heads?: true }
		)) & { reload: boolean } = { reload: true }
	) {
		const heads = await this.headsIndex.load({
			replicate: true, // TODO this.replication.replicate(x) => true/false
			timeout: opts.fetchEntryTimeout,
			reload: opts.reload,
			cache: { update: true, reset: true },
		});

		if (heads) {
			// Load the log
			if ((opts as { heads?: true }).heads) {
				await this.reset(heads);
			} else {
				const amount = (opts as { amount?: number }).amount;
				if (amount != null && amount >= 0 && amount < heads.length) {
					throw new Error(
						"You are not loading all heads, this will lead to unexpected behaviours on write. Please load at least load: " +
							amount +
							" entries"
					);
				}

				await this.join(heads instanceof Entry ? [heads] : heads, {
					/* length: amount, */
					timeout: opts?.fetchEntryTimeout,
					cache: {
						update: false,
					},
				});
			}
		}
	}

	static async fromEntry<T>(
		store: Blocks,
		identity: Identity,
		entryOrHash: string | string[] | Entry<T> | Entry<T>[],
		options: {
			id?: Uint8Array;
			/* length?: number; TODO */
			timeout?: number;
		} & LogOptions<T> = { id: randomBytes(32) }
	): Promise<Log<T>> {
		const log = new Log<T>(options.id && { id: options.id });
		await log.open(store, identity, options);
		await log.join(!Array.isArray(entryOrHash) ? [entryOrHash] : entryOrHash, {
			timeout: options.timeout,
			trim: options.trim,
			verifySignatures: true,
		});
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
			entry: Entry<any>
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
