import { deserialize, field, fixedArray, variant } from "@dao-xyz/borsh";
import { type AnyStore } from "@peerbit/any-store";
import { type Blocks, cidifyString } from "@peerbit/blocks-interface";
import {
	type Identity,
	SignatureWithKey,
	X25519Keypair,
	randomBytes,
	sha256Base64Sync,
} from "@peerbit/crypto";
import { type Indices } from "@peerbit/indexer-interface";
import { create } from "@peerbit/indexer-sqlite3";
import { type Keychain } from "@peerbit/keychain";
import { type Change } from "./change.js";
import {
	LamportClock as Clock,
	HLC,
	LamportClock,
	Timestamp,
} from "./clock.js";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import {
	EntryIndex,
	type MaybeResolveOptions,
	type ResultsIterator,
	type ReturnTypeFromResolveOptions,
} from "./entry-index.js";
import { type EntryWithRefs } from "./entry-with-refs.js";
import {
	type CanAppend,
	type EncryptionTemplateMaybeEncrypted,
	Entry,
	EntryType,
	Payload,
	ShallowEntry,
	type ShallowOrFullEntry,
} from "./entry.js";
import { findUniques } from "./find-uniques.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import { Trim, type TrimOptions } from "./trim.js";

const { LastWriteWins } = Sorting;

export type LogEvents<T> = {
	onChange?: (change: Change<T> /* , reference?: R */) => void;
	onGidRemoved?: (gids: string[]) => Promise<void> | void;
};

export type MemoryProperties = {
	storage?: AnyStore;
	indexer?: Indices;
};

export type LogProperties<T> = {
	keychain?: Keychain;
	encoding?: Encoding<T>;
	clock?: LamportClock;
	sortFn?: Sorting.SortFn;
	trim?: TrimOptions;
	canAppend?: CanAppend<T>;
};

export type LogOptions<T> = LogProperties<T> & LogEvents<T> & MemoryProperties;

export type AppendOptions<T> = {
	meta?: {
		type?: EntryType;
		gidSeed?: Uint8Array;
		data?: Uint8Array;
		timestamp?: Timestamp;
		next?: Entry<any>[] | ShallowEntry[];
	};

	identity?: Identity;
	signers?: ((
		data: Uint8Array,
	) => Promise<SignatureWithKey> | SignatureWithKey)[];

	trim?: TrimOptions;
	encryption?: {
		keypair: X25519Keypair;
		receiver: EncryptionTemplateMaybeEncrypted;
	};
	onChange?: OnChange<T>;
	canAppend?: CanAppend<T>;
};

type OnChange<T> = (
	change: Change<T>,
	reference?: undefined,
) => void | Promise<void>;

export type JoinableEntry = {
	meta: {
		clock: {
			timestamp: Timestamp;
		};
		next: string[];
		gid: string;
		type: EntryType;
	};
	hash: string;
};

export const ENTRY_JOIN_SHAPE = {
	hash: true,
	meta: { type: true, next: true, gid: true, clock: true },
} as const;

@variant(0)
export class Log<T> {
	@field({ type: fixedArray("u8", 32) })
	private _id: Uint8Array;

	/* private _sortFn!: Sorting.ISortFunction; */
	private _storage!: Blocks;
	private _hlc!: HLC;

	// Identity
	private _identity!: Identity;

	// Keeping track of entries
	private _entryIndex!: EntryIndex<T>;
	/* 	private _headsIndex!: HeadsIndex<T>;
		private _values!: Values<T>;
	 */
	// Index of all next pointers in this log
	/* private _nextsIndex!: Map<string, Set<string>>; */
	private _keychain?: Keychain;
	private _encoding!: Encoding<T>;
	private _trim!: Trim<T>;
	private _canAppend?: CanAppend<T>;
	private _onChange?: OnChange<T>;
	private _closed = true;
	private _closeController!: AbortController;
	private _loadedOnce = false;
	private _indexer!: Indices;
	private _joining!: Map<string, Promise<any>>; // entry hashes that are currently joining into this log
	private _sortFn!: Sorting.SortFn;

	constructor(properties?: { id?: Uint8Array }) {
		this._id = properties?.id || randomBytes(32);
	}

	async open(store: Blocks, identity: Identity, options: LogOptions<T> = {}) {
		if (store == null) {
			throw LogError.BlockStoreNotDefinedError();
		}

		if (identity == null) {
			throw new Error("Identity is required");
		}

		if (this.closed === false) {
			throw new Error("Already open");
		}

		this._closeController = new AbortController();

		const { encoding, trim, keychain, indexer, onGidRemoved, sortFn } = options;

		// TODO do correctly with tie breaks
		this._sortFn = sortFn || LastWriteWins;

		this._storage = store;
		this._indexer = indexer || (await create());
		await this._indexer.start?.();

		this._encoding = encoding || NO_ENCODING;
		this._joining = new Map();

		// Identity
		this._identity = identity;

		// encoder/decoder
		this._keychain = keychain;

		// Clock
		this._hlc = new HLC();

		const id = this.id;
		if (!id) {
			throw new Error("Id not set");
		}

		this._entryIndex = new EntryIndex({
			store: this._storage,
			init: (e) => e.init(this),
			onGidRemoved,
			index: await (
				await this._indexer.scope("heads")
			).init({ schema: ShallowEntry }),
			publicKey: this._identity.publicKey,
			sort: this._sortFn,
		});
		await this._entryIndex.init();
		/* 	this._values = new Values(this._entryIndex, this._sortFn); */

		this._trim = new Trim(
			{
				index: this._entryIndex,
				deleteNode: async (node: ShallowEntry) => {
					await this.get(node.hash);
					await this._entryIndex.delete(node.hash);
					await this._storage.rm(node.hash);
					return node;
				},
				sortFn: this._sortFn,
				getLength: () => this.length,
			},
			trim,
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
		this._closeController = new AbortController();
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
		if (this._closed) {
			throw new Error("Closed");
		}
		return this._entryIndex.length;
	}

	get canAppend() {
		return this._canAppend;
	}

	/**
	 * Checks if a entry is part of the log
	 * @param {string} hash The hash of the entry
	 * @returns {boolean}
	 */

	has(cid: string) {
		return this._entryIndex.has(cid);
	}
	/**
	 * Get all entries sorted. Don't use this method anywhere where performance matters
	 */
	async toArray(): Promise<Entry<T>[]> {
		// we call init, because the values might be unitialized
		return this.entryIndex.query([], this.sortFn.sort, true).all();
	}

	/**
	 * Returns the head index
	 */

	getHeads<R extends MaybeResolveOptions = false>(
		resolve: R = false as R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		return this.entryIndex.getHeads(undefined, resolve);
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

	get blocks(): Blocks {
		return this._storage;
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
	 * Get an entry.
	 * @param {string} [hash] The hashes of the entry
	 */
	get(
		hash: string,
		options?: { timeout?: number },
	): Promise<Entry<T> | undefined> {
		return this._entryIndex.get(
			hash,
			options ? { type: "full", timeout: options.timeout } : undefined,
		);
	}

	/**
	 * Get a entry with shallow representation
	 * @param {string} [hash] The hashes of the entry
	 */
	async getShallow(
		hash: string,
		options?: { timeout?: number },
	): Promise<ShallowEntry | undefined> {
		return (await this._entryIndex.getShallow(hash))?.value;
	}

	/* 
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
	 */
	async getReferenceSamples(
		from: Entry<T>,
		options?: { pointerCount?: number; memoryLimit?: number },
	): Promise<Entry<T>[]> {
		const hashes = new Set<string>();
		const pointerCount = options?.pointerCount || 0;
		const memoryLimit = options?.memoryLimit;
		const maxDistance = Math.min(pointerCount, this.entryIndex.length);
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
	 * @param {T} data The data to be appended
	 * @param {AppendOptions} [options] The options for the append
	 * @returns {{ entry: Entry<T>; removed: ShallowEntry[] }} The appended entry and an array of removed entries
	 */
	async append(
		data: T,
		options: AppendOptions<T> = {},
	): Promise<{ entry: Entry<T>; removed: ShallowEntry[] }> {
		// Update the clock (find the latest clock)
		if (options.meta?.next) {
			for (const n of options.meta.next) {
				if (!n.hash)
					throw new Error(
						"Expecting nexts to already be saved. missing hash for one or more entries",
					);
			}
		}

		await this.load({ reload: false });

		const nexts: Sorting.SortableEntry[] =
			options.meta?.next ||
			(await this.entryIndex
				.getHeads(undefined, { type: "shape", shape: Sorting.ENTRY_SORT_SHAPE })
				.all());

		// Calculate max time for log/graph
		const clock = new Clock({
			id: this._identity.publicKey.bytes,
			timestamp: options?.meta?.timestamp || this._hlc.now(),
		});

		const entry = await Entry.create<T>({
			store: this._storage,
			identity: options.identity || this._identity,
			signers: options.signers,
			data,
			meta: {
				clock,
				type: options.meta?.type,
				gidSeed: options.meta?.gidSeed,
				data: options.meta?.data,
				next: nexts,
			},

			encoding: this._encoding,
			encryption: options.encryption
				? {
						keypair: options.encryption.keypair,
						receiver: {
							...options.encryption.receiver,
						},
					}
				: undefined,
			canAppend: options.canAppend || this._canAppend,
		});

		if (!entry.hash) {
			throw new Error("Unexpected");
		}

		if (entry.meta.type !== EntryType.CUT) {
			for (const e of nexts) {
				if (!(await this.has(e.hash))) {
					let entry: Entry<any>;
					if (e instanceof Entry) {
						entry = e;
					} else {
						let resolved = await this.entryIndex.get(e.hash);
						if (!resolved) {
							// eslint-disable-next-line no-console
							console.warn("Unexpected missing entry when joining", e.hash);
							continue;
						}
						entry = resolved;
					}
					await this.join([entry]);
				}
			}
		}

		await this.entryIndex.put(entry, {
			unique: true,
			isHead: true,
			toMultiHash: false,
		});

		const removed = await this.processEntry(entry);

		entry.init({ encoding: this._encoding, keychain: this._keychain });

		const trimmed = await this.trim(options?.trim);

		if (trimmed) {
			for (const entry of trimmed) {
				removed.push(entry);
			}
		}

		const changes: Change<T> = {
			added: [entry],
			removed,
		};

		await (options?.onChange || this._onChange)?.(changes);
		return { entry, removed };
	}

	async reset(entries?: Entry<T>[]) {
		const heads = await this.getHeads(true).all();
		await this._entryIndex.clear();
		await this._onChange?.({ added: [], removed: heads });
		await this.join(entries || heads);
	}

	async remove(
		entry: ShallowOrFullEntry<T> | ShallowOrFullEntry<T>[],
		options?: { recursively?: boolean },
	): Promise<Change<T>> {
		await this.load({ reload: false });
		const entries = Array.isArray(entry) ? entry : [entry];

		if (entries.length === 0) {
			return {
				added: [],
				removed: [],
			};
		}

		const change: Change<T> = {
			added: [],
			removed: Array.isArray(entry) ? entry : [entry],
		};

		await this._onChange?.(change);

		if (options?.recursively) {
			await this.deleteRecursively(entry);
		} else {
			for (const entry of entries) {
				await this.delete(entry.hash);
			}
		}

		return change;
	}

	/* iterator(options?: {
		from?: "tail" | "head";
		amount?: number;
	}): IterableIterator<string> {
		const from = options?.from || "tail";
		const amount = typeof options?.amount === "number" ? options?.amount : -1;
		let next = from === "tail" ? this._values.tail : this._values.head;
		const nextFn = from === "tail" ? (e: any) => e.prev : (e: any) => e.next;
		return (function* () {
			let counter = 0;
			while (next) {
				if (amount >= 0 && counter >= amount) {
					return;
				}

				yield next.value;
				counter++;

				next = nextFn(next);
			}
		})();
	} */

	async trim(option: TrimOptions | undefined = this._trim.options) {
		return this._trim.trim(option);
	}

	async join(
		entriesOrLog:
			| (string | Entry<T> | ShallowEntry | EntryWithRefs<T>)[]
			| Log<T>
			| ResultsIterator<Entry<any>>,
		options?: {
			verifySignatures?: boolean;
			trim?: TrimOptions;
			timeout?: number;
		},
	): Promise<void> {
		let entries: Entry<T>[];
		let references: Map<string, Entry<T>> = new Map();

		if (entriesOrLog instanceof Log) {
			if (entriesOrLog.entryIndex.length === 0) return;
			entries = await entriesOrLog.toArray();
			for (const element of entries) {
				references.set(element.hash, element);
			}
		} else if (Array.isArray(entriesOrLog)) {
			if (entriesOrLog.length === 0) {
				return;
			}

			entries = [];
			for (const element of entriesOrLog) {
				if (element instanceof Entry) {
					entries.push(element);
					references.set(element.hash, element);
				} else if (typeof element === "string") {
					let entry = await Entry.fromMultihash<T>(this._storage, element, {
						timeout: options?.timeout,
					});
					if (!entry) {
						throw new Error("Missing entry in join by hash: " + element);
					}
					entries.push(entry);
				} else if (element instanceof ShallowEntry) {
					let entry = await Entry.fromMultihash<T>(
						this._storage,
						element.hash,
						{
							timeout: options?.timeout,
						},
					);
					if (!entry) {
						throw new Error("Missing entry in join by hash: " + element.hash);
					}
					entries.push(entry);
				} else {
					entries.push(element.entry);
					references.set(element.entry.hash, element.entry);

					for (const ref of element.references) {
						references.set(ref.hash, ref);
					}
				}
			}
		} else {
			let all = await entriesOrLog.all(); // TODO dont load all at once
			if (all.length === 0) {
				return;
			}

			entries = all;
		}

		let heads: Map<string, boolean> = new Map();
		for (const entry of entries) {
			if (heads.has(entry.hash)) {
				continue;
			}
			heads.set(entry.hash, true);
			for (const next of await entry.getNext()) {
				heads.set(next, false);
			}
		}

		for (const entry of entries) {
			const p = this.joinRecursively(entry, {
				references,
				isHead: heads.get(entry.hash)!,
				...options,
			});
			this._joining.set(entry.hash, p);
			p.finally(() => {
				this._joining.delete(entry.hash);
			});
			await p;
		}
	}

	/**
	 * Bottom up join of entries into the log
	 * @param entry
	 * @param options
	 * @returns
	 */

	private async joinRecursively(
		entry: Entry<T>,
		options: {
			verifySignatures?: boolean;
			trim?: TrimOptions;
			length?: number;
			references?: Map<string, Entry<T>>;
			isHead: boolean;
			timeout?: number;
		},
	) {
		if (this.entryIndex.length > (options?.length ?? Number.MAX_SAFE_INTEGER)) {
			return;
		}

		if (!entry.hash) {
			throw new Error("Unexpected");
		}

		if (await this.has(entry.hash)) {
			return;
		}

		entry.init(this);

		if (options?.verifySignatures) {
			if (!(await entry.verifySignatures())) {
				throw new Error(
					'Invalid signature entry with hash "' + entry.hash + '"',
				);
			}
		}

		if (this?._canAppend && !(await this?._canAppend(entry))) {
			return;
		}

		const headsWithGid: JoinableEntry[] = await this.entryIndex
			.getHeads(entry.gid, { type: "shape", shape: ENTRY_JOIN_SHAPE })
			.all();
		if (headsWithGid) {
			for (const v of headsWithGid) {
				// TODO second argument should be a time compare instead? what about next nexts?
				// and check the cut entry is newer than the current 'entry'
				if (
					v.meta.type === EntryType.CUT &&
					v.meta.next.includes(entry.hash) &&
					Sorting.compare(entry, v, this._sortFn) < 0
				) {
					return; // already deleted
				}
			}
		}

		if (entry.meta.type !== EntryType.CUT) {
			for (const a of entry.next) {
				if (!(await this.has(a))) {
					const nested =
						options.references?.get(a) ||
						(await Entry.fromMultihash<T>(this._storage, a, {
							timeout: options?.timeout,
						}));

					if (!nested) {
						throw new Error("Missing entry in joinRecursively: " + a);
					}

					const p = this.joinRecursively(
						nested,
						options.isHead ? { ...options, isHead: false } : options,
					);
					this._joining.set(nested.hash, p);
					p.finally(() => {
						this._joining.delete(nested.hash);
					});
					await p;
				}
			}
		}

		const clock = await entry.getClock();
		this._hlc.update(clock.timestamp);

		await this._entryIndex.put(entry, {
			unique: false,
			isHead: options.isHead,
			toMultiHash: true,
		});

		const removed = await this.processEntry(entry);
		const trimmed = await this.trim(options?.trim);

		if (trimmed) {
			for (const entry of trimmed) {
				removed.push(entry);
			}
		}

		await this?._onChange?.({ added: [entry], removed: removed });
	}

	private async processEntry(entry: Entry<T>): Promise<ShallowEntry[]> {
		if (entry.meta.type === EntryType.CUT) {
			return this.deleteRecursively(entry, true);
		}
		return [];
	}

	/// TODO simplify methods below
	async deleteRecursively(
		from: ShallowOrFullEntry<T> | ShallowOrFullEntry<T>[],
		skipFirst = false,
	) {
		const stack = Array.isArray(from) ? [...from] : [from];
		const promises: (Promise<void> | void)[] = [];
		let counter = 0;
		const deleted: ShallowEntry[] = [];

		while (stack.length > 0) {
			const entry = stack.pop()!;
			const skip = counter === 0 && skipFirst;
			if (!skip) {
				const has = await this.has(entry.hash);
				if (has) {
					// TODO test last argument: It is for when multiple heads point to the same entry, hence we might visit it multiple times? or a concurrent delete process is doing it before us.
					const deletedEntry = await this.delete(entry.hash);
					if (deletedEntry) {
						/* 	this._nextsIndex.delete(entry.hash); */
						deleted.push(deletedEntry);
					}
				}
			}

			for (const next of entry.meta.next) {
				const nextFromNext = this.entryIndex.getHasNext(next);
				const entriesThatHasNext = await nextFromNext.all();

				// if there are no entries which is not of "CUT" type, we can safely delete the next entry
				// figureately speaking, these means where are cutting all branches to a stem, so we can delete the stem as well
				let hasAlternativeNext = !!entriesThatHasNext.find(
					(x) => x.meta.type !== EntryType.CUT,
				);
				if (!hasAlternativeNext) {
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

	async delete(hash: string): Promise<ShallowEntry | undefined> {
		await this._trim.deleteFromCache(hash);
		const removedEntry = await this._entryIndex.delete(hash);
		return removedEntry;
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
			(payload.getValue(this.encoding) as any).toString(),
	): Promise<string> {
		return (
			await Promise.all(
				(await this.toArray())
					.slice()
					.reverse()
					.map(async (e, idx) => {
						const parents: Entry<any>[] = Entry.findDirectChildren(
							e,
							await this.toArray(),
						);
						const len = parents.length;
						let padding = new Array(Math.max(len - 1, 0));
						padding = len > 1 ? padding.fill("  ") : padding;
						padding = len > 0 ? padding.concat(["└─"]) : padding;
						return (
							padding.join("") +
							(payloadMapper?.(e.payload) || (e.payload as any as string))
						);
					}),
			)
		).join("\n");
	}

	async close() {
		// Don't return early here if closed = true, because "load" might create processes that needs to be closed
		this._closed = true; // closed = true before doing below, else we might try to open the headsIndex cache because it is closed as we assume log is still open
		this._closeController.abort();
		await this._indexer?.stop?.();
		this._indexer = undefined as any;
		this._loadedOnce = false;
	}

	async drop() {
		// Don't return early here if closed = true, because "load" might create processes that needs to be closed
		this._closed = true; // closed = true before doing below, else we might try to open the headsIndex cache because it is closed as we assume log is still open
		this._closeController.abort();
		await this.entryIndex?.clear();
		await this._indexer?.drop();
		await this._indexer?.stop?.();
	}

	async recover() {
		// merge existing
		const existing = await this.getHeads(true).all();

		const allHeads: Map<string, Entry<any>> = new Map();
		for (const head of existing) {
			allHeads.set(head.hash, head);
		}

		// fetch all possible entries
		for await (const [key, value] of this._storage.iterator()) {
			if (allHeads.has(key)) {
				continue;
			}
			try {
				cidifyString(key);
			} catch (error) {
				continue;
			}

			try {
				const der = deserialize(value, Entry);
				der.hash = key;
				der.init(this);
				allHeads.set(key, der);
			} catch (error) {
				continue; // invalid entries
			}
		}

		// assume they are valid, (let access control reject them if not)
		await this.load({ reload: true, heads: [...allHeads.values()] });
	}

	async load(
		opts: {
			heads?: Entry<T>[];
			fetchEntryTimeout?: number;
			reset?: boolean;
			ignoreMissing?: boolean;
			timeout?: number;
			reload?: boolean;
		} = {},
	) {
		if (this.closed) {
			throw new Error("Closed");
		}

		if (this._loadedOnce && !opts.reload && !opts.reset) {
			return;
		}

		this._loadedOnce = true;

		const providedCustomHeads = Array.isArray(opts["heads"]);

		const heads = providedCustomHeads
			? (opts["heads"] as Array<Entry<T>>)
			: await this._entryIndex
					.getHeads(undefined, {
						type: "full",
						signal: this._closeController.signal,
						ignoreMissing: opts.ignoreMissing,
						timeout: opts.timeout,
					})
					.all();

		if (heads) {
			// Load the log
			if (providedCustomHeads || opts.reset) {
				await this.reset(heads as any as Entry<any>[]);
			} else {
				await this.join(heads instanceof Entry ? [heads] : heads, {
					timeout: opts?.fetchEntryTimeout,
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
		} & LogOptions<T> = { id: randomBytes(32) },
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
	 * @param {Array<Entry<T>>} entries - Entries to search heads from
	 * @returns {Array<Entry<T>>}
	 */
	static findHeads<T>(entries: Entry<T>[]) {
		const indexReducer = (
			res: { [key: string]: string },
			entry: Entry<any>,
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
			_arr: any,
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
			arr: Entry<any>[],
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
