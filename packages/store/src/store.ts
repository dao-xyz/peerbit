import mapSeries from "p-each-series";
import PQueue from "p-queue";
import {
	Log,
	ISortFunction,
	LogOptions,
	Identity,
	CanAppend,
	JSON_ENCODING,
	Change,
	TrimOptions,
	TrimToByteLengthOption,
	TrimToLengthOption,
} from "@dao-xyz/peerbit-log";
import {
	Encoding,
	EncryptionTemplateMaybeEncrypted,
} from "@dao-xyz/peerbit-log";
import { Entry } from "@dao-xyz/peerbit-log";
import { BlockStore, stringifyCid } from "@dao-xyz/libp2p-direct-block";
import Cache from "@dao-xyz/lazy-level";
import { variant, option, field, vec, Constructor } from "@dao-xyz/borsh";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Snapshot } from "./snapshot.js";
import {
	AccessError,
	PublicKeyEncryptionResolver,
} from "@dao-xyz/peerbit-crypto";
import { EntryWithRefs } from "./entry-with-refs.js";
import { waitForAsync } from "@dao-xyz/peerbit-time";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import path from "path-browserify";
import { v4 as uuid } from "uuid";
import { join } from "./replicator.js";
import { createBlock, getBlockValue } from "@dao-xyz/libp2p-direct-block";
export const logger = loggerFn({ module: "store" });

export class CachedValue {}
export type AddOperationOptions<T> = {
	skipCanAppendCheck?: boolean;
	identity?: Identity;
	nexts?: Entry<T>[];
	reciever?: EncryptionTemplateMaybeEncrypted;
};

@variant(0)
export class CID {
	@field({ type: "string" })
	hash: string;

	constructor(opts?: { hash: string }) {
		if (opts) {
			this.hash = opts.hash;
		}
	}
}

@variant(0)
export class CachePath {
	@field({ type: "string" })
	path: string;

	constructor(path: string) {
		this.path = path;
	}
}

@variant(0)
export class UnsfinishedReplication {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(opts?: { hashes: string[] }) {
		if (opts) {
			this.hashes = opts.hashes;
		}
	}
}

@variant(0)
export class HeadsCache {
	@field({ type: vec("string") })
	heads: string[];

	@field({ type: option("string") })
	last?: string;

	@field({ type: "u64" })
	counter: bigint;

	constructor(heads: string[], counter: bigint, last?: string) {
		this.heads = heads;
		this.last = last;
		this.counter = counter;
	}
}

export interface IStoreOptions<T> {
	/**
	 * f set to true, will throw an error if the database can't be found locally. (Default: false)
	 */
	localOnly?: boolean;

	/**
	 * The directory where data will be stored
	 */
	directory?: string;
	onClose?: (store: Store<T>) => void;
	onDrop?: (store: Store<T>) => void;
	onLoad?: (store: Store<T>) => void;
	onLoadProgress?: (store: Store<T>, entry: Entry<T>) => void;
	onWrite?: (store: Store<T>, _entry: Entry<T>) => void;
	onOpen?: (store: Store<any>) => Promise<void>;
	onReplicationQueued?: (store: Store<any>, entry: Entry<T>) => void; // TODO, do we need this?
	onReplicationFetch?: (store: Store<any>, entry: Entry<T>) => void; // TODO, do we need this?
	onReplicationComplete?: (store: Store<any>) => void; // TODO, do we need this?
	onReady?: (store: Store<T>) => void; // TODO, do we need this?
	onUpdate?: (change: Change<T>) => void;
	encryption?: PublicKeyEncryptionResolver;
	replicationConcurrency?: number;
	sortFn?: ISortFunction;
	trim?: TrimToByteLengthOption | TrimToLengthOption;
}

export interface IInitializationOptions<T>
	extends IStoreOptions<T>,
		IInitializationOptionsDefault<T> {
	resolveCache: (store: Store<any>) => Promise<Cache> | Cache;
	replicator?: (entry: Entry<T>) => Promise<boolean>;
}

interface IInitializationOptionsDefault<T> {
	replicationConcurrency?: number;
	cacheId: string;
}

export const DefaultOptions: IInitializationOptionsDefault<any> = {
	replicationConcurrency: 32,
	cacheId: "id",
};

export interface Initiable<T> {
	init?(
		blockstore: BlockStore,
		identity: Identity,
		options: IInitializationOptions<T>
	): Promise<this>;
}

@variant(0)
export class Store<T> implements Initiable<T> {
	@field({ type: "u32" })
	_storeIndex: number; // how to ensure unqiueness

	_canAppend?: CanAppend<T>;
	_onUpdate?: (change: Change<T>) => Promise<void> | void;
	_onUpdateOption?: (change: Change<T>) => Promise<void> | void;

	// An access controller that is note part of the store manifest, usefull for circular store -> access controller -> store structures

	private _options: IInitializationOptions<T>;
	identity: Identity;

	private _headsPathCounter = 0;
	headsPath: string;
	private _lastHeadsPath?: string;
	private _lastHeadsCount = 0n;

	removedHeadsPath: string;
	private _lastRemovedHeadsPath?: string;
	private _lastRemovedHeadsCount = 0n;

	snapshotPath: string;
	initialized: boolean;
	encoding: Encoding<T> = JSON_ENCODING;

	private _store: BlockStore;
	private _cache: Cache;
	private _oplog: Log<T>;
	private _queue: PQueue<any, any>;

	private _key: string;

	private _saveFile: (file: any) => Promise<string>;
	private _loadFile: (cid: string) => Promise<Uint8Array | undefined>;

	constructor(properties?: { storeIndex: number }) {
		if (properties) {
			this._storeIndex = properties?.storeIndex;
		}
	}

	setup(properties: {
		encoding: Encoding<T>;
		canAppend: CanAppend<T>;
		onUpdate: (change: Change<T>) => void;
	}) {
		this.encoding = properties.encoding;
		this.onUpdate = properties.onUpdate;
		this.canAppend = properties.canAppend;
	}

	async init(
		store: BlockStore,
		identity: Identity,
		options: IInitializationOptions<T>
	): Promise<this> {
		if (this.initialized) {
			throw new Error("Already initialized");
		}

		this._saveFile = async (file) => store.put(await createBlock(file, "raw"));
		this._loadFile = async (file) => {
			const block = await store.get<Uint8Array>(file);
			if (block) {
				return getBlockValue<Uint8Array>(block);
			}
			return undefined;
		};

		// Set ipfs since we are to save the store
		this._store = store;

		// Set the options (we will use the topic property after thiis)
		const opts = { ...DefaultOptions, ...options };
		this._options = opts;

		// Cache
		this._cache = await this._options.resolveCache(this);
		await this._cache.open();

		// Create IDs, names and paths
		this.identity = identity;
		this._onUpdateOption = options.onUpdate;

		this.headsPath = path.join(options.cacheId, this.id, "_heads");
		this.removedHeadsPath = path.join(
			options.cacheId,
			this.id,
			"_heads_removed"
		);
		await this.loadLastHeadsPath();

		this.snapshotPath = path.join(options.cacheId, this.id, "snapshot");

		// Create the operations log
		this._oplog = new Log<T>(this._store, identity, this.logOptions);

		// addOperation and log-joins queue. Adding ops and joins to the queue
		// makes sure they get processed sequentially to avoid race conditions
		this._queue = new PQueue({ concurrency: 1 });
		if (this._options.onOpen) {
			await this._options.onOpen(this);
		}
		this.initialized = true;

		return this;
	}

	async updateCachedHeads(
		change: {
			added: (Entry<T> | string)[];
			removed: (Entry<T> | string)[];
		},
		reset?: boolean
	) {
		if (typeof reset !== "boolean") {
			if (this.oplog.headsIndex.index.size <= change.added.length) {
				let addedIsAllHeads = true;
				for (const entry of change.added) {
					const hash = typeof entry === "string" ? entry : entry.hash;
					if (!this.oplog.headsIndex.get(hash)) {
						addedIsAllHeads = false;
					}
				}
				reset = addedIsAllHeads;
			} else {
				// added size < head size, meaning we have not rewritten all heads
				reset = false;
			}
		}

		// If 'reset' then dont keep references to old heads caches, assume new cache will fully describe all heads
		const updateHashes = async (
			headsPath: string,
			lastPath: string | undefined,
			lastCounter: bigint,
			hashes: string[]
		): Promise<{ counter: bigint; newPath: string }> => {
			const newHeadsPath = path.join(
				headsPath,
				String(this._headsPathCounter),
				uuid()
			);
			const counter = lastCounter + BigInt(hashes.length);
			await this._cache.set(
				headsPath,
				serialize(new CachePath(newHeadsPath.toString()))
			);
			await this._cache.set(
				newHeadsPath,
				serialize(new HeadsCache(hashes, counter, lastPath))
			);
			return { counter, newPath: newHeadsPath };
		};

		// TODO dont delete old before saving new
		if (reset) {
			const paths = [
				path.join(this.headsPath, String(this._headsPathCounter)),
				path.join(this.removedHeadsPath, String(this._headsPathCounter)),
			];
			for (const p of paths) {
				await this._cache.deleteByPrefix(p + "/");
			}

			this._lastHeadsPath = undefined;
			this._lastRemovedHeadsPath = undefined;
			this._lastHeadsCount = 0n;
			this._lastRemovedHeadsCount = 0n;

			this._headsPathCounter += 1;
		}

		if (change.added.length > 0) {
			const update = await updateHashes(
				this.headsPath,
				this._lastHeadsPath,
				this._lastHeadsCount,
				change.added.map((x) => (typeof x === "string" ? x : x.hash))
			);
			this._lastHeadsPath = update.newPath;
			this._lastHeadsCount = update.counter;
		}

		if (this._lastHeadsPath) {
			// only add removed heads if we actually have added heads, else these are pointless
			if (change.removed.length > 0) {
				const update = await updateHashes(
					this.removedHeadsPath,
					this._lastRemovedHeadsPath,
					this._lastRemovedHeadsCount,
					change.removed.map((x) => (typeof x === "string" ? x : x.hash))
				);
				this._lastRemovedHeadsPath = update.newPath;
				this._lastRemovedHeadsCount = update.counter;
				if (
					update.counter > 0n &&
					2n * update.counter >= this._lastHeadsCount
				) {
					const resetToHeads = await this.getCachedHeads(
						this._lastHeadsPath,
						this._lastRemovedHeadsPath
					);
					await this.updateCachedHeads(
						{ added: resetToHeads, removed: [] },
						true
					);
				}
			}
		}
	}

	async idle(): Promise<void> {
		// Wait for the operations queue to finish processing
		// to make sure everything that all operations that have
		// been queued will be written to disk
		await this._queue?.onIdle();
		await this._cache?.idle();
	}

	async getCachedHeads(
		lastHeadsPath: string | undefined = this._lastHeadsPath,
		lastRemovedHeadsPath: string | undefined = this._lastRemovedHeadsPath
	): Promise<string[]> {
		if (!this._cache) {
			return [];
		}
		const getHashes = async (
			start: string | undefined,
			filter?: Set<string>
		) => {
			const result: string[] = [];
			let next = start;
			while (next) {
				const cache = await this._cache
					.get(next)
					.then((bytes) => bytes && deserialize(bytes, HeadsCache));
				next = cache?.last;
				cache?.heads.forEach((head) => {
					if (filter && filter.has(head)) {
						return;
					}

					result.push(head);
				});
			}
			return result;
		};

		const removedHeads = new Set(await getHashes(lastRemovedHeadsPath));
		const heads = await getHashes(lastHeadsPath, removedHeads);
		return heads; // Saved heads - removed heads
	}

	get id(): string {
		if (typeof this._storeIndex !== "number") {
			throw new Error("Store index not set");
		}
		return this._storeIndex.toString();
	}

	get oplog(): Log<any> {
		return this._oplog;
	}

	get key() {
		return this._key;
	}

	get store(): BlockStore {
		return this._store;
	}
	get options(): IInitializationOptions<T> {
		return this._options;
	}

	get logOptions(): LogOptions<T> {
		return {
			logId: this.id,
			encryption: this._options.encryption,
			encoding: this.encoding,
			sortFn: this._options.sortFn,
			trim: this._options.trim && {
				// I can trim if I am not a replicator of an entry
				canTrim:
					this.options.replicator &&
					(async (entry) => !(await this.options.replicator!(entry))),
				...this._options.trim,
			},
		};
	}

	get cache(): Cache {
		return this._cache;
	}
	setIdentity(identity: Identity) {
		this.identity = identity;
		this._oplog.setIdentity(identity);
	}

	set canAppend(canAppend: CanAppend<T> | undefined) {
		this._canAppend = canAppend;
	}

	get canAppend(): CanAppend<T> | undefined {
		return this._canAppend;
	}

	set onUpdate(onUpdate: (change: Change<T>) => void) {
		this._onUpdate = onUpdate;
	}

	get closed() {
		return !this._oplog;
	}
	async close() {
		if (!this.initialized) {
			return;
		}
		if (this._options.onClose) {
			await this._options.onClose(this);
		}

		await this.idle();
		await this._cache.close();

		this._oplog = null as any;
		this._lastHeadsPath = undefined;
		this._lastRemovedHeadsPath = undefined;
		this._lastRemovedHeadsCount = 0n;
		this._lastHeadsCount = 0n;

		// Database is now closed
		return Promise.resolve();
	}

	/**
	 * Drops a database and removes local data
	 */
	async drop() {
		if (!this._oplog && !this._cache) {
			return; // already dropped
		}

		if (this._options.onDrop) {
			await this._options.onDrop(this);
		}

		if (this._cache.status !== "open") {
			await this._cache.open();
		}

		await this._cache.del(this.headsPath);
		await this._cache.del(this.snapshotPath);

		await this.close();

		// Reset
		// TODO fix types
		this._oplog = undefined as any;
		this._cache = undefined as any;
		this.initialized = false; // call this last because (close() expect initialized to be able to function)
	}

	private async loadHeads(): Promise<string[]> {
		if (!this.initialized) {
			throw new Error("Store needs to be initialized before loaded");
		}

		if (this._cache.status !== "open") {
			await this._cache.open();
		}

		await this.loadLastHeadsPath();
		const heads = await this.getCachedHeads(
			this._lastHeadsPath,
			this._lastRemovedHeadsPath
		);
		return heads;
	}

	async load(amount?: number, opts?: { fetchEntryTimeout?: number }) {
		if (!this.initialized) {
			throw new Error("Store needs to be initialized before loaded");
		}

		if (this._cache.status !== "open") {
			await this._cache.open();
		}
		amount = amount ?? -1;

		if (this._options.onLoad) {
			await this._options.onLoad(this);
		}

		const heads = await this.loadHeads();

		// Load the log
		const log = await Log.fromEntryHash(this._store, this.identity, heads, {
			...this.logOptions,
			length: amount,
			timeout: opts?.fetchEntryTimeout,
			onFetched: this._onLoadProgress.bind(this),
			concurrency: this._options.replicationConcurrency,
		});

		this._oplog = log;

		// Update the index
		if (heads.length > 0) {
			await this._updateIndex({ added: log.values, removed: [] });
		}

		this._options.onReady && this._options.onReady(this);
	}

	async loadLastHeadsPath() {
		this._lastHeadsPath = await this._cache
			.get(this.headsPath)
			.then((bytes) => bytes && deserialize(bytes, CachePath).path);
		this._lastRemovedHeadsPath = await this._cache
			.get(this.removedHeadsPath)
			.then((bytes) => bytes && deserialize(bytes, CachePath).path);
		this._lastHeadsCount = this._lastHeadsPath
			? await this.getCachedHeadsCount(this._lastHeadsPath)
			: 0n;
		this._lastRemovedHeadsCount = this._lastRemovedHeadsPath
			? await this.getCachedHeadsCount(this._lastRemovedHeadsPath)
			: 0n;
	}

	async getCachedHeadsCount(headPath?: string): Promise<bigint> {
		if (!headPath) {
			return 0n;
		}
		return (
			(
				await this._cache
					.get(headPath)
					.then((bytes) => bytes && deserialize(bytes, HeadsCache))
			)?.counter || 0n
		);
	}

	async addOperation(
		data: T,
		options?: AddOperationOptions<T>
	): Promise<{ entry: Entry<T>; removed: Entry<T>[] }> {
		if (this.closed) {
			throw new Error("Store is closed");
		}

		const change = await this._oplog.append(data, {
			nexts: options?.nexts,
			reciever: options?.reciever,
			canAppend: options?.skipCanAppendCheck ? undefined : this.canAppend,
			identity: options?.identity,
		});

		logger.debug("Appended entry with hash: " + change.entry.hash);
		const changes: Change<T> = {
			added: [change.entry],
			removed: change.removed,
		};
		await Promise.all([
			this.updateCachedHeads(changes),
			this._updateIndex(changes),
		]);
		this._options.onWrite && this._options.onWrite(this, change.entry);
		return change;
	}

	async removeOperation(
		entry: Entry<T> | Entry<T>[],
		options?: { recursively?: boolean }
	): Promise<Change<T>> {
		const entries = Array.isArray(entry) ? entry : [entry];
		if (entries.length === 0) {
			return {
				added: [],
				removed: [],
			};
		}

		if (options?.recursively) {
			await this.oplog.deleteRecursively(entry);
		} else {
			for (const entry of entries) {
				await this.oplog.delete(entry);
			}
		}
		const change: Change<T> = {
			added: [],
			removed: Array.isArray(entry) ? entry : [entry],
		};

		await Promise.all([
			this.updateCachedHeads(change),
			this._updateIndex(change),
		]);
		return change;
	}

	/**
	 *
	 * @param entries
	 * @returns change
	 */
	async sync(
		entries: (EntryWithRefs<T> | Entry<T> | string)[],
		options: { canAppend?: CanAppend<T>; save: boolean } = { save: true }
	): Promise<boolean> {
		logger.debug(`Sync request #${entries.length}`);
		if (entries.length === 0) {
			return false;
		}

		const handle = async (headToHandle: EntryWithRefs<T> | Entry<T>) => {
			const allEntries =
				headToHandle instanceof Entry
					? [headToHandle]
					: [headToHandle.entry, ...headToHandle.references];
			await Promise.all(
				allEntries.map((h) =>
					h.init({
						encoding: this.oplog._encoding,
						encryption: this.oplog._encryption,
					})
				)
			);
			const entry =
				headToHandle instanceof Entry ? headToHandle : headToHandle.entry;

			this._options.onReplicationQueued &&
				this._options.onReplicationQueued(this, entry);

			const canAppend = options?.canAppend || this.canAppend;
			if (canAppend && !(await canAppend(entry))) {
				logger.debug("Not allowd to append head " + entry.hash);
				return Promise.resolve(null);
			}
			await Promise.all(
				allEntries.map(async (head) => {
					const headHash = head.hash;
					head.hash = undefined as any;
					try {
						const block = await createBlock(serialize(head), "raw");
						const hash = options?.save
							? await this._store.put(block)
							: stringifyCid(block.cid);
						head.hash = headHash;
						if (head.hash === undefined) {
							head.hash = hash; // can happen if you sync entries that you load directly from ipfs
						} else if (hash !== head.hash) {
							logger.error("Head hash didn't match the contents");
							throw new Error("Head hash didn't match the contents");
						}
					} catch (error) {
						logger.error(error);
						throw error;
					}
				})
			);

			return headToHandle;
		};
		const hash = (entry: EntryWithRefs<T> | Entry<T> | string) => {
			if (entry instanceof Entry) {
				return entry.hash;
			} else if (typeof entry === "string") {
				return entry;
			}
			return entry.entry.hash;
		};

		const newEntries: (Entry<T> | EntryWithRefs<T>)[] = [];
		for (const entry of entries) {
			const h = hash(entry);
			if (h && this.oplog.has(h)) {
				continue;
			}
			newEntries.push(
				typeof entry === "string"
					? await Entry.fromMultihash(this._store, entry)
					: entry
			);
		}

		if (newEntries.length === 0) {
			return false;
		}

		const saved = await mapSeries(newEntries, handle);
		const change = await join(
			saved as EntryWithRefs<T>[] | Entry<T>[],
			this._oplog,
			{
				concurrency: this._options.replicationConcurrency,
				onFetched: (entry) =>
					this._options.onReplicationFetch &&
					this._options.onReplicationFetch(this, entry),
			}
		);

		await Promise.all([
			this.updateCachedHeads(change),
			this._updateIndex(change),
		]);
		this._options.onReplicationComplete &&
			this._options.onReplicationComplete(this);
		return true;
	}

	async saveSnapshot() {
		const snapshotData = this._oplog.toSnapshot();
		const buf = serialize(
			new Snapshot({
				id: snapshotData.id,
				heads: snapshotData.heads,
				size: BigInt(snapshotData.values.length),
				values: snapshotData.values,
			})
		);

		const snapshot = await this._saveFile(buf);
		await this._cache.set(
			this.snapshotPath,
			serialize(new CID({ hash: snapshot }))
		);

		await waitForAsync(
			async () =>
				(await this._cache
					.get(this.snapshotPath)
					.then((bytes) => bytes && deserialize(bytes, CID))) !== undefined,
			{ delayInterval: 200, timeout: 10 * 1000 }
		);

		logger.debug(`Saved snapshot: ${snapshot}`);
		return [snapshot];
	}

	async loadFromSnapshot() {
		if (this._options.onLoad) {
			await this._options.onLoad(this);
		}
		await this.sync([]);

		const snapshotCID = await this._cache
			.get(this.snapshotPath)
			.then((bytes) => bytes && deserialize(bytes, CID));
		if (snapshotCID) {
			const file = await this._loadFile(snapshotCID.hash);
			if (!file) {
				throw new Error("Missing snapshot");
			}
			const snapshotData = deserialize(file, Snapshot);

			// Fetch the entries
			// Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
			if (snapshotData) {
				this._oplog = await Log.fromEntry(
					this._store,
					this.identity,
					snapshotData.heads,
					{
						sortFn: this._options.sortFn,
						length: -1,
						timeout: 1000,
						onFetched: this._onLoadProgress.bind(this),
					}
				);
				await this._updateIndex({
					added: this._oplog.values,
					removed: [],
				});
				this._options.onReplicationComplete &&
					this._options.onReplicationComplete(this);
			}
			this._options.onReady && this._options.onReady(this);
		} else {
			throw new Error(`Snapshot for ${this.id} not found!`);
		}

		return this;
	}

	async _updateIndex(change: Change<T>) {
		// TODO add better error handling
		try {
			if (this._onUpdate) {
				await this._onUpdate(change);
			}
		} catch (error) {
			if (error instanceof AccessError) {
				// fail silently for now
				logger.info("Could not _onUpdate due to AccessError");
			} else {
				throw error;
			}
		}

		try {
			if (this._onUpdateOption) {
				await this._onUpdateOption(change);
			}
		} catch (error) {
			if (error instanceof AccessError) {
				// fail silently for now
				logger.info("Could not _onUpdateOption due to AccessError");
			} else {
				throw error;
			}
		}
	}

	/* Loading progress callback */
	_onLoadProgress(entry: Entry<any>) {
		this._options.onLoadProgress && this._options.onLoadProgress(this, entry);
	}

	clone(): Store<T> {
		return deserialize(
			serialize(this),
			this.constructor as any as Constructor<any>
		);
	}
}
