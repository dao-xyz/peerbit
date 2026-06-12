import { type AnyStore } from "@peerbit/any-store-interface";
import {
	createPersistenceBackend,
	type RustAnyStorePersistenceBackend,
} from "./persistence.js";

type NativeAnyStore = {
	get(key: string): Uint8Array | undefined;
	has_many(keys: string[]): boolean[];
	put(key: string, value: Uint8Array): void;
	delete(key: string): boolean;
	put_many(keys: string[], values: Uint8Array[]): void;
	get_many(keys: string[]): Array<Uint8Array | undefined>;
	delete_many(keys: string[]): number;
	clear(): void;
	len(): number;
	size(): number;
	entries(): Array<[string, Uint8Array]>;
};

type JournaledNativeAnyStore = NativeAnyStore & {
	snapshot(): Uint8Array;
	load_snapshot(bytes: Uint8Array): void;
	apply_journal(bytes: Uint8Array): number;
	encode_put_record(key: string, value: Uint8Array): Uint8Array;
	encode_put_records(keys: string[], values: Uint8Array[]): Uint8Array;
	encode_delete_record(key: string): Uint8Array;
	encode_delete_records(keys: string[]): Uint8Array;
	encode_clear_record(): Uint8Array;
};

type WasmModule = {
	default(input?: unknown): Promise<unknown>;
	initSync(input?: unknown): unknown;
	NativeAnyStore: new () => NativeAnyStore;
};

export type RustAnyStoreOptions = {
	compactOnClose?: boolean;
	durability?: "normal" | "strict";
};

type StoreStatus = "opening" | "open" | "closing" | "closed";

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/any_store_rust.js";
		wasmModulePromise = import(/* @vite-ignore */ wasmModulePath) as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		const processLike = (globalThis as { process?: { versions?: { node?: string } } })
			.process;
		if (processLike?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(
				/* @vite-ignore */ fsPromises
			)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../wasm/any_store_rust_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL("../wasm/any_store_rust_bg.wasm", import.meta.url),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes);

export class RustAnyStore implements AnyStore {
	private native?: NativeAnyStore;
	private persistence?: RustAnyStorePersistenceBackend;
	private openPromise?: Promise<void>;
	private lifecycleQueue: Promise<unknown> = Promise.resolve();
	private explicitlyClosed = false;
	private pendingCloses = 0;
	private mutationQueue: Promise<unknown> = Promise.resolve();
	private queuedMutations = 0;
	private journalQueue: Promise<unknown> = Promise.resolve();
	private journalError?: unknown;
	private children = new Map<string, RustAnyStore>();
	private _status: StoreStatus = "closed";

	constructor(
		readonly directory?: string,
		private readonly level: string[] = [],
		private readonly options: RustAnyStoreOptions = {},
	) {}

	status(): StoreStatus {
		return this._status;
	}

	async open(): Promise<void> {
		if (!this.explicitlyClosed) {
			if (this._status === "open") {
				return;
			}
			if (this.openPromise) {
				return this.openPromise;
			}
		}
		this.explicitlyClosed = false;
		const open = this.enqueueLifecycle(async () => {
			if (this._status === "open") {
				return;
			}
			this._status = "opening";
			try {
				await this.openInternal();
				this._status = "open";
			} catch (error) {
				this._status = "closed";
				this.native = undefined;
				this.persistence = undefined;
				throw error;
			}
		});
		const wrapped: Promise<void> = open.finally(() => {
			if (this.openPromise === wrapped) {
				this.openPromise = undefined;
			}
		});
		this.openPromise = wrapped;
		return wrapped;
	}

	/**
	 * After close() resolves (or rejects) the store always reaches "closed"
	 * and releases its native state; a pending journal failure makes close()
	 * reject with that original error, matching how the level backend
	 * propagates close-time failures. Mutations and reads after close()
	 * reject until open() is called again.
	 */
	async close(): Promise<void> {
		this.explicitlyClosed = true;
		if (this._status === "closed" && !this.openPromise) {
			return;
		}
		// Only mutations enqueued before this call drain into the closing
		// store; later ones either reject (still closed) or wait for a
		// queued re-open.
		const drainTail = this.mutationQueue;
		this.pendingCloses++;
		return this.enqueueLifecycle(() => this.closeInternal(drainTail)).finally(
			() => {
				this.pendingCloses--;
			},
		);
	}

	private async closeInternal(drainTail: Promise<unknown>): Promise<void> {
		if (this._status === "closed") {
			return;
		}
		this._status = "closing";
		let closeError: unknown;
		await drainTail;
		try {
			await this.waitForJournal();
		} catch (error) {
			closeError = error;
		}
		for (const child of this.children.values()) {
			try {
				await child.close();
			} catch (error) {
				closeError ??= error;
			}
		}
		if (this.native && this.directory && this.options.compactOnClose !== false) {
			try {
				await this.compact();
			} catch (error) {
				closeError ??= error;
			}
		}
		const persistence = this.persistence;
		this.persistence = undefined;
		this.native = undefined;
		try {
			await persistence?.close();
		} catch (error) {
			closeError ??= error;
		}
		this._status = "closed";
		if (closeError) {
			throw closeError;
		}
	}

	private enqueueLifecycle<T>(fn: () => Promise<T>): Promise<T> {
		const next = this.lifecycleQueue.then(fn);
		this.lifecycleQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next;
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		const native = await this.ensureOpen();
		return native.get(key) ?? undefined;
	}

	put(key: string, value: Uint8Array): Promise<void> | void {
		const native = this.openTransientNative();
		if (native) {
			native.put(key, value);
			return;
		}

		const bytes = copyBytes(value);
		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_record(key, bytes),
				);
			}
			native.put(key, bytes);
		});
	}

	/**
	 * Fast path for content-addressed immutable bytes; callers must not mutate
	 * value after calling.
	 */
	putImmutable(key: string, value: Uint8Array): Promise<void> | void {
		const native = this.openTransientNative();
		if (native) {
			native.put(key, value);
			return;
		}
		const normalNative = this.openNormalDurabilityNative();
		if (normalNative && this.directory) {
			const journalError = this.takePendingJournalError();
			if (journalError) {
				return Promise.reject(journalError);
			}
			this.recordJournal(
				this.journaledNative(normalNative).encode_put_record(key, value),
			);
			normalNative.put(key, value);
			return;
		}

		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_record(key, value),
				);
			}
			native.put(key, value);
		});
	}

	putMany(entries: Iterable<readonly [string, Uint8Array]>): Promise<void> | void {
		const inputPairs = Array.from(entries);
		if (inputPairs.length === 0) {
			return;
		}
		const native = this.openTransientNative();
		if (native) {
			native.put_many(
				inputPairs.map(([key]) => key),
				inputPairs.map(([, value]) => value),
			);
			return;
		}

		const pairs = inputPairs.map(([key, value]) => [key, copyBytes(value)] as const);
		if (pairs.length === 0) {
			return;
		}
		const keys = pairs.map(([key]) => key);
		const values = pairs.map(([, value]) => value);
		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_records(keys, values),
				);
			}
			native.put_many(keys, values);
		});
	}

	/**
	 * Fast path for content-addressed immutable bytes; callers must not mutate
	 * values after calling.
	 */
	putManyImmutable(
		entries: Iterable<readonly [string, Uint8Array]>,
	): Promise<void> | void {
		const inputPairs = Array.from(entries);
		if (inputPairs.length === 0) {
			return;
		}
		const keys = inputPairs.map(([key]) => key);
		const values = inputPairs.map(([, value]) => value);
		const native = this.openTransientNative();
		if (native) {
			native.put_many(keys, values);
			return;
		}
		const normalNative = this.openNormalDurabilityNative();
		if (normalNative && this.directory) {
			const journalError = this.takePendingJournalError();
			if (journalError) {
				return Promise.reject(journalError);
			}
			this.recordJournal(
				this.journaledNative(normalNative).encode_put_records(keys, values),
			);
			normalNative.put_many(keys, values);
			return;
		}

		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_records(keys, values),
				);
			}
			native.put_many(keys, values);
		});
	}

	del(key: string): Promise<void> | void {
		const native = this.openTransientNative();
		if (native) {
			native.delete(key);
			return;
		}

		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_delete_record(key),
				);
			}
			native.delete(key);
		});
	}

	delMany(keys: Iterable<string>): Promise<number> | number {
		const keyList = Array.from(keys);
		if (keyList.length === 0) {
			return 0;
		}
		const native = this.openTransientNative();
		if (native) {
			return native.delete_many(keyList);
		}

		return this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_delete_records(keyList),
				);
			}
			return native.delete_many(keyList);
		});
	}

	async getMany(keys: Iterable<string>): Promise<Array<Uint8Array | undefined>> {
		const native = await this.ensureOpen();
		return native.get_many(Array.from(keys)).map((value) => value ?? undefined);
	}

	async hasMany(keys: string[]): Promise<boolean[]> {
		const native = await this.ensureOpen();
		return native.has_many(keys);
	}

	async sublevel(name: string): Promise<AnyStore> {
		let child = this.children.get(name);
		if (!child) {
			child = new RustAnyStore(this.directory, [...this.level, name], this.options);
			this.children.set(name, child);
		}
		if (this._status === "open") {
			await child.open();
		}
		return child;
	}

	iterator(): {
		[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
	} {
		return {
			[Symbol.asyncIterator]: () => {
				let entriesPromise: Promise<Array<[string, Uint8Array]>> | undefined;
				let index = 0;
				return {
					next: async () => {
						entriesPromise ??= this.ensureOpen().then((native) => native.entries());
						const entries = await entriesPromise;
						if (index >= entries.length) {
							return { done: true, value: undefined };
						}
						const [key, value] = entries[index++];
						return {
							done: false,
							value: [key, value] as [string, Uint8Array],
						};
					},
				};
			},
		};
	}

	async clear(): Promise<void> {
		await this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(this.journaledNative(native).encode_clear_record());
			}
			native.clear();
		});
		for (const child of this.children.values()) {
			await child.clear();
			await child.close();
			// clear() resets children of a still-open parent; this close is an
			// internal reset, not a user-facing close, so children stay usable.
			child.explicitlyClosed = false;
		}
		if (this.directory) {
			await this.removeSublevelsDirectory();
		}
	}

	async size(): Promise<number> {
		return (await this.ensureOpen()).size();
	}

	persisted(): boolean {
		return this.directory != null;
	}

	private async openInternal(): Promise<void> {
		const wasm = await loadWasm();
		this.native = new wasm.NativeAnyStore();
		if (!this.directory) {
			return;
		}
		const native = this.journaledNative(this.native);
		this.persistence = await createPersistenceBackend(this.directory, this.level);
		const snapshot = await this.persistence.readSnapshot();
		if (snapshot && snapshot.byteLength > 0) {
			native.load_snapshot(snapshot);
		}
		const journal = await this.persistence.readJournal();
		if (journal && journal.byteLength > 0) {
			const applied = native.apply_journal(journal);
			if (applied < journal.byteLength) {
				// Torn tail from a mid-write crash: rewrite the checkpoint so new
				// records are not appended after the unreadable bytes and lost on
				// the next replay.
				await this.persistence.writeSnapshot(native.snapshot());
			}
		}
	}

	private async ensureOpen(allowDraining = false): Promise<NativeAnyStore> {
		if (this._status === "open" && this.native) {
			return this.native;
		}
		// Mutations already enqueued before close() drain against the live
		// native while the store is closing.
		if (allowDraining && this.native && this._status === "closing") {
			return this.native;
		}
		if (this.explicitlyClosed) {
			throw new Error("RustAnyStore is closed");
		}
		if (this._status !== "open") {
			await this.open();
		}
		if (!this.native) {
			throw new Error("RustAnyStore is not open");
		}
		return this.native;
	}

	private openTransientNative(): NativeAnyStore | undefined {
		if (
			this.directory == null &&
			!this.explicitlyClosed &&
			this.pendingCloses === 0 &&
			this._status === "open" &&
			this.native &&
			this.queuedMutations === 0
		) {
			return this.native;
		}
	}

	private openNormalDurabilityNative(): NativeAnyStore | undefined {
		if (
			this.options.durability !== "strict" &&
			!this.explicitlyClosed &&
			this.pendingCloses === 0 &&
			this._status === "open" &&
			this.native &&
			this.queuedMutations === 0
		) {
			return this.native;
		}
	}

	private takePendingJournalError(): unknown {
		const error = this.journalError;
		this.journalError = undefined;
		return error;
	}

	private enqueueMutation<T>(fn: (native: NativeAnyStore) => Promise<T>): Promise<T> {
		const journalError = this.takePendingJournalError();
		if (journalError) {
			return Promise.reject(journalError);
		}
		if (this.explicitlyClosed) {
			return Promise.reject(new Error("RustAnyStore is closed"));
		}
		this.queuedMutations++;
		// A mutation enqueued behind a pending close belongs to the queued
		// re-open and must not drain into the closing native.
		const drainAllowed = this.pendingCloses === 0;
		const next = this.mutationQueue.then(async () =>
			fn(await this.ensureOpen(drainAllowed)),
		);
		this.mutationQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next.finally(() => {
			this.queuedMutations--;
		});
	}

	private journaledNative(native: NativeAnyStore): JournaledNativeAnyStore {
		if (!("encode_put_record" in native)) {
			throw new Error("RustAnyStore native store does not expose journal records");
		}
		return native as JournaledNativeAnyStore;
	}

	private async compact(): Promise<void> {
		const native = this.native;
		if (!native) {
			return;
		}
		const journaled = this.journaledNative(native);
		await this.waitForJournal();
		if (!this.persistence) {
			throw new Error("RustAnyStore persistence backend is not open");
		}
		await this.persistence.writeSnapshot(journaled.snapshot());
	}

	private recordJournal(record: Uint8Array): Promise<void> {
		const write = this.journalQueue
			.then(() => {
				if (!this.persistence) {
					throw new Error("RustAnyStore persistence backend is not open");
				}
				return this.persistence.appendJournal(
					record,
					this.options.durability ?? "normal",
				);
			})
			.catch((error) => {
				this.journalError = error;
				throw error;
			});
		this.journalQueue = write.then(
			() => undefined,
			() => undefined,
		);
		return this.options.durability === "strict" ? write : Promise.resolve();
	}

	private async waitForJournal(): Promise<void> {
		await this.journalQueue;
		const journalError = this.takePendingJournalError();
		if (journalError) {
			throw journalError;
		}
	}

	private async removeSublevelsDirectory(): Promise<void> {
		await this.persistence?.removeSublevels();
	}
}

export const createStore = (
	directory?: string,
	options: RustAnyStoreOptions = {},
): RustAnyStore => new RustAnyStore(directory, [], options);
