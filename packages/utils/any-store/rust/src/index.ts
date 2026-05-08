import { type AnyStore } from "@peerbit/any-store-interface";
import {
	createPersistenceBackend,
	type RustAnyStorePersistenceBackend,
} from "./persistence.js";

type NativeAnyStore = {
	get(key: string): Uint8Array | undefined;
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
	apply_journal(bytes: Uint8Array): void;
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
	NativeRedbAnyStore: new () => NativeAnyStore;
};

export type RustAnyStoreEngine = "custom-wal" | "redb";

export type RustAnyStoreOptions = {
	compactOnClose?: boolean;
	durability?: "normal" | "strict";
	engine?: RustAnyStoreEngine;
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

const copyBytes = (bytes: Uint8Array): Uint8Array =>
	new Uint8Array(bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength
		? bytes
		: bytes.slice());

export class RustAnyStore implements AnyStore {
	private native?: NativeAnyStore;
	private persistence?: RustAnyStorePersistenceBackend;
	private openPromise?: Promise<void>;
	private mutationQueue: Promise<unknown> = Promise.resolve();
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
		if (this._status === "open") {
			return;
		}
		if (this.openPromise) {
			return this.openPromise;
		}
		this._status = "opening";
		this.openPromise = this.openInternal()
			.then(() => {
				this._status = "open";
			})
			.catch((error) => {
				this._status = "closed";
				this.native = undefined;
				this.persistence = undefined;
				throw error;
			})
			.finally(() => {
				this.openPromise = undefined;
			});
		return this.openPromise;
	}

	async close(): Promise<void> {
		if (this._status === "closed") {
			return;
		}
		this._status = "closing";
		await this.mutationQueue;
		await this.waitForJournal();
		for (const child of this.children.values()) {
			await child.close();
		}
		if (this.native && this.directory && this.options.compactOnClose !== false) {
			await this.compact();
		}
		await this.persistence?.close();
		this.persistence = undefined;
		this.native = undefined;
		this._status = "closed";
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		const native = await this.ensureOpen();
		const value = native.get(key);
		return value == null ? undefined : copyBytes(value);
	}

	async put(key: string, value: Uint8Array): Promise<void> {
		const bytes = copyBytes(value);
		await this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_record(key, bytes),
				);
			}
			native.put(key, bytes);
		});
	}

	async putMany(entries: Iterable<readonly [string, Uint8Array]>): Promise<void> {
		const pairs = Array.from(entries, ([key, value]) => [key, copyBytes(value)] as const);
		if (pairs.length === 0) {
			return;
		}
		const keys = pairs.map(([key]) => key);
		const values = pairs.map(([, value]) => value);
		await this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_put_records(keys, values),
				);
			}
			native.put_many(keys, values);
		});
	}

	async del(key: string): Promise<void> {
		await this.enqueueMutation(async (native) => {
			if (this.directory) {
				await this.recordJournal(
					this.journaledNative(native).encode_delete_record(key),
				);
			}
			native.delete(key);
		});
	}

	async delMany(keys: Iterable<string>): Promise<number> {
		const keyList = Array.from(keys);
		if (keyList.length === 0) {
			return 0;
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
		return native
			.get_many(Array.from(keys))
			.map((value) => (value == null ? undefined : copyBytes(value)));
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
							value: [key, copyBytes(value)] as [string, Uint8Array],
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
		if (this.directory && this.options.engine === "redb") {
			throw new Error(
				"@peerbit/any-store-rust redb engine is transient until a byte-range OPFS/Node backend lands",
			);
		}
		this.native =
			this.options.engine === "redb"
				? new wasm.NativeRedbAnyStore()
				: new wasm.NativeAnyStore();
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
			native.apply_journal(journal);
		}
	}

	private async ensureOpen(): Promise<NativeAnyStore> {
		if (this._status !== "open") {
			await this.open();
		}
		if (!this.native) {
			throw new Error("RustAnyStore is not open");
		}
		return this.native;
	}

	private enqueueMutation<T>(fn: (native: NativeAnyStore) => Promise<T>): Promise<T> {
		const next = this.mutationQueue.then(async () => fn(await this.ensureOpen()));
		this.mutationQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next;
	}

	private journaledNative(native: NativeAnyStore): JournaledNativeAnyStore {
		if (!("encode_put_record" in native)) {
			throw new Error("RustAnyStore engine does not expose journal records");
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
		if (this.journalError) {
			throw this.journalError;
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
