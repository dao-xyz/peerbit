import type { AnyStore } from "@peerbit/any-store";
import {
	createStore as createRustStore,
	type RustAnyStoreOptions,
} from "@peerbit/any-store-rust";
import {
	create as createRustIndexer,
	type RustIndexerOptions,
} from "@peerbit/indexer-rust";
import {
	createNativeLogBlockStore,
	type NativeLogBlockStore,
} from "@peerbit/log-rust";
import type {
	CreateInstanceOptions,
	StorageCreateOptions,
	StoreFactory,
} from "./peer.js";

export type PeerbitRustStorageOptions = {
	default?: RustAnyStoreOptions;
	blocks?: RustAnyStoreOptions;
	nativeLogBlocks?: boolean;
	keychain?: RustAnyStoreOptions;
};

export type PeerbitRustOptions = {
	storage?: PeerbitRustStorageOptions;
	indexer?: RustIndexerOptions;
};

export type PeerbitRustCreateOptions = {
	storage: StorageCreateOptions;
	indexer: NonNullable<CreateInstanceOptions["indexer"]>;
};

class LazyNativeLogBlockStore implements AnyStore {
	private store?: NativeLogBlockStore;
	private openPromise?: Promise<void>;

	getNativeLogBlockStoreHandle(): unknown {
		return this.store?.getNativeLogBlockStoreHandle();
	}

	status(): "opening" | "open" | "closing" | "closed" {
		return this.store?.status() ?? "closed";
	}

	async open(): Promise<void> {
		if (this.store?.status() === "open") {
			return;
		}
		this.openPromise ??= (async () => {
			this.store ??= await createNativeLogBlockStore();
			await this.store.open();
		})().finally(() => {
			this.openPromise = undefined;
		});
		return this.openPromise;
	}

	async close(): Promise<void> {
		await this.openPromise;
		await this.store?.close();
	}

	private async ready(): Promise<NativeLogBlockStore> {
		await this.open();
		return this.store!;
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		return (await this.ready()).get(key);
	}

	async hasMany(keys: string[]): Promise<boolean[]> {
		return (await this.ready()).hasMany(keys);
	}

	async put(key: string, value: Uint8Array): Promise<void> {
		await (await this.ready()).put(key, value);
	}

	async putImmutable(key: string, value: Uint8Array): Promise<void> {
		await (await this.ready()).putImmutable(key, value);
	}

	async putMany(entries: Iterable<readonly [string, Uint8Array]>): Promise<void> {
		await (await this.ready()).putMany(Array.from(entries));
	}

	async putManyImmutable(
		entries: Iterable<readonly [string, Uint8Array]>,
	): Promise<void> {
		await (await this.ready()).putManyImmutable(Array.from(entries));
	}

	async del(key: string): Promise<void> {
		await (await this.ready()).del(key);
	}

	async sublevel(): Promise<LazyNativeLogBlockStore> {
		return new LazyNativeLogBlockStore();
	}

	iterator() {
		const self = this;
		return {
			async *[Symbol.asyncIterator]() {
				for await (const entry of await (await self.ready()).iterator()) {
					yield entry;
				}
			},
		};
	}

	async clear(): Promise<void> {
		await (await this.ready()).clear();
	}

	async size(): Promise<number> {
		return (await this.ready()).size();
	}

	persisted(): boolean {
		return false;
	}
}

const createRustStoreFactory =
	(options: RustAnyStoreOptions | undefined): StoreFactory =>
	(directory) =>
		createRustStore(directory, options);

const createRustBlocksStoreFactory =
	(
		options: RustAnyStoreOptions | undefined,
		nativeLogBlocks: boolean | undefined,
	): StoreFactory =>
	(directory) =>
		nativeLogBlocks
			? new LazyNativeLogBlockStore()
			: createRustStore(directory, options);

export const createRustStorageOptions = (
	options: PeerbitRustStorageOptions = {},
): StorageCreateOptions => ({
	storeFactory: createRustStoreFactory(options.default),
	blocksStoreFactory: createRustBlocksStoreFactory(
		options.blocks ?? options.default,
		options.nativeLogBlocks,
	),
	keychainStoreFactory: createRustStoreFactory(
		options.keychain ?? options.default,
	),
});

export const createRustPeerbitOptions = (
	options: PeerbitRustOptions = {},
): PeerbitRustCreateOptions => ({
	storage: createRustStorageOptions(options.storage),
	indexer: (directory) => createRustIndexer(directory, options.indexer),
});
