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
import { createNativeWireSyncSession } from "@peerbit/native-backbone";
import { createRustCoreStream } from "@peerbit/network-rust";
import type {
	CreateInstanceOptions,
	NativeNetworkCreateOptions,
	StorageCreateOptions,
	StoreFactory,
} from "./peer.js";

export type PeerbitRustStorageOptions = {
	default?: RustAnyStoreOptions;
	blocks?: RustAnyStoreOptions;
	nativeLogBlocks?: boolean;
	keychain?: RustAnyStoreOptions;
};

export type PeerbitRustNetworkOptions = {
	/**
	 * Run the DirectStream engine and the protocol codecs (topic control,
	 * fanout tree, block exchange) on the native core of
	 * `@peerbit/network-rust`. Default true.
	 */
	rustCore?: boolean;
	/**
	 * Decode+verify inbound pubsub frames in the native-backbone wasm module
	 * and stash shared-log raw exchange-head payloads there for the fused
	 * receive (no JS entry decode, no JS block-byte copies). Default true.
	 */
	wireSync?: boolean;
	/**
	 * Advertise native shared-log defaults (native backbone data plane,
	 * native graph, raw exchange-heads sync + the wire-sync session) to
	 * programs opened on this client. Default true.
	 */
	sharedLogDefaults?: boolean;
};

export type PeerbitRustOptions = {
	storage?: PeerbitRustStorageOptions;
	indexer?: RustIndexerOptions;
	/** Native network plane; `false` keeps the js-libp2p wire path. */
	network?: boolean | PeerbitRustNetworkOptions;
};

export type PeerbitRustCreateOptions = {
	storage: StorageCreateOptions;
	indexer: NonNullable<CreateInstanceOptions["indexer"]>;
	network?: NativeNetworkCreateOptions;
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

export const createRustNetworkOptions = (
	options: PeerbitRustNetworkOptions = {},
): NativeNetworkCreateOptions => ({
	rustCore:
		options.rustCore === false ? undefined : () => createRustCoreStream(),
	wireSync:
		options.wireSync === false
			? undefined
			: (selfHash) => createNativeWireSyncSession({ selfHash }),
	sharedLogDefaults: options.sharedLogDefaults,
});

export const createRustPeerbitOptions = (
	options: PeerbitRustOptions = {},
): PeerbitRustCreateOptions => ({
	storage: createRustStorageOptions(options.storage),
	indexer: (directory) => createRustIndexer(directory, options.indexer),
	...(options.network === false
		? {}
		: {
				network: createRustNetworkOptions(
					options.network === true ? {} : options.network,
				),
			}),
});
