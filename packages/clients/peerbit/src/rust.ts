import {
	createStore as createRustStore,
	type RustAnyStoreOptions,
} from "@peerbit/any-store-rust";
import {
	create as createRustIndexer,
	type RustIndexerOptions,
} from "@peerbit/indexer-rust";
import type {
	CreateInstanceOptions,
	StorageCreateOptions,
	StoreFactory,
} from "./peer.js";

export type PeerbitRustStorageOptions = {
	default?: RustAnyStoreOptions;
	blocks?: RustAnyStoreOptions;
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

const createRustStoreFactory =
	(options: RustAnyStoreOptions | undefined): StoreFactory =>
	(directory) =>
		createRustStore(directory, options);

export const createRustStorageOptions = (
	options: PeerbitRustStorageOptions = {},
): StorageCreateOptions => ({
	storeFactory: createRustStoreFactory(options.default),
	blocksStoreFactory: createRustStoreFactory(options.blocks ?? options.default),
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
