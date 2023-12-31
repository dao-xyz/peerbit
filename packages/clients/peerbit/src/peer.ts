import { AnyStore } from "../../../utils/any-store/lib/esm/index.js";
import { multiaddr, Multiaddr, isMultiaddr } from "@multiformats/multiaddr";
import type { Libp2p } from "libp2p";
import { Ed25519Keypair, Ed25519PublicKey } from "@peerbit/crypto";
import {
	Program,
	Address,
	ProgramClient,
	ProgramHandler
} from "@peerbit/program";
import { DirectSub } from "@peerbit/pubsub";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { waitFor } from "@peerbit/time";
import "@libp2p/peer-id";

import {
	createLibp2pExtended,
	Libp2pExtended,
	Libp2pCreateOptions as ClientCreateOptions
} from "./libp2p.js";
import { DirectBlock } from "@peerbit/blocks";
import { LevelDatastore } from "datastore-level";
import { logger as loggerFn } from "@peerbit/logger";
import { OpenOptions } from "@peerbit/program";
import { resolveBootstrapAddresses } from "./bootstrap.js";
import { createStore } from "@peerbit/any-store";
import { DefaultKeychain } from "@peerbit/keychain";

export const logger = loggerFn({ module: "client" });

export type OptionalCreateOptions = {
	libp2pExternal?: boolean;
};
export type CreateOptions = {
	directory?: string;
	memory: AnyStore;
	identity: Ed25519Keypair;
} & OptionalCreateOptions;
type ExtractArgs<T> = T extends Program<infer Args> ? Args : never;
type Libp2pOptions = { libp2p?: Libp2pExtended | ClientCreateOptions };
type SimpleLibp2pOptions = { relay?: boolean };
export type CreateInstanceOptions = (SimpleLibp2pOptions | Libp2pOptions) & {
	directory?: string;
} & OptionalCreateOptions;

const isLibp2pInstance = (libp2p: Libp2pExtended | ClientCreateOptions) =>
	!!(libp2p as Libp2p).getMultiaddrs;

const createCache = async (
	directory: string | undefined,
	options?: { reset?: boolean }
) => {
	const cache = createStore(directory);

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache.clear();
	}
	return cache;
};

const SELF_IDENTITY_KEY_ID = new TextEncoder().encode("__self__");

export class Peerbit implements ProgramClient {
	_libp2p: Libp2pExtended;

	directory?: string;

	private _memory: AnyStore;
	private _libp2pExternal?: boolean = false;

	// Libp2p peerid in Identity form
	private _identity: Ed25519Keypair;
	private _handler: ProgramHandler;

	constructor(libp2p: Libp2pExtended, options: CreateOptions) {
		if (libp2p == null) {
			throw new Error("Libp2p required");
		}
		this._libp2p = libp2p;
		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					this.libp2p.peerId.type
			);
		}

		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error("Only Ed25519 peerIds are supported");
		}

		this._identity = options.identity;
		this.directory = options.directory;
		this._memory = options.memory;
		this._libp2pExternal = options.libp2pExternal;
	}

	static async create(options: CreateInstanceOptions = {}): Promise<Peerbit> {
		await sodium.ready; // Some of the modules depends on sodium to be readyy

		let libp2pExtended: Libp2pExtended | undefined = (options as Libp2pOptions)
			.libp2p as Libp2pExtended;
		const asRelay = (options as SimpleLibp2pOptions).relay;

		const directory = options.directory;
		const hasDir = directory != null;

		const memory = await createCache(
			directory != null ? path.join(directory, "/cache") : undefined
		);

		const blocksDirectory = hasDir
			? path.join(options["directory"], "/blocks").toString()
			: undefined;

		const keychainDirectory = hasDir
			? path.join(options["directory"], "/keychain").toString()
			: undefined;

		const datastore = hasDir
			? new LevelDatastore(
					path.join(options["directory"], "/libp2p").toString()
				)
			: undefined;

		if (datastore) {
			await datastore.open();
		}

		const libp2pExternal = libp2pExtended && isLibp2pInstance(libp2pExtended);
		if (!libp2pExternal) {
			const extendedOptions: ClientCreateOptions | undefined =
				libp2pExtended as any as ClientCreateOptions;
			const keychain = new DefaultKeychain({
				store: createStore(keychainDirectory)
			});
			const peerId =
				extendedOptions?.peerId ||
				(await (
					await keychain.exportById(SELF_IDENTITY_KEY_ID, Ed25519Keypair)
				)?.toPeerId());
			libp2pExtended = await createLibp2pExtended({
				...extendedOptions,
				peerId,
				services: {
					keychain: (c) => keychain,
					blocks: (c) =>
						new DirectBlock(c, {
							canRelayMessage: asRelay,
							directory: blocksDirectory
						}),
					pubsub: (c) => new DirectSub(c, { canRelayMessage: asRelay }),
					...extendedOptions?.services
				},
				datastore
			});
		}
		if (datastore) {
			const stopFn = libp2pExtended.stop.bind(libp2pExtended);
			libp2pExtended.stop = async () => {
				await stopFn();
				await datastore?.close();
			};
		}

		if (
			libp2pExtended.status === "stopped" ||
			libp2pExtended.status === "stopping"
		) {
			await libp2pExtended.start();
		}

		if (libp2pExtended.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					libp2pExtended.peerId.type
			);
		}

		const identity = Ed25519Keypair.fromPeerId(libp2pExtended.peerId);
		try {
			await libp2pExtended.services.keychain.import({
				keypair: identity,
				id: SELF_IDENTITY_KEY_ID
			});
		} catch (error: any) {
			if (error.code == "ERR_KEY_ALREADY_EXISTS") {
				// Do nothing
			} else {
				throw error;
			}
		}

		const peer = new Peerbit(libp2pExtended, {
			directory,
			memory: memory,
			libp2pExternal,
			identity
		});
		return peer;
	}
	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get identity(): Ed25519Keypair {
		return this._identity;
	}

	get peerId() {
		return this.libp2p.peerId;
	}

	get services() {
		return this.libp2p.services;
	}

	get handler(): ProgramHandler {
		return this._handler;
	}

	getMultiaddrs(): Multiaddr[] {
		return this.libp2p.getMultiaddrs();
	}
	/**
	 * Dial a peer with an Ed25519 peerId
	 */
	async dial(
		address: string | Multiaddr | Multiaddr[] | ProgramClient
	): Promise<boolean> {
		const maddress =
			typeof address == "string"
				? multiaddr(address)
				: isMultiaddr(address) || Array.isArray(address)
					? address
					: address.getMultiaddrs();
		const connection = await this.libp2p.dial(maddress);
		const publicKey = Ed25519PublicKey.fromPeerId(connection.remotePeer);

		// TODO, do this as a promise instead using the onPeerConnected vents in pubsub and blocks
		return (
			(await waitFor(
				() =>
					this.libp2p.services.pubsub.peers.has(publicKey.hashcode()) &&
					this.libp2p.services.blocks.peers.has(publicKey.hashcode())
			)) || false
		);
	}

	async start() {
		await this._memory.open();

		if (this.libp2p.status === "stopped" || this.libp2p.status === "stopping") {
			this._libp2pExternal = false; // this means we will also close libp2p client on close
			return this.libp2p.start();
		}
	}
	async stop() {
		await this._handler?.stop();
		await this._memory.close();

		// Close libp2p (after above)
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}
	}

	async bootstrap() {
		const addresses = await resolveBootstrapAddresses();
		if (addresses.length === 0) {
			throw new Error("Failed to find any addresses to dial");
		}
		return Promise.all(addresses.map((x) => this.dial(x)));
	}

	/**
	 * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
	 * and to replicate heads (and updates) which is requested by another peer
	 * @param store
	 * @param options
	 * @returns
	 */

	async open<S extends Program<ExtractArgs<S>>>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<ExtractArgs<S>, S> = {}
	): Promise<S> {
		return (
			this._handler || (this._handler = new ProgramHandler({ client: this }))
		).open(storeOrAddress, options);
	}

	get memory() {
		return this._memory;
	}
}
