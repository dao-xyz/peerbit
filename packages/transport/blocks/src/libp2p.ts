import { deserialize, serialize } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";
import type { AnyStore } from "@peerbit/any-store-interface";
import type { GetOptions, Blocks as IBlocks } from "@peerbit/blocks-interface";
import { getPublicKeyFromPeerId, type PublicSignKey } from "@peerbit/crypto";
import { DirectStream } from "@peerbit/stream";
import {
	type DirectStreamComponents,
	type RustCoreStream,
} from "@peerbit/stream";
import {
	createRequestTransportContext,
	type DataMessage,
	type WaitForPeersFn,
} from "@peerbit/stream-interface";
import type { Block } from "multiformats/block";
import { AnyBlockStore } from "./any-blockstore.js";
import { BlockMessage, BlockRequest, BlockResponse, RemoteBlocks } from "./remote.js";

export type DirectBlockComponents = DirectStreamComponents;

export class DirectBlock extends DirectStream implements IBlocks {
	private remoteBlocks: RemoteBlocks;
	private onDataFn: any;
	private onPeerConnectedFn: any;

	constructor(
		components: DirectBlockComponents,
		options?: {
			directory?: string;
			localStore?: AnyStore;
			canRelayMessage?: boolean;
			localTimeout?: number;
			messageProcessingConcurrency?: number;
			eagerBlocks?: boolean | { cacheSize?: number };
			resolveProviders?: (
				cid: string,
				options?: { signal?: AbortSignal },
			) => Promise<string[] | undefined> | string[] | undefined;
			watchProviders?: (
				cid: string,
				options: {
					signal?: AbortSignal;
					onProviders: (providers: string[]) => void;
				},
			) => void | { close: () => void } | (() => void);
			onPut?: (cid: string) => Promise<void> | void;
			providerCache?:
				| boolean
				| {
						maxEntries?: number;
						ttlMs?: number;
						maxProvidersPerCid?: number;
				  };
			requeryOnReachable?: number;
			/**
			 * Run the block-exchange protocol on the native DirectStream core
			 * (`@peerbit/network-rust`): codec, provider resolution and caches
			 * execute in wasm, and natively stored blocks are served without
			 * surfacing their bytes to JS. Defaults to the same rust-core mode
			 * as the underlying DirectStream.
			 */
			rustCore?: RustCoreStream | false;
		},
	) {
		if (options?.directory && options.localStore) {
			throw new Error("DirectBlock options cannot include both directory and localStore");
		}

		super(components, ["/peerbit/direct-block/1.0.0"], {
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			canRelayMessage: options?.canRelayMessage ?? true,
			connectionManager: {
				dialer: false,
				pruner: false,
			},
			rustCore: options?.rustCore,
		});

		const blockExchange = this.rustCore?.blockExchange;
		const defaultResolveProviders = () => {
			if (blockExchange) {
				const negotiated = [...this.peers.keys()];
				const connected: string[] = [];
				for (const conn of this.components.connectionManager.getConnections()) {
					try {
						connected.push(
							getPublicKeyFromPeerId(conn.remotePeer).hashcode(),
						);
					} catch {
						// ignore unexpected key types
					}
				}
				return blockExchange.defaultProviderCandidates(
					negotiated,
					connected,
					this.publicKeyHash,
				);
			}
			const out: string[] = [];
			const push = (hash?: string) => {
				if (!hash) return;
				if (hash === this.publicKeyHash) return;
				// Small bounded list; avoid Set allocations on hot paths.
				if (out.includes(hash)) return;
				out.push(hash);
			};

			// Prefer peers we've already negotiated streams with for this protocol.
			for (const h of this.peers.keys()) {
				push(h);
				if (out.length >= 32) return out;
			}

			// Fall back to currently connected libp2p peers (even if we haven't opened
			// a `/peerbit/direct-block` stream yet). This makes "join by hash" flows work
			// without requiring an explicit `remote.from` list.
			for (const conn of this.components.connectionManager.getConnections()) {
				try {
					push(getPublicKeyFromPeerId(conn.remotePeer).hashcode());
				} catch {
					// ignore unexpected key types
				}
				if (out.length >= 32) break;
			}

			return out;
		};
		this.remoteBlocks = new RemoteBlocks({
			local: new AnyBlockStore(
				options?.localStore ?? createStore(options?.directory),
			),
			publish: (message, options) =>
				this.publish(this.encodeBlockMessage(message), options),
			localTimeout: options?.localTimeout || 1000,
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			waitFor: this.waitFor.bind(this) as WaitForPeersFn,
			publicKey: this.publicKey,
			eagerBlocks: options?.eagerBlocks,
			resolveProviders: options?.resolveProviders ?? defaultResolveProviders,
			watchProviders: options?.watchProviders,
			onPut: options?.onPut,
			providerCache: options?.providerCache,
			requeryOnReachable: options?.requeryOnReachable,
			rust: blockExchange
				? {
						exchange: blockExchange,
						publishRaw: (payload, options) => this.publish(payload, options),
					}
				: undefined,
		});

		this.onDataFn = (data: CustomEvent<DataMessage>) => {
			data.detail?.data?.length &&
				data.detail?.data.length > 0 &&
				this.remoteBlocks.onMessage(
					this.decodeBlockMessage(data.detail.data!),
					{
						from: data.detail.header.signatures?.publicKeys[0]?.hashcode(),
						transport: createRequestTransportContext(data.detail),
					},
				);
		};
		this.onPeerConnectedFn = (evt: CustomEvent<PublicSignKey>) =>
			this.remoteBlocks.onReachable(evt.detail);
	}

	private encodeBlockMessage(message: BlockRequest | BlockResponse): Uint8Array {
		const blockExchange = this.rustCore?.blockExchange;
		if (blockExchange) {
			if (message instanceof BlockRequest) {
				return blockExchange.encodeBlockRequest(message.cid);
			}
			if (message instanceof BlockResponse) {
				return blockExchange.encodeBlockResponse(message.cid, message.bytes);
			}
		}
		return serialize(message);
	}

	private decodeBlockMessage(bytes: Uint8Array): BlockMessage {
		const blockExchange = this.rustCore?.blockExchange;
		if (blockExchange) {
			const decoded = blockExchange.decodeBlockMessage(bytes);
			return decoded.type === "request"
				? new BlockRequest(decoded.cid)
				: new BlockResponse(decoded.cid, decoded.bytes);
		}
		return deserialize(bytes, BlockMessage);
	}

	getNativeLogBlockStoreHandle(): unknown {
		return this.remoteBlocks.getNativeLogBlockStoreHandle();
	}

	async put(
		bytes: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): Promise<string> {
		return this.remoteBlocks.put(bytes);
	}

	async putMany(
		blocks: Array<
			Uint8Array | { block: Block<any, any, any, any>; cid: string }
		>,
	): Promise<string[]> {
		return this.remoteBlocks.putMany(blocks);
	}

	async putKnown(cid: string, bytes: Uint8Array): Promise<string> {
		return this.remoteBlocks.putKnown(cid, bytes);
	}

	async putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): Promise<string[]> {
		return this.remoteBlocks.putKnownMany(blocks);
	}

	async has(cid: string) {
		return this.remoteBlocks.has(cid);
	}
	async hasMany(cids: string[]): Promise<boolean[]> {
		return this.remoteBlocks.hasMany(cids);
	}
	async get(
		cid: string,
		options?: GetOptions | undefined,
	): Promise<Uint8Array | undefined> {
		return this.remoteBlocks.get(cid, options);
	}

	async getMany(
		cids: string[],
		options?: GetOptions | undefined,
	): Promise<Array<Uint8Array | undefined>> {
		return this.remoteBlocks.getMany(cids, options);
	}

	hintProviders(cid: string, providers: string[]) {
		this.remoteBlocks.hintProviders(cid, providers);
	}

	async rm(cid: string) {
		return this.remoteBlocks.rm(cid);
	}

	async rmMany(cids: string[]) {
		return this.remoteBlocks.rmMany(cids);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this.remoteBlocks.iterator()) {
			yield [key, value];
		}
	}

	async start(): Promise<void> {
		this.addEventListener("data", this.onDataFn);
		this.addEventListener("peer:reachable", this.onPeerConnectedFn);
		await super.start();
		await this.remoteBlocks.start();
	}

	async stop(): Promise<void> {
		this.removeEventListener("data", this.onDataFn);
		this.removeEventListener("peer:reachable", this.onPeerConnectedFn);
		await super.stop();
		await this.remoteBlocks.stop();
	}

	async size() {
		return this.remoteBlocks?.size() || 0;
	}
	get status() {
		return this.remoteBlocks?.status || this.started;
	}

	persisted(): boolean | Promise<boolean> {
		return this.remoteBlocks.persisted();
	}
}
