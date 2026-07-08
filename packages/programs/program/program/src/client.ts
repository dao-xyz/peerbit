import type { PeerId as Libp2pPeerId, PeerId } from "@libp2p/interface";
import type { Multiaddr } from "@multiformats/multiaddr";
import type { AnyStore } from "@peerbit/any-store-interface";
import { type Blocks } from "@peerbit/blocks-interface";
import type {
	Ed25519PublicKey,
	Identity,
	PublicSignKey,
} from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { type IPeerbitKeychain } from "@peerbit/keychain";
import { type PubSub } from "@peerbit/pubsub-interface";
import type { Address } from "./address.js";
import type {
	CanOpen,
	ExtractArgs,
	Manageable,
	OpenOptions,
} from "./handler.js";

export interface Client<T extends Manageable<ExtractArgs<T>>> {
	peerId: Libp2pPeerId;
	identity: Identity<Ed25519PublicKey>;
	/**
	 * Root directory backing this client's durable storage, when it runs
	 * on-disk. `undefined` for memory-only clients. Programs opened on the
	 * client may derive per-program persistence locations under it (e.g.
	 * shared-log auto-persists its replication coordinates here). The
	 * concrete client (`Peerbit`) already exposes this; declaring it on the
	 * interface lets programs read it without depending on the client package.
	 */
	directory?: string;
	getMultiaddrs: () => Multiaddr[];
	dial(
		address: string | Multiaddr | Multiaddr[],
		options?: {
			dialTimeoutMs?: number;
			serviceWaitTimeoutMs?: number;
			readiness?: "connection" | "services" | "services-and-fanout";
			signal?: AbortSignal;
		},
	): Promise<boolean>;
	hangUp(address: PeerId | PublicSignKey | string | Multiaddr): Promise<void>;
	services: {
		pubsub: PubSub;
		blocks: Blocks;
		keychain: IPeerbitKeychain;
	};
	storage: AnyStore;
	indexer: Indices;
	start(): Promise<void>;
	stop(): Promise<void>;
	open<S extends T & CanOpen<ExtractArgs<S>>>(
		program: S | Address,
		options?: OpenOptions<S>,
	): Promise<S>;
}
