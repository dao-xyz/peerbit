import type { PeerId as Libp2pPeerId } from "@libp2p/interface/peer-id";
import { Blocks } from "@peerbit/blocks-interface";
import { PubSub } from "@peerbit/pubsub-interface";
import { Ed25519PublicKey, Identity } from "@peerbit/crypto";
import { Keychain } from "@peerbit/keychain";
import type { AnyStore } from "@peerbit/any-store";
import { Multiaddr } from "@multiformats/multiaddr";
import { Address } from "./address.js";
import { CanOpen, ExtractArgs, Manageable, OpenOptions } from "./handler.js";

export interface Client<T extends Manageable<ExtractArgs<T>>> {
	peerId: Libp2pPeerId;
	identity: Identity<Ed25519PublicKey>;
	getMultiaddrs: () => Multiaddr[];
	dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean>;
	services: {
		pubsub: PubSub;
		blocks: Blocks;
		keychain: Keychain;
	};
	memory: AnyStore;
	start(): Promise<void>;
	stop(): Promise<void>;
	open<S extends T & CanOpen<ExtractArgs<S>>>(
		program: S | Address,
		options?: OpenOptions<S>
	): Promise<S>;
}
