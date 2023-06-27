import type { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";
import { Blocks } from "@peerbit/blocks-interface";
import { PubSub } from "@peerbit/pubsub-interface";
import { Ed25519PublicKey, Identity, Keychain } from "@peerbit/crypto";
import type { SimpleLevel } from "@dao-xyz/lazy-level";
import { Multiaddr } from "@multiformats/multiaddr";
import { Address } from "./address";

export type WithArgs<Args> = { args?: Args };
export type WithParent<T> = { parent?: T };
export type CanOpen<Args> = { open(args?: Args): Promise<void> };

export interface Client<T extends P, P> {
	peerId: Libp2pPeerId;
	identity: Identity<Ed25519PublicKey>;
	getMultiaddrs: () => Multiaddr[];
	dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean>;
	services: {
		pubsub: PubSub;
		blocks: Blocks;
	};
	memory?: SimpleLevel;
	keychain?: Keychain;
	start(): Promise<void>;
	stop(): Promise<void>;
	open<TExt extends T & CanOpen<Args>, Args = any>(
		program: TExt | Address,
		options?: WithArgs<Args> & WithParent<P>
	): Promise<TExt>;
}
