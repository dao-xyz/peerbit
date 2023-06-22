import type { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";
import { Ed25519PublicKey, Identity, Keychain } from "@peerbit/crypto";
import { Multiaddr } from "@multiformats/multiaddr";
import { Blocks } from "@peerbit/blocks-interface";
import { PubSub } from "@peerbit/pubsub-interface";
import type { SimpleLevel } from "@dao-xyz/lazy-level";
export type MaybePromise<T> = Promise<T> | T;

export interface Peerbit {
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
	stop(): Promise<void>;
}
