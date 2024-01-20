import { createLibp2p, Libp2p, Libp2pOptions, ServiceFactoryMap } from "libp2p";
import { DirectSub } from "@peerbit/pubsub";
import { DirectBlock } from "@peerbit/blocks";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { transports, relay, listen } from "./transports.js";
import { identify } from "@libp2p/identify";
import { CircuitRelayService } from "@libp2p/circuit-relay-v2";
import { yamux } from "@chainsafe/libp2p-yamux";
import { DefaultKeychain, Keychain } from "@peerbit/keychain";

export type Libp2pExtendServices = {
	pubsub: DirectSub;
	blocks: DirectBlock;
	keychain: Keychain;
};
export type Libp2pExtended = Libp2p<
	{ relay: CircuitRelayService; identify: any } & Libp2pExtendServices
>;

export type Libp2pCreateOptions = Libp2pOptions<
	Libp2pExtendServices & { relay: CircuitRelayService; identify: any }
>;

export type Libp2pCreateOptionsWithServices = Libp2pCreateOptions & {
	services: ServiceFactoryMap<Libp2pExtendServices>;
};

export const createLibp2pExtended = (
	opts: Libp2pCreateOptions = {
		services: {
			blocks: (c) => new DirectBlock(c),
			pubsub: (c) => new DirectSub(c),
			keychain: (c) => new DefaultKeychain()
		}
	}
): Promise<Libp2pExtended> => {
	const relayIdentify = {
		relay: relay(),
		identify: identify()
	};

	// https://github.com/libp2p/js-libp2p/issues/1757
	Object.keys(relayIdentify).forEach((key) => {
		if (relayIdentify[key] === undefined) {
			delete relayIdentify[key];
		}
	});

	return createLibp2p({
		...opts,
		connectionManager: {
			minConnections: 0,
			...opts.connectionManager
		},
		addresses: {
			listen: listen(),
			...opts.addresses
		},
		transports: opts.transports || transports(),
		connectionEncryption: opts.connectionEncryption || [noise()],
		streamMuxers: opts.streamMuxers || [yamux(), mplex()],
		services: {
			...relayIdentify,
			pubsub:
				opts.services?.pubsub ||
				((c) =>
					new DirectSub(c, {
						canRelayMessage: true
						// auto dial true
						// auto prune true
					})),
			blocks: opts.services?.blocks || ((c) => new DirectBlock(c)),
			keychain: opts.services?.keychain || ((c) => new DefaultKeychain()),
			...opts.services
		}
	});
};
