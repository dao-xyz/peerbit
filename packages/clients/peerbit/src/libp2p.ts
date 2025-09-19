import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import type { CircuitRelayService } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { DirectBlock } from "@peerbit/blocks";
import {
	DefaultCryptoKeychain,
	type IPeerbitKeychain,
	keychain,
} from "@peerbit/keychain";
import { DirectSub } from "@peerbit/pubsub";
import {
	type Libp2p,
	type Libp2pOptions,
	type ServiceFactoryMap,
	createLibp2p,
} from "libp2p";
import { listen, relay, transports } from "./transports.js";

export type Libp2pExtendServices = {
	pubsub: DirectSub;
	blocks: DirectBlock;
	keychain: IPeerbitKeychain;
};
export type Libp2pExtended = Libp2p<
	{ relay: CircuitRelayService; identify: any } & Libp2pExtendServices
>;

export type Libp2pCreateOptions = Libp2pOptions<
	Partial<Libp2pExtendServices & { relay: CircuitRelayService; identify: any }>
>;

export type PartialLibp2pCreateOptions = Libp2pOptions<
	Partial<Libp2pExtendServices & { relay: CircuitRelayService; identify: any }>
>;

export type Libp2pCreateOptionsWithServices = Libp2pCreateOptions & {
	services: ServiceFactoryMap<Libp2pExtendServices>;
};

export const createLibp2pExtended = (
	opts: PartialLibp2pCreateOptions = {
		services: {
			blocks: (c: any) => new DirectBlock(c),
			pubsub: (c: any) => new DirectSub(c),
			keychain: keychain(),
		},
	},
): Promise<Libp2pExtended> => {
	let extraServices: any = {};

	if (opts.services?.["relay"] === null) {
		delete opts.services?.["relay"];
	} else if (!opts.services?.["relay"]) {
		const relayComponent = relay();
		if (relayComponent) {
			// will be null in browser
			extraServices["relay"] = relayComponent;
		}
	}

	if (!opts.services?.["identify"]) {
		extraServices["identify"] = identify();
	}

	return createLibp2p({
		...opts,
		connectionManager: {
			inboundStreamProtocolNegotiationTimeout: 1e4,
			inboundUpgradeTimeout: 1e4,
			outboundStreamProtocolNegotiationTimeout: 1e4,
			outboundUpgradeTimeout: 1e4,
			reconnectRetries: 0, // https://github.com/libp2p/js-libp2p/issues/3289
			...opts.connectionManager,
		},
		addresses: {
			listen: listen(),
			...opts.addresses,
		},
		connectionMonitor: {
			abortConnectionOnPingFailure: false,
			...opts?.connectionMonitor,
		},

		transports: opts.transports || transports(),
		connectionEncrypters: opts.connectionEncrypters || [noise()],
		streamMuxers: opts.streamMuxers || [yamux()],
		services: {
			pubsub:
				opts.services?.pubsub ||
				((c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						// auto dial true
						// auto prune true
					})),
			blocks: opts.services?.blocks || ((c) => new DirectBlock(c)),
			keychain: opts.services?.keychain || ((c) => new DefaultCryptoKeychain()),
			...opts.services,
			...extraServices,
		},
	});
};
