import { createLibp2p, Libp2p, Libp2pOptions, ServiceFactoryMap } from "libp2p";
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import { DirectBlock } from "@dao-xyz/libp2p-direct-block";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { transports, relay } from "./transports.js";
import { identifyService } from "libp2p/identify";
import { CircuitRelayService } from "libp2p/dist/src/circuit-relay/index.js";

export type Libp2pExtendServices = {
	pubsub: DirectSub;
	blocks: DirectBlock;
};
export type Libp2pExtended = Libp2p<
	{ relay: CircuitRelayService; identify: any } & Libp2pExtendServices
>;

export type CreateOptions = Libp2pOptions<
	Libp2pExtendServices & { relay: CircuitRelayService; identify: any }
>;

export type CreateOptionsWithServices = CreateOptions & {
	services: ServiceFactoryMap<Libp2pExtendServices>;
};

export const createLibp2pExtended = (
	opts: CreateOptions = {
		services: {
			blocks: (c) => new DirectBlock(c),
			pubsub: (c) => new DirectSub(c),
		},
	}
): Promise<Libp2pExtended> => {
	const relayIdentify = {
		relay: relay(),
		identify: identifyService(),
	};

	// https://github.com/libp2p/js-libp2p/issues/1757
	Object.keys(relayIdentify).forEach((key) => {
		if (relayIdentify[key] === undefined) {
			delete relayIdentify[key];
		}
	});

	return createLibp2p({
		connectionManager: {
			minConnections: 0,
		},
		addresses: {
			listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"],
		},
		transports: transports(),
		connectionEncryption: [noise()],
		streamMuxers: [mplex()],
		...opts,
		services: {
			...relayIdentify,
			pubsub:
				opts.services?.pubsub ||
				((c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						signaturePolicy: "StrictNoSign",
						connectionManager: {
							autoDial: true,
						},
					})),
			blocks: opts.services?.blocks || ((c) => new DirectBlock(c)),
			...opts.services,
		},
	});
};
