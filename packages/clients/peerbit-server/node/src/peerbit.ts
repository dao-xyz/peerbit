import { DirectBlock } from "@peerbit/blocks";
import { DirectSub } from "@peerbit/pubsub";
import { Peerbit } from "peerbit";

export const create = (properties: { directory?: string; domain?: string }) => {
	return Peerbit.create({
		libp2p: {
			addresses: {
				announce: properties.domain
					? [
							`/dns4/${properties.domain}/tcp/8001`,
							`/dns4/${properties.domain}/tcp/8002/ws`,
					  ]
					: undefined,
				listen: ["/ip4/127.0.0.1/tcp/8001", "/ip4/127.0.0.1/tcp/8002/ws"],
			},
			connectionManager: {
				maxConnections: Infinity,
				minConnections: 0,
			},
			services: {
				blocks: (c) => new DirectBlock(c, { canRelayMessage: true }),
				pubsub: (c) => new DirectSub(c, { canRelayMessage: true }),
			},
		},
		directory: properties.directory,
	});
};
