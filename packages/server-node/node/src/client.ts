import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import { Peerbit } from "@dao-xyz/peerbit";

export const create = (directory: string) => {
	return Peerbit.create({
		libp2p: {
			addresses: {
				listen: ["/ip4/127.0.0.1/tcp/8001", "/ip4/127.0.0.1/tcp/8002/ws"],
			},
			connectionManager: {
				maxConnections: Infinity,
				minConnections: 0,
			},
			services: {
				pubsub: (c) => new DirectSub(c, { canRelayMessage: true }),
			},
		},
		directory,
	});
};
