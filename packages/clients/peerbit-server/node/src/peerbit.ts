import { DirectBlock } from "@peerbit/blocks";
import { DirectSub } from "@peerbit/pubsub";
import { Peerbit } from "peerbit";
import path from "path";

export const LIBP2P_LISTEN_PORT = 8001;
export const create = (properties: {
	directory?: string;
	domain?: string;
	listenPort?: number;
}) => {
	const listenPort = properties.listenPort ?? LIBP2P_LISTEN_PORT;
	const blocksDirectory =
		properties.directory != null
			? path.join(properties.directory, "/blocks").toString()
			: undefined;

	return Peerbit.create({
		libp2p: {
			addresses: {
				announce: properties.domain
					? [
							`/dns4/${properties.domain}/tcp/4002`,
							`/dns4/${properties.domain}/tcp/4003/wss`,
					  ]
					: undefined,
				listen: [
					`/ip4/127.0.0.1/tcp/${listenPort}`,
					`/ip4/127.0.0.1/tcp/${
						listenPort !== 0 ? listenPort + 1 : listenPort
					}/ws`,
				],
			},
			connectionManager: {
				maxConnections: Infinity,
				minConnections: 0,
			},
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						directory: blocksDirectory,
						canRelayMessage: true,
					}),
				pubsub: (c) => new DirectSub(c, { canRelayMessage: true }),
			},
		},
		directory: properties.directory,
	});
};
