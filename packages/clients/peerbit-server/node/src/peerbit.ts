import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { DirectBlock } from "@peerbit/blocks";
import type { Ed25519Keypair } from "@peerbit/crypto";
import { DirectSub } from "@peerbit/pubsub";
import path from "path";
import { Peerbit } from "peerbit";
import { concat } from "uint8arrays";

export const LIBP2P_LISTEN_PORT = 8001;
export const create = (properties: {
	directory?: string;
	domain?: string;
	listenPort?: number;
	keypair: Ed25519Keypair;
}) => {
	const listenPort = properties.listenPort ?? LIBP2P_LISTEN_PORT;
	const blocksDirectory =
		properties.directory != null
			? path.join(properties.directory, "/blocks").toString()
			: undefined;
	const privateKey = privateKeyFromRaw(
		concat([
			properties.keypair.privateKey.privateKey,
			properties.keypair.publicKey.publicKey,
		]),
	);

	return Peerbit.create({
		libp2p: {
			privateKey: privateKey,
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
