import { keys } from "@libp2p/crypto";
import { DirectBlock } from "@peerbit/blocks";
import type { Ed25519Keypair } from "@peerbit/crypto";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "@peerbit/pubsub";
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
	const topicRootControlPlane = new TopicRootControlPlane({
		defaultCandidates: [properties.keypair.publicKey.hashcode()],
	});
	let fanoutInstance: FanoutTree | undefined;
	const getOrCreateFanout = (c: any) => {
		if (!fanoutInstance) {
			fanoutInstance = new FanoutTree(c, {
				connectionManager: false,
				topicRootControlPlane,
			});
		}
		return fanoutInstance;
	};
	const blocksDirectory =
		properties.directory != null
			? path.join(properties.directory, "/blocks").toString()
			: undefined;
	const privateKey = keys.privateKeyFromRaw(
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
							/* `/dns4/${properties.domain}/udp/4004/webrtc-direct`, */ // TODO: add back when webrtc-direct is supported in browser
						]
					: undefined,
				listen: (() => {
					if (properties.domain) {
						// Direct binds on public ports, keep only local WS for nginx TLS->WS proxy
						return [
							`/ip4/0.0.0.0/tcp/4002`,
							`/ip4/127.0.0.1/tcp/${
								listenPort !== 0 ? listenPort + 1 : listenPort
							}/ws`,
							/* `/ip4/0.0.0.0/udp/4004/webrtc-direct`, */ // TODO: add back when webrtc-direct is supported in browser
						];
					}
					// Local-only defaults (no domain)
					return [
						`/ip4/127.0.0.1/tcp/${listenPort}`,
						`/ip4/127.0.0.1/tcp/${listenPort !== 0 ? listenPort + 1 : listenPort}/ws`,
						/* `/ip4/127.0.0.1/udp/${listenPort !== 0 ? listenPort + 2 : listenPort}/webrtc-direct`, */ // TODO: add back when webrtc-direct is supported in browser
					];
				})(),
			},
			connectionMonitor: {
				abortConnectionOnPingFailure: false,
			},
			connectionManager: {
				inboundStreamProtocolNegotiationTimeout: 1e4,
				inboundUpgradeTimeout: 1e4,
				outboundStreamProtocolNegotiationTimeout: 1e4,
				maxConnections: Infinity,
				reconnectRetries: 0, // https://github.com/libp2p/js-libp2p/issues/3289
			},

			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						directory: blocksDirectory,
						canRelayMessage: true,
					}),
				pubsub: (c) =>
					new TopicControlPlane(c, {
						canRelayMessage: true,
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
						hostShards: true,
					}),
				fanout: (c) => getOrCreateFanout(c),
			},
		},
		directory: properties.directory,
	});
};
