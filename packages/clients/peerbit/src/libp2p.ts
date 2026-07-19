import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import type { CircuitRelayService } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import type { StreamMuxerFactory } from "@libp2p/interface";
import { DirectBlock } from "@peerbit/blocks";
import { type IPeerbitKeychain, keychain } from "@peerbit/keychain";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "@peerbit/pubsub";
import {
	type Libp2p,
	type Libp2pOptions,
	type ServiceFactoryMap,
	createLibp2p,
} from "libp2p";
import { listen, relay, transports } from "./transports.js";

export type Libp2pExtendServices = {
	pubsub: TopicControlPlane;
	fanout: FanoutTree;
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

/**
 * Yamux profile negotiated between Peerbit peers that starts each logical
 * stream with enough credit for the default eight concurrent 512 KiB block
 * responses.
 *
 * This must use a distinct protocol id. `@chainsafe/libp2p-yamux` applies a
 * configured initial window to both send and receive credit without
 * advertising it on `/yamux/1.0.0`, so using a larger value under the standard
 * id can overrun an older peer's 256 KiB receive window. Keeping standard
 * Yamux as the second choice preserves mixed-version interoperability.
 */
export const PEERBIT_YAMUX_PROTOCOL = "/peerbit/yamux/1.0.0";
export const PEERBIT_YAMUX_INITIAL_STREAM_WINDOW_SIZE = 4 * 1024 * 1024;
export const PEERBIT_YAMUX_MAX_STREAM_WINDOW_SIZE = 16 * 1024 * 1024;

export const createPeerbitStreamMuxers = (): Array<
	() => StreamMuxerFactory
> => [
	() => {
		const factory = yamux({
			streamOptions: {
				initialStreamWindowSize: PEERBIT_YAMUX_INITIAL_STREAM_WINDOW_SIZE,
				maxStreamWindowSize: PEERBIT_YAMUX_MAX_STREAM_WINDOW_SIZE,
			},
		})();
		const createStreamMuxer = factory.createStreamMuxer.bind(factory);
		factory.protocol = PEERBIT_YAMUX_PROTOCOL;
		factory.createStreamMuxer = (connection) => {
			const muxer = createStreamMuxer(connection);
			muxer.protocol = PEERBIT_YAMUX_PROTOCOL;
			return muxer;
		};
		return factory;
	},
	yamux(),
];

export const createLibp2pExtended = (
	opts: PartialLibp2pCreateOptions = {},
): Promise<Libp2pExtended> => {
	const topicRootControlPlane = new TopicRootControlPlane();
	let extraServices: any = {};
	let fanoutInstance: FanoutTree | undefined;
	const configuredFanoutFactory =
		opts.services?.fanout ||
		((c) =>
			new FanoutTree(c, { connectionManager: false, topicRootControlPlane }));
	const getOrCreateFanout = (c: any) => {
		if (!fanoutInstance) {
			fanoutInstance = configuredFanoutFactory(c) as FanoutTree;
		}
		return fanoutInstance;
	};

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
		streamMuxers: opts.streamMuxers || createPeerbitStreamMuxers(),
		services: {
			...opts.services,
			pubsub:
				opts.services?.pubsub ||
				((c) =>
					new TopicControlPlane(c, {
						canRelayMessage: true,
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
						// auto dial true
						// auto prune true
					})),
			fanout: (c) => getOrCreateFanout(c),
			blocks: opts.services?.blocks || ((c) => new DirectBlock(c)),
			keychain: opts.services?.keychain || keychain(),
			...extraServices,
		},
	}).then((libp2p) => libp2p as Libp2pExtended);
};
