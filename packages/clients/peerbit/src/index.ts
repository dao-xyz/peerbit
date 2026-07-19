export * from "./peer.js";
export * from "./bootstrap.js";
export type { BootstrapRecoveryOptions } from "./bootstrap-recovery.js";
export {
	createLibp2pExtended,
	createPeerbitStreamMuxers,
	PEERBIT_YAMUX_PROTOCOL,
	PEERBIT_YAMUX_INITIAL_STREAM_WINDOW_SIZE,
	PEERBIT_YAMUX_MAX_STREAM_WINDOW_SIZE,
	type Libp2pExtendServices,
	type Libp2pExtended,
	type Libp2pCreateOptions,
	type Libp2pCreateOptionsWithServices,
} from "./libp2p.js";
