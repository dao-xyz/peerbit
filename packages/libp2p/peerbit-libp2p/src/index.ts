import { createLibp2p, Libp2p } from "libp2p";
import type { Components } from "libp2p/components";
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import { DirectBlock } from "@dao-xyz/libp2p-direct-block";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import type { RecursivePartial } from "@libp2p/interfaces";
import type { Transport } from "@libp2p/interface-transport";
import { AddressManagerInit } from "libp2p/address-manager";
import { PeerId } from "@libp2p/interface-peer-id";
import { ConnectionManagerInit } from "libp2p/connection-manager";
import { transports, relay } from "./transports.js";
import { identifyService } from "libp2p/identify";
import { CircuitRelayService } from "libp2p/dist/src/circuit-relay/index.js";

export type Libp2pExtendServices = {
	directsub: DirectSub;
	directblock: DirectBlock;
};
export type Libp2pExtended = Libp2p<
	{ relay: CircuitRelayService; identify: any } & Libp2pExtendServices
>;
export type ExtendedServicesOptions = {
	directsub: (components) => DirectSub;
	directblock: (components) => DirectBlock;
};
export type CreateOptions = {
	transports?: RecursivePartial<(components: Components) => Transport>[];
	addresses?: RecursivePartial<AddressManagerInit>;
	peerId?: RecursivePartial<PeerId>;
	connectionManager?: RecursivePartial<ConnectionManagerInit>;
	directory?: string;
	services?: ExtendedServicesOptions;
};

export type CreateLibp2pExtendedOptions = {
	libp2p?: CreateOptions;
};
export const createLibp2pExtended = (
	opts: CreateOptions = {}
): Promise<Libp2pExtended> =>
	createLibp2p({
		peerId: opts?.peerId,
		connectionManager: opts?.connectionManager || {
			minConnections: 0,
		},
		addresses: opts?.addresses || {
			listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"],
		},
		transports: opts?.transports || transports(),
		connectionEncryption: [noise()],
		streamMuxers: [mplex()],
		services: {
			relay: relay(),
			identify: identifyService(),
			directsub:
				opts.services?.directsub ||
				((c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						signaturePolicy: "StrictNoSign",
						connectionManager: {
							autoDial: true,
						},
					})),
			directblock: opts.services?.directblock || ((c) => new DirectBlock(c)),
		},
	});
