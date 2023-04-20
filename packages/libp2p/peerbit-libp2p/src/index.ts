import { createLibp2p, Libp2p } from "libp2p";
import { Components } from "libp2p/components";
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import {
	DirectBlock,
	MemoryLevelBlockStore,
	LevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import type { RecursivePartial } from "@libp2p/interfaces";
import type { Transport } from "@libp2p/interface-transport";
import { Level } from "level";
import { AddressManagerInit } from "libp2p/address-manager";
import { PeerId } from "@libp2p/interface-peer-id";
import { ConnectionManagerInit } from "libp2p/connection-manager";
import { transports, relay } from "./transports.js";

export type Libp2pExtended = Libp2p & {
	directsub: DirectSub;
	directblock: DirectBlock;
};

type CreateOptions = {
	transports?: RecursivePartial<(components: Components) => Transport>[];
	addresses?: RecursivePartial<AddressManagerInit>;
	peerId?: RecursivePartial<PeerId>;
	connectionManager?: RecursivePartial<ConnectionManagerInit>;
	directory?: string;
};
type ExtendedOptions = {
	blocks?: {
		directory?: string;
	};
	pubsub?: {
		autoDial: boolean;
	};
};

const isNode = typeof window === undefined || typeof window === "undefined";
export type CreateLibp2pExtendedOptions = ExtendedOptions & {
	libp2p?: Libp2p | CreateOptions;
};
export const createLibp2pExtended: (
	args?: CreateLibp2pExtendedOptions
) => Promise<Libp2pExtended> = async (args) => {
	let peer: Libp2pExtended;
	if ((args?.libp2p as Libp2p)?.start) {
		peer = args?.libp2p as Libp2pExtended;
	} else {
		const opts = args?.libp2p as CreateOptions | undefined;
		peer = (await createLibp2p({
			peerId: opts?.peerId,
			connectionManager: opts?.connectionManager || {
				minConnections: 0,
			},
			addresses: opts?.addresses || {
				listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"],
			},
			transports: opts?.transports || transports(),
			relay: relay(),
			connectionEncryption: [noise()],
			streamMuxers: [mplex()],
		})) as Libp2pExtended;
	}

	if (!peer.directsub) {
		peer.directsub = new DirectSub(peer, {
			canRelayMessage: true,
			signaturePolicy: "StrictNoSign",
			connectionManager: {
				autoDial: args?.pubsub?.autoDial,
			},
		});
	} else if (args?.pubsub) {
		throw new Error(
			"Directsub already seet on client, but 'pubsub' constructor arguments are provided which is unexpected"
		);
	}

	peer.directblock =
		peer.directblock ||
		new DirectBlock(
			peer,
			args?.blocks?.directory
				? new LevelBlockStore(new Level(args.blocks.directory!))
				: new MemoryLevelBlockStore()
		);

	const start = peer.start.bind(peer);

	peer.start = async () => {
		if (!peer.isStarted()) {
			await start();
		}
		await Promise.all([peer.directblock.start(), peer.directsub.start()]);
	};

	const stop = peer.stop.bind(peer);

	peer.stop = async () => {
		await stop();
		await Promise.all([peer.directblock.stop(), peer.directsub.stop()]);
	};
	return peer;
};
