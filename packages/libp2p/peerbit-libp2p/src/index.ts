import { createLibp2p, Libp2p } from "libp2p";

import { Components } from 'libp2p/components'
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import {
	DirectBlock,
	MemoryLevelBlockStore,
	LevelBlockStore
} from "@dao-xyz/libp2p-direct-block";
import { noise } from '@dao-xyz/libp2p-noise';
import { mplex } from '@libp2p/mplex';
import type { RecursivePartial } from '@libp2p/interfaces'
import type { Transport } from '@libp2p/interface-transport'
import { Level } from 'level'
import { Program } from '@dao-xyz/peerbit-program'
import { webSockets } from "@libp2p/websockets"
import { AddressManagerInit } from "libp2p/address-manager";
import { PeerId } from '@libp2p/interface-peer-id'
import { ConnectionManagerConfig } from "libp2p/connection-manager";

export interface DB { someProprty: Program }

export type Libp2pExtended = Libp2p & {
	directsub: DirectSub;
	directblock: DirectBlock;
};

type CreateOptions = {
	transports?: RecursivePartial<(components: Components) => Transport>[],
	addresses?: RecursivePartial<AddressManagerInit>,
	peerId?: RecursivePartial<PeerId>,
	connectionManager?: RecursivePartial<ConnectionManagerConfig>
	directory?: string
}
type ExtendedOptions = {
	blocks?: {
		directory?: string
	}
}
export const createLibp2pExtended: (args?: (ExtendedOptions & { libp2p?: Libp2p | CreateOptions })) => Promise<Libp2pExtended> = async (args) => {
	let peer: Libp2pExtended;
	if (((args?.libp2p as Libp2p)?.start)) {
		peer = args?.libp2p as Libp2pExtended
	}
	else {
		const opts = args?.libp2p as (CreateOptions | undefined);
		peer = await createLibp2p({
			peerId: opts?.peerId,
			connectionManager: opts?.connectionManager || { autoDial: false },
			addresses: opts?.addresses || { listen: ["/ip4/127.0.0.1/tcp/0"] },
			transports: opts?.transports || [webSockets()],
			connectionEncryption: [noise()],
			streamMuxers: [mplex()],
		}) as Libp2pExtended
	}

	peer.directsub = new DirectSub(peer, {
		canRelayMessage: true,
		signaturePolicy: "StrictNoSign",
	});

	peer.directblock = new DirectBlock(peer, {
		localStore: args?.blocks?.directory ? new LevelBlockStore(new Level(args.blocks.directory!)) : new MemoryLevelBlockStore()
	});

	const start = peer.start.bind(peer);

	peer.start = async () => {
		if (!peer.isStarted()) {
			await start();
		}
		await Promise.all([peer.directblock.start(), peer.directsub.start()])
	}

	const stop = peer.stop.bind(peer);

	peer.stop = async () => {
		await stop();
		await Promise.all([peer.directblock.stop(), peer.directsub.stop()])

	}
	return peer;
}