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
import { tcp } from '@libp2p/tcp';
export interface DB {
	someProprty: Program
}


export type Libp2pExtended = Libp2p & {
	directsub: DirectSub;
	directblock: DirectBlock;
};

type CreateOptions = {
	transports?: RecursivePartial<(components: Components) => Transport>[],
	listen?: string[],
	directory?: string
}
export const createLibp2pExtended: (args: CreateOptions | Libp2p) => Promise<Libp2pExtended> = async (args) => {
	const peer = ((args as Libp2p).start) ? args as Libp2pExtended : await createLibp2p({
		connectionManager: {
			autoDial: false,
		},
		addresses: {
			listen: (args as CreateOptions).listen || ["/ip4/127.0.0.1/tcp/0"],
		},
		transports: (args as CreateOptions).transports || [tcp()],
		connectionEncryption: [noise()],
		streamMuxers: [mplex()],
	}) as Libp2pExtended;
	peer.directsub = new DirectSub(peer, {
		canRelayMessage: true,
		signaturePolicy: "StrictNoSign",
	});
	peer.directblock = new DirectBlock(peer, {
		localStore: (args as CreateOptions).directory ? new LevelBlockStore(new Level((args as CreateOptions).directory!)) : new MemoryLevelBlockStore()
	});

	let start = peer.start.bind(peer);
	peer.start = async () => {
		if (!peer.isStarted()) {
			await start();
		}
		await Promise.all([peer.directblock.start(), peer.directsub.start()])
	}

	let stop = peer.stop.bind(peer);
	peer.stop = async () => {
		await stop();
		await Promise.all([peer.directblock.stop(), peer.directsub.stop()])

	}
	return peer;
}