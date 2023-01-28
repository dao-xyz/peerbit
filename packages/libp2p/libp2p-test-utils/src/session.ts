import { createLibp2p, Libp2p } from "libp2p";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";

import { setMaxListeners } from "events";
import { RecursivePartial } from "@libp2p/interfaces";
import { Datastore } from "interface-datastore";

export type LibP2POptions = {
	datastore?: RecursivePartial<Datastore> | undefined;
};
export class LSession<T extends Libp2p = Libp2p> {
	peers: T[];

	constructor(peers: T[]) {
		this.peers = peers;
	}

	async connect(groups?: T[][]) {
		// Connect the nodes
		const connectPromises: Promise<any>[] = [];
		if (!groups) {
			groups = [this.peers];
		}
		for (const group of groups) {
			for (let i = 0; i < group.length - 1; i++) {
				for (let j = i + 1; j < group.length; j++) {
					await group[i].peerStore.addressBook.set(
						group[j].peerId,
						group[j].getMultiaddrs()
					);
					connectPromises.push(group[i].dial(group[j].peerId));
				}
			}
		}

		await Promise.all(connectPromises);
		return this;
	}
	static async connected<T extends Libp2p = Libp2p>(n: number) {
		const libs = (await LSession.disconnected<T>(n)).peers;
		return new LSession(libs).connect();
	}

	static async disconnected<T extends Libp2p = Libp2p>(
		n: number,
		options?: LibP2POptions
	) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<T>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				const node = await createLibp2p({
					connectionManager: {
						autoDial: false,
					},
					addresses: {
						listen: ["/ip4/127.0.0.1/tcp/0"],
					},
					datastore: options?.datastore,
					transports: [tcp()],
					connectionEncryption: [noise()],
					streamMuxers: [mplex()],
				});
				return node as T;
			};
			promises.push(result());
		}

		const libs = (await Promise.all(promises)) as T[];
		return new LSession(libs);
	}

	stop(): Promise<any> {
		return Promise.all(
			this.peers.map(async (p) => {
				return p.stop();
			})
		);
	}
}
