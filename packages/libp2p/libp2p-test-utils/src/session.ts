import { createLibp2p, Libp2p } from "libp2p";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { setMaxListeners } from "events";
import { RecursivePartial } from "@libp2p/interfaces";
import { Datastore } from "interface-datastore";
import { relay, transports } from "./transports.js";
import { ConnectionManagerInit } from "libp2p/dist/src/connection-manager";
import { Transport } from "@libp2p/interface-transport";
import { Components } from "libp2p/components";

export type LibP2POptions = {
	transports?:
		| RecursivePartial<(components: Components) => Transport>[]
		| undefined;
	connectionManager?: RecursivePartial<ConnectionManagerInit>;
	datastore?: RecursivePartial<Datastore> | undefined;
	browser?: boolean;
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
					const toDial = group[j]
						.getMultiaddrs()
						.filter((x) => x.protoCodes().includes(290) === false);
					connectPromises.push(group[i].dial(toDial)); // By default don't connect to relayed (p2p-circuit) peers
				}
			}
		}

		await Promise.all(connectPromises);
		return this;
	}
	static async connected<T extends Libp2p = Libp2p>(
		n: number,
		options?: LibP2POptions | LibP2POptions[]
	) {
		const libs = (await LSession.disconnected<T>(n, options)).peers;
		return new LSession(libs).connect();
	}

	static async disconnected<T extends Libp2p = Libp2p>(
		n: number,
		options?: LibP2POptions | LibP2POptions[]
	) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<T>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				const node = await createLibp2p({
					addresses: {
						listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"],
					},
					connectionManager: (options?.[i] || options)?.connectionManager ?? {
						minConnections: 0,
					},
					datastore: (options?.[i] || options)?.datastore,
					transports:
						(options?.[i] || options).transports ??
						transports((options?.[i] || options)?.browser),
					relay: (options?.[i] || options)?.browser ? undefined : relay(),
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
