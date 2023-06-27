import { createLibp2p, Libp2p, ServiceFactoryMap } from "libp2p";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { setMaxListeners } from "events";
import { RecursivePartial } from "@libp2p/interfaces";
import { Datastore } from "interface-datastore";
import { relay, transports } from "./transports.js";
import { ConnectionManagerInit } from "libp2p/dist/src/connection-manager";
import { Transport } from "@libp2p/interface-transport";
import { Components } from "libp2p/components";
import { identifyService } from "libp2p/identify";
import { CircuitRelayService } from "libp2p/dist/src/circuit-relay/index.js";
import type { Multiaddr } from "@multiformats/multiaddr";

export type LibP2POptions<T extends Record<string, unknown>> = {
	transports?:
		| RecursivePartial<(components: Components) => Transport>[]
		| undefined;
	connectionManager?: RecursivePartial<ConnectionManagerInit>;
	datastore?: RecursivePartial<Datastore> | undefined;
	browser?: boolean;
	services?: ServiceFactoryMap<T>;
	start?: boolean;
};

type DefaultServices = { relay: CircuitRelayService; identify: any };
type Libp2pWithServices<T> = Libp2p<T & DefaultServices>;
export class LSession<T> {
	peers: Libp2pWithServices<T & DefaultServices>[];

	constructor(peers: Libp2pWithServices<T & DefaultServices>[]) {
		this.peers = peers;
	}

	async connect(
		groups?: {
			getMultiaddrs: () => Multiaddr[];
			dial: (addres: Multiaddr[]) => Promise<any>;
		}[][]
	) {
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
	static async connected<T extends Record<string, unknown>>(
		n: number,
		options?: LibP2POptions<T> | LibP2POptions<T>[]
	) {
		const libs = (await LSession.disconnected<T>(n, options)).peers;
		return new LSession(libs).connect();
	}

	static async disconnected<T extends Record<string, unknown>>(
		n: number,
		options?: LibP2POptions<T> | LibP2POptions<T>[]
	) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<Libp2p<T>>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				const node = await createLibp2p<T>({
					addresses: {
						listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"],
					},
					connectionManager: (options?.[i] || options)?.connectionManager ?? {
						minConnections: 0,
					},
					datastore: (options?.[i] || options)?.datastore,
					transports:
						(options?.[i] || options)?.transports ??
						transports((options?.[i] || options)?.browser),
					services: {
						relay: (options?.[i] || options)?.browser ? undefined : relay(),
						identify: identifyService(),
						...(options?.[i] || options)?.services,
					},
					connectionEncryption: [noise()],
					streamMuxers: [mplex()],
					start: (options?.[i] || options)?.start,
				});
				return node;
			};
			promises.push(result());
		}

		const libs = (await Promise.all(promises)) as Libp2p<T & DefaultServices>[];
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
