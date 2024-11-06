import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { type CircuitRelayService } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import type { Multiaddr } from "@multiformats/multiaddr";
import { waitFor } from "@peerbit/time";
import { setMaxListeners } from "events";
import { type Libp2p, type Libp2pOptions, createLibp2p } from "libp2p";
import { listen, relay, transports } from "./transports.js";

type DefaultServices = { relay: CircuitRelayService; identify: any };
type Libp2pWithServices<T> = Libp2p<T & DefaultServices>;

export class TestSession<T> {
	peers: Libp2pWithServices<T & DefaultServices>[];

	constructor(peers: Libp2pWithServices<T & DefaultServices>[]) {
		this.peers = peers;
	}

	async connect(
		groups?: {
			getMultiaddrs: () => Multiaddr[];
			dial: (addres: Multiaddr[]) => Promise<any>;
		}[][],
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
						.filter((x) => !x.protoCodes().includes(290));
					connectPromises.push(group[i].dial(toDial)); // By default don't connect to relayed (p2p-circuit) peers
				}
			}
		}

		await Promise.all(connectPromises);
		return this;
	}

	async connectLine(
		groups?: {
			getMultiaddrs: () => Multiaddr[];
			dial: (addres: Multiaddr[]) => Promise<any>;
		}[][],
	) {
		const connectPromises: Promise<any>[] = [];
		if (!groups) {
			groups = [this.peers];
		}
		for (const group of groups) {
			for (let i = 0; i < group.length - 1; i++) {
				const toDial = group[i + 1]
					.getMultiaddrs()
					.filter((x) => !x.protoCodes().includes(290));
				connectPromises.push(group[i].dial(toDial)); // By default don't connect to relayed (p2p-circuit) peers
			}
		}

		await Promise.all(connectPromises);
		return this;
	}

	static async connected<T extends Record<string, unknown>>(
		n: number,
		options?: Libp2pOptions<T> | Libp2pOptions<T>[],
	) {
		const libs = (await TestSession.disconnected<T>(n, options)).peers;
		return new TestSession<T>(libs).connect();
	}

	static async disconnected<T extends Record<string, unknown>>(
		n: number,
		options?: Libp2pOptions<T> | Libp2pOptions<T>[],
	) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<Libp2p<T>>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				const definedOptions: Libp2pOptions<T> | undefined =
					(options as any)?.[i] || options;

				const services: any = {
					identify: identify(),
					...definedOptions?.services,
				};
				if (definedOptions?.services?.relay !== null) {
					services.relay = relay();
				} else {
					delete services.relay;
				}

				const node = await createLibp2p<T>({
					addresses: {
						listen: listen(),
					},
					connectionManager: definedOptions?.connectionManager,
					privateKey: definedOptions?.privateKey,
					datastore: definedOptions?.datastore,
					transports: definedOptions?.transports ?? transports(),
					connectionMonitor: {
						enabled: false,
					},

					services,
					connectionEncrypters: [noise()],
					streamMuxers: definedOptions?.streamMuxers || [yamux()],
					start: definedOptions?.start,
				});
				await waitFor(() => node.status === "started");
				return node;
			};
			promises.push(result());
		}

		const libs = (await Promise.all(promises)) as Libp2p<T & DefaultServices>[];
		return new TestSession<T>(libs);
	}

	stop(): Promise<any> {
		return Promise.all(
			this.peers.map(async (p) => {
				return p.stop();
			}),
		);
	}
}
