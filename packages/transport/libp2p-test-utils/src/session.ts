import { createLibp2p, Libp2p, Libp2pOptions, ServiceFactoryMap } from "libp2p";
import { noise } from "@dao-xyz/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { setMaxListeners } from "events";
import { relay, transports } from "./transports.js";
import { identify } from "@libp2p/identify";
import { CircuitRelayService } from "@libp2p/circuit-relay-v2";
import type { Multiaddr } from "@multiformats/multiaddr";

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
		options?: Libp2pOptions<T> | Libp2pOptions<T>[]
	) {
		const libs = (await TestSession.disconnected<T>(n, options)).peers;
		return new TestSession(libs).connect();
	}

	static async disconnected<T extends Record<string, unknown>>(
		n: number,
		options?: Libp2pOptions<T> | Libp2pOptions<T>[]
	) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<Libp2p<T>>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				const node = await createLibp2p<T>({
					addresses: {
						listen: ["/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"]
					},
					connectionManager: (options?.[i] || options)?.connectionManager ?? {
						minConnections: 0
					},
					peerId: (options?.[i] || options)?.peerId,
					datastore: (options?.[i] || options)?.datastore,
					transports:
						(options?.[i] || options)?.transports ??
						transports((options?.[i] || options)?.browser),
					services: {
						relay: (options?.[i] || options)?.browser ? undefined : relay(),
						identify: identify(),
						...(options?.[i] || options)?.services
					},
					connectionEncryption: [noise()],
					streamMuxers: [mplex({ disconnectThreshold: 10 })],
					start: (options?.[i] || options)?.start
				});
				return node;
			};
			promises.push(result());
		}

		const libs = (await Promise.all(promises)) as Libp2p<T & DefaultServices>[];
		return new TestSession(libs);
	}

	stop(): Promise<any> {
		return Promise.all(
			this.peers.map(async (p) => {
				return p.stop();
			})
		);
	}
}
