import { createLibp2p, Libp2p } from "libp2p";
import { Components } from 'libp2p/src/components'
import { noise } from "@dao-xyz/libp2p-noise";

import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import waitForPeers from "./wait-for-peers";
import { setMaxListeners } from "events";
import { kadDHT } from "@libp2p/kad-dht";
import { LevelDatastore } from 'datastore-level';
import { RecursivePartial } from '@libp2p/interfaces'
import { Datastore } from 'interface-datastore'
import type { DualDHT, QueryEvent, SingleDHT } from '@libp2p/interface-dht'

export type LibP2POptions = { datastore?: RecursivePartial<Datastore> | undefined, dht: RecursivePartial<((components: Components) => DualDHT) | undefined> };
export class LSession {
	peers: Libp2p[];

	constructor(peers: Libp2p[]) {
		this.peers = peers;
	}

	async connect(groups?: Libp2p[][]) {
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

		// Subscribe to initial topics
		/*  if (pubsubTopics) {
			 for (const topic of pubsubTopics) {
				 for (const lib of this.peers) {
					 lib.pubsub.subscribe(topic);
				 }
				 for (let i = 0; i < this.peers.length - 1; i++) {
					 for (let j = i + 1; j < this.peers.length; j++) {
						 await waitForPeers(this.peers[i], this.peers[j], topic);
					 }
				 }
			 }
		 } */
		return this;

	}
	static async connected(n: number) {
		const libs = (await LSession.disconnected(n)).peers;
		return (new LSession(libs)).connect();
	}

	static async disconnected(n: number, options?: LibP2POptions) {
		// Allow more than 11 listneers
		setMaxListeners(Infinity);

		// create nodes
		const promises: Promise<Libp2p>[] = [];
		for (let i = 0; i < n; i++) {
			const result = async () => {
				let msgCounter = 0;
				const node = await createLibp2p({
					connectionManager: {
						autoDial: false,
					},
					addresses: {
						listen: ["/ip4/127.0.0.1/tcp/0"],
					},
					dht: options?.dht,
					datastore: options?.datastore,
					transports: [tcp()],
					connectionEncryption: [noise()],
					streamMuxers: [mplex()],

				});
				await node.start();
				return node;
			};
			promises.push(result());
		}

		const libs = await Promise.all(promises);
		return new LSession(libs);
	}

	stop(): Promise<any> {
		return Promise.all(this.peers.map(async (p) => { return p.stop() }));
	}
}
