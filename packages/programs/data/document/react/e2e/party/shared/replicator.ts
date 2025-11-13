import { Peerbit } from "peerbit";
import { webSockets } from "@libp2p/websockets";
import { PartyDocumentStore } from "./data.js";

export type RunningReplicator = {
	addresses: string[];
	stop: () => Promise<void>;
};

export async function startReplicator(): Promise<RunningReplicator> {
	const peer = await Peerbit.create({
		libp2p: {
			addresses: {
				listen: ["/ip4/127.0.0.1/tcp/0/ws"],
			},
			transports: [webSockets()],
			connectionGater: {
				denyDialMultiaddr: () => false,
			},
		},
	});

	await peer.open(PartyDocumentStore.createFixed(), {
		existing: "reuse",
		args: { replicate: true },
	});

	const addresses = peer
		.getMultiaddrs()
		.map((addr) => addr.toString())
		.filter((addr) => addr.includes("/ws"));

	return {
		addresses,
		stop: () => peer.stop(),
	};
}
