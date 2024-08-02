import { multiaddr } from "@multiformats/multiaddr";
import { randomBytes } from "@peerbit/crypto";
import { Peerbit } from "peerbit";
import { useEffect, useReducer, useState } from "react";
import { LogToReplicate } from "./utils.js";

/* import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import {
	circuitRelayTransport,
} from "@libp2p/circuit-relay-v2"; */

const client = await Peerbit.create({
	libp2p: {
		connectionGater: {
			denyDialMultiaddr: () => {
				// by default we refuse to dial local addresses from the browser since they
				// are usually sent by remote peers broadcasting undialable multiaddrs but
				// here we are explicitly connecting to a local node so do not deny dialing
				// any discovered address
				return false;
			},
		},
		/* addresses: {
			listen: []
		},
		transports: [circuitRelayTransport(), webSockets({ filter: filters.all })], */
	},
});

const log = await client.open(new LogToReplicate());
log.log.append(randomBytes(32));

const queryParameters = new URLSearchParams(window.location.search);
const relayAddrs = queryParameters.get("relay");
if (!relayAddrs) {
	throw new Error("No relay provided");
}

// /p2p-circuit/webrtc/p2p/
console.log("DIAL", decodeURIComponent(relayAddrs));
client
	.dial(multiaddr(decodeURIComponent(relayAddrs)))
	.then((conn) => {
		console.log("CONNECTED!", conn);
	})
	.catch((err) => {
		console.error(err);
	})
	.finally(() => {
		console.log("FINALLY");
	});

export const App = () => {
	const [logLength, setLogLength] = useState(0);
	const [replicators, setReplicators] = useState(0);

	const [_, forceUpdate] = useReducer((x) => x + 1, 0);

	useEffect(() => {
		let interval = setInterval(() => {
			setLogLength(log.log.log.length);
			forceUpdate();
		}, 100);

		log.log.events.addEventListener("replication:change", (evt) => {
			log.log.getReplicators().then((replicators) => {
				setReplicators(replicators.size);
				forceUpdate();
			});
		});

		return () => {
			clearInterval(interval);
		};
	}, []);
	return (
		<div style={{ display: "flex", flexDirection: "column" }}>
			<span data-testid="peer-id">{client.peerId.toString()}</span>
			<span>Peer counter</span>
			<span data-testid="replicators">{replicators}</span>
			<span>Log length</span>
			<span data-testid="log-length">{logLength}</span>
		</div>
	);
};
