import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { circuitRelayTransport } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { webRTC } from "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";
import { all } from "@libp2p/websockets/filters";
import { multiaddr } from "@multiformats/multiaddr";
import { ready } from "@peerbit/crypto";
import { createLibp2p } from "libp2p";
import { useEffect, useReducer } from "react";
import { TestDirectStream } from "./../../shared/utils.js";

await ready;

const client = await createLibp2p<{ stream: TestDirectStream; identify: any }>({
	services: {
		stream: (c) => new TestDirectStream(c),
		identify: identify(),
	},
	connectionGater: {
		denyDialMultiaddr: () => false,
	},
	transports: [
		circuitRelayTransport({
			reservationCompletionTimeout: 5000,
		}),
		webRTC(),
		webSockets({ filter: all }),
	],
	addresses: {
		listen: ["/webrtc", "/p2p-circuit"],
	},
	streamMuxers: [yamux()],
	connectionEncrypters: [noise()],
});
let receivedData = 0;

export const App = () => {
	const queryParameters = new URLSearchParams(window.location.search);

	const [_, forceUpdate] = useReducer((x) => x + 1, 0);

	client.services.stream.addEventListener("peer:reachable", () => {
		console.log(client.services.stream.peers.size);
		forceUpdate();
	});
	client.services.stream.addEventListener("peer:unreachable", () => {
		forceUpdate();
	});

	useEffect(() => {
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

		client.services.stream.addEventListener("data", (d) => {
			receivedData += d.detail.bytes().length;
			forceUpdate();
		});
		const interval = setInterval(() => {
			client.services.stream.publish(new Uint8Array([1, 2, 3]));
		}, 1000);

		return () => {
			clearInterval(interval);
		};
	}, []);
	return (
		<div style={{ display: "flex", flexDirection: "column" }}>
			<span data-testid="peer-id">{client.peerId.toString()}</span>
			<span data-testid="peer-counter">
				{client.services.stream.peers.size}
			</span>
			<span data-testid="received-data">{receivedData}</span>
		</div>
	);
};
