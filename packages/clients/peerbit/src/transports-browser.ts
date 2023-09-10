import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { webRTC } from "@libp2p/webrtc";
import { all } from "@libp2p/websockets/filters";

export const transports = () => [
	webSockets({ filter: all }),
	circuitRelayTransport({
		discoverRelays: 1
	}),
	webRTC({})
];

export const relay = () => undefined;

export const listen: () => string[] | undefined = () => ["/webrtc"];
