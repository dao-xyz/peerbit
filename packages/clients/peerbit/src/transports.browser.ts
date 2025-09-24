import { circuitRelayTransport } from "@libp2p/circuit-relay-v2";
import {
	webRTC,
	/* , webRTCDirect */
} from "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";
import { all } from "@libp2p/websockets/filters";

export const transports = () => [
	webSockets({ filter: all }),
	circuitRelayTransport({
		reservationCompletionTimeout: 5000,
	}),
	webRTC({}),
	/* webRTCDirect({}), */ // TODO: add back when webrtc-direct is supported in browser
];

export const relay = () => undefined as any;

export const listen: () => string[] | undefined = () => [
	"/webrtc",
	"/p2p-circuit",
];
