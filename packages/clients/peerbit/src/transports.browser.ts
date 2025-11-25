import { circuitRelayTransport } from "@libp2p/circuit-relay-v2";
import { type Transport } from "@libp2p/interface";
import { webRTC } from /* , webRTCDirect */ "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";

export const transports = (): Array<(components: any) => Transport> => [
	// todo: add types
	webSockets(),
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
