import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { webRTC } from "@libp2p/webrtc";

export const transports = () => [
	webSockets(),
	circuitRelayTransport(),
	webRTC({}),
];

export const relay = () => undefined;
