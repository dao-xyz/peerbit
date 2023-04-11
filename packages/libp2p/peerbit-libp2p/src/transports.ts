import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { webRTC } from "@libp2p/webrtc";
import { circuitRelayServer } from "libp2p/circuit-relay";

export const transports = () => [
	webSockets(),
	circuitRelayTransport(),
	tcp(),
	webRTC({}),
];

export const relay = () => circuitRelayServer();
