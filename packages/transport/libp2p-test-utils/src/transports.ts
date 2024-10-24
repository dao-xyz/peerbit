import {
	circuitRelayServer,
	circuitRelayTransport,
} from "@libp2p/circuit-relay-v2";
import type { Transport } from "@libp2p/interface";
import { webRTC } from "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import type { Components } from "libp2p/components";

export const transports = (): Array<(components: Components) => Transport> => [
	circuitRelayTransport({
		reservationCompletionTimeout: 5000,
	}),
	webRTC({}),
	webSockets({ filter: filters.all }),
];

// applyDefaultLimit: false because of https://github.com/libp2p/js-libp2p/issues/2622
export const relay = () =>
	circuitRelayServer({ reservations: { applyDefaultLimit: false } });

export const listen = () => [
	"/ip4/127.0.0.1/tcp/0",
	"/ip4/127.0.0.1/tcp/0/ws",
	"/p2p-circuit",
];
