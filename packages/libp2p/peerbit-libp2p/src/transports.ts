import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { circuitRelayServer } from "libp2p/circuit-relay";

export const transports = () => [webSockets(), circuitRelayTransport(), tcp()];

export const relay = () => circuitRelayServer();
