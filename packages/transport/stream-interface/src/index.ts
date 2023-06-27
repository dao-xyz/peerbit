import { PublicSignKey } from "@peerbit/crypto";
import type { PeerId } from "@libp2p/interface-peer-id";

export interface PeerStreamEvents {
	"stream:inbound": CustomEvent<never>;
	"stream:outbound": CustomEvent<never>;
	close: CustomEvent<never>;
}

export * from "./messages.js";

export interface WaitForPeer {
	waitFor(peer: PeerId | PublicSignKey): Promise<void>;
}
