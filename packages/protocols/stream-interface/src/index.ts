import { PublicSignKey } from "@peerbit/crypto";
import type { PeerId } from "@libp2p/interface-peer-id";

export interface PeerEvents {
	"peer:reachable": CustomEvent<PublicSignKey>;
	"peer:unreachable": CustomEvent<PublicSignKey>;
}

export * from "./messages.js";

export interface WaitForPeer {
	waitFor(peer: PeerId | PublicSignKey): Promise<void>;
}
