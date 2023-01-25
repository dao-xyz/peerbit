import { field, variant } from "@dao-xyz/borsh";
import { PlainKey } from "./key.js";
import { PeerId } from "@libp2p/interface-peer-id";
import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import {
	Sec256k1Keccak256Keypair,
	Secp256k1Keccak256PublicKey,
} from "./sepc256k1keccak256.js";

@variant(0)
export class PeerIdAddress extends PlainKey {
	@field({ type: "string" })
	address: string;

	constructor(properties?: { address: string }) {
		super();
		if (properties) {
			this.address = properties.address;
		}
	}

	equals(other: any): boolean {
		if (other instanceof PeerIdAddress) {
			return this.address === other.address;
		}
		return false;
	}
	toString(): string {
		return "ipfs/" + this.address;
	}
}

export const getKeypairFromPeerId = (
	peerId: PeerId
): Ed25519Keypair | Sec256k1Keccak256Keypair => {
	if (peerId.type === "Ed25519") {
		return Ed25519Keypair.from(peerId);
	}
	if (peerId.type === "secp256k1") {
		return Sec256k1Keccak256Keypair.from(peerId);
	}
	throw new Error("Unsupported key type");
};

export const getPublicKeyFromPeerId = (
	peerId: PeerId
): Ed25519PublicKey | Secp256k1Keccak256PublicKey => {
	if (peerId.type === "Ed25519") {
		return Ed25519PublicKey.from(peerId);
	}
	if (peerId.type === "secp256k1") {
		return Secp256k1Keccak256PublicKey.from(peerId);
	}
	throw new Error("Unsupported key type");
};
