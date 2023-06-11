import { PeerId } from "@libp2p/interface-peer-id";
import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import { Secp256k1Keypair, Secp256k1PublicKey } from "./sepc256k1.js";

export const getKeypairFromPeerId = (
	peerId: PeerId
): Ed25519Keypair | Secp256k1Keypair => {
	if (peerId.type === "Ed25519") {
		return Ed25519Keypair.fromPeerId(peerId);
	}
	if (peerId.type === "secp256k1") {
		return Secp256k1Keypair.fromPeerId(peerId);
	}
	throw new Error("Unsupported key type");
};

export const getPublicKeyFromPeerId = (
	peerId: PeerId
): Ed25519PublicKey | Secp256k1PublicKey => {
	if (peerId.type === "Ed25519") {
		return Ed25519PublicKey.fromPeerId(peerId);
	}
	if (peerId.type === "secp256k1") {
		return Secp256k1PublicKey.from(peerId);
	}
	throw new Error("Unsupported key type");
};
