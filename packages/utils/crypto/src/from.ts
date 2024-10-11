import { type PeerId, type PrivateKey } from "@libp2p/interface";
import {
	Ed25519Keypair,
	Ed25519PrivateKey,
	Ed25519PublicKey,
} from "./ed25519.js";
import {
	Secp256k1Keypair,
	Secp256k1PrivateKey,
	Secp256k1PublicKey,
} from "./sepc256k1.js";

export const getKeypairFromPrivateKey = (
	privateKey: PrivateKey,
): Ed25519Keypair | Secp256k1Keypair => {
	if (privateKey.type === "Ed25519") {
		return new Ed25519Keypair({
			privateKey: new Ed25519PrivateKey({
				privateKey: privateKey.raw.slice(0, 32),
			}),
			publicKey: new Ed25519PublicKey({ publicKey: privateKey.publicKey.raw }),
		});
	}
	if (privateKey.type === "secp256k1") {
		return new Secp256k1Keypair({
			privateKey: new Secp256k1PrivateKey({
				privateKey: privateKey.raw.slice(0, 32),
			}),
			publicKey: new Secp256k1PublicKey({
				publicKey: privateKey.publicKey.raw,
			}),
		});
	}
	throw new Error("Unsupported key type");
};

export const getPublicKeyFromPeerId = (
	peerId: PeerId,
): Ed25519PublicKey | Secp256k1PublicKey => {
	if (peerId.type === "Ed25519") {
		return Ed25519PublicKey.fromPeerId(peerId);
	}
	if (peerId.type === "secp256k1") {
		return Secp256k1PublicKey.fromPeerId(peerId);
	}
	throw new Error("Unsupported key type");
};
