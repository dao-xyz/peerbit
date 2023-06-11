import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import { PublicSignKey } from "./key.js";
import { Secp256k1Keypair, Secp256k1PublicKey } from "./sepc256k1.js";
import { SignatureWithKey } from "./signature.js";
import { PeerId } from "@libp2p/interface-peer-id";

/**
 * Can sign
 */
export type Secp256k1Identity = {
	publicKey: Secp256k1PublicKey;
	sign: (data: Uint8Array) => Promise<SignatureWithKey>;
};

/**
 * Can sign and send/recieve encrypted messages
 */
export type Ed25519Identity = {
	publicKey: Ed25519PublicKey;
	sign: (data: Uint8Array) => Promise<SignatureWithKey>;
};

/* export interface Identity {
	publicKey: PublicSignKey,
	sign: (data: Uint8Array) => Promise<SignatureWithKey>;
};


export const createIdentity = (from: PeerId | Ed25519Keypair | Secp256k1Keypair) => {

} */
