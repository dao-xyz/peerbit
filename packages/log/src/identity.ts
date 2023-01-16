import {
    Ed25519PrivateKey,
    Ed25519PublicKey,
    Secp256k1Keccak256PublicKey,
} from "@dao-xyz/peerbit-crypto";

/**
 * Can sign
 */
export type Secp256k1Identity = {
    publicKey: Secp256k1Keccak256PublicKey;
    sign: (data: Uint8Array) => Promise<Uint8Array> | Uint8Array;
};

/**
 * Can sign and send/recieve encrypted messages
 */
export type Ed25519Identity = {
    publicKey: Ed25519PublicKey;
    privateKey: Ed25519PrivateKey;
    sign: (data: Uint8Array) => Promise<Uint8Array> | Uint8Array;
};

export type Identity = Ed25519Identity | Secp256k1Identity;
