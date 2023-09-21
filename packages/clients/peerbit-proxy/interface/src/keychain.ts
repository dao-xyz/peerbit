import {
	Ed25519PublicKey,
	Ed25519Keypair,
	Keypair,
	X25519PublicKey,
	X25519Keypair
} from "@peerbit/crypto";
import { field, variant, option } from "@dao-xyz/borsh";
import { Message } from "./message.js";

@variant(8)
export abstract class KeyChainMessage extends Message {}

@variant(0)
export class REQ_ImportKey extends KeyChainMessage {
	@field({ type: Ed25519Keypair })
	keypair: Ed25519Keypair;

	@field({ type: Uint8Array })
	keyId: Uint8Array;

	constructor(keypair: Ed25519Keypair, keyId: Uint8Array) {
		super();
		this.keypair = keypair;
		this.keyId = keyId;
	}
}

@variant(1)
export class RESP_ImportKey extends KeyChainMessage {}

abstract class PublicKeyWrapped {
	key: Ed25519PublicKey | X25519PublicKey;
}

@variant(0)
class PublicSignKeyWrapped extends PublicKeyWrapped {
	@field({ type: Ed25519PublicKey })
	key: Ed25519PublicKey;

	constructor(key: Ed25519PublicKey) {
		super();
		this.key = key;
	}
}

@variant(1)
class EncryptionKeyWrapped extends PublicKeyWrapped {
	@field({ type: X25519PublicKey })
	key: X25519PublicKey;

	constructor(key: X25519PublicKey) {
		super();
		this.key = key;
	}
}

@variant(2)
export class REQ_ExportKeypairByKey extends KeyChainMessage {
	@field({ type: PublicKeyWrapped })
	publicKey: PublicKeyWrapped;

	constructor(publicKey: Ed25519PublicKey | X25519PublicKey) {
		super();
		this.publicKey =
			publicKey instanceof Ed25519PublicKey
				? new PublicSignKeyWrapped(publicKey)
				: new EncryptionKeyWrapped(publicKey);
	}
}

@variant(3)
export class RESP_ExportKeypairByKey extends KeyChainMessage {
	@field({ type: option(Keypair) })
	keypair?: X25519Keypair | Ed25519Keypair;

	constructor(keypair?: X25519Keypair | Ed25519Keypair) {
		super();
		this.keypair = keypair;
	}
}

@variant(4)
export class REQ_ExportKeypairById extends KeyChainMessage {
	@field({ type: Uint8Array })
	keyId: Uint8Array;

	@field({ type: "string" })
	type: "ed25519" | "x25519";

	constructor(keyId: Uint8Array, type: "ed25519" | "x25519") {
		super();
		this.keyId = keyId;
		this.type = type;
	}
}

@variant(5)
export class RESP_ExportKeypairById extends KeyChainMessage {
	@field({ type: option(Keypair) })
	keypair?: X25519Keypair | Ed25519Keypair;

	constructor(keypair?: X25519Keypair | Ed25519Keypair) {
		super();
		this.keypair = keypair;
	}
}
