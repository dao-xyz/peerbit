import { type AbstractType, field, option, variant } from "@dao-xyz/borsh";
import {
	ByteKey,
	Ed25519Keypair,
	Keypair,
	PublicKeyEncryptionKey,
	PublicSignKey,
	Secp256k1Keypair,
	X25519Keypair,
} from "@peerbit/crypto";
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

interface PublicKeyWrapped {
	key: PublicKeyEncryptionKey | PublicSignKey;
}

abstract class PublicKeyWrapped implements PublicKeyWrapped {}

@variant(0)
class PublicSignKeyWrapped extends PublicKeyWrapped {
	@field({ type: PublicSignKey })
	key: PublicSignKey;

	constructor(key: PublicSignKey) {
		super();
		this.key = key;
	}
}

@variant(1)
class EncryptionKeyWrapped extends PublicKeyWrapped {
	@field({ type: PublicKeyEncryptionKey })
	key: PublicKeyEncryptionKey;

	constructor(key: PublicKeyEncryptionKey) {
		super();
		this.key = key;
	}
}

interface KeyOrKeypair {
	key: Keypair | ByteKey;
}

abstract class KeyOrKeypair implements KeyOrKeypair {}

@variant(0)
class KeypairWrapped extends KeyOrKeypair {
	@field({ type: Keypair })
	key: Keypair;

	constructor(key: Keypair) {
		super();
		this.key = key;
	}
}

@variant(1)
class PlainKeywrapped extends KeyOrKeypair {
	@field({ type: ByteKey })
	key: ByteKey;

	constructor(key: ByteKey) {
		super();
		this.key = key;
	}
}

@variant(2)
export class REQ_ExportKeypairByKey extends KeyChainMessage {
	@field({ type: PublicKeyWrapped })
	publicKey: PublicKeyWrapped;

	constructor(publicKey: PublicSignKey | PublicKeyEncryptionKey) {
		super();
		this.publicKey =
			publicKey instanceof PublicSignKey
				? new PublicSignKeyWrapped(publicKey)
				: new EncryptionKeyWrapped(publicKey);
	}
}

@variant(3)
export class RESP_ExportKeypairByKey extends KeyChainMessage {
	@field({ type: option(KeyOrKeypair) })
	keypair?: KeyOrKeypair;

	constructor(keypair?: Keypair) {
		super();
		if (keypair) {
			this.keypair = new KeypairWrapped(keypair);
		}
	}
}

type KeyStringType = "ed25519" | "x25519" | "secp256k1" | "bytekey";
const getKeyStringType = (
	type: AbstractType<
		Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey
	>,
): KeyStringType => {
	if (type === Ed25519Keypair) {
		return "ed25519";
	} else if (type === Secp256k1Keypair) {
		return "x25519";
	} else if (type === X25519Keypair) {
		return "secp256k1";
	} else if (type === ByteKey) {
		return "bytekey";
	}
	throw new Error("Unsupported key type: " + type?.name);
};

const getKeyTypeFromString = (type: KeyStringType) => {
	if (type === "ed25519") {
		return Ed25519Keypair;
	} else if (type === "x25519") {
		return X25519Keypair;
	} else if (type === "secp256k1") {
		return Secp256k1Keypair;
	} else if (type === "bytekey") {
		return ByteKey;
	}
	throw new Error("Unsupported key type: " + type);
};

@variant(4)
export class REQ_ExportKeypairById extends KeyChainMessage {
	@field({ type: Uint8Array })
	keyId: Uint8Array;

	@field({ type: "string" })
	private stringType: KeyStringType;

	constructor(
		keyId: Uint8Array,
		type: AbstractType<
			Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey
		>,
	) {
		super();
		this.keyId = keyId;
		this.stringType = getKeyStringType(type);
	}

	get type() {
		return getKeyTypeFromString(this.stringType);
	}
}

@variant(5)
export class RESP_ExportKeypairById extends KeyChainMessage {
	@field({ type: option(KeyOrKeypair) })
	keypair?: KeyOrKeypair;

	constructor(keyOrKeypair?: Keypair | ByteKey) {
		super();
		if (keyOrKeypair) {
			this.keypair =
				keyOrKeypair instanceof Keypair
					? new KeypairWrapped(keyOrKeypair)
					: new PlainKeywrapped(keyOrKeypair);
		}
	}
}
