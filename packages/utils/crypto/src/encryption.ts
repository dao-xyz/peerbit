export * from "./errors.js";
import {
	AbstractType,
	deserialize,
	field,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { arraysEqual } from "@dao-xyz/peerbit-borsh-utils";
import { AccessError } from "./errors.js";
import sodium from "libsodium-wrappers";
import { X25519Keypair, X25519PublicKey, X25519SecretKey } from "./x25519.js";
import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import { randomBytes } from "./random.js";

const NONCE_LENGTH = 24;

export interface PublicKeyEncryptionResolver {
	getEncryptionKeypair: GetEncryptionKeypair;
	getAnyKeypair: GetAnyKeypair;
}
export type GetAnyKeypair =
	| ((
			publicKey: (X25519PublicKey | Ed25519PublicKey)[]
	  ) => Promise<
			{ index: number; keypair: X25519Keypair | Ed25519Keypair } | undefined
	  >)
	| X25519Keypair;
export type GetEncryptionKeypair =
	| (() => X25519Keypair | Ed25519Keypair)
	| (() => Promise<X25519Keypair> | Promise<Ed25519Keypair>)
	| X25519Keypair
	| Ed25519Keypair;

@variant(0)
export abstract class MaybeEncrypted<T> {
	/**
	 * Will throw error if not decrypted
	 */
	get decrypted(): DecryptedThing<T> {
		throw new Error("Not implented");
	}

	decrypt(
		keyResolver?: GetAnyKeypair
	): Promise<DecryptedThing<T>> | DecryptedThing<T> {
		throw new Error("Not implemented");
	}
	equals(other: MaybeEncrypted<T>): boolean {
		throw new Error("Not implemented");
	}

	/**
	 * Clear cached data
	 */
	clear() {
		throw new Error("Not implemented");
	}

	abstract get byteLength(): number;
}

@variant(0)
export class DecryptedThing<T> extends MaybeEncrypted<T> {
	@field({ type: Uint8Array })
	_data?: Uint8Array;

	constructor(props?: { data?: Uint8Array; value?: T }) {
		super();
		if (props) {
			this._data = props.data;
			this._value = props.value;
		}
	}

	_value?: T;
	getValue(clazz: AbstractType<T>): T {
		if (this._value) {
			return this._value;
		}
		if (!this._data) {
			throw new Error("Missing data");
		}
		return deserialize(this._data, clazz);
	}

	async encrypt(
		keyResolver: GetEncryptionKeypair,
		...recieverPublicKeys: (X25519PublicKey | Ed25519PublicKey)[]
	): Promise<EncryptedThing<T>> {
		const bytes = serialize(this);
		const epheremalKey = sodium.crypto_secretbox_keygen();
		const nonce = randomBytes(NONCE_LENGTH); // crypto random is faster than sodim random
		const cipher = sodium.crypto_secretbox_easy(bytes, nonce, epheremalKey);

		let encryptionKeypair =
			typeof keyResolver === "function" ? await keyResolver() : keyResolver;
		if (encryptionKeypair instanceof Ed25519Keypair) {
			encryptionKeypair = await X25519Keypair.from(encryptionKeypair);
		}
		const x25519Keypair = encryptionKeypair as X25519Keypair;
		const recieverX25519PublicKeys = await Promise.all(
			recieverPublicKeys.map((key) => {
				if (key instanceof Ed25519PublicKey) {
					return X25519PublicKey.from(key);
				}
				return key;
			})
		);

		const ks = recieverX25519PublicKeys.map((recieverPublicKey) => {
			const kNonce = randomBytes(NONCE_LENGTH); // crypto random is faster than sodium random
			return new K({
				encryptedKey: new CipherWithNonce({
					cipher: sodium.crypto_box_easy(
						epheremalKey,
						kNonce,
						recieverPublicKey.publicKey,
						x25519Keypair.secretKey.secretKey
					),
					nonce: kNonce,
				}),
				recieverPublicKey,
			});
		});

		const enc = new EncryptedThing<T>({
			encrypted: new Uint8Array(cipher),
			nonce,
			envelope: new Envelope({
				senderPublicKey: x25519Keypair.publicKey,
				ks,
			}),
		});
		enc._decrypted = this;
		return enc;
	}

	get decrypted(): DecryptedThing<T> {
		return this;
	}

	decrypt(): DecryptedThing<T> {
		return this;
	}

	equals(other: MaybeEncrypted<T>) {
		if (other instanceof DecryptedThing) {
			return arraysEqual(this._data, other._data);
		} else {
			return false;
		}
	}

	clear() {
		this._value = undefined;
	}

	get byteLength() {
		return this._data!.byteLength;
	}
}

@variant(0)
export class CipherWithNonce {
	@field({ type: Uint8Array })
	nonce: Uint8Array;

	@field({ type: Uint8Array })
	cipher: Uint8Array;

	constructor(props?: { nonce: Uint8Array; cipher: Uint8Array }) {
		if (props) {
			this.nonce = props.nonce;
			this.cipher = props.cipher;
		}
	}

	equals(other: CipherWithNonce): boolean {
		if (other instanceof CipherWithNonce) {
			return (
				arraysEqual(this.nonce, other.nonce) &&
				arraysEqual(this.cipher, other.cipher)
			);
		} else {
			return false;
		}
	}
}

@variant(0)
export class K {
	@field({ type: CipherWithNonce })
	_encryptedKey: CipherWithNonce;

	@field({ type: X25519PublicKey })
	_recieverPublicKey: X25519PublicKey;

	constructor(props?: {
		encryptedKey: CipherWithNonce;
		recieverPublicKey: X25519PublicKey;
	}) {
		if (props) {
			this._encryptedKey = props.encryptedKey;
			this._recieverPublicKey = props.recieverPublicKey;
		}
	}

	equals(other: K): boolean {
		if (other instanceof K) {
			return (
				this._encryptedKey.equals(other._encryptedKey) &&
				this._recieverPublicKey.equals(other._recieverPublicKey)
			);
		} else {
			return false;
		}
	}
}

@variant(0)
export class Envelope {
	@field({ type: X25519PublicKey })
	_senderPublicKey: X25519PublicKey;

	@field({ type: vec(K) })
	_ks: K[];

	constructor(props?: { senderPublicKey: X25519PublicKey; ks: K[] }) {
		if (props) {
			this._senderPublicKey = props.senderPublicKey;
			this._ks = props.ks;
		}
	}

	equals(other: Envelope): boolean {
		if (other instanceof Envelope) {
			if (!this._senderPublicKey.equals(other._senderPublicKey)) {
				return false;
			}

			if (this._ks.length !== other._ks.length) {
				return false;
			}
			for (let i = 0; i < this._ks.length; i++) {
				if (!this._ks[i].equals(other._ks[i])) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}
}

@variant(1)
export class EncryptedThing<T> extends MaybeEncrypted<T> {
	@field({ type: Uint8Array })
	_encrypted: Uint8Array;

	@field({ type: Uint8Array })
	_nonce: Uint8Array;

	@field({ type: Envelope })
	_envelope: Envelope;

	constructor(props?: {
		encrypted: Uint8Array;
		nonce: Uint8Array;
		envelope: Envelope;
	}) {
		super();
		if (props) {
			this._encrypted = props.encrypted;
			this._nonce = props.nonce;
			this._envelope = props.envelope;
		}
	}

	_decrypted?: DecryptedThing<T>;
	get decrypted(): DecryptedThing<T> {
		if (!this._decrypted) {
			throw new Error(
				"Entry has not been decrypted, invoke decrypt method before"
			);
		}
		return this._decrypted;
	}

	async decrypt(keyResolver?: GetAnyKeypair): Promise<DecryptedThing<T>> {
		if (this._decrypted) {
			return this._decrypted;
		}

		if (!keyResolver) {
			throw new Error("Expecting key resolver");
		}

		// We only need to open with one of the keys
		let key:
			| { index: number; keypair: X25519Keypair | Ed25519Keypair }
			| undefined;
		if (keyResolver instanceof X25519Keypair) {
			for (const [i, k] of this._envelope._ks.entries()) {
				if (k._recieverPublicKey.equals(keyResolver.publicKey)) {
					key = {
						index: i,
						keypair: keyResolver,
					};
				}
			}
		} else {
			key = await keyResolver(
				this._envelope._ks.map((k) => k._recieverPublicKey)
			);
		}

		if (key) {
			const k = this._envelope._ks[key.index];
			let secretKey: X25519SecretKey = undefined as any;
			if (key.keypair instanceof X25519Keypair) {
				secretKey = key.keypair.secretKey;
			} else {
				secretKey = await X25519SecretKey.from(key.keypair.privateKey);
			}
			let epheremalKey: Uint8Array;
			try {
				epheremalKey = sodium.crypto_box_open_easy(
					k._encryptedKey.cipher,
					k._encryptedKey.nonce,
					this._envelope._senderPublicKey.publicKey,
					secretKey.secretKey
				);
			} catch (error) {
				throw new AccessError("Failed to decrypt");
			}

			// TODO: is nested decryption necessary?
			/*  let der: any = this;
			 let counter = 0;
			 while (der instanceof EncryptedThing) {
				 const decrypted = await sodium.crypto_secretbox_open_easy(this._encrypted, this._nonce, epheremalKey);
				 der = deserialize(decrypted, DecryptedThing)
				 counter += 1;
				 if (counter >= 10) {
					 throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
				 }
			 } */

			const der = deserialize(
				sodium.crypto_secretbox_open_easy(
					this._encrypted,
					this._nonce,
					epheremalKey
				),
				DecryptedThing
			);
			this._decrypted = der as DecryptedThing<T>;
		} else {
			throw new AccessError("Failed to resolve decryption key");
		}
		return this._decrypted;
	}

	equals(other: MaybeEncrypted<T>): boolean {
		if (other instanceof EncryptedThing) {
			if (!arraysEqual(this._encrypted, other._encrypted)) {
				return false;
			}
			if (!arraysEqual(this._nonce, other._nonce)) {
				return false;
			}

			if (!this._envelope.equals(other._envelope)) {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	clear() {
		this._decrypted = undefined;
	}

	get byteLength() {
		return this._encrypted.byteLength; // ignore other metdata for now in the size calculation
	}
}
