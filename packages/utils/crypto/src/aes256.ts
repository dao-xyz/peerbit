import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { PlainKey } from "./key";
import { compare } from "@peerbit/uint8arrays";
import { toHexString } from "./utils";

import sodium from "libsodium-wrappers";

@variant(0)
export class Aes256Key extends PlainKey {
	@field({ type: fixedArray("u8", 32) })
	key: Uint8Array;

	constructor(properties: { key: Uint8Array }) {
		super();
		if (properties.key.length !== 32) {
			throw new Error("Expecting key to have length 32");
		}
		this.key = properties.key;
	}

	static async create(): Promise<Aes256Key> {
		await sodium.ready;
		const generated = sodium.crypto_secretbox_keygen();
		const kp = new Aes256Key({
			key: generated
		});

		return kp;
	}

	equals(other: Aes256Key): boolean {
		if (other instanceof Aes256Key) {
			return compare(this.key, other.key) === 0;
		}
		return false;
	}
	toString(): string {
		return "aes256/" + toHexString(this.key);
	}
}
