import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { PlainKey } from "./key";
import { compare } from "@peerbit/uint8arrays";
import { toHexString } from "./utils";

import sodium from "libsodium-wrappers";

@variant(0)
export class XSalsa20Poly1305 extends PlainKey {
	@field({ type: fixedArray("u8", 32) })
	key: Uint8Array;

	constructor(properties: { key: Uint8Array }) {
		super();
		if (properties.key.length !== 32) {
			throw new Error("Expecting key to have length 32");
		}
		this.key = properties.key;
	}

	static async create(): Promise<XSalsa20Poly1305> {
		await sodium.ready;
		const generated = sodium.crypto_secretbox_keygen();
		const kp = new XSalsa20Poly1305({
			key: generated
		});

		return kp;
	}

	equals(other: PlainKey): boolean {
		if (other instanceof XSalsa20Poly1305) {
			return compare(this.key, other.key) === 0;
		}
		return false;
	}
	toString(): string {
		return "xsalsa20poly1305/" + toHexString(this.key);
	}
}
