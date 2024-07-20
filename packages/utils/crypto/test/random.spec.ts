/* import { webcrypto } from "crypto"; */
import { expect } from "chai";
import { randomBytes } from "../src/random.js";

/* globalThis.crypto = webcrypto as any; */

const testRandom = (bytes: Uint8Array) => {
	const set = new Set();
	for (const byte of bytes) {
		set.add(byte);
	}
	// check some randomness
	if (set.size === 1) {
		throw new Error();
	}
};
it("randomBytes", async () => {
	const bytes = randomBytes(32);
	testRandom(bytes);
	expect(bytes).to.have.length(32);
});
