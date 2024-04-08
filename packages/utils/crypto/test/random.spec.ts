import { randomBytes } from "../src/random.js";
import { randomBytes as randomBytesBrowser } from "../src/random.js";
/* import { webcrypto } from "crypto"; */
import { expect } from "chai";


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
it("native", async () => {
	const bytes = randomBytes(32);
	testRandom(bytes);
	expect(bytes).to.have.length(32);
});

it("browser", async () => {
	const bytes = randomBytesBrowser(32);
	testRandom(bytes);
	expect(bytes).to.have.length(32);
});
