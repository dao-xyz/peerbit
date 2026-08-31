import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { IdentityAccessController } from "../src/index.js";

// Frozen from the published @peerbit/identity-access-controller@6.0.102 codec.
const ROOT = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => index),
});
const STORE_ID = Uint8Array.from({ length: 32 }, (_, index) => 0xa0 + index);
const V1_HEX =
	"000c0000006964656e746974795f61636c0009000000646f63756d656e7473000a0000007368617265645f6c6f67009e2d8d513fc3b60e46607c529c27401780f02a4e442a449d6ae0daa2e778f714000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063000900000072656c6174696f6e730009000000646f63756d656e7473000a0000007368617265645f6c6f6700aae10f44b60863545a174cd2a6fd568d51e46cc893ab24bac98a21080f0be345000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063000f000000747275737465645f6e6574776f726b00000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0009000000646f63756d656e7473000a0000007368617265645f6c6f67007ff6fdb86167d03a4fd1b62796521a0992b29eeac0a70c680630c6c75bf744e3000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063";
const V1_LENGTH = 381;
const V1_ADDRESS = "zb2rhjXpihTjok32Pna6Qb199Wnyzi1x5gEcASokVi7mA7WTW";

const createV1 = () =>
	new IdentityAccessController({ id: STORE_ID, rootTrust: ROOT });
const toHex = (value: unknown) => Buffer.from(serialize(value)).toString("hex");

describe("IdentityAccessController v1 codec compatibility", () => {
	it("pins the published bytes, decoder, and program address", async () => {
		const encoded = createV1();
		const bytes = serialize(encoded);
		expect(bytes).to.have.length(V1_LENGTH);
		expect(Buffer.from(bytes).toString("hex")).to.equal(V1_HEX);
		expect((await encoded.calculateAddress()).address).to.equal(V1_ADDRESS);

		const decoded = deserialize(Buffer.from(V1_HEX, "hex"), Program);
		expect(decoded).to.be.instanceOf(IdentityAccessController);
		expect(toHex(decoded)).to.equal(V1_HEX);
		expect((await decoded.calculateAddress()).address).to.equal(V1_ADDRESS);
	});
});
