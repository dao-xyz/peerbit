import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { TrustedNetwork } from "@peerbit/trusted-network";
import { expect } from "chai";
import { ClockService } from "../src/index.js";

// Frozen from the published @peerbit/clock-service@3.2.126 codec.
const ROOT = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => index),
});
const STORE_ID = Uint8Array.from({ length: 32 }, (_, index) => 0xa0 + index);
const V1_HEX =
	"000d000000636c6f636b5f736572766963650003000000727063000f000000747275737465645f6e6574776f726b00000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0009000000646f63756d656e7473000a0000007368617265645f6c6f6700a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063";
const V1_LENGTH = 178;
const V1_ADDRESS = "zb2rhiDEBSh5XqAihf53dGavmWhnuSbNn249x7McHDEG3pVaJ";

const createV1 = () =>
	new ClockService({
		trustedNetwork: new TrustedNetwork({ id: STORE_ID, rootTrust: ROOT }),
	});
const toHex = (value: unknown) => Buffer.from(serialize(value)).toString("hex");

describe("ClockService v1 codec compatibility", () => {
	it("pins the published bytes, decoder, and program address", async () => {
		const encoded = createV1();
		const bytes = serialize(encoded);
		expect(bytes).to.have.length(V1_LENGTH);
		expect(Buffer.from(bytes).toString("hex")).to.equal(V1_HEX);
		expect((await encoded.calculateAddress()).address).to.equal(V1_ADDRESS);

		const decoded = deserialize(Buffer.from(V1_HEX, "hex"), Program);
		expect(decoded).to.be.instanceOf(ClockService);
		expect(toHex(decoded)).to.equal(V1_HEX);
		expect((await decoded.calculateAddress()).address).to.equal(V1_ADDRESS);
	});
});
