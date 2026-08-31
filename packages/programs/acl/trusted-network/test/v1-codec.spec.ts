import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey, Secp256k1PublicKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { IdentityRelation, TrustedNetwork } from "../src/index.js";

// Frozen from the published @peerbit/trusted-network@6.0.102 codec.
const ROOT = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => index),
});
const STORE_ID = Uint8Array.from({ length: 32 }, (_, index) => 0xa0 + index);
const SECP256K1_ROOT = new Secp256k1PublicKey({
	publicKey: Buffer.from(
		"0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
		"hex",
	),
});
const RELATION_TO = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => 0x40 + index),
});
const RELATION_HEX =
	"0034ae6006a4f4de6f0671688e060022d3a392debfb213f3b29549d8c2c51b96420000101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f00404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f";
const RELATION_ID_HEX =
	"34ae6006a4f4de6f0671688e060022d3a392debfb213f3b29549d8c2c51b9642";

const CASES = [
	{
		name: "Ed25519",
		root: ROOT,
		hex: "000f000000747275737465645f6e6574776f726b00000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0009000000646f63756d656e7473000a0000007368617265645f6c6f6700a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063",
		length: 152,
		address: "zb2rhhA5X8AXc1DPRva4c2WPeiVRQMXLaBRCETa7KDyqBEnQU",
	},
	{
		name: "secp256k1",
		root: SECP256K1_ROOT,
		hex: "000f000000747275737465645f6e6574776f726b010279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817980009000000646f63756d656e7473000a0000007368617265645f6c6f6700a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063",
		length: 153,
		address: "zb2rhojVZiC6L43eXn4RzyGppL1EhERs6JCDNMJdeD4wxymCn",
	},
] as const;

const toHex = (value: unknown) => Buffer.from(serialize(value)).toString("hex");

describe("TrustedNetwork v1 codec compatibility", () => {
	it("pins the published IdentityRelation bytes and identifier", () => {
		const from = new Ed25519PublicKey({
			publicKey: Uint8Array.from({ length: 32 }, (_, index) => 0x10 + index),
		});
		const relation = new IdentityRelation({ from, to: RELATION_TO });
		expect(toHex(relation)).to.equal(RELATION_HEX);
		expect(Buffer.from(relation.id).toString("hex")).to.equal(RELATION_ID_HEX);

		const decoded = deserialize(
			Buffer.from(RELATION_HEX, "hex"),
			IdentityRelation,
		);
		expect(toHex(decoded)).to.equal(RELATION_HEX);
		expect(Buffer.from(decoded.id).toString("hex")).to.equal(RELATION_ID_HEX);
	});

	for (const fixture of CASES) {
		it(`pins the published ${fixture.name} bytes, decoder, and address`, async () => {
			const encoded = new TrustedNetwork({
				id: STORE_ID,
				rootTrust: fixture.root,
			});
			const bytes = serialize(encoded);
			expect(bytes).to.have.length(fixture.length);
			expect(Buffer.from(bytes).toString("hex")).to.equal(fixture.hex);
			expect((await encoded.calculateAddress()).address).to.equal(
				fixture.address,
			);

			const decoded = deserialize(Buffer.from(fixture.hex, "hex"), Program);
			expect(decoded).to.be.instanceOf(TrustedNetwork);
			expect((decoded as TrustedNetwork).rootTrust.equals(fixture.root)).to.be
				.true;
			expect(toHex(decoded)).to.equal(fixture.hex);
			expect((await decoded.calculateAddress()).address).to.equal(
				fixture.address,
			);
		});
	}
});
