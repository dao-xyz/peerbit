import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { expect } from "chai";
import { execFile } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as publicApi from "../src/index.js";
import { TrustedNetwork } from "../src/index.js";
import {
	EncryptionKeyCommitmentV2,
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
	TRUSTED_NETWORK_V2_NETWORK_ID_DOMAIN,
	TRUSTED_NETWORK_V2_POLICY_DIGEST_DOMAIN,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TrustedNetworkRole,
	TrustedNetworkV2,
	assertPolicySnapshotBodyV2,
	decodePolicySnapshotBodyV2,
	decodeTrustedNetworkV2,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} from "../src/v2.js";

const execFileAsync = promisify(execFile);
const AUTHORITY = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => 0x10 + index),
});
const WRITER = new Ed25519PublicKey({
	publicKey: Uint8Array.from({ length: 32 }, (_, index) => 0x40 + index),
});
const NETWORK_NONCE = Uint8Array.from(
	{ length: 32 },
	(_, index) => 0x70 + index,
);
const ZERO_DIGEST = new Uint8Array(32);

const NETWORK_ID_HEX =
	"22528424d2e1bcd346c864944d530b2065c1463cac0ad88d8778bbe0ed16b23a";
const POLICY_DIGEST_HEX =
	"feacf4689ca759a97c7246a308bae35ca335083c03aaac5b912753e7f45dc46a";
const DESCRIPTOR_HEX =
	"02000200707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f00101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2ffeacf4689ca759a97c7246a308bae35ca335083c03aaac5b912753e7f45dc46a0101";
const POLICY_BODY_HEX =
	"020122528424d2e1bcd346c864944d530b2065c1463cac0ad88d8778bbe0ed16b23a000000000000000000000000000000000000000000000000000000000000000000000000000000000200000000101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f010000404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f0a00";
const PROGRAM_HEX =
	"0012000000747275737465645f6e6574776f726b5f763202000200707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f00101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2ffeacf4689ca759a97c7246a308bae35ca335083c03aaac5b912753e7f45dc46a0101";
const PROGRAM_ADDRESS = "zb2rhgksu6TdYdhJABtWZhuNFp6Tyvi64WfUmpR9AmZXtjrLD";
const V1_PROGRAM_HEX =
	"000f000000747275737465645f6e6574776f726b00000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0009000000646f63756d656e7473000a0000007368617265645f6c6f6700a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063";

const hex = (value: Uint8Array | unknown): string =>
	Buffer.from(value instanceof Uint8Array ? value : serialize(value)).toString(
		"hex",
	);

const createFixture = () => {
	const descriptorWithoutGenesis = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: NETWORK_NONCE,
		policyAuthority: AUTHORITY,
		genesisPolicyDigest: ZERO_DIGEST,
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const networkId = deriveNetworkIdV2(descriptorWithoutGenesis);
	const policy = new PolicySnapshotBodyV2({
		networkId,
		sequence: 0n,
		previousPolicyDigest: ZERO_DIGEST,
		bindings: [
			new PolicySubjectBindingV2({
				signingKey: AUTHORITY,
				roles: TrustedNetworkRole.ADMIN,
			}),
			new PolicySubjectBindingV2({
				signingKey: WRITER,
				roles: TrustedNetworkRole.WRITER | TrustedNetworkRole.REPLICATOR,
			}),
		],
	});
	const policyDigest = digestPolicySnapshotBodyV2(policy);
	const descriptor = new NetworkDescriptorV2({
		protocolVersion: descriptorWithoutGenesis.protocolVersion,
		networkNonce: descriptorWithoutGenesis.networkNonce,
		policyAuthority: descriptorWithoutGenesis.policyAuthority,
		genesisPolicyDigest: policyDigest,
		policyHashProfile: descriptorWithoutGenesis.policyHashProfile,
		entrySignatureProfile: descriptorWithoutGenesis.entrySignatureProfile,
	});
	return {
		descriptor,
		networkId,
		policy,
		policyDigest,
		program: new TrustedNetworkV2({ descriptor }),
	};
};

describe("TrustedNetwork v2 codec and version fence", () => {
	it("pins the internal policy codec, domains, roles, digests, and address", async () => {
		const fixture = createFixture();
		assertPolicySnapshotBodyV2(fixture.policy, fixture.descriptor);

		expect(TRUSTED_NETWORK_V2_NETWORK_ID_DOMAIN).to.equal(
			"peerbit/trusted-network/v2/network-id/v1",
		);
		expect(TRUSTED_NETWORK_V2_POLICY_DIGEST_DOMAIN).to.equal(
			"peerbit/trusted-network/v2/policy-body/v1",
		);
		expect(TrustedNetworkRole).to.deep.equal({
			ADMIN: 1,
			WRITER: 2,
			READER: 4,
			REPLICATOR: 8,
		});
		expect(Object.isFrozen(TrustedNetworkRole)).to.be.true;
		expect(
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
		).to.equal(1);
		expect(TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES).to.equal(128 * 1024);
		expect(hex(fixture.networkId)).to.equal(NETWORK_ID_HEX);
		expect(hex(fixture.policyDigest)).to.equal(POLICY_DIGEST_HEX);
		expect(hex(fixture.descriptor)).to.equal(DESCRIPTOR_HEX);
		expect(serialize(fixture.descriptor)).to.have.length(103);
		expect(hex(fixture.policy)).to.equal(POLICY_BODY_HEX);
		expect(serialize(fixture.policy)).to.have.length(148);
		expect(hex(fixture.program)).to.equal(PROGRAM_HEX);
		expect(serialize(fixture.program)).to.have.length(126);
		expect((await fixture.program.calculateAddress()).address).to.equal(
			PROGRAM_ADDRESS,
		);

		const decodedDescriptor = deserialize(
			Buffer.from(DESCRIPTOR_HEX, "hex"),
			NetworkDescriptorV2,
		);
		expect(hex(decodedDescriptor)).to.equal(DESCRIPTOR_HEX);
		expect(
			hex(
				decodePolicySnapshotBodyV2(
					Buffer.from(POLICY_BODY_HEX, "hex"),
					decodedDescriptor,
				),
			),
		).to.equal(POLICY_BODY_HEX);
		const decodedProgram = decodeTrustedNetworkV2(
			Buffer.from(PROGRAM_HEX, "hex"),
		);
		expect(hex(decodedProgram)).to.equal(PROGRAM_HEX);
		expect((await decodedProgram.calculateAddress()).address).to.equal(
			PROGRAM_ADDRESS,
		);
	});

	it("fails closed on unsupported, malformed, and non-canonical inputs", () => {
		const fixture = createFixture();
		const unsupportedDescriptor = new NetworkDescriptorV2({
			...fixture.descriptor,
			protocolVersion: 3,
		});
		expect(() =>
			decodeTrustedNetworkV2(
				serialize(new TrustedNetworkV2({ descriptor: unsupportedDescriptor })),
			),
		).to.throw("Unsupported TrustedNetwork protocol version");

		const unknownRolePolicy = new PolicySnapshotBodyV2({
			networkId: fixture.networkId,
			sequence: 0n,
			previousPolicyDigest: ZERO_DIGEST,
			bindings: [
				new PolicySubjectBindingV2({
					signingKey: AUTHORITY,
					roles: TrustedNetworkRole.ADMIN | 0x10,
				}),
			],
		});
		expect(() =>
			decodePolicySnapshotBodyV2(
				serialize(unknownRolePolicy),
				fixture.descriptor,
			),
		).to.throw("unknown role bits");

		const prematureEncryptionPolicy = new PolicySnapshotBodyV2({
			networkId: fixture.networkId,
			sequence: 0n,
			previousPolicyDigest: ZERO_DIGEST,
			bindings: [
				new PolicySubjectBindingV2({
					signingKey: AUTHORITY,
					roles: TrustedNetworkRole.ADMIN,
					encryptionKeyCommitment: new EncryptionKeyCommitmentV2({
						profile: 1,
						digest: new Uint8Array(32),
					}),
				}),
			],
		});
		expect(() =>
			decodePolicySnapshotBodyV2(
				serialize(prematureEncryptionPolicy),
				fixture.descriptor,
			),
		).to.throw("encryption commitments are not supported yet");

		const unknownRecord = Buffer.from(POLICY_BODY_HEX, "hex");
		unknownRecord[0] = 3;
		expect(() =>
			decodePolicySnapshotBodyV2(unknownRecord, fixture.descriptor),
		).to.throw();
		expect(() =>
			decodeTrustedNetworkV2(Buffer.from(PROGRAM_HEX.slice(0, -2), "hex")),
		).to.throw();
		expect(() =>
			decodeTrustedNetworkV2(
				Buffer.concat([Buffer.from(PROGRAM_HEX, "hex"), Buffer.from([0])]),
			),
		).to.throw();
	});

	it("rejects invalid descriptor profiles and policy invariants", () => {
		const fixture = createFixture();
		const decodeDescriptor = (descriptor: NetworkDescriptorV2) =>
			decodeTrustedNetworkV2(serialize(new TrustedNetworkV2({ descriptor })));

		for (const [field, message] of [
			["policyHashProfile", "policy hash profile"],
			["entrySignatureProfile", "entry signature profile"],
		] as const) {
			expect(() =>
				decodeDescriptor(
					new NetworkDescriptorV2({
						...fixture.descriptor,
						[field]: 2,
					}),
				),
			).to.throw(message);
		}

		const makePolicy = (
			overrides: Partial<{
				networkId: Uint8Array;
				sequence: bigint;
				previousPolicyDigest: Uint8Array;
				bindings: PolicySubjectBindingV2[];
			}>,
		) => new PolicySnapshotBodyV2({ ...fixture.policy, ...overrides });
		const binding = (signingKey: Ed25519PublicKey, roles: number) =>
			new PolicySubjectBindingV2({ signingKey, roles });
		const wrongNetworkId = Uint8Array.from(fixture.networkId);
		wrongNetworkId[0] ^= 0xff;
		const nonZeroDigest = Uint8Array.from({ length: 32 }, () => 1);
		const wrongGenesisDigest = Uint8Array.from(
			fixture.descriptor.genesisPolicyDigest,
		);
		wrongGenesisDigest[0] ^= 0xff;
		const wrongGenesisDescriptor = new NetworkDescriptorV2({
			...fixture.descriptor,
			genesisPolicyDigest: wrongGenesisDigest,
		});

		const cases: Array<{
			name: string;
			body: PolicySnapshotBodyV2;
			descriptor?: NetworkDescriptorV2;
			message: string;
		}> = [
			{
				name: "out-of-order bindings",
				body: makePolicy({ bindings: [...fixture.policy.bindings].reverse() }),
				message: "sorted and unique",
			},
			{
				name: "duplicate bindings",
				body: makePolicy({
					bindings: [
						binding(AUTHORITY, TrustedNetworkRole.ADMIN),
						binding(AUTHORITY, TrustedNetworkRole.ADMIN),
					],
				}),
				message: "sorted and unique",
			},
			{
				name: "zero roles",
				body: makePolicy({ bindings: [binding(AUTHORITY, 0)] }),
				message: "no roles",
			},
			{
				name: "missing authority",
				body: makePolicy({
					bindings: [binding(WRITER, TrustedNetworkRole.WRITER)],
				}),
				message: "authority must hold ADMIN",
			},
			{
				name: "authority without ADMIN",
				body: makePolicy({
					bindings: [binding(AUTHORITY, TrustedNetworkRole.WRITER)],
				}),
				message: "authority must hold ADMIN",
			},
			{
				name: "non-authority ADMIN",
				body: makePolicy({
					bindings: [
						binding(AUTHORITY, TrustedNetworkRole.ADMIN),
						binding(WRITER, TrustedNetworkRole.ADMIN),
					],
				}),
				message: "Only the policy authority",
			},
			{
				name: "wrong network",
				body: makePolicy({ networkId: wrongNetworkId }),
				message: "another network",
			},
			{
				name: "non-zero genesis parent",
				body: makePolicy({ previousPolicyDigest: nonZeroDigest }),
				message: "zero previous digest",
			},
			{
				name: "zero non-genesis parent",
				body: makePolicy({ sequence: 1n }),
				message: "must name its previous digest",
			},
			{
				name: "mismatched genesis digest",
				body: fixture.policy,
				descriptor: wrongGenesisDescriptor,
				message: "does not match the descriptor",
			},
		];

		for (const { name, body, descriptor, message } of cases) {
			expect(
				() =>
					decodePolicySnapshotBodyV2(
						serialize(body),
						descriptor ?? fixture.descriptor,
					),
				name,
			).to.throw(message);
		}
	});

	it("is internal, non-activatable, and distinct from V1", async () => {
		const { program } = createFixture();
		expect("TrustedNetworkV2" in publicApi).to.be.false;
		await expect(program.beforeOpen({} as never)).to.be.rejectedWith(
			"decode-only codec",
		);
		await expect(program.open()).to.be.rejectedWith("decode-only codec");
		expect((await program.calculateAddress()).address).to.not.equal(
			"zb2rhhA5X8AXc1DPRva4c2WPeiVRQMXLaBRCETa7KDyqBEnQU",
		);
		expect(() =>
			deserialize(Buffer.from(PROGRAM_HEX, "hex"), TrustedNetwork),
		).to.throw();
	});

	it("lets a fresh public V1 decoder read V1 and reject V2", async function () {
		this.timeout(30_000);
		const packageRoot = path.resolve(
			path.dirname(fileURLToPath(import.meta.url)),
			"..",
			"..",
		);
		const script = `
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
const entry = await import("@peerbit/trusted-network");
if ("TrustedNetworkV2" in entry) throw new Error("V2 leaked from the public entry");
const v1Hex = ${JSON.stringify(V1_PROGRAM_HEX)};
const v2Hex = ${JSON.stringify(PROGRAM_HEX)};
const v1 = deserialize(Buffer.from(v1Hex, "hex"), Program);
if (!(v1 instanceof entry.TrustedNetwork)) throw new Error("V1 did not decode as TrustedNetwork");
if (Buffer.from(serialize(v1)).toString("hex") !== v1Hex) throw new Error("V1 bytes changed");
let rejectedAsProgram = false;
try { deserialize(Buffer.from(v2Hex, "hex"), Program); } catch { rejectedAsProgram = true; }
if (!rejectedAsProgram) throw new Error("legacy Program decoder accepted V2");
let rejectedAsV1 = false;
try { deserialize(Buffer.from(v2Hex, "hex"), entry.TrustedNetwork); } catch { rejectedAsV1 = true; }
if (!rejectedAsV1) throw new Error("legacy TrustedNetwork decoder accepted V2");
let subpathRejected = false;
try {
  await import("@peerbit/trusted-network/v2");
} catch (error) {
  if (error?.code !== "ERR_PACKAGE_PATH_NOT_EXPORTED") throw error;
  subpathRejected = true;
}
if (!subpathRejected) throw new Error("V2 package subpath is public");
console.log("legacy version fence OK");
`;
		const { stdout } = await execFileAsync(
			process.execPath,
			["--input-type=module", "-e", script],
			{ cwd: packageRoot, timeout: 20_000 },
		);
		expect(stdout).to.include("legacy version fence OK");
	});
});
