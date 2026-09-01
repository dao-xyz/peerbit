import { serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { expect } from "chai";
import { compare } from "uint8arrays";
import * as publicApi from "../src/index.js";
import {
	NetworkDescriptorV2,
	OperationPolicyProofV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_OPERATION_POLICY_PROOF_BYTES,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
	TrustedNetworkRole,
	assertOperationPolicyProofV2,
	assertResourceFenceV2,
	decodeOperationPolicyProofV2,
	decodeResourceFenceV2,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} from "../src/v2.js";

const bytes32 = (value: number): Uint8Array => new Uint8Array(32).fill(value);
const ZERO = bytes32(0);
const RESOURCE_ID = bytes32(0xa1);
const AUTHORITY = new Ed25519PublicKey({ publicKey: bytes32(0x11) });
const WRITER = new Ed25519PublicKey({ publicKey: bytes32(0x22) });

const bindings = [
	new PolicySubjectBindingV2({
		signingKey: AUTHORITY,
		roles: TrustedNetworkRole.ADMIN,
	}),
	new PolicySubjectBindingV2({
		signingKey: WRITER,
		roles: TrustedNetworkRole.WRITER,
	}),
].sort((left, right) =>
	compare(serialize(left.signingKey), serialize(right.signingKey)),
);
const descriptorSeed = new NetworkDescriptorV2({
	protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	networkNonce: bytes32(0x33),
	policyAuthority: AUTHORITY,
	genesisPolicyDigest: ZERO,
	policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	entrySignatureProfile:
		TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
});
const genesisPolicy = new PolicySnapshotBodyV2({
	networkId: deriveNetworkIdV2(descriptorSeed),
	sequence: 0n,
	previousPolicyDigest: ZERO,
	bindings,
});
const DESCRIPTOR = new NetworkDescriptorV2({
	...descriptorSeed,
	genesisPolicyDigest: digestPolicySnapshotBodyV2(genesisPolicy),
});
const NETWORK_ID = deriveNetworkIdV2(DESCRIPTOR);

const fence = (): ResourceFenceV2 =>
	new ResourceFenceV2({
		networkId: NETWORK_ID,
		resourceId: RESOURCE_ID,
		fenceSequence: 0n,
		previousFenceDigest: ZERO,
		policySequence: 0n,
		policyDigest: DESCRIPTOR.genesisPolicyDigest,
		contentEpoch: 0n,
		epochManifestDigest: ZERO,
	});
const proof = (): OperationPolicyProofV2 =>
	new OperationPolicyProofV2({
		networkId: NETWORK_ID,
		resourceId: RESOURCE_ID,
		policySequence: 0n,
		policyDigest: DESCRIPTOR.genesisPolicyDigest,
		fenceDigest: bytes32(0x40),
		contentEpoch: 0n,
	});
const goldenFence = (): ResourceFenceV2 =>
	new ResourceFenceV2({
		networkId: NETWORK_ID,
		resourceId: RESOURCE_ID,
		fenceSequence: 0x0102030405060708n,
		previousFenceDigest: bytes32(0xb2),
		policySequence: 0x1112131415161718n,
		policyDigest: bytes32(0xc3),
		contentEpoch: 0x2122232425262728n,
		epochManifestDigest: bytes32(0xd4),
	});
const goldenProof = (): OperationPolicyProofV2 =>
	new OperationPolicyProofV2({
		networkId: NETWORK_ID,
		resourceId: RESOURCE_ID,
		policySequence: 0x3132333435363738n,
		policyDigest: bytes32(0xe5),
		fenceDigest: bytes32(0xf6),
		contentEpoch: 0x4142434445464748n,
	});

describe("TrustedNetwork v2 resource codecs", () => {
	it("keeps the resource records out of the package entry point", () => {
		expect("ResourceFenceV2" in publicApi).to.equal(false);
		expect("OperationPolicyProofV2" in publicApi).to.equal(false);
		expect("decodeResourceFenceV2" in publicApi).to.equal(false);
		expect("decodeOperationPolicyProofV2" in publicApi).to.equal(false);
	});

	it("pins the exact variants, field order, lengths, and golden bytes", () => {
		const fenceBytes = serialize(goldenFence());
		const proofBytes = serialize(goldenProof());
		expect(fenceBytes).to.have.length(
			TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
		);
		expect(proofBytes).to.have.length(
			TRUSTED_NETWORK_V2_OPERATION_POLICY_PROOF_BYTES,
		);
		expect(Buffer.from(fenceBytes).toString("hex")).to.equal(
			"0202f5e12b12ca9fc87dbf48a98f0e660c74f4e0c90f3c8254a549a5b032cfaddb7ba1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a10807060504030201b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b21817161514131211c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c32827262524232221d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
		);
		expect(Buffer.from(proofBytes).toString("hex")).to.equal(
			"0203f5e12b12ca9fc87dbf48a98f0e660c74f4e0c90f3c8254a549a5b032cfaddb7ba1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a13837363534333231e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f64847464544434241",
		);
	});

	it("captures exact input before decode without invoking caller iterators", () => {
		const fenceBytes = serialize(fence());
		const proofBytes = serialize(proof());
		const expectedFenceHex = Buffer.from(fenceBytes).toString("hex");
		const expectedProofHex = Buffer.from(proofBytes).toString("hex");
		let iteratorCalls = 0;
		Object.defineProperty(proofBytes, Symbol.iterator, {
			value: () => {
				iteratorCalls++;
				return [][Symbol.iterator]();
			},
		});
		const decodedFence = decodeResourceFenceV2(fenceBytes, DESCRIPTOR);
		const decodedProof = decodeOperationPolicyProofV2(proofBytes, DESCRIPTOR);
		fenceBytes.fill(0xff);
		proofBytes.fill(0xee);
		expect(iteratorCalls).to.equal(0);
		expect(Buffer.from(serialize(decodedFence)).toString("hex")).to.equal(
			expectedFenceHex,
		);
		expect(Buffer.from(serialize(decodedProof)).toString("hex")).to.equal(
			expectedProofHex,
		);
	});

	it("rejects every non-exact extent before decode", () => {
		for (const length of [0, 185, 187, 1024]) {
			expect(() =>
				decodeResourceFenceV2(new Uint8Array(length), DESCRIPTOR),
			).to.throw("186 bytes");
		}
		for (const length of [0, 145, 147, 1024]) {
			expect(() =>
				decodeOperationPolicyProofV2(new Uint8Array(length), DESCRIPTOR),
			).to.throw("146 bytes");
		}
		const shadowed = new Uint8Array(187);
		Object.defineProperty(shadowed, "byteLength", { value: 186 });
		expect(() => decodeResourceFenceV2(shadowed, DESCRIPTOR)).to.throw(
			"186 bytes",
		);
		expect(() =>
			decodeResourceFenceV2(new Proxy(serialize(fence()), {}), DESCRIPTOR),
		).to.throw("186 bytes");

		const detached = new Uint8Array(
			TRUSTED_NETWORK_V2_OPERATION_POLICY_PROOF_BYTES,
		);
		detached.set(serialize(proof()));
		structuredClone(detached.buffer, { transfer: [detached.buffer] });
		expect(() => decodeOperationPolicyProofV2(detached, DESCRIPTOR)).to.throw(
			"146 bytes",
		);
	});

	it("round trips maximum u64 values and rejects out-of-range assertions", () => {
		const maximum = 0xffffffffffffffffn;
		const maxFence = goldenFence();
		maxFence.fenceSequence = maximum;
		maxFence.policySequence = maximum;
		maxFence.contentEpoch = maximum;
		const decodedFence = decodeResourceFenceV2(
			serialize(maxFence),
			DESCRIPTOR,
		);
		expect(decodedFence.fenceSequence).to.equal(maximum);
		expect(decodedFence.policySequence).to.equal(maximum);
		expect(decodedFence.contentEpoch).to.equal(maximum);

		const maxProof = goldenProof();
		maxProof.policySequence = maximum;
		maxProof.contentEpoch = maximum;
		const decodedProof = decodeOperationPolicyProofV2(
			serialize(maxProof),
			DESCRIPTOR,
		);
		expect(decodedProof.policySequence).to.equal(maximum);
		expect(decodedProof.contentEpoch).to.equal(maximum);

		const negativeFenceSequence = fence();
		negativeFenceSequence.fenceSequence = -1n;
		expect(() =>
			assertResourceFenceV2(negativeFenceSequence, DESCRIPTOR),
		).to.throw("fenceSequence must be a u64");
		const overflowingPolicySequence = fence();
		overflowingPolicySequence.policySequence = maximum + 1n;
		expect(() =>
			assertResourceFenceV2(overflowingPolicySequence, DESCRIPTOR),
		).to.throw("policySequence must be a u64");
		const negativeContentEpoch = proof();
		negativeContentEpoch.contentEpoch = -1n;
		expect(() =>
			assertOperationPolicyProofV2(negativeContentEpoch, DESCRIPTOR),
		).to.throw("contentEpoch must be a u64");
	});

	it("fails closed on network replay, malformed fields, and unknown variants", () => {
		const wrongNetworkFence = fence();
		wrongNetworkFence.networkId = bytes32(0xee);
		expect(() =>
			decodeResourceFenceV2(serialize(wrongNetworkFence), DESCRIPTOR),
		).to.throw("another network");
		const wrongNetworkProof = proof();
		wrongNetworkProof.networkId = bytes32(0xee);
		expect(() =>
			decodeOperationPolicyProofV2(serialize(wrongNetworkProof), DESCRIPTOR),
		).to.throw("another network");

		const badInitial = fence();
		badInitial.previousFenceDigest = bytes32(1);
		expect(() =>
			decodeResourceFenceV2(serialize(badInitial), DESCRIPTOR),
		).to.throw("zero previous digest");
		const badSuccessor = fence();
		badSuccessor.fenceSequence = 1n;
		expect(() =>
			decodeResourceFenceV2(serialize(badSuccessor), DESCRIPTOR),
		).to.throw("must name its predecessor");

		const unknownVariant = serialize(fence());
		unknownVariant[1] = 0xff;
		expect(() => decodeResourceFenceV2(unknownVariant, DESCRIPTOR)).to.throw();

		const unknownProofVariant = serialize(proof());
		unknownProofVariant[1] = 0xff;
		expect(() =>
			decodeOperationPolicyProofV2(unknownProofVariant, DESCRIPTOR),
		).to.throw();
	});
});
