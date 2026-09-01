import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { PublicSignKey, sha256Sync } from "@peerbit/crypto";
import {
	Program,
	type ProgramClient,
	type ProgramInitializationOptions,
} from "@peerbit/program";
import { compare, concat, equals } from "uint8arrays";

/**
 * Decode-only TrustedNetwork v2 codec scaffold.
 *
 * This module is intentionally absent from the package entry point. It pins
 * the policy, resource-fence-body, and operation-proof wire contracts without
 * exposing a usable controller, resource validator, or encryption path.
 */

export const TRUSTED_NETWORK_V2_PROTOCOL_VERSION = 2;
export const TRUSTED_NETWORK_V2_POLICY_HASH_SHA256 = 1;

/**
 * Authority-record profile 1 accepts only a policy-snapshot or resource-fence
 * EntryV0 whose normal toSignable() bytes (including causal metadata) have
 * exactly one successfully verified signature. The signer's canonical
 * public-key bytes must equal the descriptor authority. Protected-resource
 * adapters must define a separate bounded profile for operation entries.
 */
export const TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE = 1;

/**
 * Consensus ceiling for the canonical serialized EntryV0 carrying a v2 policy
 * snapshot. Every replica applies this exact bound before decoding the entry;
 * it is part of signature profile 1 rather than a replica-local resource knob.
 */
export const TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES = 128 * 1024;

/**
 * Consensus bounds for a canonical signed resource-fence EntryV0. These are
 * protocol constants, not resource-adapter configuration.
 */
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES = 8 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS = 64;

export const TRUSTED_NETWORK_V2_NETWORK_ID_DOMAIN =
	"peerbit/trusted-network/v2/network-id/v1";
export const TRUSTED_NETWORK_V2_POLICY_DIGEST_DOMAIN =
	"peerbit/trusted-network/v2/policy-body/v1";

/** Fixed first-profile record lengths. Changing either requires a new variant. */
export const TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES = 186;
export const TRUSTED_NETWORK_V2_OPERATION_POLICY_PROOF_BYTES = 146;

export const TrustedNetworkRole = Object.freeze({
	ADMIN: 0x01,
	WRITER: 0x02,
	READER: 0x04,
	REPLICATOR: 0x08,
} as const);

export const TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS =
	TrustedNetworkRole.ADMIN |
	TrustedNetworkRole.WRITER |
	TrustedNetworkRole.READER |
	TrustedNetworkRole.REPLICATOR;

const ZERO_DIGEST = new Uint8Array(32);
const textEncoder = new TextEncoder();
const TYPED_ARRAY_PROTOTYPE = Object.getPrototypeOf(Uint8Array.prototype);
const TYPED_ARRAY_BYTE_LENGTH = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	"byteLength",
)!.get!;
const TYPED_ARRAY_TAG = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	Symbol.toStringTag,
)!.get!;

export const exactUint8ArrayByteLengthV2 = (value: unknown): number => {
	if (
		!ArrayBuffer.isView(value) ||
		TYPED_ARRAY_TAG.call(value) !== "Uint8Array"
	) {
		throw new TypeError("Expected a genuine Uint8Array");
	}
	const byteLength = TYPED_ARRAY_BYTE_LENGTH.call(value) as number;
	if (byteLength === 0) {
		Uint8Array.prototype.set.call(new Uint8Array(0), value as Uint8Array);
	}
	return byteLength;
};

export const copyUint8ArrayWithLengthV2 = (
	value: Uint8Array,
	byteLength: number,
): Uint8Array => {
	const copy = new Uint8Array(byteLength);
	Uint8Array.prototype.set.call(copy, value);
	return copy;
};

export const copyUint8ArrayV2 = (value: Uint8Array): Uint8Array => {
	const byteLength = exactUint8ArrayByteLengthV2(value);
	return copyUint8ArrayWithLengthV2(value, byteLength);
};

const assertByte = (value: number, label: string): void => {
	if (!Number.isInteger(value) || value < 0 || value > 0xff) {
		throw new Error(`${label} must be a u8`);
	}
};

const assertU16 = (value: number, label: string): void => {
	if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
		throw new Error(`${label} must be a u16`);
	}
};

const assertU64 = (value: bigint, label: string): void => {
	if (typeof value !== "bigint" || value < 0n || value > 0xffffffffffffffffn) {
		throw new Error(`${label} must be a u64`);
	}
};

const assertBytes32 = (value: Uint8Array, label: string): void => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(value);
	} catch {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
	if (byteLength !== 32) {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
};

const lengthPrefix = (value: Uint8Array): Uint8Array => {
	const length = new Uint8Array(4);
	new DataView(length.buffer).setUint32(0, value.byteLength, true);
	return concat([length, value]);
};

const encodeU16 = (value: number): Uint8Array => {
	const bytes = new Uint8Array(2);
	new DataView(bytes.buffer).setUint16(0, value, true);
	return bytes;
};

const assertCanonicalEncoding = (bytes: Uint8Array, value: unknown): void => {
	if (!equals(bytes, serialize(value))) {
		throw new Error("TrustedNetwork v2 encoding is not canonical");
	}
};

@variant([2, 0])
export class NetworkDescriptorV2 {
	@field({ type: "u16" })
	protocolVersion: number;

	@field({ type: fixedArray("u8", 32) })
	networkNonce: Uint8Array;

	@field({ type: PublicSignKey })
	policyAuthority: PublicSignKey;

	@field({ type: fixedArray("u8", 32) })
	genesisPolicyDigest: Uint8Array;

	@field({ type: "u8" })
	policyHashProfile: number;

	@field({ type: "u8" })
	entrySignatureProfile: number;

	constructor(properties?: {
		protocolVersion: number;
		networkNonce: Uint8Array;
		policyAuthority: PublicSignKey;
		genesisPolicyDigest: Uint8Array;
		policyHashProfile: number;
		entrySignatureProfile: number;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

export class EncryptionKeyCommitmentV2 {
	@field({ type: "u8" })
	profile: number;

	@field({ type: fixedArray("u8", 32) })
	digest: Uint8Array;

	constructor(properties?: { profile: number; digest: Uint8Array }) {
		if (properties) Object.assign(this, properties);
	}
}

export class PolicySubjectBindingV2 {
	@field({ type: PublicSignKey })
	signingKey: PublicSignKey;

	@field({ type: "u8" })
	roles: number;

	@field({ type: option(EncryptionKeyCommitmentV2) })
	encryptionKeyCommitment?: EncryptionKeyCommitmentV2;

	constructor(properties?: {
		signingKey: PublicSignKey;
		roles: number;
		encryptionKeyCommitment?: EncryptionKeyCommitmentV2;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

@variant([2, 1])
export class PolicySnapshotBodyV2 {
	@field({ type: fixedArray("u8", 32) })
	networkId: Uint8Array;

	@field({ type: "u64" })
	sequence: bigint;

	@field({ type: fixedArray("u8", 32) })
	previousPolicyDigest: Uint8Array;

	@field({ type: vec(PolicySubjectBindingV2) })
	bindings: PolicySubjectBindingV2[];

	constructor(properties?: {
		networkId: Uint8Array;
		sequence: bigint;
		previousPolicyDigest: Uint8Array;
		bindings: PolicySubjectBindingV2[];
	}) {
		if (properties) Object.assign(this, properties);
	}
}

/** Canonical unsigned payload intended for an authenticated resource fence. */
@variant([2, 2])
export class ResourceFenceV2 {
	@field({ type: fixedArray("u8", 32) })
	networkId: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	resourceId: Uint8Array;

	@field({ type: "u64" })
	fenceSequence: bigint;

	@field({ type: fixedArray("u8", 32) })
	previousFenceDigest: Uint8Array;

	@field({ type: "u64" })
	policySequence: bigint;

	@field({ type: fixedArray("u8", 32) })
	policyDigest: Uint8Array;

	@field({ type: "u64" })
	contentEpoch: bigint;

	@field({ type: fixedArray("u8", 32) })
	epochManifestDigest: Uint8Array;

	constructor(properties?: {
		networkId: Uint8Array;
		resourceId: Uint8Array;
		fenceSequence: bigint;
		previousFenceDigest: Uint8Array;
		policySequence: bigint;
		policyDigest: Uint8Array;
		contentEpoch: bigint;
		epochManifestDigest: Uint8Array;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

/** Constant-size policy commitment intended for a signed resource operation. */
@variant([2, 3])
export class OperationPolicyProofV2 {
	@field({ type: fixedArray("u8", 32) })
	networkId: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	resourceId: Uint8Array;

	@field({ type: "u64" })
	policySequence: bigint;

	@field({ type: fixedArray("u8", 32) })
	policyDigest: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	fenceDigest: Uint8Array;

	@field({ type: "u64" })
	contentEpoch: bigint;

	constructor(properties?: {
		networkId: Uint8Array;
		resourceId: Uint8Array;
		policySequence: bigint;
		policyDigest: Uint8Array;
		fenceDigest: Uint8Array;
		contentEpoch: bigint;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

@variant("trusted_network_v2")
export class TrustedNetworkV2 extends Program<never> {
	@field({ type: NetworkDescriptorV2 })
	descriptor: NetworkDescriptorV2;

	constructor(properties?: { descriptor: NetworkDescriptorV2 }) {
		super();
		if (properties) this.descriptor = properties.descriptor;
	}

	override async beforeOpen(
		_node: ProgramClient,
		_options?: ProgramInitializationOptions<never, this>,
	): Promise<never> {
		throw new Error(
			"TrustedNetwork v2 is a decode-only codec and cannot be activated",
		);
	}

	async open(_args?: never): Promise<never> {
		throw new Error(
			"TrustedNetwork v2 is a decode-only codec and cannot be activated",
		);
	}
}

export const assertNetworkDescriptorV2 = (
	descriptor: NetworkDescriptorV2,
): void => {
	assertU16(descriptor.protocolVersion, "protocolVersion");
	if (descriptor.protocolVersion !== TRUSTED_NETWORK_V2_PROTOCOL_VERSION) {
		throw new Error("Unsupported TrustedNetwork protocol version");
	}
	assertBytes32(descriptor.networkNonce, "networkNonce");
	if (!(descriptor.policyAuthority instanceof PublicSignKey)) {
		throw new Error("policyAuthority must be a public signing key");
	}
	assertBytes32(descriptor.genesisPolicyDigest, "genesisPolicyDigest");
	assertByte(descriptor.policyHashProfile, "policyHashProfile");
	if (descriptor.policyHashProfile !== TRUSTED_NETWORK_V2_POLICY_HASH_SHA256) {
		throw new Error("Unsupported TrustedNetwork policy hash profile");
	}
	assertByte(descriptor.entrySignatureProfile, "entrySignatureProfile");
	if (
		descriptor.entrySignatureProfile !==
		TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE
	) {
		throw new Error("Unsupported TrustedNetwork entry signature profile");
	}
};

export const deriveNetworkIdV2 = (
	descriptor: NetworkDescriptorV2,
): Uint8Array => {
	assertNetworkDescriptorV2(descriptor);
	return sha256Sync(
		concat([
			lengthPrefix(textEncoder.encode(TRUSTED_NETWORK_V2_NETWORK_ID_DOMAIN)),
			encodeU16(descriptor.protocolVersion),
			descriptor.networkNonce,
			lengthPrefix(serialize(descriptor.policyAuthority)),
		]),
	);
};

export const digestPolicySnapshotBodyV2 = (
	body: PolicySnapshotBodyV2,
): Uint8Array => {
	const bytes = serialize(body);
	return sha256Sync(
		concat([
			lengthPrefix(textEncoder.encode(TRUSTED_NETWORK_V2_POLICY_DIGEST_DOMAIN)),
			lengthPrefix(bytes),
		]),
	);
};

export const assertPolicySnapshotBodyV2 = (
	body: PolicySnapshotBodyV2,
	descriptor: NetworkDescriptorV2,
): void => {
	assertNetworkDescriptorV2(descriptor);
	assertBytes32(body.networkId, "networkId");
	if (!equals(body.networkId, deriveNetworkIdV2(descriptor))) {
		throw new Error("Policy snapshot belongs to another network");
	}
	assertU64(body.sequence, "sequence");
	assertBytes32(body.previousPolicyDigest, "previousPolicyDigest");
	if (body.sequence === 0n) {
		if (!equals(body.previousPolicyDigest, ZERO_DIGEST)) {
			throw new Error("Genesis policy must use the zero previous digest");
		}
	} else if (equals(body.previousPolicyDigest, ZERO_DIGEST)) {
		throw new Error("A non-genesis policy must name its previous digest");
	}
	if (!Array.isArray(body.bindings) || body.bindings.length === 0) {
		throw new Error("Policy snapshot must contain bindings");
	}

	let previousKeyBytes: Uint8Array | undefined;
	let authorityBinding: PolicySubjectBindingV2 | undefined;
	for (const binding of body.bindings) {
		if (!(binding.signingKey instanceof PublicSignKey)) {
			throw new Error("Policy subject must be a public signing key");
		}
		const keyBytes = serialize(binding.signingKey);
		if (
			previousKeyBytes !== undefined &&
			compare(previousKeyBytes, keyBytes) >= 0
		) {
			throw new Error("Policy bindings must be sorted and unique");
		}
		previousKeyBytes = keyBytes;

		assertByte(binding.roles, "roles");
		if (binding.roles === 0) {
			throw new Error("Policy bindings with no roles are not canonical");
		}
		if ((binding.roles & ~TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS) !== 0) {
			throw new Error("Policy binding contains unknown role bits");
		}
		if (binding.encryptionKeyCommitment !== undefined) {
			throw new Error(
				"TrustedNetwork v2 encryption commitments are not supported yet",
			);
		}

		if (binding.signingKey.equals(descriptor.policyAuthority)) {
			authorityBinding = binding;
		} else if ((binding.roles & TrustedNetworkRole.ADMIN) !== 0) {
			throw new Error("Only the policy authority may hold ADMIN");
		}
	}
	if (
		authorityBinding === undefined ||
		(authorityBinding.roles & TrustedNetworkRole.ADMIN) === 0
	) {
		throw new Error("Policy authority must hold ADMIN");
	}
	if (
		body.sequence === 0n &&
		!equals(digestPolicySnapshotBodyV2(body), descriptor.genesisPolicyDigest)
	) {
		throw new Error("Genesis policy digest does not match the descriptor");
	}
};

export const decodeTrustedNetworkV2 = (bytes: Uint8Array): TrustedNetworkV2 => {
	const network = deserialize(bytes, TrustedNetworkV2);
	assertCanonicalEncoding(bytes, network);
	assertNetworkDescriptorV2(network.descriptor);
	return network;
};

export const decodePolicySnapshotBodyV2 = (
	bytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): PolicySnapshotBodyV2 => {
	const byteLength = exactUint8ArrayByteLengthV2(bytes);
	// [2, 1] variant (2), network id (32), sequence (8), and previous
	// digest (32) place the bindings vec<u32 LE> count at offset 74 and its
	// elements at offset 78. Bound that count by captured input before Borsh can
	// allocate from it. This is only a feasibility guard, not a member cap.
	const bindingsCountOffset = 74;
	const bindingsOffset = 78;
	if (byteLength < bindingsOffset) {
		throw new Error("Policy snapshot body has truncated bindings framing");
	}
	const bindingsCount =
		bytes[bindingsCountOffset]! +
		bytes[bindingsCountOffset + 1]! * 0x100 +
		bytes[bindingsCountOffset + 2]! * 0x10000 +
		bytes[bindingsCountOffset + 3]! * 0x1000000;
	// An Ed25519 key variant and key consume 33 bytes; roles and the absent
	// encryption-commitment option consume one byte each. This 35-byte minimum
	// is structural, so it bounds allocation without imposing a policy size.
	const minimumBindingBytes = 35;
	if (
		bindingsCount >
		Math.floor((byteLength - bindingsOffset) / minimumBindingBytes)
	) {
		throw new Error("Policy snapshot body has impossible bindings count");
	}
	const body = deserialize(bytes, PolicySnapshotBodyV2);
	assertCanonicalEncoding(bytes, body);
	assertPolicySnapshotBodyV2(body, descriptor);
	return body;
};

export const assertResourceFenceV2 = (
	fence: ResourceFenceV2,
	descriptor: NetworkDescriptorV2,
): void => {
	assertNetworkDescriptorV2(descriptor);
	assertBytes32(fence.networkId, "networkId");
	if (!equals(fence.networkId, deriveNetworkIdV2(descriptor))) {
		throw new Error("Resource fence belongs to another network");
	}
	assertBytes32(fence.resourceId, "resourceId");
	assertU64(fence.fenceSequence, "fenceSequence");
	assertBytes32(fence.previousFenceDigest, "previousFenceDigest");
	if (fence.fenceSequence === 0n) {
		if (!equals(fence.previousFenceDigest, ZERO_DIGEST)) {
			throw new Error(
				"Initial resource fence must use the zero previous digest",
			);
		}
	} else if (equals(fence.previousFenceDigest, ZERO_DIGEST)) {
		throw new Error("A non-initial resource fence must name its predecessor");
	}
	assertU64(fence.policySequence, "policySequence");
	assertBytes32(fence.policyDigest, "policyDigest");
	assertU64(fence.contentEpoch, "contentEpoch");
	assertBytes32(fence.epochManifestDigest, "epochManifestDigest");
};

export const assertOperationPolicyProofV2 = (
	proof: OperationPolicyProofV2,
	descriptor: NetworkDescriptorV2,
): void => {
	assertNetworkDescriptorV2(descriptor);
	assertBytes32(proof.networkId, "networkId");
	if (!equals(proof.networkId, deriveNetworkIdV2(descriptor))) {
		throw new Error("Operation policy proof belongs to another network");
	}
	assertBytes32(proof.resourceId, "resourceId");
	assertU64(proof.policySequence, "policySequence");
	assertBytes32(proof.policyDigest, "policyDigest");
	assertBytes32(proof.fenceDigest, "fenceDigest");
	assertU64(proof.contentEpoch, "contentEpoch");
};

const decodeExactCanonicalV2 = <T>(
	bytes: Uint8Array,
	exactLength: number,
	type: new () => T,
): T => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(bytes);
	} catch {
		throw new Error(
			`TrustedNetwork v2 record must contain ${exactLength} bytes`,
		);
	}
	if (byteLength !== exactLength) {
		throw new Error(
			`TrustedNetwork v2 record must contain ${exactLength} bytes`,
		);
	}
	const captured = copyUint8ArrayWithLengthV2(bytes, byteLength);
	const value = deserialize(captured, type);
	assertCanonicalEncoding(captured, value);
	return value;
};

export const decodeResourceFenceV2 = (
	bytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): ResourceFenceV2 => {
	const fence = decodeExactCanonicalV2(
		bytes,
		TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
		ResourceFenceV2,
	);
	assertResourceFenceV2(fence, descriptor);
	return fence;
};

export const decodeOperationPolicyProofV2 = (
	bytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): OperationPolicyProofV2 => {
	const proof = decodeExactCanonicalV2(
		bytes,
		TRUSTED_NETWORK_V2_OPERATION_POLICY_PROOF_BYTES,
		OperationPolicyProofV2,
	);
	assertOperationPolicyProofV2(proof, descriptor);
	return proof;
};
