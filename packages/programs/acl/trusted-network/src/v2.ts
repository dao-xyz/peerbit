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
 * the policy wire contract needed by the next slice without exposing a usable
 * controller, policy engine, resource fence, or encryption path.
 */

export const TRUSTED_NETWORK_V2_PROTOCOL_VERSION = 2;
export const TRUSTED_NETWORK_V2_POLICY_HASH_SHA256 = 1;

/**
 * Profile 1 accepts only an EntryV0 whose normal toSignable() bytes (including
 * causal metadata) have exactly one successfully verified signature. The
 * signer's canonical public-key bytes must equal the descriptor authority.
 */
export const TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE = 1;

export const TRUSTED_NETWORK_V2_NETWORK_ID_DOMAIN =
	"peerbit/trusted-network/v2/network-id/v1";
export const TRUSTED_NETWORK_V2_POLICY_DIGEST_DOMAIN =
	"peerbit/trusted-network/v2/policy-body/v1";

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
	if (!(value instanceof Uint8Array) || value.byteLength !== 32) {
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
	const body = deserialize(bytes, PolicySnapshotBodyV2);
	assertCanonicalEncoding(bytes, body);
	assertPolicySnapshotBodyV2(body, descriptor);
	return body;
};
