import {
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { PublicSignKey, type SignatureWithKey, verify } from "@peerbit/crypto";
import { EntryType } from "@peerbit/log";
import { compare, equals } from "uint8arrays";
import {
	assertCanonicalSecp256k1SignatureV2,
	canonicalDirectParentsV2,
	scanCanonicalPublicEntryV0V2,
} from "./v2-authority-entry.js";
import {
	NetworkDescriptorV2,
	OperationPolicyProofV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	decodeOperationPolicyProofV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

/** Internal first profile for signed protected-resource operations. */
export const TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE = 1;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES = 128 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES =
	64 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_METADATA_BYTES =
	16 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_DIRECT_PARENTS = 64;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_SIGNATURES = 8;

/**
 * Public signed payload for profile 1 operations. The manifest commitment is
 * repeated here so an operation cannot be reinterpreted by another manifest
 * that happens to use the same policy, fence, and content epoch identifiers.
 */
@variant([2, 4])
export class ResourceOperationEnvelopeV2 {
	@field({ type: "u8" })
	profile: number;

	@field({ type: OperationPolicyProofV2 })
	policy: OperationPolicyProofV2;

	@field({ type: fixedArray("u8", 32) })
	epochManifestDigest: Uint8Array;

	@field({ type: Uint8Array })
	applicationPayload: Uint8Array;

	constructor(properties?: {
		profile: number;
		policy: OperationPolicyProofV2;
		epochManifestDigest: Uint8Array;
		applicationPayload: Uint8Array;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

// Variant (2), profile (1), fixed OperationPolicyProofV2 (146), manifest (32),
// and the application-payload u32 framing (4).
export const TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES = 185;

const assertCanonicalSignaturesV2 = async (
	signatures: SignatureWithKey[],
	signableBytes: Uint8Array,
	clockId: Uint8Array,
): Promise<Uint8Array[]> => {
	const signerKeys: Uint8Array[] = [];
	const seenKeys = new Set<string>();
	let clockSigner = false;
	let previousSignature: Uint8Array | undefined;
	for (const signature of signatures) {
		if (!(signature.publicKey instanceof PublicSignKey)) {
			throw new Error("Resource operation EntryV0 uses an unsupported signer");
		}
		if (
			previousSignature !== undefined &&
			compare(previousSignature, signature.signature) >= 0
		) {
			throw new Error(
				"Resource operation EntryV0 signatures must be canonically ordered",
			);
		}
		previousSignature = signature.signature;
		const keyBytes = serialize(signature.publicKey);
		let keyId = "";
		for (const byte of keyBytes) keyId += byte.toString(16).padStart(2, "0");
		if (seenKeys.has(keyId)) {
			throw new Error("Resource operation EntryV0 signers must be unique");
		}
		seenKeys.add(keyId);
		if (equals(clockId, keyBytes)) clockSigner = true;
		if (keyBytes[0] === 1) {
			assertCanonicalSecp256k1SignatureV2(
				signature.signature,
				"Resource operation",
			);
		}
		let verified = false;
		try {
			verified = await verify(signature, signableBytes);
		} catch {
			// Unsupported prehashes and malformed signatures fail closed.
		}
		if (!verified) {
			throw new Error("Resource operation EntryV0 signature is invalid");
		}
		signerKeys.push(copyUint8ArrayWithLengthV2(keyBytes, keyBytes.byteLength));
	}
	if (!clockSigner) {
		throw new Error(
			"Resource operation EntryV0 clock id must identify one verified signer",
		);
	}
	return signerKeys;
};

export const decodeResourceOperationEnvelopeV2 = (
	bytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): ResourceOperationEnvelopeV2 => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(bytes);
	} catch {
		throw new Error("Resource operation payload must be a Uint8Array");
	}
	if (
		byteLength <
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES ||
		byteLength >
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES +
				TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES
	) {
		throw new Error(
			`Resource operation application payload may contain at most ${TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES} bytes`,
		);
	}
	const captured = copyUint8ArrayWithLengthV2(bytes, byteLength);
	const applicationLengthOffset =
		TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES - 4;
	const applicationLength = new DataView(
		captured.buffer,
		captured.byteOffset,
		captured.byteLength,
	).getUint32(applicationLengthOffset, true);
	if (
		applicationLength >
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES ||
		applicationLength +
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES !==
			captured.byteLength
	) {
		throw new Error("Resource operation payload framing is invalid");
	}
	const envelope = deserialize(captured, ResourceOperationEnvelopeV2);
	if (!equals(captured, serialize(envelope))) {
		throw new Error("Resource operation envelope is not canonical");
	}
	if (envelope.profile !== TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE) {
		throw new Error("Unsupported resource operation profile");
	}
	decodeOperationPolicyProofV2(serialize(envelope.policy), descriptor);
	return envelope;
};

export type AuthenticatedResourceOperationSnapshotV2 = Readonly<{
	entryBytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
	envelope: ResourceOperationEnvelopeV2;
	gid: string;
	metaData?: Uint8Array;
	directParents: ReadonlyArray<Readonly<{ cid: string; digest: Uint8Array }>>;
	verifiedSignerKeyBytes: readonly Uint8Array[];
}>;

/** Opaque, unforgeable output of the operation EntryV0 authenticator. */
export type AuthenticatedResourceOperationEntryV2 = Readonly<{
	entryCid: string;
}>;

const AUTHENTICATED = new WeakMap<
	object,
	AuthenticatedResourceOperationSnapshotV2
>();

const copyEnvelopeV2 = (
	envelope: ResourceOperationEnvelopeV2,
): ResourceOperationEnvelopeV2 =>
	deserialize(serialize(envelope), ResourceOperationEnvelopeV2);

const copySnapshotV2 = (
	snapshot: AuthenticatedResourceOperationSnapshotV2,
): AuthenticatedResourceOperationSnapshotV2 => ({
	entryBytes: copyUint8ArrayWithLengthV2(
		snapshot.entryBytes,
		snapshot.entryBytes.byteLength,
	),
	entryCid: snapshot.entryCid,
	digest: copyUint8ArrayWithLengthV2(
		snapshot.digest,
		snapshot.digest.byteLength,
	),
	envelope: copyEnvelopeV2(snapshot.envelope),
	gid: snapshot.gid,
	metaData:
		snapshot.metaData === undefined
			? undefined
			: copyUint8ArrayWithLengthV2(
					snapshot.metaData,
					snapshot.metaData.byteLength,
				),
	directParents: snapshot.directParents.map((parent) => ({
		cid: parent.cid,
		digest: copyUint8ArrayWithLengthV2(parent.digest, parent.digest.byteLength),
	})),
	verifiedSignerKeyBytes: snapshot.verifiedSignerKeyBytes.map((key) =>
		copyUint8ArrayWithLengthV2(key, key.byteLength),
	),
});

export type AuthenticateResourceOperationEntryV2Properties = Readonly<{
	entryBytes: Uint8Array;
	descriptor: NetworkDescriptorV2;
	expectedResourceId: Uint8Array;
	expectedGid: string;
}>;

const captureBytes32V2 = (value: Uint8Array, label: string): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(value);
	} catch {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
	if (byteLength !== 32) {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
	return copyUint8ArrayWithLengthV2(value, byteLength);
};

/**
 * Authenticate a bounded, canonical, public signed operation EntryV0. This
 * authenticates bytes and immutable context only. It does not prove an
 * accepted policy/fence prefix, WRITER authority, or causal finality.
 */
export const authenticateResourceOperationEntryV2 = async ({
	entryBytes,
	descriptor,
	expectedResourceId,
	expectedGid,
}: AuthenticateResourceOperationEntryV2Properties): Promise<AuthenticatedResourceOperationEntryV2> => {
	assertNetworkDescriptorV2(descriptor);
	const capturedDescriptor = deserialize(
		serialize(descriptor),
		NetworkDescriptorV2,
	);
	assertNetworkDescriptorV2(capturedDescriptor);
	if (typeof expectedGid !== "string") {
		throw new Error("Expected resource gid must be a string");
	}
	const resourceId = captureBytes32V2(
		expectedResourceId,
		"Expected resource id",
	);
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(entryBytes);
	} catch {
		throw new Error("Resource operation must use canonical EntryV0 bytes");
	}
	if (
		byteLength < 1 ||
		byteLength > TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES
	) {
		throw new Error(
			`Resource operation EntryV0 must contain 1-${TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES} bytes`,
		);
	}
	const captured = copyUint8ArrayWithLengthV2(entryBytes, byteLength);
	const scanned = scanCanonicalPublicEntryV0V2(captured, {
		label: "Resource operation",
		minimumSignatures: 1,
		maximumSignatures: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_SIGNATURES,
		maximumDirectParents:
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_DIRECT_PARENTS,
		maximumMetadataBytes:
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_METADATA_BYTES,
	});
	if (scanned.hasHash) {
		throw new Error("Resource operation EntryV0 must not embed its hash");
	}
	if (
		scanned.reservedBytes[0] !== 0 ||
		scanned.reservedBytes[1] !== 0 ||
		scanned.reservedBytes[2] !== 0 ||
		scanned.reservedBytes[3] !== 0
	) {
		throw new Error("Resource operation EntryV0 reserved bytes must be zero");
	}
	const meta = scanned.meta;
	if (meta.type !== EntryType.APPEND) {
		throw new Error("Resource operation EntryV0 must be an APPEND entry");
	}
	if (meta.gid !== expectedGid) {
		throw new Error("Resource operation belongs to another resource log");
	}
	if (meta.next.length !== scanned.directParentCount) {
		throw new Error(
			"Resource operation EntryV0 direct-parent framing is inconsistent",
		);
	}
	const directParents = canonicalDirectParentsV2(
		meta.next,
		"Resource operation",
	);
	const verifiedSignerKeyBytes = await assertCanonicalSignaturesV2(
		scanned.signatures,
		scanned.signableBytes,
		meta.clock.id,
	);
	const envelope = decodeResourceOperationEnvelopeV2(
		scanned.payloadBytes,
		capturedDescriptor,
	);
	if (!equals(envelope.policy.resourceId, resourceId)) {
		throw new Error("Resource operation belongs to another resource");
	}
	const prepared = await calculateRawCid(captured);
	const snapshot: AuthenticatedResourceOperationSnapshotV2 = {
		entryBytes: captured,
		entryCid: prepared.cid,
		digest: copyUint8ArrayWithLengthV2(
			prepared.block.cid.multihash.digest,
			prepared.block.cid.multihash.digest.byteLength,
		),
		envelope,
		gid: meta.gid,
		metaData:
			meta.data === undefined
				? undefined
				: copyUint8ArrayWithLengthV2(meta.data, meta.data.byteLength),
		directParents,
		verifiedSignerKeyBytes,
	};
	const token = Object.freeze({ entryCid: snapshot.entryCid });
	AUTHENTICATED.set(token, copySnapshotV2(snapshot));
	return token;
};

/** Internal bridge; a forged or cross-module token fails closed. */
export const readAuthenticatedResourceOperationEntryV2 = (
	token: AuthenticatedResourceOperationEntryV2,
): AuthenticatedResourceOperationSnapshotV2 | undefined => {
	if (token === null || typeof token !== "object") return undefined;
	const snapshot = AUTHENTICATED.get(token);
	return snapshot === undefined ? undefined : copySnapshotV2(snapshot);
};
