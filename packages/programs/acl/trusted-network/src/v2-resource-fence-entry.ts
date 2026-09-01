import { serialize } from "@dao-xyz/borsh";
import {
	calculateRawCid,
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { EntryType, type Meta } from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	type AuthenticatedAuthorityEntryV0V2,
	authenticateAuthorityEntryV0V2,
} from "./v2-authority-entry.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
	copyUint8ArrayWithLengthV2,
	decodeResourceFenceV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

const RESOURCE_FENCE_AUTHORITY_LIMITS_V2 = Object.freeze({
	maximumEntryBytes: TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
});

const SECP256K1_SIGNATURE_TEXT_BYTES = 132;
const SECP256K1_LOW_S_MAX_HEX =
	"7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0";

const isLowerHexByteV2 = (value: number): boolean =>
	(value >= 0x30 && value <= 0x39) || (value >= 0x61 && value <= 0x66);

const assertCanonicalSecp256k1SignatureV2 = (signature: Uint8Array): void => {
	if (
		signature.byteLength !== SECP256K1_SIGNATURE_TEXT_BYTES ||
		signature[0] !== 0x30 ||
		signature[1] !== 0x78
	) {
		throw new Error("Authority EntryV0 secp256k1 signature is not canonical");
	}
	for (let i = 2; i < signature.byteLength; i++) {
		if (!isLowerHexByteV2(signature[i]!)) {
			throw new Error("Authority EntryV0 secp256k1 signature is not canonical");
		}
	}
	if (
		signature[130] !== 0x31 ||
		(signature[131] !== 0x62 && signature[131] !== 0x63)
	) {
		throw new Error("Authority EntryV0 secp256k1 signature is not canonical");
	}
	for (let i = 0; i < SECP256K1_LOW_S_MAX_HEX.length; i++) {
		const actual = signature[66 + i]!;
		const maximum = SECP256K1_LOW_S_MAX_HEX.charCodeAt(i);
		if (actual < maximum) break;
		if (actual > maximum) {
			throw new Error("Authority EntryV0 secp256k1 signature is not canonical");
		}
	}
};

const canonicalDirectParentsV2 = (
	parents: string[],
): Array<{ cid: string; digest: Uint8Array }> => {
	const seen = new Set<string>();
	return parents.map((cid) => {
		let parsed: ReturnType<typeof cidifyString>;
		try {
			parsed = cidifyString(cid);
		} catch {
			throw new Error(
				"Authority EntryV0 direct parents must use canonical CIDv1/raw/sha2-256",
			);
		}
		if (
			!cid ||
			parsed.version !== 1 ||
			parsed.code !== codecMap.raw.code ||
			parsed.multihash.code !== defaultHasher.code ||
			parsed.multihash.digest.byteLength !== 32 ||
			stringifyCid(parsed) !== cid
		) {
			throw new Error(
				"Authority EntryV0 direct parents must use canonical CIDv1/raw/sha2-256",
			);
		}
		if (seen.has(cid)) {
			throw new Error("Authority EntryV0 direct parents must be unique");
		}
		seen.add(cid);
		return {
			cid,
			digest: copyUint8ArrayWithLengthV2(
				parsed.multihash.digest,
				parsed.multihash.digest.byteLength,
			),
		};
	});
};

type ResourceFenceEntryV0ProfileV2 = {
	meta: Meta;
	directParents: Array<{ cid: string; digest: Uint8Array }>;
};

const assertResourceFenceEntryV0ProfileV2 = (
	authenticated: AuthenticatedAuthorityEntryV0V2,
): ResourceFenceEntryV0ProfileV2 => {
	if (authenticated.hasHash) {
		throw new Error(
			"Authority EntryV0 has invalid resource hash option: embedded hash",
		);
	}
	if (
		authenticated.reservedBytes[0] !== 0 ||
		authenticated.reservedBytes[1] !== 0 ||
		authenticated.reservedBytes[2] !== 0 ||
		authenticated.reservedBytes[3] !== 0
	) {
		throw new Error("Authority EntryV0 reserved bytes must be zero");
	}
	if (
		authenticated.directParentCount >
		TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS
	) {
		throw new Error(
			`Authority EntryV0 may contain at most ${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS} direct parents`,
		);
	}
	if (
		authenticated.payloadBytes.byteLength !==
		TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES
	) {
		throw new Error(
			`Authority EntryV0 payload must contain exactly ${TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES} bytes`,
		);
	}

	const meta = authenticated.entry.meta;
	if (!equals(authenticated.metaBytes, serialize(meta))) {
		throw new Error("Authority EntryV0 nested encoding is not canonical");
	}
	const authorityBytes = serialize(authenticated.descriptor.policyAuthority);
	if (meta.type !== EntryType.APPEND) {
		throw new Error("Authority EntryV0 must be an APPEND entry");
	}
	if (meta.next.length !== authenticated.directParentCount) {
		throw new Error("Authority EntryV0 direct-parent framing is inconsistent");
	}
	if (!equals(meta.clock.id, authorityBytes)) {
		throw new Error("Authority EntryV0 clock id is not the policy authority");
	}
	const signature = authenticated.entry.signatures[0]!;
	if (serialize(signature.publicKey)[0] === 1) {
		assertCanonicalSecp256k1SignatureV2(signature.signature);
	}
	return { meta, directParents: canonicalDirectParentsV2(meta.next) };
};

export type AuthenticatedResourceFenceEntryV2 = Readonly<{
	entryBytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
	body: ResourceFenceV2;
	gid: string;
	metaData?: Uint8Array;
	directParents: ReadonlyArray<Readonly<{ cid: string; digest: Uint8Array }>>;
}>;

export type AuthenticateResourceFenceEntryV2Properties = Readonly<{
	entryBytes: Uint8Array;
	descriptor: NetworkDescriptorV2;
	expectedResourceId: Uint8Array;
	expectedGid: string;
}>;

const captureResourceIdV2 = (resourceId: Uint8Array): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(resourceId);
	} catch {
		throw new Error("Expected resource id must contain exactly 32 bytes");
	}
	if (byteLength !== 32) {
		throw new Error("Expected resource id must contain exactly 32 bytes");
	}
	return copyUint8ArrayWithLengthV2(resourceId, byteLength);
};

const toResourceFenceTokenV2 = (
	authenticated: AuthenticatedAuthorityEntryV0V2,
	body: ResourceFenceV2,
	entryCid: string,
	digest: Uint8Array,
	gid: string,
	metaData: Uint8Array | undefined,
	directParents: Array<{ cid: string; digest: Uint8Array }>,
): AuthenticatedResourceFenceEntryV2 => ({
	entryBytes: authenticated.entryBytes,
	entryCid,
	digest,
	body,
	gid,
	metaData,
	directParents,
});

/**
 * Authenticate a raw resource-fence EntryV0 against immutable resource
 * context. The returned token does not prove policy acceptance, causal
 * ancestry, fork freedom, freshness, or durability.
 */
export const authenticateResourceFenceEntryV2 = async ({
	entryBytes,
	descriptor,
	expectedResourceId,
	expectedGid,
}: AuthenticateResourceFenceEntryV2Properties): Promise<AuthenticatedResourceFenceEntryV2> => {
	if (typeof expectedGid !== "string") {
		throw new Error("Expected resource gid must be a string");
	}
	const capturedResourceId = captureResourceIdV2(expectedResourceId);
	let profile: ResourceFenceEntryV0ProfileV2 | undefined;
	const authenticated = await authenticateAuthorityEntryV0V2(
		entryBytes,
		descriptor,
		RESOURCE_FENCE_AUTHORITY_LIMITS_V2.maximumEntryBytes,
		(candidate) => {
			profile = assertResourceFenceEntryV0ProfileV2(candidate);
		},
	);
	if (profile === undefined) {
		throw new Error("Resource fence EntryV0 profile was not applied");
	}
	if (profile.meta.gid !== expectedGid) {
		throw new Error("Resource fence belongs to another resource log");
	}
	const body = decodeResourceFenceV2(
		authenticated.payloadBytes,
		authenticated.descriptor,
	);
	if (!equals(body.resourceId, capturedResourceId)) {
		throw new Error("Resource fence belongs to another resource");
	}
	const prepared = await calculateRawCid(authenticated.entryBytes);
	return toResourceFenceTokenV2(
		authenticated,
		body,
		prepared.cid,
		copyUint8ArrayWithLengthV2(
			prepared.block.cid.multihash.digest,
			prepared.block.cid.multihash.digest.byteLength,
		),
		profile.meta.gid,
		profile.meta.data === undefined
			? undefined
			: copyUint8ArrayWithLengthV2(
					profile.meta.data,
					profile.meta.data.byteLength,
				),
		profile.directParents,
	);
};
