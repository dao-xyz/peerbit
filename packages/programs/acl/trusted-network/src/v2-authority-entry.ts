import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { verify } from "@peerbit/crypto";
import {
	type CanonicalPublicEntryV0Scan as CanonicalPublicEntryV0ScanV2,
	scanCanonicalPublicEntryV0 as scanCanonicalPublicEntryV0V2,
} from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	NetworkDescriptorV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

export const TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS = 128;

export {
	type CanonicalPublicEntryV0ScanLimits as CanonicalPublicEntryV0ScanLimitsV2,
	type CanonicalPublicEntryV0Scan as CanonicalPublicEntryV0ScanV2,
	scanCanonicalPublicEntryV0 as scanCanonicalPublicEntryV0V2,
} from "@peerbit/log";

const SECP256K1_SIGNATURE_TEXT_BYTES = 132;
const SECP256K1_LOW_S_MAX_HEX =
	"7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0";

const isLowerHexByteV2 = (value: number): boolean =>
	(value >= 0x30 && value <= 0x39) || (value >= 0x61 && value <= 0x66);

export const assertCanonicalSecp256k1SignatureV2 = (
	signature: Uint8Array,
	label: string,
): void => {
	if (
		signature.byteLength !== SECP256K1_SIGNATURE_TEXT_BYTES ||
		signature[0] !== 0x30 ||
		signature[1] !== 0x78
	) {
		throw new Error(`${label} EntryV0 secp256k1 signature is not canonical`);
	}
	for (let index = 2; index < signature.byteLength; index++) {
		if (!isLowerHexByteV2(signature[index]!)) {
			throw new Error(`${label} EntryV0 secp256k1 signature is not canonical`);
		}
	}
	if (
		signature[130] !== 0x31 ||
		(signature[131] !== 0x62 && signature[131] !== 0x63)
	) {
		throw new Error(`${label} EntryV0 secp256k1 signature is not canonical`);
	}
	for (let index = 0; index < SECP256K1_LOW_S_MAX_HEX.length; index++) {
		const actual = signature[66 + index]!;
		const maximum = SECP256K1_LOW_S_MAX_HEX.charCodeAt(index);
		if (actual < maximum) break;
		if (actual > maximum) {
			throw new Error(`${label} EntryV0 secp256k1 signature is not canonical`);
		}
	}
};

export const canonicalDirectParentsV2 = (
	parents: readonly string[],
	label: string,
): Array<{ cid: string; digest: Uint8Array }> => {
	const seen = new Set<string>();
	return parents.map((cid) => {
		let parsed: ReturnType<typeof cidifyString>;
		try {
			parsed = cidifyString(cid);
		} catch {
			throw new Error(
				`${label} EntryV0 direct parents must use canonical CIDv1/raw/sha2-256`,
			);
		}
		if (
			!cid ||
			cid.length > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS ||
			parsed.version !== 1 ||
			parsed.code !== codecMap.raw.code ||
			parsed.multihash.code !== defaultHasher.code ||
			parsed.multihash.digest.byteLength !== 32 ||
			stringifyCid(parsed) !== cid
		) {
			throw new Error(
				`${label} EntryV0 direct parents must use canonical CIDv1/raw/sha2-256`,
			);
		}
		if (seen.has(cid)) {
			throw new Error(`${label} EntryV0 direct parents must be unique`);
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

export type AuthenticatedAuthorityEntryV0V2 = {
	descriptor: NetworkDescriptorV2;
	entryBytes: Uint8Array;
	entry: CanonicalPublicEntryV0ScanV2["entry"];
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	reservedBytes: Uint8Array;
	hasHash: boolean;
	directParentCount: number;
};

export type AuthorityEntryV0ProfileV2 = (
	entry: AuthenticatedAuthorityEntryV0V2,
) => void;

const validateMaximumEntryBytesV2 = (maximumEntryBytes: number): void => {
	if (!Number.isSafeInteger(maximumEntryBytes) || maximumEntryBytes < 1) {
		throw new Error("Invalid internal authority EntryV0 byte limit");
	}
};

export const captureAuthorityEntryV0BytesV2 = (
	entryBytes: Uint8Array,
	maximumEntryBytes: number,
): Uint8Array => {
	validateMaximumEntryBytesV2(maximumEntryBytes);
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(entryBytes);
	} catch {
		throw new Error("Authority entry must use canonical EntryV0 bytes");
	}
	if (byteLength < 1 || byteLength > maximumEntryBytes) {
		throw new Error(
			`Authority EntryV0 must contain 1-${maximumEntryBytes} bytes`,
		);
	}
	return copyUint8ArrayWithLengthV2(entryBytes, byteLength);
};

/**
 * Authenticate one bounded raw EntryV0 authority envelope. This establishes
 * canonical bytes and the sole public authority signature only. It does not
 * establish a concrete record profile, policy acceptance, causal ancestry,
 * freshness, or durability.
 */
export const authenticateCapturedAuthorityEntryV0V2 = async (
	capturedEntryBytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
	assertProfile?: AuthorityEntryV0ProfileV2,
): Promise<AuthenticatedAuthorityEntryV0V2> => {
	assertNetworkDescriptorV2(descriptor);
	const authorityBytes = serialize(descriptor.policyAuthority);
	const scanned = scanCanonicalPublicEntryV0V2(capturedEntryBytes, {
		label: "Authority",
		minimumSignatures: 1,
		maximumSignatures: 1,
	});
	const capturedDescriptor = deserialize(
		serialize(descriptor),
		NetworkDescriptorV2,
	);
	assertNetworkDescriptorV2(capturedDescriptor);
	if (!equals(serialize(capturedDescriptor.policyAuthority), authorityBytes)) {
		throw new Error("Network descriptor changed during capture");
	}
	const signatures = scanned.signatures;
	if (!equals(serialize(signatures[0]!.publicKey), authorityBytes)) {
		throw new Error("Authority EntryV0 signer is not the policy authority");
	}
	const authenticated: AuthenticatedAuthorityEntryV0V2 = {
		descriptor: capturedDescriptor,
		entryBytes: capturedEntryBytes,
		entry: scanned.entry,
		metaBytes: scanned.metaBytes,
		payloadBytes: scanned.payloadBytes,
		reservedBytes: scanned.reservedBytes,
		hasHash: scanned.hasHash,
		directParentCount: scanned.directParentCount,
	};
	assertProfile?.(authenticated);

	let signatureIsValid = false;
	try {
		signatureIsValid = await verify(signatures[0]!, scanned.signableBytes);
	} catch {
		// Unsupported prehashes and malformed signatures fail closed.
	}
	if (!signatureIsValid) {
		throw new Error("Authority EntryV0 signature is invalid");
	}
	return authenticated;
};

export const authenticateAuthorityEntryV0V2 = async (
	entryBytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
	maximumEntryBytes: number,
	assertProfile?: AuthorityEntryV0ProfileV2,
): Promise<AuthenticatedAuthorityEntryV0V2> =>
	authenticateCapturedAuthorityEntryV0V2(
		captureAuthorityEntryV0BytesV2(entryBytes, maximumEntryBytes),
		descriptor,
		assertProfile,
	);
