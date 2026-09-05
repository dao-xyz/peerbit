import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { DecryptedThing, type SignatureWithKey, verify } from "@peerbit/crypto";
import { Entry, EntryV0, type Meta, NO_ENCODING } from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	NetworkDescriptorV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

export const TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS = 128;

export type CanonicalPublicEntryV0ScanLimitsV2 = Readonly<{
	label: string;
	minimumSignatures: number;
	maximumSignatures: number;
	maximumDirectParents?: number;
	maximumMetadataBytes?: number;
}>;

export type CanonicalPublicEntryV0ScanV2 = Readonly<{
	entry: EntryV0<Uint8Array>;
	meta: Meta;
	signatures: SignatureWithKey[];
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signableBytes: Uint8Array;
	reservedBytes: Uint8Array;
	hasHash: boolean;
	directParentCount: number;
	signatureCount: number;
}>;

class BoundsReaderV2 {
	private offset = 0;
	private readonly view: DataView;

	constructor(
		private readonly bytes: Uint8Array,
		private readonly label: string,
	) {
		this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}

	get position(): number {
		return this.offset;
	}

	get remaining(): number {
		return this.bytes.byteLength - this.offset;
	}

	readU8(field: string): number {
		return this.readExact(1, field)[0]!;
	}

	readU32(field: string): number {
		this.requireRemaining(4, field);
		const value = this.view.getUint32(this.offset, true);
		this.offset += 4;
		return value;
	}

	readBytes(field: string): Uint8Array {
		return this.readExact(this.readU32(`${field} length`), field);
	}

	readExact(byteLength: number, field: string): Uint8Array {
		this.requireRemaining(byteLength, field);
		const start = this.offset;
		this.offset += byteLength;
		return this.bytes.subarray(start, this.offset);
	}

	expectU8(expected: number, field: string): void {
		if (this.readU8(field) !== expected) {
			throw new Error(`${this.label} EntryV0 has invalid ${field}`);
		}
	}

	expectDone(field: string): void {
		if (this.offset !== this.bytes.byteLength) {
			throw new Error(`${this.label} EntryV0 has trailing ${field} bytes`);
		}
	}

	private requireRemaining(byteLength: number, field: string): void {
		if (
			!Number.isSafeInteger(byteLength) ||
			byteLength < 0 ||
			byteLength > this.bytes.byteLength - this.offset
		) {
			throw new Error(`${this.label} EntryV0 has truncated ${field}`);
		}
	}
}

const readPublicWrapperV2 = (
	reader: BoundsReaderV2,
	label: string,
	field: string,
): Uint8Array => {
	if (
		reader.readU8(`${field} MaybeEncrypted variant`) !== 0 ||
		reader.readU8(`${field} DecryptedThing variant`) !== 0
	) {
		throw new Error(`${label} EntryV0 ${field} must be public`);
	}
	return reader.readBytes(field);
};

const scanMetaV2 = (
	bytes: Uint8Array,
	limits: CanonicalPublicEntryV0ScanLimitsV2,
): number => {
	const reader = new BoundsReaderV2(bytes, limits.label);
	reader.expectU8(0, "metadata variant");
	reader.expectU8(0, "clock variant");
	reader.readBytes("clock id");
	reader.expectU8(0, "timestamp variant");
	reader.readExact(8, "timestamp wall time");
	reader.readExact(4, "timestamp logical time");
	reader.readBytes("gid");
	const parentCount = reader.readU32("direct-parent count");
	if (parentCount > Math.floor(reader.remaining / 4)) {
		throw new Error(
			`${limits.label} EntryV0 has impossible direct-parent count`,
		);
	}
	if (
		limits.maximumDirectParents !== undefined &&
		parentCount > limits.maximumDirectParents
	) {
		throw new Error(
			`${limits.label} EntryV0 may contain at most ${limits.maximumDirectParents} direct parents`,
		);
	}
	for (let index = 0; index < parentCount; index++) {
		reader.readBytes("direct parent");
	}
	reader.readU8("entry type");
	const metadataOption = reader.readU8("metadata data option");
	if (metadataOption === 1) {
		const metadata = reader.readBytes("metadata data");
		if (
			limits.maximumMetadataBytes !== undefined &&
			metadata.byteLength > limits.maximumMetadataBytes
		) {
			throw new Error(
				`${limits.label} EntryV0 metadata may contain at most ${limits.maximumMetadataBytes} bytes`,
			);
		}
	} else if (metadataOption !== 0) {
		throw new Error(`${limits.label} EntryV0 has invalid metadata data option`);
	}
	reader.expectDone("metadata");
	return parentCount;
};

const scanPayloadV2 = (bytes: Uint8Array, label: string): Uint8Array => {
	const reader = new BoundsReaderV2(bytes, label);
	reader.expectU8(0, "payload variant");
	const data = reader.readBytes("payload data");
	reader.expectDone("payload");
	return data;
};

const scanSignatureV2 = (bytes: Uint8Array, label: string): void => {
	const reader = new BoundsReaderV2(bytes, label);
	reader.expectU8(0, "signature variant");
	reader.readBytes("signature data");
	const keyVariant = reader.readU8("signature public-key variant");
	const keyLength = keyVariant === 0 ? 32 : keyVariant === 1 ? 33 : undefined;
	if (keyLength === undefined) {
		throw new Error(`${label} EntryV0 uses an unsupported signing key`);
	}
	reader.readExact(keyLength, "signature public key");
	reader.readU8("signature prehash");
	reader.expectDone("signature");
};

/**
 * Bounds-scans and canonically decodes one already-captured public EntryV0.
 * Record meaning, signer authority, reserved bytes, hash option, and causal
 * semantics remain the responsibility of the caller's domain profile.
 */
export const scanCanonicalPublicEntryV0V2 = (
	entryBytes: Uint8Array,
	limits: CanonicalPublicEntryV0ScanLimitsV2,
): CanonicalPublicEntryV0ScanV2 => {
	if (
		!Number.isSafeInteger(limits.minimumSignatures) ||
		!Number.isSafeInteger(limits.maximumSignatures) ||
		limits.minimumSignatures < 1 ||
		limits.maximumSignatures < limits.minimumSignatures
	) {
		throw new Error("Invalid internal EntryV0 signature bounds");
	}
	const reader = new BoundsReaderV2(entryBytes, limits.label);
	reader.expectU8(0, "entry variant");
	const metaBytes = readPublicWrapperV2(reader, limits.label, "metadata");
	const payloadContainerBytes = readPublicWrapperV2(
		reader,
		limits.label,
		"payload",
	);
	const reservedBytes = reader.readExact(4, "reserved bytes");
	const signablePrefixLength = reader.position;
	if (reader.readU8("signatures option") !== 1) {
		throw new Error(`${limits.label} EntryV0 must contain signatures`);
	}
	reader.expectU8(0, "signatures variant");
	const signatureCount = reader.readU32("signature count");
	if (
		signatureCount < limits.minimumSignatures ||
		signatureCount > limits.maximumSignatures ||
		signatureCount > Math.floor(reader.remaining / 6)
	) {
		const expected =
			limits.minimumSignatures === 1 && limits.maximumSignatures === 1
				? "exactly one signature"
				: limits.minimumSignatures === limits.maximumSignatures
					? `exactly ${limits.minimumSignatures}`
					: `${limits.minimumSignatures}-${limits.maximumSignatures} signatures`;
		throw new Error(`${limits.label} EntryV0 must contain ${expected}`);
	}
	for (let index = 0; index < signatureCount; index++) {
		scanSignatureV2(
			readPublicWrapperV2(reader, limits.label, "signature"),
			limits.label,
		);
	}
	const hashOption = reader.readU8("hash option");
	if (hashOption === 1) {
		reader.readBytes("hash");
	} else if (hashOption !== 0) {
		throw new Error(`${limits.label} EntryV0 has invalid hash option`);
	}
	reader.expectDone("storage");

	const directParentCount = scanMetaV2(metaBytes, limits);
	const payloadBytes = scanPayloadV2(payloadContainerBytes, limits.label);
	const signableBytes = new Uint8Array(signablePrefixLength + 2);
	signableBytes.set(entryBytes.subarray(0, signablePrefixLength));

	const entry = deserialize(entryBytes, Entry);
	if (!(entry instanceof EntryV0)) {
		throw new Error(`${limits.label} entry must use EntryV0`);
	}
	if (!equals(entryBytes, serialize(entry))) {
		throw new Error(`${limits.label} EntryV0 storage is not canonical`);
	}
	if (
		!(entry._meta instanceof DecryptedThing) ||
		!(entry._payload instanceof DecryptedThing)
	) {
		throw new Error(
			`${limits.label} EntryV0 metadata and payload must be public`,
		);
	}
	if (
		entry._signatures === undefined ||
		entry._signatures.signatures.length !== signatureCount ||
		entry._signatures.signatures.some(
			(signature) => !(signature instanceof DecryptedThing),
		)
	) {
		throw new Error(`${limits.label} EntryV0 signatures must be public`);
	}
	entry.init({ encoding: NO_ENCODING });
	const meta = entry.meta;
	if (!equals(metaBytes, serialize(meta))) {
		throw new Error(`${limits.label} EntryV0 nested encoding is not canonical`);
	}
	if (!equals(payloadBytes, entry.payload.data)) {
		throw new Error(`${limits.label} EntryV0 payload framing is inconsistent`);
	}
	const signatures = entry.signatures;
	if (signatures.length !== signatureCount) {
		throw new Error(
			`${limits.label} EntryV0 signature framing is inconsistent`,
		);
	}
	return {
		entry,
		meta,
		signatures,
		metaBytes,
		payloadBytes,
		signableBytes,
		reservedBytes,
		hasHash: hashOption === 1,
		directParentCount,
		signatureCount,
	};
};

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
