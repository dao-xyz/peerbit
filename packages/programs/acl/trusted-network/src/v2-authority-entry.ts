import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	calculateRawCid,
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { DecryptedThing, verify } from "@peerbit/crypto";
import { Entry, EntryType, EntryV0, NO_ENCODING } from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	NetworkDescriptorV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

/** Internal limits selected by a concrete TrustedNetwork v2 record profile. */
export type AuthorityEntryV0LimitsV2 = {
	maximumEntryBytes: number;
	maximumDirectParents: number;
	exactPayloadBytes?: number;
};

export type AuthenticatedAuthorityEntryV0V2 = {
	descriptor: NetworkDescriptorV2;
	entryBytes: Uint8Array;
	entryCid: string;
	entryDigest: Uint8Array;
	gid: string;
	metaData?: Uint8Array;
	directParents: Array<{ cid: string; digest: Uint8Array }>;
	payloadBytes: Uint8Array;
};

type ScannedAuthorityEntryV0 = {
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signatureBytes: Uint8Array;
	signableBytes: Uint8Array;
};

class BoundsReaderV2 {
	private offset = 0;
	private readonly view: DataView;

	constructor(private readonly bytes: Uint8Array) {
		this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}

	get position(): number {
		return this.offset;
	}

	readU8(label: string): number {
		return this.readExact(1, label)[0]!;
	}

	readU32(label: string): number {
		this.requireRemaining(4, label);
		const value = this.view.getUint32(this.offset, true);
		this.offset += 4;
		return value;
	}

	readBytes(label: string): Uint8Array {
		return this.readExact(this.readU32(`${label} length`), label);
	}

	readExact(byteLength: number, label: string): Uint8Array {
		this.requireRemaining(byteLength, label);
		const start = this.offset;
		this.offset += byteLength;
		return this.bytes.subarray(start, this.offset);
	}

	expectU8(expected: number, label: string): void {
		if (this.readU8(label) !== expected) {
			throw new Error(`Authority EntryV0 has invalid ${label}`);
		}
	}

	expectDone(label: string): void {
		if (this.offset !== this.bytes.byteLength) {
			throw new Error(`Authority EntryV0 has trailing ${label} bytes`);
		}
	}

	private requireRemaining(byteLength: number, label: string): void {
		if (
			!Number.isSafeInteger(byteLength) ||
			byteLength < 0 ||
			byteLength > this.bytes.byteLength - this.offset
		) {
			throw new Error(`Authority EntryV0 has truncated ${label}`);
		}
	}
}

const readPublicWrapperV2 = (
	reader: BoundsReaderV2,
	label: string,
): Uint8Array => {
	reader.expectU8(0, `${label} MaybeEncrypted variant`);
	reader.expectU8(0, `${label} DecryptedThing variant`);
	return reader.readBytes(label);
};

const scanMetaV2 = (
	metaBytes: Uint8Array,
	authorityBytes: Uint8Array,
	maximumDirectParents: number,
): void => {
	const reader = new BoundsReaderV2(metaBytes);
	reader.expectU8(0, "metadata variant");
	reader.expectU8(0, "clock variant");
	if (!equals(reader.readBytes("clock id"), authorityBytes)) {
		throw new Error("Authority EntryV0 clock id is not the policy authority");
	}
	reader.expectU8(0, "timestamp variant");
	reader.readExact(8, "timestamp wall time");
	reader.readExact(4, "timestamp logical time");
	reader.readBytes("gid");

	const directParentCount = reader.readU32("direct-parent count");
	if (directParentCount > maximumDirectParents) {
		throw new Error(
			`Authority EntryV0 may contain at most ${maximumDirectParents} direct parents`,
		);
	}
	for (let i = 0; i < directParentCount; i++) {
		reader.readBytes("direct parent");
	}

	if (reader.readU8("entry type") !== EntryType.APPEND) {
		throw new Error("Authority EntryV0 must be an APPEND entry");
	}
	const metaDataOption = reader.readU8("metadata data option");
	if (metaDataOption === 1) {
		reader.readBytes("metadata data");
	} else if (metaDataOption !== 0) {
		throw new Error("Authority EntryV0 has invalid metadata data option");
	}
	reader.expectDone("metadata");
};

const scanPayloadV2 = (
	payloadBytes: Uint8Array,
	exactPayloadBytes?: number,
): void => {
	const reader = new BoundsReaderV2(payloadBytes);
	reader.expectU8(0, "payload variant");
	const data = reader.readBytes("payload data");
	if (
		exactPayloadBytes !== undefined &&
		data.byteLength !== exactPayloadBytes
	) {
		throw new Error(
			`Authority EntryV0 payload must contain exactly ${exactPayloadBytes} bytes`,
		);
	}
	reader.expectDone("payload");
};

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

const scanSignatureV2 = (
	signatureBytes: Uint8Array,
	authorityBytes: Uint8Array,
): void => {
	const reader = new BoundsReaderV2(signatureBytes);
	reader.expectU8(0, "signature variant");
	const signature = reader.readBytes("signature data");
	const publicKeyVariant = reader.readU8("signature public-key variant");
	const publicKeyLength =
		publicKeyVariant === 0 ? 32 : publicKeyVariant === 1 ? 33 : undefined;
	if (publicKeyLength === undefined) {
		throw new Error("Authority EntryV0 uses an unsupported signing key");
	}
	const publicKey = reader.readExact(publicKeyLength, "signature public key");
	const serializedPublicKey = new Uint8Array(publicKeyLength + 1);
	serializedPublicKey[0] = publicKeyVariant;
	serializedPublicKey.set(publicKey, 1);
	if (!equals(serializedPublicKey, authorityBytes)) {
		throw new Error("Authority EntryV0 signer is not the policy authority");
	}
	if (publicKeyVariant === 1) {
		assertCanonicalSecp256k1SignatureV2(signature);
	}
	reader.readU8("signature prehash");
	reader.expectDone("signature");
};

const scanAuthorityEntryV0 = (
	entryBytes: Uint8Array,
	authorityBytes: Uint8Array,
	limits: AuthorityEntryV0LimitsV2,
): ScannedAuthorityEntryV0 => {
	const reader = new BoundsReaderV2(entryBytes);
	reader.expectU8(0, "entry variant");
	const metaBytes = readPublicWrapperV2(reader, "metadata");
	const payloadBytes = readPublicWrapperV2(reader, "payload");
	const reserved = reader.readExact(4, "reserved bytes");
	if (
		reserved[0] !== 0 ||
		reserved[1] !== 0 ||
		reserved[2] !== 0 ||
		reserved[3] !== 0
	) {
		throw new Error("Authority EntryV0 reserved bytes must be zero");
	}
	const signablePrefixLength = reader.position;
	reader.expectU8(1, "signatures option");
	reader.expectU8(0, "signatures variant");
	if (reader.readU32("signature count") !== 1) {
		throw new Error("Authority EntryV0 must contain exactly one signature");
	}
	const signatureBytes = readPublicWrapperV2(reader, "signature");
	reader.expectU8(0, "hash option");
	reader.expectDone("storage");

	scanMetaV2(metaBytes, authorityBytes, limits.maximumDirectParents);
	scanPayloadV2(payloadBytes, limits.exactPayloadBytes);
	scanSignatureV2(signatureBytes, authorityBytes);
	const signableBytes = new Uint8Array(signablePrefixLength + 2);
	signableBytes.set(entryBytes.subarray(0, signablePrefixLength));
	return { metaBytes, payloadBytes, signatureBytes, signableBytes };
};

const validateLimitsV2 = (limits: AuthorityEntryV0LimitsV2): void => {
	if (
		!Number.isSafeInteger(limits.maximumEntryBytes) ||
		limits.maximumEntryBytes < 1 ||
		!Number.isSafeInteger(limits.maximumDirectParents) ||
		limits.maximumDirectParents < 0 ||
		(limits.exactPayloadBytes !== undefined &&
			(!Number.isSafeInteger(limits.exactPayloadBytes) ||
				limits.exactPayloadBytes < 0))
	) {
		throw new Error("Invalid internal authority EntryV0 limits");
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
			digest: copyUint8ArrayV2(parsed.multihash.digest),
		};
	});
};

/**
 * Authenticate one bounded raw EntryV0 authority envelope. This establishes
 * canonical bytes, authority signature, and structural facts only. It does not
 * establish policy acceptance, causal ancestry, freshness, or durability.
 */
export const authenticateAuthorityEntryV0V2 = async (
	entryBytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
	limits: AuthorityEntryV0LimitsV2,
): Promise<AuthenticatedAuthorityEntryV0V2> => {
	validateLimitsV2(limits);
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(entryBytes);
	} catch {
		throw new Error("Authority entry must use canonical EntryV0 bytes");
	}
	if (byteLength < 1 || byteLength > limits.maximumEntryBytes) {
		throw new Error(
			`Authority EntryV0 must contain 1-${limits.maximumEntryBytes} bytes`,
		);
	}
	const capturedEntryBytes = copyUint8ArrayWithLengthV2(entryBytes, byteLength);

	assertNetworkDescriptorV2(descriptor);
	const authorityBytes = serialize(descriptor.policyAuthority);
	const scanned = scanAuthorityEntryV0(
		capturedEntryBytes,
		authorityBytes,
		limits,
	);
	const capturedDescriptor = deserialize(
		serialize(descriptor),
		NetworkDescriptorV2,
	);
	assertNetworkDescriptorV2(capturedDescriptor);
	if (!equals(serialize(capturedDescriptor.policyAuthority), authorityBytes)) {
		throw new Error("Network descriptor changed during capture");
	}
	const entry = deserialize(capturedEntryBytes, Entry);
	if (!(entry instanceof EntryV0)) {
		throw new Error("Authority entry must use EntryV0");
	}
	if (!equals(capturedEntryBytes, serialize(entry))) {
		throw new Error("Authority EntryV0 storage is not canonical");
	}
	if (!(entry._meta instanceof DecryptedThing)) {
		throw new Error("Authority EntryV0 metadata must be public");
	}
	if (!(entry._payload instanceof DecryptedThing)) {
		throw new Error("Authority EntryV0 payload must be public");
	}
	if (
		entry._signatures === undefined ||
		entry._signatures.signatures.length !== 1 ||
		!(entry._signatures.signatures[0] instanceof DecryptedThing)
	) {
		throw new Error("Authority EntryV0 must contain one public signature");
	}

	entry.init({ encoding: NO_ENCODING });
	const meta = entry.meta;
	const payload = entry.payload;
	const signatures = entry.signatures;
	if (
		!equals(scanned.metaBytes, serialize(meta)) ||
		!equals(scanned.payloadBytes, serialize(payload)) ||
		!equals(scanned.signatureBytes, serialize(signatures[0]!))
	) {
		throw new Error("Authority EntryV0 nested encoding is not canonical");
	}
	if (
		meta.type !== EntryType.APPEND ||
		meta.next.length > limits.maximumDirectParents ||
		!equals(meta.clock.id, authorityBytes)
	) {
		throw new Error("Authority EntryV0 metadata does not match its profile");
	}
	if (
		signatures.length !== 1 ||
		!equals(serialize(signatures[0]!.publicKey), authorityBytes)
	) {
		throw new Error("Authority EntryV0 signer is not the policy authority");
	}
	if (
		limits.exactPayloadBytes !== undefined &&
		payload.data.byteLength !== limits.exactPayloadBytes
	) {
		throw new Error(
			`Authority EntryV0 payload must contain exactly ${limits.exactPayloadBytes} bytes`,
		);
	}

	let signatureIsValid = false;
	try {
		signatureIsValid = await verify(signatures[0]!, scanned.signableBytes);
	} catch {
		// Unsupported prehashes and malformed signatures fail closed.
	}
	if (!signatureIsValid) {
		throw new Error("Authority EntryV0 signature is invalid");
	}

	const prepared = await calculateRawCid(capturedEntryBytes);
	return {
		descriptor: capturedDescriptor,
		entryBytes: capturedEntryBytes,
		entryCid: prepared.cid,
		entryDigest: copyUint8ArrayV2(prepared.block.cid.multihash.digest),
		gid: meta.gid,
		metaData: meta.data === undefined ? undefined : copyUint8ArrayV2(meta.data),
		directParents: canonicalDirectParentsV2(meta.next),
		payloadBytes: copyUint8ArrayV2(payload.data),
	};
};
