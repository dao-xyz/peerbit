import { deserialize, serialize } from "@dao-xyz/borsh";
import { DecryptedThing, verify } from "@peerbit/crypto";
import { Entry, EntryV0, NO_ENCODING } from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	NetworkDescriptorV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

export type AuthenticatedAuthorityEntryV0V2 = {
	descriptor: NetworkDescriptorV2;
	entryBytes: Uint8Array;
	entry: EntryV0<Uint8Array>;
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	reservedBytes: Uint8Array;
	hasHash: boolean;
	directParentCount: number;
};

export type AuthorityEntryV0ProfileV2 = (
	entry: AuthenticatedAuthorityEntryV0V2,
) => void;

type ScannedEntryV0StructureV2 = {
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signableBytes: Uint8Array;
	reservedBytes: Uint8Array;
	hasHash: boolean;
	directParentCount: number;
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

	get remaining(): number {
		return this.bytes.byteLength - this.offset;
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
	if (
		reader.readU8(`${label} MaybeEncrypted variant`) !== 0 ||
		reader.readU8(`${label} DecryptedThing variant`) !== 0
	) {
		throw new Error(`Authority EntryV0 ${label} must be public`);
	}
	return reader.readBytes(label);
};

const scanMetaStructureV2 = (metaBytes: Uint8Array): number => {
	const reader = new BoundsReaderV2(metaBytes);
	reader.expectU8(0, "metadata variant");
	reader.expectU8(0, "clock variant");
	reader.readBytes("clock id");
	reader.expectU8(0, "timestamp variant");
	reader.readExact(8, "timestamp wall time");
	reader.readExact(4, "timestamp logical time");
	reader.readBytes("gid");

	const directParentCount = reader.readU32("direct-parent count");
	// Every Borsh string starts with a four-byte length. Reject an impossible
	// input-backed count before iterating or invoking the generic decoder.
	if (directParentCount > Math.floor(reader.remaining / 4)) {
		throw new Error("Authority EntryV0 has impossible direct-parent count");
	}
	for (let i = 0; i < directParentCount; i++) {
		reader.readBytes("direct parent");
	}

	reader.readU8("entry type");
	const metaDataOption = reader.readU8("metadata data option");
	if (metaDataOption === 1) {
		reader.readBytes("metadata data");
	} else if (metaDataOption !== 0) {
		throw new Error("Authority EntryV0 has invalid metadata data option");
	}
	reader.expectDone("metadata");
	return directParentCount;
};

const scanPayloadStructureV2 = (payloadBytes: Uint8Array): Uint8Array => {
	const reader = new BoundsReaderV2(payloadBytes);
	reader.expectU8(0, "payload variant");
	const data = reader.readBytes("payload data");
	reader.expectDone("payload");
	return data;
};

const scanSignatureStructureV2 = (signatureBytes: Uint8Array): void => {
	const reader = new BoundsReaderV2(signatureBytes);
	reader.expectU8(0, "signature variant");
	reader.readBytes("signature data");
	const publicKeyVariant = reader.readU8("signature public-key variant");
	const publicKeyLength =
		publicKeyVariant === 0 ? 32 : publicKeyVariant === 1 ? 33 : undefined;
	if (publicKeyLength === undefined) {
		throw new Error("Authority EntryV0 uses an unsupported signing key");
	}
	reader.readExact(publicKeyLength, "signature public key");
	reader.readU8("signature prehash");
	reader.expectDone("signature");
};

const scanEntryV0StructureV2 = (
	entryBytes: Uint8Array,
): ScannedEntryV0StructureV2 => {
	const reader = new BoundsReaderV2(entryBytes);
	reader.expectU8(0, "entry variant");
	const metaBytes = readPublicWrapperV2(reader, "metadata");
	const payloadContainerBytes = readPublicWrapperV2(reader, "payload");
	const reservedBytes = reader.readExact(4, "reserved bytes");
	const signablePrefixLength = reader.position;
	if (reader.readU8("signatures option") !== 1) {
		throw new Error("Authority EntryV0 must contain exactly one signature");
	}
	reader.expectU8(0, "signatures variant");
	if (reader.readU32("signature count") !== 1) {
		throw new Error("Authority EntryV0 must contain exactly one signature");
	}
	const signatureBytes = readPublicWrapperV2(reader, "signature");
	const hashOption = reader.readU8("hash option");
	if (hashOption === 1) {
		reader.readBytes("hash");
	} else if (hashOption !== 0) {
		throw new Error("Authority EntryV0 has invalid hash option");
	}
	reader.expectDone("storage");

	const directParentCount = scanMetaStructureV2(metaBytes);
	const payloadBytes = scanPayloadStructureV2(payloadContainerBytes);
	scanSignatureStructureV2(signatureBytes);
	const signableBytes = new Uint8Array(signablePrefixLength + 2);
	signableBytes.set(entryBytes.subarray(0, signablePrefixLength));
	return {
		metaBytes,
		payloadBytes,
		signableBytes,
		reservedBytes,
		hasHash: hashOption === 1,
		directParentCount,
	};
};

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
	const scanned = scanEntryV0StructureV2(capturedEntryBytes);
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
	const payload = entry.payload;
	const signatures = entry.signatures;
	if (
		signatures.length !== 1 ||
		!equals(serialize(signatures[0]!.publicKey), authorityBytes)
	) {
		throw new Error("Authority EntryV0 signer is not the policy authority");
	}
	if (!equals(scanned.payloadBytes, payload.data)) {
		throw new Error("Authority EntryV0 payload framing is inconsistent");
	}
	const authenticated: AuthenticatedAuthorityEntryV0V2 = {
		descriptor: capturedDescriptor,
		entryBytes: capturedEntryBytes,
		entry,
		metaBytes: scanned.metaBytes,
		payloadBytes: payload.data,
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
